#!/usr/bin/env python3
"""
NekoAlpha Autopilot v7 - PrimeSignal Strategy (24/7)

Merged and fixed from v5/v6:
- secure env-based credentials
- robust request retries and timeouts
- fixed EMA-200 data length bug
- fixed SHORT TP/SL PnL direction bug
- fixed symbol file path handling on local workspace
- fallback symbol discovery from Binance Futures exchangeInfo
- persisted recently_closed across restarts
- quantity/minNotional validation before placing orders
"""

import hashlib
import hmac
import json
import logging
import os
import sys
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

import requests


def load_cfg_file(path):
    data = {}
    if not path or not os.path.exists(path):
        return data

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                data[key.strip()] = value.strip()
    except OSError:
        return {}

    return data


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.environ.get("CONFIG_FILE", os.path.join(SCRIPT_DIR, "config.cfg"))
FILE_CONFIG = load_cfg_file(CFG_PATH)


def cfg_get(key, default=""):
    env_val = os.environ.get(key)
    if env_val is not None and env_val != "":
        return env_val
    return FILE_CONFIG.get(key, default)


def cfg_int(key, default):
    try:
        return int(str(cfg_get(key, default)).strip())
    except (TypeError, ValueError):
        return int(default)


def cfg_float(key, default):
    try:
        return float(str(cfg_get(key, default)).strip())
    except (TypeError, ValueError):
        return float(default)


def cfg_bool(key, default=False):
    value = str(cfg_get(key, "1" if default else "0")).strip().lower()
    return value in ("1", "true", "yes", "on")


def normalize_margin_type(value, default="CROSS"):
    normalized = str(value).strip().upper()
    if normalized == "CROSS":
        return "CROSSED"
    if normalized in ("CROSSED", "ISOLATED"):
        return normalized

    fallback = str(default).strip().upper()
    return "ISOLATED" if fallback == "ISOLATED" else "CROSSED"


def cfg_margin_type(key, default="CROSS"):
    return normalize_margin_type(cfg_get(key, default), default)


def margin_type_label(value):
    normalized = str(value).strip().upper()
    return "CROSS" if normalized == "CROSSED" else (normalized or "UNKNOWN")


API_KEY = cfg_get("BINANCE_API_KEY", "")
SECRET = cfg_get("BINANCE_SECRET", "")
TELEGRAM_BOT_TOKEN = cfg_get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL = cfg_get("TELEGRAM_CHANNEL", "")

if not API_KEY or not SECRET:
    print("ERROR: Set BINANCE_API_KEY and BINANCE_SECRET env vars")
    raise SystemExit(1)


BASE_FUTURES = cfg_get("BASE_FUTURES", "https://fapi.binance.com")


#Runtime/risk config
SCAN_INTERVAL_SEC = cfg_int("SCAN_INTERVAL_SEC", 30)
REQUEST_TIMEOUT_SEC = cfg_int("REQUEST_TIMEOUT_SEC", 10)
REQUEST_RETRIES = cfg_int("REQUEST_RETRIES", 3)
REQUEST_BACKOFF_SEC = cfg_float("REQUEST_BACKOFF_SEC", 1.4)

MAX_MARGIN_PERCENT = cfg_float("MAX_MARGIN_PERCENT", 30)
ENTRY_PERCENT = cfg_float("ENTRY_PERCENT", 4)
MAX_POSITIONS = cfg_int("MAX_POSITIONS", 4)
MIN_ENTRY_USDT = cfg_float("MIN_ENTRY_USDT", 3)
LEVERAGE = cfg_int("LEVERAGE", 15)
MARGIN_TYPE = cfg_margin_type("MARGIN_TYPE", "CROSS")

#Exit logic based on PnL percent from entry price
TP_PERCENT = cfg_float("TP_PERCENT", 3)
SL_PERCENT = cfg_float("SL_PERCENT", 1.8)

#Signal filters
MIN_QUOTE_VOLUME = cfg_float("MIN_QUOTE_VOLUME", 20000000)
MIN_PRICE_CHANGE_ABS = cfg_float("MIN_PRICE_CHANGE_ABS", 2.0)

#Cooldown state
RECENTLY_CLOSED_TIMEOUT_SEC = cfg_int("RECENTLY_CLOSED_TIMEOUT_SEC", 900)
RECENTLY_CLOSED_FILE = cfg_get("RECENTLY_CLOSED_FILE", "recently_closed.json")
DRY_RUN = cfg_bool("DRY_RUN", False)
FORCE_COLOR = cfg_bool("FORCE_COLOR", False)
NO_COLOR = cfg_bool("NO_COLOR", False)
OUTBOUND_IP_REFRESH_SEC = cfg_int("OUTBOUND_IP_REFRESH_SEC", 1800)
OUTBOUND_IP_SERVICE = cfg_get("OUTBOUND_IP_SERVICE", "https://api.ipify.org/?format=json")

LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


logger = logging.getLogger("v7-futures")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")


class SoftColorFormatter(logging.Formatter):
    RESET = "\033[0m"
    LEVEL_COLORS = {
        logging.DEBUG: "\033[38;5;111m",    # soft cyan
        logging.INFO: "\033[38;5;114m",     # soft green
        logging.WARNING: "\033[38;5;222m",  # soft yellow
        logging.ERROR: "\033[38;5;210m",    # soft red
        logging.CRITICAL: "\033[38;5;203m", # bright red
    }

    def format(self, record):
        message = super().format(record)
        color = self.LEVEL_COLORS.get(record.levelno, "")
        if not color:
            return message
        return f"{color}{message}{self.RESET}"


def should_use_color():
    if NO_COLOR:
        return False
    if FORCE_COLOR:
        return True
    if not sys.stdout or not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    term = os.environ.get("TERM", "").lower()
    if not term or term == "dumb":
        return False
    return True

stream_handler = logging.StreamHandler()
if should_use_color():
    stream_handler.setFormatter(SoftColorFormatter("%(asctime)s | %(levelname)s | %(message)s"))
else:
    stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "v7-futures.log"), when="midnight", backupCount=7, encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


session = requests.Session()
session.headers.update({"X-MBX-APIKEY": API_KEY})
_outbound_ip_cache = {"value": "unknown", "fetched_at": 0.0}


def get_signature(params: str) -> str:
    return hmac.new(SECRET.encode(), params.encode(), hashlib.sha256).hexdigest()


def safe_request(method: str, url: str, signed: bool = False, **kwargs):
    """HTTP request with timeout, retries, backoff, and JSON-safe response handling."""
    kwargs.setdefault("timeout", REQUEST_TIMEOUT_SEC)
    attempt = 0

    while attempt < REQUEST_RETRIES:
        try:
            response = session.request(method, url, **kwargs)

            # Retry transient server/rate-limit issues
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")

            response.raise_for_status()
            return response
        except requests.Timeout:
            attempt += 1
            wait = REQUEST_BACKOFF_SEC ** attempt
            logger.warning("Timeout (%s/%s): %s", attempt, REQUEST_RETRIES, url)
            time.sleep(wait)
        except requests.RequestException as exc:
            attempt += 1
            wait = REQUEST_BACKOFF_SEC ** attempt
            logger.warning("Request error (%s/%s): %s | %s", attempt, REQUEST_RETRIES, url, exc)
            time.sleep(wait)

    msg = "signed" if signed else "public"
    logger.error("Request failed after retries (%s): %s", msg, url)
    return None


def json_or_none(response):
    if not response:
        return None
    try:
        return response.json()
    except ValueError as exc:
        logger.error("JSON parse error: %s", exc)
        return None


def get_outbound_ip(force=False):
    now = time.time()
    cached_value = _outbound_ip_cache.get("value", "unknown")
    cached_ts = float(_outbound_ip_cache.get("fetched_at", 0.0))

    if not force and cached_value != "unknown" and (now - cached_ts) < OUTBOUND_IP_REFRESH_SEC:
        return cached_value

    for attempt in range(1, 3):
        try:
            response = requests.get(OUTBOUND_IP_SERVICE, timeout=REQUEST_TIMEOUT_SEC)
            response.raise_for_status()

            ip_value = "unknown"
            try:
                payload = response.json()
                ip_value = str(payload.get("ip", "")).strip() or "unknown"
            except ValueError:
                ip_value = response.text.strip() or "unknown"

            if ip_value != "unknown":
                _outbound_ip_cache["value"] = ip_value
                _outbound_ip_cache["fetched_at"] = now
                return ip_value
        except requests.RequestException as exc:
            if attempt == 2 and force:
                logger.warning("Failed fetching outbound IP: %s", exc)
            time.sleep(0.5 * attempt)

    return cached_value


def load_recently_closed():
    file_path = os.path.join(SCRIPT_DIR, RECENTLY_CLOSED_FILE)
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {k: float(v) for k, v in data.items()}
        return {}
    except (OSError, ValueError, TypeError) as exc:
        logger.warning("Failed loading recently_closed file: %s", exc)
        return {}


def save_recently_closed(state):
    file_path = os.path.join(SCRIPT_DIR, RECENTLY_CLOSED_FILE)
    temp_path = f"{file_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(temp_path, file_path)
    except OSError as exc:
        logger.warning("Failed saving recently_closed file: %s", exc)


def clean_recently_closed(state):
    now = time.time()
    expired = [symbol for symbol, ts in state.items() if now - ts > RECENTLY_CLOSED_TIMEOUT_SEC]
    for symbol in expired:
        state.pop(symbol, None)
    if expired:
        save_recently_closed(state)


def get_balance():
    ts = int(time.time() * 1000)
    params = f"timestamp={ts}"
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v3/account?{params}&signature={sig}"

    r = safe_request("GET", url, signed=True)
    data = json_or_none(r)
    if not data or "code" in data:
        logger.error("Balance fetch failed: %s", data)
        return 0.0, 0.0, 0.0

    try:
        total = float(data.get("totalWalletBalance", 0))
        available = float(data.get("availableBalance", 0))
        margin = float(data.get("totalMarginBalance", 0))
        return total, available, margin
    except (TypeError, ValueError) as exc:
        logger.error("Balance parse error: %s", exc)
        return 0.0, 0.0, 0.0


def get_positions():
    ts = int(time.time() * 1000)
    params = f"timestamp={ts}"
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v2/positionRisk?{params}&signature={sig}"

    r = safe_request("GET", url, signed=True)
    data = json_or_none(r)
    if not isinstance(data, list):
        return []
    return data


def get_klines(symbol, interval="1h", limit=250):
    url = f"{BASE_FUTURES}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    r = safe_request("GET", url)
    data = json_or_none(r)
    if not isinstance(data, list):
        return []

    closes = []
    for candle in data:
        try:
            closes.append(float(candle[4]))
        except (TypeError, ValueError, IndexError):
            continue
    return closes


def calculate_ema(prices, period=50):
    if not prices or len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = (p - ema) * multiplier + ema
    return ema


def calculate_rsi(prices, period=14):
    if not prices or len(prices) <= period:
        return 50.0

    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = prices[-i] - prices[-i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff

    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_support_resistance(prices):
    if not prices or len(prices) < 20:
        return None, None
    window = prices[-50:] if len(prices) >= 50 else prices
    support = min(window)
    resistance = max(window)
    return support, resistance


def analyze_symbol(symbol):
    # Fixed EMA-200 issue by fetching enough candles
    prices_15m = get_klines(symbol, "15m", 300)
    prices_1h = get_klines(symbol, "1h", 300)
    prices_4h = get_klines(symbol, "4h", 200)

    if len(prices_1h) < 220:
        return None

    current = prices_1h[-1]
    ema_21 = calculate_ema(prices_1h, 21)
    ema_50 = calculate_ema(prices_1h, 50)
    ema_200 = calculate_ema(prices_1h, 200)
    if not ema_21 or not ema_50 or not ema_200:
        return None

    trend = "LONG" if current > ema_200 else "SHORT"
    rsi = calculate_rsi(prices_1h, 14)

    sup_15m, res_15m = get_support_resistance(prices_15m)
    sup_1h, res_1h = get_support_resistance(prices_1h)
    sup_4h, res_4h = get_support_resistance(prices_4h)

    support_candidates = [x for x in (sup_15m, sup_1h, sup_4h) if x]
    resistance_candidates = [x for x in (res_15m, res_1h, res_4h) if x]
    if not support_candidates or not resistance_candidates:
        return None

    support = max([s for s in support_candidates if s <= current], default=min(support_candidates))
    resistance = min([r for r in resistance_candidates if r >= current], default=max(resistance_candidates))

    range_height = resistance - support
    if range_height <= 0:
        return None

    # Pattern detection
    in_consolidation = abs(current - support) / range_height < 0.3
    pattern = "Bullish Rectangle" if in_consolidation and trend == "LONG" else "Consolidation" if in_consolidation else "Trend"

    # TP/SL with fib extension and structure-aware SL
    if trend == "LONG":
        tp1 = current + (range_height * 1.272)
        tp2 = current + (range_height * 1.618)
        sl = min(ema_21 * 0.98, support * 0.98)
    else:
        tp1 = current - (range_height * 1.272)
        tp2 = current - (range_height * 1.618)
        sl = max(ema_21 * 1.02, resistance * 1.02)

    return {
        "symbol": symbol,
        "price": current,
        "ema_21": ema_21,
        "ema_50": ema_50,
        "ema_200": ema_200,
        "rsi": rsi,
        "trend": trend,
        "support": support,
        "resistance": resistance,
        "range_height": range_height,
        "pattern": pattern,
        "tp1": tp1,
        "tp2": tp2,
        "sl": sl,
    }


def generate_signal(analysis):
    s = analysis
    direction = s["trend"]
    direction_emoji = "📈" if direction == "LONG" else "📉"
    trend_text = "BULLISH" if direction == "LONG" else "BEARISH"

    entry_breakout = s["resistance"] * (1.01 if direction == "LONG" else 0.99)
    entry_bounce = s["support"] if direction == "LONG" else s["resistance"]

    if direction == "LONG":
        if s["rsi"] < 30:
            insight = f"LONG oversold bounce setup near support {s['support']:.4f}."
        elif s["rsi"] > 70:
            insight = "LONG but stretched RSI; prefer retracement entry near EMA-21."
        else:
            insight = f"LONG trend above EMA-200 inside {s['support']:.4f}-{s['resistance']:.4f}."
    else:
        if s["rsi"] > 70:
            insight = f"SHORT overbought setup; breakdown below {s['support']:.4f} is trigger."
        elif s["rsi"] < 30:
            insight = "SHORT trend still valid; wait weak bounce failure near resistance."
        else:
            insight = f"SHORT trend below EMA-200 with resistance {s['resistance']:.4f}."

    signal = (
        f"🚨 {s['symbol']} TECHNICAL ANALYSIS\n"
        f"🔗 Chart: https://www.tradingview.com/chart/?symbol=BINANCE:{s['symbol']}\n\n"
        f"⚙️ Setup:\n"
        f"- Bias: {direction_emoji} {trend_text}\n"
        f"- Margin: {margin_type_label(MARGIN_TYPE)} | {LEVERAGE}x\n\n"
        f"📊 Indicators:\n"
        f"- RSI(14): {s['rsi']:.1f}\n"
        f"- EMA21: {s['ema_21']:.6f}\n"
        f"- EMA50: {s['ema_50']:.6f}\n"
        f"- EMA200: {s['ema_200']:.6f}\n"
        f"- Trend: {trend_text}\n"
        f"- Pattern: {s['pattern']}\n\n"
        f"🧱 Structure (Multi-TF):\n"
        f"- Support: {s['support']:.6f}\n"
        f"- Resistance: {s['resistance']:.6f}\n"
        f"- Range: {(s['resistance'] - s['support']):.6f}\n\n"
        f"🎯 Entry Strategies:\n"
        f"1) Breakout: {entry_breakout:.6f}\n"
        f"2) Bounce/Reject: {entry_bounce:.6f}\n\n"
        f"🧠 Insight: {insight}\n\n"
        f"🏁 Targets:\n"
        f"- Entry Ref: {s['price']:.6f}\n"
        f"- TP1: {s['tp1']:.6f} (Fib 1.272)\n"
        f"- TP2: {s['tp2']:.6f} (Fib 1.618)\n"
        f"- SL: {s['sl']:.6f}\n"
        f"⏰ Timeframe: 1H"
    )
    return signal


def build_exit_message(status, symbol, direction, pnl_pct, before_balance, after_balance):
    status_emoji = "✅" if status == "TP" else "🛑"
    direction_emoji = "📈" if direction == "LONG" else "📉"
    balance_delta = after_balance - before_balance

    return (
        f"{status_emoji} {status} HIT\n"
        f"🪙 Symbol: {symbol}\n"
        f"{direction_emoji} Direction: {direction}\n"
        f"📊 PnL: {pnl_pct:+.2f}%\n"
        f"⚙️ Margin: {margin_type_label(MARGIN_TYPE)} | {LEVERAGE}x\n"
        f"💰 Balance Before: {format_usdt(before_balance)}\n"
        f"💵 Balance After: {format_usdt(after_balance)}\n"
        f"🔄 Delta: {format_signed_usdt(balance_delta)}"
    )


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL:
        logger.info("SIGNAL\n%s", message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHANNEL,
        "text": message[:4000],
        "disable_web_page_preview": True,
    }

    for attempt in range(1, 4):
        try:
            response = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT_SEC)
            if response.ok:
                return
            logger.warning("Telegram send failed (%s/3): %s", attempt, response.text)
        except requests.RequestException as exc:
            logger.warning("Telegram request error (%s/3): %s", attempt, exc)
        time.sleep(min(3, attempt))


def load_symbols_from_file():
    candidates = [
        os.path.join(SCRIPT_DIR, "futures_symbols.json"),
        os.path.join(SCRIPT_DIR, "futures-symbols.json"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                symbols = [str(x).strip().upper() for x in data if isinstance(x, str)]
                if symbols:
                    logger.info("Loaded %s symbols from %s", len(symbols), os.path.basename(path))
                    return set(symbols)
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("Failed reading symbols file %s: %s", path, exc)
    return set()


def get_exchange_info():
    url = f"{BASE_FUTURES}/fapi/v1/exchangeInfo"
    r = safe_request("GET", url)
    data = json_or_none(r)
    if not data or "symbols" not in data:
        return {}, set()

    symbol_meta = {}
    tradable_symbols = set()

    for s in data.get("symbols", []):
        try:
            symbol = s["symbol"]
            status = s.get("status")
            quote_asset = s.get("quoteAsset")
            contract_type = s.get("contractType")
            if status != "TRADING" or quote_asset != "USDT" or contract_type != "PERPETUAL":
                continue

            step_size = 1.0
            min_qty = 0.0
            min_notional = 0.0
            for f in s.get("filters", []):
                filter_type = f.get("filterType")
                if filter_type in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    step_size = float(f.get("stepSize", 1))
                    min_qty = float(f.get("minQty", 0))
                elif filter_type in ("MIN_NOTIONAL", "NOTIONAL"):
                    min_notional = float(f.get("notional", f.get("minNotional", 0)))

            symbol_meta[symbol] = {
                "step_size": step_size,
                "min_qty": min_qty,
                "min_notional": min_notional,
            }
            tradable_symbols.add(symbol)
        except (KeyError, TypeError, ValueError):
            continue

    return symbol_meta, tradable_symbols


def round_step(qty, step, mode="floor"):
    if step <= 0:
        return float(qty)

    d_qty = Decimal(str(qty))
    d_step = Decimal(str(step))
    units = d_qty / d_step
    rounding = ROUND_FLOOR if mode == "floor" else ROUND_CEILING
    units = units.to_integral_value(rounding=rounding)
    rounded = units * d_step
    return float(rounded)


def format_qty(value):
    text = f"{float(value):.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def format_usdt(value):
    try:
        return f"{float(value):,.2f} USDT"
    except (TypeError, ValueError):
        return "n/a"


def format_signed_usdt(value):
    try:
        return f"{float(value):+,.2f} USDT"
    except (TypeError, ValueError):
        return "n/a"


def get_price(symbol):
    url = f"{BASE_FUTURES}/fapi/v1/ticker/price?symbol={symbol}"
    r = safe_request("GET", url)
    data = json_or_none(r)
    if not data or "price" not in data:
        return None
    try:
        return float(data["price"])
    except (TypeError, ValueError):
        return None


def set_leverage(symbol, leverage):
    ts = int(time.time() * 1000)
    params = f"symbol={symbol}&leverage={leverage}&timestamp={ts}"
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v1/leverage?{params}&signature={sig}"
    safe_request("POST", url, signed=True)


def get_symbol_margin_type(symbol):
    for position in get_positions():
        if position.get("symbol") != symbol:
            continue

        margin_type = str(position.get("marginType", "")).strip().upper()
        if margin_type:
            return margin_type

        isolated_flag = str(position.get("isolated", "")).strip().lower()
        if isolated_flag in ("1", "true", "yes", "on"):
            return "ISOLATED"
        if isolated_flag in ("0", "false", "no", "off"):
            return "CROSSED"
        break
    return ""


def set_margin_type(symbol, margin_type):
    target = normalize_margin_type(margin_type, "CROSS")
    current = get_symbol_margin_type(symbol)
    if current == target:
        return True

    ts = int(time.time() * 1000)
    params = f"symbol={symbol}&marginType={target}&timestamp={ts}"
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v1/marginType?{params}&signature={sig}"

    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = session.post(url, timeout=REQUEST_TIMEOUT_SEC)
            data = json_or_none(response)

            if response.ok:
                logger.info("Margin type %s => %s", symbol, margin_type_label(target))
                return True

            if isinstance(data, dict) and data.get("code") == -4046:
                return True

            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")

            logger.warning(
                "Set margin type failed %s (%s/%s): %s",
                symbol,
                attempt,
                REQUEST_RETRIES,
                data or response.text,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Margin type request error %s (%s/%s): %s",
                symbol,
                attempt,
                REQUEST_RETRIES,
                exc,
            )
        time.sleep(REQUEST_BACKOFF_SEC ** attempt)

    return False


def compute_order_qty(symbol, amount_usdt, symbol_meta):
    price = get_price(symbol)
    if not price or price <= 0:
        return None, None

    meta = symbol_meta.get(symbol, {})
    step_size = float(meta.get("step_size", 1))
    min_qty = float(meta.get("min_qty", 0))
    min_notional = float(meta.get("min_notional", 0))

    raw_qty = amount_usdt / price
    qty = round_step(raw_qty, step_size, mode="floor")

    if qty < min_qty:
        qty = round_step(min_qty, step_size, mode="ceil")

    notional = qty * price
    if min_notional > 0 and notional < min_notional:
        qty = round_step(min_notional / price, step_size, mode="ceil")
        notional = qty * price

    if qty <= 0:
        return None, None

    return qty, price


def open_position(symbol, side, amount_usdt, symbol_meta):
    qty, price = compute_order_qty(symbol, amount_usdt, symbol_meta)
    if not qty:
        logger.warning("Skip order %s: invalid qty", symbol)
        return None

    qty_str = format_qty(qty)

    if DRY_RUN:
        logger.info("[DRY_RUN] Open %s %s qty=%s", side, symbol, qty_str)
        return {"dry_run": True, "symbol": symbol, "side": side, "quantity": qty_str}

    if not set_margin_type(symbol, MARGIN_TYPE):
        logger.warning("Skip order %s: failed to set margin mode %s", symbol, margin_type_label(MARGIN_TYPE))
        return None

    set_leverage(symbol, LEVERAGE)

    ts = int(time.time() * 1000)
    params = f"symbol={symbol}&side={side}&type=MARKET&quantity={qty_str}&timestamp={ts}"
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v1/order?{params}&signature={sig}"
    r = safe_request("POST", url, signed=True)
    data = json_or_none(r)
    if not data or "code" in data:
        logger.warning("Open order failed %s: %s", symbol, data)
        return None

    logger.info(
        "Opened %s %s qty=%s price=%s mode=%s leverage=%sx",
        side,
        symbol,
        qty,
        price,
        margin_type_label(MARGIN_TYPE),
        LEVERAGE,
    )
    return data


def close_position(symbol, qty, direction, symbol_meta):
    meta = symbol_meta.get(symbol, {})
    step_size = float(meta.get("step_size", 1))
    normalized_qty = round_step(abs(qty), step_size, mode="floor")
    if normalized_qty <= 0:
        normalized_qty = round_step(abs(qty), step_size, mode="ceil")
    if normalized_qty <= 0:
        logger.warning("Skip close %s: invalid normalized qty", symbol)
        return None

    qty_str = format_qty(normalized_qty)
    side = "BUY" if direction == "SHORT" else "SELL"

    if DRY_RUN:
        logger.info("[DRY_RUN] Close %s %s qty=%s", side, symbol, qty_str)
        return {"dry_run": True, "symbol": symbol, "side": side, "quantity": qty_str}

    ts = int(time.time() * 1000)
    params = (
        f"symbol={symbol}&side={side}&type=MARKET&reduceOnly=true"
        f"&quantity={qty_str}&timestamp={ts}"
    )
    sig = get_signature(params)
    url = f"{BASE_FUTURES}/fapi/v1/order?{params}&signature={sig}"
    r = safe_request("POST", url, signed=True)
    data = json_or_none(r)
    if not data or "code" in data:
        logger.warning("Close order failed %s: %s", symbol, data)
        return None
    return data


def scan_opportunities(valid_symbols):
    r = safe_request("GET", f"{BASE_FUTURES}/fapi/v1/ticker/24hr")
    data = json_or_none(r)
    if not isinstance(data, list):
        return []

    opportunities = []
    for item in data:
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        if symbol not in valid_symbols:
            continue

        try:
            pct = float(item.get("priceChangePercent", 0))
            vol = float(item.get("quoteVolume", 0))
        except (TypeError, ValueError):
            continue

        if vol < MIN_QUOTE_VOLUME or abs(pct) < MIN_PRICE_CHANGE_ABS:
            continue

        analysis = analyze_symbol(symbol)
        if not analysis:
            continue

        if (analysis["trend"] == "LONG" and pct > 0) or (analysis["trend"] == "SHORT" and pct < 0):
            opportunities.append(analysis)

    opportunities.sort(key=lambda x: x["range_height"])
    return opportunities


def autopilot(state, symbol_meta, valid_symbols):
    clean_recently_closed(state)
    logger.info("NekoAlpha v7 run at %s UTC", datetime.now(timezone.utc).strftime("%H:%M"))

    total, avail, margin = get_balance()
    if total <= 0:
        logger.error("Failed to get balance")
        return 0

    margin_used = max(margin - avail, 0)
    margin_percent = (margin_used / total) * 100 if total > 0 else 0
    logger.info("Balance=%.2f | MarginUsed=%.2f%%", total, margin_percent)

    positions = get_positions()
    open_pos = []

    for p in positions:
        try:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue

            symbol = p.get("symbol")
            entry = float(p.get("entryPrice", 0))
            mark = float(p.get("markPrice", 0))
            direction = "SHORT" if amt < 0 else "LONG"

            # Fixed bug: SHORT pnl direction differs from LONG
            if direction == "LONG":
                pnl_pct = ((mark - entry) / entry) * 100
            else:
                pnl_pct = ((entry - mark) / entry) * 100

            status = ""
            if pnl_pct >= TP_PERCENT:
                status = "TP"
            elif pnl_pct <= -SL_PERCENT:
                status = "SL"

            logger.info("Position %s | %s | PnL=%+.2f%% %s", symbol, direction, pnl_pct, status)

            if status:
                before_total, _, _ = get_balance()
                if before_total <= 0:
                    before_total = total

                close_result = close_position(symbol, abs(amt), direction, symbol_meta)
                if close_result:
                    after_total, after_avail, after_margin = get_balance()
                    if after_total <= 0:
                        after_total = before_total
                        after_avail = avail
                        after_margin = margin

                    total = after_total
                    avail = after_avail
                    margin = after_margin
                    margin_used = max(margin - avail, 0)
                    margin_percent = (margin_used / total) * 100 if total > 0 else 0

                    state[symbol] = time.time()
                    save_recently_closed(state)
                    send_telegram(
                        build_exit_message(
                            status,
                            symbol,
                            direction,
                            pnl_pct,
                            before_total,
                            after_total,
                        )
                    )
                else:
                    logger.warning("Failed closing %s after %s trigger", symbol, status)

            open_pos.append({"symbol": symbol, "dir": direction, "pct": pnl_pct})
        except (TypeError, ValueError, ZeroDivisionError) as exc:
            logger.warning("Position parse error: %s", exc)

    if margin_percent >= MAX_MARGIN_PERCENT or len(open_pos) >= MAX_POSITIONS:
        return len(open_pos)

    opportunities = scan_opportunities(valid_symbols)
    if not opportunities:
        logger.info("No opportunities this cycle")
        return len(open_pos)

    for opp in opportunities[:5]:
        if len(open_pos) >= MAX_POSITIONS or margin_percent >= MAX_MARGIN_PERCENT:
            break

        symbol = opp["symbol"]
        if any(p["symbol"] == symbol for p in open_pos):
            continue
        if symbol in state:
            continue

        side = "BUY" if opp["trend"] == "LONG" else "SELL"
        amount = total * (ENTRY_PERCENT / 100) * LEVERAGE
        min_amount = MIN_ENTRY_USDT * LEVERAGE
        amount = max(amount, min_amount)

        signal = generate_signal(opp)
        send_telegram(signal)

        result = open_position(symbol, side, amount, symbol_meta)
        if result:
            open_pos.append({"symbol": symbol, "dir": opp["trend"], "pct": 0.0})
            margin_percent += ENTRY_PERCENT

    return len(open_pos)


def build_valid_symbols(tradable_symbols):
    file_symbols = load_symbols_from_file()
    if file_symbols:
        valid = file_symbols.intersection(tradable_symbols)
        if valid:
            logger.info("Using %s symbols from local file after exchange filter", len(valid))
            return valid

    logger.info("Falling back to all tradable USDT perpetual futures symbols")
    return tradable_symbols


def main():
    logger.info("Starting NekoAlpha v7 (24/7 mode)")
    logger.info("Config path: %s", CFG_PATH)
    if DRY_RUN:
        logger.warning("DRY_RUN enabled: no live orders will be sent")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL:
        logger.warning("Telegram not configured: signals will be logged only")

    startup_ip = get_outbound_ip(force=True)
    logger.info("Outbound fetch IP: %s", startup_ip)

    symbol_meta, tradable_symbols = get_exchange_info()
    if not tradable_symbols:
        logger.error("Cannot load exchangeInfo symbols. Exiting.")
        return

    valid_symbols = build_valid_symbols(tradable_symbols)
    state = load_recently_closed()

    error_streak = 0
    while True:
        try:
            position_count = autopilot(state, symbol_meta, valid_symbols)
            current_ip = get_outbound_ip(force=False)
            logger.info("Cycle done. Open positions tracked: %s | fetch_ip=%s", position_count, current_ip)
            error_streak = 0
            time.sleep(SCAN_INTERVAL_SEC)
        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except Exception as exc:
            error_streak += 1
            backoff = min(300, int((REQUEST_BACKOFF_SEC ** error_streak) * 5))
            logger.exception("Runtime error: %s | retry in %ss", exc, backoff)
            time.sleep(backoff)


if __name__ == "__main__":
    main()
