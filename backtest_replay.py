#!/usr/bin/env python3
import argparse
import importlib.util
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests


BASE_FUTURES = "https://fapi.binance.com"
REQUEST_TIMEOUT_SEC = 15
REQUEST_GAP_SEC = 0.12


def load_module(script_path: Path):
    spec = importlib.util.spec_from_file_location("v7f_module", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_get_json(url, params=None, retries=4):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if response.status_code in (429, 418, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(min(5, 0.6 * attempt))


def fetch_15m_klines(symbol, limit):
    time.sleep(REQUEST_GAP_SEC)
    data = safe_get_json(
        f"{BASE_FUTURES}/fapi/v1/klines",
        params={"symbol": symbol, "interval": "15m", "limit": limit},
    )
    rows = []
    for candle in data:
        rows.append(
            {
                "open_time": int(candle[0]),
                "close_time": int(candle[6]),
                "close": float(candle[4]),
                "quote_volume": float(candle[7]),
            }
        )
    return rows


def floor_hour_ms(timestamp_ms):
    return timestamp_ms - (timestamp_ms % 3_600_000)


def floor_4h_ms(timestamp_ms):
    return timestamp_ms - (timestamp_ms % 14_400_000)


def build_partial_series(candles, index, bucket_ms):
    grouped = []
    bucket = None
    last_close = None
    for i in range(index + 1):
        ts = candles[i]["close_time"]
        current_bucket = ts - (ts % bucket_ms)
        if bucket is None or current_bucket != bucket:
            if bucket is not None and last_close is not None:
                grouped.append(last_close)
            bucket = current_bucket
        last_close = candles[i]["close"]
    if last_close is not None:
        grouped.append(last_close)
    return grouped


def analyze_symbol_at(module, symbol, candles, index):
    prices_15m = [row["close"] for row in candles[max(0, index - 299): index + 1]]
    prices_1h = build_partial_series(candles, index, 3_600_000)[-300:]
    prices_4h = build_partial_series(candles, index, 14_400_000)[-200:]

    if len(prices_1h) < 220:
        return None

    current = prices_1h[-1]
    ema_21 = module.calculate_ema(prices_1h, 21)
    ema_50 = module.calculate_ema(prices_1h, 50)
    ema_200 = module.calculate_ema(prices_1h, 200)
    if not ema_21 or not ema_50 or not ema_200:
        return None

    trend = "LONG" if current > ema_200 else "SHORT"
    rsi = module.calculate_rsi(prices_1h, 14)

    sup_15m, res_15m = module.get_support_resistance(prices_15m)
    sup_1h, res_1h = module.get_support_resistance(prices_1h)
    sup_4h, res_4h = module.get_support_resistance(prices_4h)

    support_candidates = [x for x in (sup_15m, sup_1h, sup_4h) if x]
    resistance_candidates = [x for x in (res_15m, res_1h, res_4h) if x]
    if not support_candidates or not resistance_candidates:
        return None

    support = max([s for s in support_candidates if s <= current], default=min(support_candidates))
    resistance = min([r for r in resistance_candidates if r >= current], default=max(resistance_candidates))
    range_height = resistance - support
    if range_height <= 0:
        return None

    in_consolidation = abs(current - support) / range_height < 0.3
    pattern = (
        "Bullish Rectangle"
        if in_consolidation and trend == "LONG"
        else "Consolidation"
        if in_consolidation
        else "Trend"
    )

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


def compute_24h_stats(candles, index, lookback=96):
    if index < lookback:
        return None
    current = candles[index]["close"]
    prior = candles[index - lookback]["close"]
    if prior <= 0:
        return None
    change_pct = ((current - prior) / prior) * 100.0
    quote_volume = sum(row["quote_volume"] for row in candles[index - lookback + 1: index + 1])
    return change_pct, quote_volume


def simulate(module, symbol_candles, max_trades):
    min_len = min(len(rows) for rows in symbol_candles.values())
    start_index = 980
    positions = []
    recently_closed = {}
    trades = []
    scan_samples = []
    max_new_signals = int(getattr(module, "MAX_NEW_SIGNALS_PER_CYCLE", 5))

    for index in range(start_index, min_len):
        current_ts = max(rows[index]["close_time"] for rows in symbol_candles.values())

        active_positions = []
        for position in positions:
            current_price = symbol_candles[position["symbol"]][index]["close"]
            entry = position["entry_price"]
            if position["direction"] == "LONG":
                pnl_pct = ((current_price - entry) / entry) * 100.0
            else:
                pnl_pct = ((entry - current_price) / entry) * 100.0

            status = None
            if pnl_pct >= module.TP_PERCENT:
                status = "TP"
            elif pnl_pct <= -module.SL_PERCENT:
                status = "SL"

            if status:
                recently_closed[position["symbol"]] = current_ts
                trades.append(
                    {
                        "symbol": position["symbol"],
                        "direction": position["direction"],
                        "entry_time": position["entry_time"],
                        "exit_time": current_ts,
                        "entry_price": entry,
                        "exit_price": current_price,
                        "pnl_pct": pnl_pct,
                        "status": status,
                    }
                )
                if len(trades) >= max_trades:
                    return trades, scan_samples
            else:
                active_positions.append(position)
        positions = active_positions

        cutoff = current_ts - module.RECENTLY_CLOSED_TIMEOUT_SEC * 1000
        recently_closed = {k: v for k, v in recently_closed.items() if v >= cutoff}

        if len(positions) >= module.MAX_POSITIONS:
            continue

        opportunities = []
        for symbol, candles in symbol_candles.items():
            if any(position["symbol"] == symbol for position in positions):
                continue
            if symbol in recently_closed:
                continue

            stats = compute_24h_stats(candles, index)
            if not stats:
                continue
            pct, quote_volume = stats
            if quote_volume < module.MIN_QUOTE_VOLUME or abs(pct) < module.MIN_PRICE_CHANGE_ABS:
                continue

            analysis = analyze_symbol_at(module, symbol, candles, index)
            if not analysis:
                continue
            if hasattr(module, "passes_entry_filter") and not module.passes_entry_filter(analysis):
                continue
            if hasattr(module, "passes_momentum_filter") and not module.passes_momentum_filter(analysis, pct):
                continue

            analysis["price_change_pct"] = pct
            analysis["quote_volume"] = quote_volume

            if (analysis["trend"] == "LONG" and pct > 0) or (analysis["trend"] == "SHORT" and pct < 0):
                opportunities.append(analysis)

        opportunities.sort(key=lambda row: row["range_height"])

        if opportunities:
            scan_samples.append(
                {
                    "time": current_ts,
                    "top": [f"{row['symbol']}:{row['trend']}:{row['range_height']:.6f}" for row in opportunities[:5]],
                }
            )

        eligible_signals = 0
        for opportunity in opportunities:
            if len(positions) >= module.MAX_POSITIONS:
                break
            symbol = opportunity["symbol"]
            if any(position["symbol"] == symbol for position in positions):
                continue

            eligible_signals += 1
            if eligible_signals > max_new_signals:
                break

            positions.append(
                {
                    "symbol": symbol,
                    "direction": opportunity["trend"],
                    "entry_price": symbol_candles[symbol][index]["close"],
                    "entry_time": current_ts,
                }
            )

    return trades, scan_samples


def format_time(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Replay v7-futures strategy for 10 trades.")
    parser.add_argument("--script", default="v7-futures.py")
    parser.add_argument("--trades", type=int, default=10)
    parser.add_argument("--limit", type=int, default=1500)
    parser.add_argument("--symbols-file", default="futures_symbols.json")
    parser.add_argument("--json-out", default="")
    parser.add_argument("--snapshot-in", default="")
    parser.add_argument("--snapshot-out", default="")
    args = parser.parse_args()

    module = load_module(Path(args.script).resolve())

    symbol_meta, tradable_symbols = module.get_exchange_info()
    valid_symbols = module.build_valid_symbols(tradable_symbols)
    symbols = sorted(valid_symbols)

    symbol_candles = {}
    failures = []
    if args.snapshot_in:
        snapshot = json.loads(Path(args.snapshot_in).read_text(encoding="utf-8"))
        symbol_candles = snapshot.get("symbol_candles", {})
        failures = snapshot.get("fetch_failures", [])
        print(f"Loaded snapshot from {args.snapshot_in} with {len(symbol_candles)} symbols")
    else:
        print(f"Loading 15m data for {len(symbols)} symbols...")
        for idx, symbol in enumerate(symbols, start=1):
            try:
                symbol_candles[symbol] = fetch_15m_klines(symbol, args.limit)
                if idx % 10 == 0 or idx == len(symbols):
                    print(f"  fetched {idx}/{len(symbols)}")
            except Exception as exc:
                failures.append({"symbol": symbol, "error": str(exc)})

    if failures:
        print(f"Skipped {len(failures)} symbols due to fetch errors")

    if not symbol_candles:
        raise SystemExit("No symbol data available")

    if args.snapshot_out:
        snapshot_payload = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "symbols_used": sorted(symbol_candles),
            "fetch_failures": failures,
            "symbol_candles": symbol_candles,
        }
        Path(args.snapshot_out).write_text(json.dumps(snapshot_payload), encoding="utf-8")
        print(f"Saved snapshot to {args.snapshot_out}")

    trades, scan_samples = simulate(module, symbol_candles, args.trades)
    wins = sum(1 for trade in trades if trade["status"] == "TP")
    losses = sum(1 for trade in trades if trade["status"] == "SL")
    winrate = (wins / len(trades) * 100.0) if trades else 0.0
    avg_pnl = sum(trade["pnl_pct"] for trade in trades) / len(trades) if trades else 0.0

    print("")
    print(f"Trades captured: {len(trades)}")
    print(f"Wins: {wins}")
    print(f"Losses: {losses}")
    print(f"Winrate: {winrate:.2f}%")
    print(f"Average PnL: {avg_pnl:.2f}%")
    print("")
    for i, trade in enumerate(trades, start=1):
        print(
            f"{i:02d}. {trade['symbol']} {trade['direction']} {trade['status']} "
            f"{trade['pnl_pct']:+.2f}% | {format_time(trade['entry_time'])} -> {format_time(trade['exit_time'])}"
        )

    payload = {
        "summary": {
            "trades": len(trades),
            "wins": wins,
            "losses": losses,
            "winrate": winrate,
            "average_pnl_pct": avg_pnl,
        },
        "trades": trades,
        "fetch_failures": failures,
        "scan_samples": scan_samples[-10:],
        "symbols_used": sorted(symbol_candles),
        "symbol_meta_count": len(symbol_meta),
    }

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nSaved JSON report to {args.json_out}")


if __name__ == "__main__":
    main()
