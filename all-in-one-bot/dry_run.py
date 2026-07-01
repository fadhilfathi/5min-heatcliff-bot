#!/usr/bin/env python3
"""
dry_run.py — End-to-end dry run test for all-in-one-bot.
Tests WS connections only (no import of py_clob_client_v2).
"""
import asyncio
import json
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import importlib.util

def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod

MultiBookStream = _load_module("ws_mod", ROOT_DIR / "ws-all-in-one-monitor" / "stream.py").MultiBookStream
PriceFeed = _load_module("price_mod", ROOT_DIR / "all-in-one-bot" / "binance_ws.py").PriceFeed

async def dry_run():
    print("=" * 60)
    print(" DRY RUN — All-In-One Bot (NO TRADES)")
    print("=" * 60)

    config_path = ROOT_DIR / "all-in-one-bot" / "configs" / "coins.json"
    with config_path.open("r") as f:
        coins = json.load(f)

    token_ids = [c["token_id"] for c in coins.values()]
    symbols = {coin: conf["symbol"] for coin, conf in coins.items()}

    print(f"\n[1/4] Config: {list(coins.keys())}")
    print(f"       Token IDs: {token_ids}")

    # Test Polymarket WS
    print("\n[2/4] Polymarket WS (5s connect)...")
    pm_ws = MultiBookStream()
    pm_task = asyncio.create_task(pm_ws.run_forever(token_ids))
    await asyncio.sleep(5)
    pm_status = pm_ws.health()
    pm_books = pm_ws.get_all_books()
    print(f"       Status: {pm_status}")
    print(f"       Books:  {len(pm_books)} tokens")
    for tid, state in pm_books.items():
        bid = state.best_bid()
        ask = state.best_ask()
        print(f"         {tid}: bid={bid} ask={ask}")

    # Test Binance/Bybit WS
    print("\n[3/4] Binance+Bybit WS (5s connect)...")
    pf = PriceFeed()
    b_task = asyncio.create_task(pf.run_binance())
    bit_task = asyncio.create_task(pf.run_bybit())
    await asyncio.sleep(5)
    pf_status = pf.health()
    print(f"       Status: {pf_status}")
    prices_ok = 0
    for coin in symbols:
        price = pf.get_price(coin)
        if price:
            prices_ok += 1
            print(f"         {coin}: ${price:.6f}")
        else:
            print(f"         {coin}: no data yet")
    print(f"       Prices received: {prices_ok}/{len(coins)}")

    # Summary
    print("\n[4/4] Summary")
    print("-" * 40)
    issues = []
    if pm_status != "CONNECTED":
        issues.append(f"Polymarket: {pm_status}")
    if pf_status != "OK":
        issues.append(f"Price feed: {pf_status}")
    if prices_ok < len(coins):
        issues.append(f"Missing {len(coins) - prices_ok} prices")

    print(f"  Polymarket : {pm_status}  ({len(pm_books)} books)")
    print(f"  Price Feed : {pf_status}  ({prices_ok}/{len(coins)} prices)")
    print(f"  Config     : {len(coins)} coins")
    print("-" * 40)

    if issues:
        print("  RESULT: NEEDS ATTENTION")
        for i in issues:
            print(f"    - {i}")
    else:
        print("  RESULT: ALL SYSTEMS GO")

    print("=" * 60)

    pm_ws.stop()
    pf.stop()
    pm_task.cancel()
    b_task.cancel()
    bit_task.cancel()

    return len(issues) == 0

if __name__ == "__main__":
    try:
        ok = asyncio.run(dry_run())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)