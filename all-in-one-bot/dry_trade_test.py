#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import datetime as dt
from pathlib import Path

import ws_trade_runner as mod


def main() -> int:
    coin = "BTC"
    mod.COIN_CFG = mod._load_coin_config(coin)
    mod.LOG_FILE = Path(mod.ROOT_DIR / "logs-all-in-one" / "dry_trade_test.log")
    mod.VERBOSE = False
    mod.QUIET_TAPE = False
    mod.UI_LOG_FN = None

    now = dt.datetime.now(mod.UTC)
    bucket_ts = int(now.timestamp()) - (int(now.timestamp()) % 300)
    slug = f"{mod.COIN_CFG['token_id']}-{bucket_ts}"
    end_iso = (now + dt.timedelta(seconds=90)).isoformat().replace("+00:00", "Z")
    token_up = "UPTOKEN"
    token_down = "DOWNTOKEN"

    class FakeFeed:
        def __init__(self):
            self.prices = [105000.0, 105050.0, 105060.0]

        def get_price(self, coin_name: str):
            return self.prices[-1]

        def get_price_at_or_before(self, coin_name: str, ts_ms: int):
            return self.prices[0]

    mod.PRICE_FEED = FakeFeed()

    side_books = {
        "up": {"bid": 0.62, "bid_size": 10.0, "ask": 0.66, "ask_size": 10.0, "age_ms": 0},
        "down": {"bid": 0.35, "bid_size": 10.0, "ask": 0.40, "ask_size": 10.0, "age_ms": 0},
    }
    token_sequence = [
        {"bid": 0.78, "bid_size": 10.0, "ask": 0.80, "ask_size": 10.0, "age_ms": 0},
        {"bid": 0.60, "bid_size": 10.0, "ask": 0.62, "ask_size": 10.0, "age_ms": 0},
    ]

    original_market = mod.resolve_active_current_5m_market
    original_market_side = mod.market_side_prices
    original_side_prices = mod.clob_side_prices
    original_token_prices = mod.clob_token_prices
    original_asset_move = mod.asset_move
    original_asset_velocity = mod.asset_velocity
    original_sleep = mod.time.sleep

    seq = {"i": 0}

    def fake_market():
        return {"slug": slug, "markets": [{"clobTokenIds": [token_up, token_down]}], "endDate": end_iso}

    def fake_market_side_prices(market):
        return side_books["up"]["ask"], side_books["down"]["ask"], token_up, token_down, slug, end_iso

    def fake_side_prices(up_token, down_token):
        return side_books

    def fake_token_prices(token):
        idx = min(seq["i"], len(token_sequence) - 1)
        seq["i"] += 1
        return token_sequence[idx]

    move_calls = {"n": 0}

    def fake_asset_move(bucket):
        move_calls["n"] += 1
        return 55.0 if move_calls["n"] <= 2 else 2.0

    def fake_asset_velocity(window):
        return 7.0

    mod.resolve_active_current_5m_market = fake_market
    mod.market_side_prices = fake_market_side_prices
    mod.clob_side_prices = fake_side_prices
    mod.clob_token_prices = fake_token_prices
    mod.asset_move = fake_asset_move
    mod.asset_velocity = fake_asset_velocity
    mod.time.sleep = lambda _: None

    try:
        args = mod.parse_args(["--paper-trade", "--once", "--poll-sec", "0.01", "--log-file", str(mod.LOG_FILE)])
        report = mod.run_cycle(args, cycle_no=1, traded_slugs=set())
    finally:
        mod.resolve_active_current_5m_market = original_market
        mod.market_side_prices = original_market_side
        mod.clob_side_prices = original_side_prices
        mod.clob_token_prices = original_token_prices
        mod.asset_move = original_asset_move
        mod.asset_velocity = original_asset_velocity
        mod.time.sleep = original_sleep

    print(report.get("result"))
    print(report.get("opened"))
    print(report.get("close_reason"))
    return 0 if report.get("opened") and report.get("close_reason") else 1


if __name__ == "__main__":
    raise SystemExit(main())
