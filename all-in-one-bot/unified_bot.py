#!/usr/bin/env python3
"""
unified_bot.py — All-in-One Multi-Coin Polymarket Bot.

Subscribes to Polymarket WS + external price feeds.
Launches the active coins defined in configs/coins.json.
Emits logs to UI Queue and logs-all-in-one/ folder.
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any

import requests
from dotenv import load_dotenv

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds, AssetType, BalanceAllowanceParams, MarketOrderArgsV2,
    OrderArgsV2, OrderType
)

# Insert local modules from hyphenated folders without renaming user paths
import importlib.util

ROOT_DIR = Path(__file__).resolve().parents[1]

def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod

PriceFeed = _load_module("binance_ws_mod", ROOT_DIR / "all-in-one-bot" / "binance_ws.py").PriceFeed
MultiBookStream = _load_module("ws_stream_mod", ROOT_DIR / "ws-all-in-one-monitor" / "stream.py").MultiBookStream

UTC = timezone.utc
CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
BOT_SPECS = {
    "BTC": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_btc_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "3", "--target-balance", "10", "--max-bid-exit", "0", "--poll-sec", "0.1"],
    },
    "ETH": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_eth_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
    "SOL": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_sol_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
    "BNB": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_bnb_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
    "HYPE": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_hype_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
    "XRP": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_xrp_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
    "DOGE": {
        "script": ROOT_DIR / "all-in-one-bot" / "trade_doge_ws.py",
        "args": ["--execute", "--stake-usd", "1", "--max-stake-usd", "1", "--max-session-loss-usd", "4", "--target-balance", "15", "--max-bid-exit", "0", "--poll-sec", "0.2"],
    },
}
# BOT_SPECS may include inactive runners; only coins present in configs/coins.json are launched.

class UnifiedBot:
    def __init__(self, ui_queue=None, max_session_loss_usd=1.0, target_balance=None, max_trades=0, verbose_logging=False, config_path: Optional[Path] = None, log_dir: Optional[Path] = None, child_extra_args: Optional[list[str]] = None):
        self.ui_queue = ui_queue
        
        root_dir = Path(__file__).resolve().parent
        load_dotenv(root_dir / ".." / ".env")
        self.log_dir = log_dir or (root_dir / ".." / "logs-all-in-one")
        self.log_dir.mkdir(exist_ok=True)

        # Configure file logging first
        log_file_name = datetime.now(UTC).strftime("unified_bot_%Y%m%d_%H%M%S.log")
        log_path = self.log_dir / log_file_name
        logging.basicConfig(
            level=logging.DEBUG if verbose_logging else logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
            handlers=[
                logging.FileHandler(log_path, encoding="utf-8"),
            ]
        )
        self.log = logging.getLogger("unified_bot")
        self.log.info("Unified Bot logging to %s", log_path)

        self.config_path = config_path or (root_dir / "configs" / "coins.json")
        self.configs = self._load_configs(self.config_path)
        self.child_extra_args = list(child_extra_args or [])
        
        self.pm_ws = MultiBookStream()
        self.price_feed = PriceFeed()
        
        self.positions = {coin: {
            "shares": 0.0, "avg_price": 0.0, "status": "SCANNING",
            "bid": 0.0, "ask": 0.0, "pnl": 0.0,
            "up_ask": 0.0, "down_ask": 0.0,
            "move": None, "secs": 0, "side": "", "wins": 0, "losses": 0,
            "session_pnl": 0.0, "_last_pnl": 0.0,
        } for coin in self.configs.keys()}
        self.positions["_meta"] = {
            "session_balance_pnl": 0.0,
            "start_balance": None,
            "current_balance": None,
        }
        self._prev_shares: dict[str, float] = {coin: 0.0 for coin in self.configs.keys()}
        self._bucket_open_prices: dict[str, Optional[float]] = {coin: None for coin in self.configs.keys()}
        self._bucket_open_bucket: dict[str, Optional[int]] = {coin: None for coin in self.configs.keys()}
        self._stopped = False
        self.auth_client = self._create_client()

        # Session control limits
        self.max_session_loss_usd: float = max_session_loss_usd
        self.target_balance: Optional[float] = target_balance
        self.max_trades: int = max_trades
        self.trade_count: int = 0
        self.session_pnl: float = 0.0
        self.start_balance: Optional[float] = None
        self._last_balance_check: float = 0.0
        self._cached_balance: Optional[float] = None
        self.child_threads: dict[str, threading.Thread] = {}
        self.child_stop = threading.Event()

        self.log.info("Attempting to resolve token IDs from slugs...")
        self.coin_tokens = self._resolve_token_ids()
        self.tokens = [token for token_ids in self.coin_tokens.values() for token in token_ids]
        if not self.tokens:
            self.log.error("No active 5m up/down markets found. Exiting.")
            sys.exit(1)
        self.log.info("Resolved token IDs by coin: %s", self.coin_tokens)
        self.log.info("Resolved token IDs for subscription: %s", self.tokens)
        self.current_bucket = int(time.time()) // 300
        
    def _create_client(self) -> ClobClient:
        key = os.getenv("PM_PRIVATE_KEY", "")
        funder = os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS") or ""
        api_key = os.getenv("PM_API_KEY", "")
        api_secret = os.getenv("PM_API_SECRET", "")
        api_pass = os.getenv("PM_API_PASSPHRASE", "")
        if not all([key, funder, api_key, api_secret, api_pass]):
            raise RuntimeError("missing credentials — check .env")
        c = ClobClient(host=CLOB_BASE_URL, chain_id=137, key=key, signature_type=os.getenv("PM_SIGNATURE_TYPE", "2"), funder=funder)
        c.set_api_creds(ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass))
        return c

    def _load_configs(self, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def log_ui(self, coin: str, msg: str):
        if msg.startswith("gate: seconds_left"): return # ponytail: silence seconds_left gate spam
        self.log.info("[%s] %s", coin, msg)
        if self.ui_queue:
            timestamp = time.strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] [bold blue]{coin}[/bold blue] {msg}"
            if self.ui_queue.full():
                self.ui_queue.get()
            self.ui_queue.put(log_entry)

    def _get_balance(self, force=False) -> Optional[float]:
        now = time.time()
        if not force and self._last_balance_check > (now - 30):
            return self._cached_balance
        try:
            payload = self.auth_client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            for key in ("balance", "collateral", "available", "allowance"):
                if isinstance(payload, dict) and payload.get(key) is not None:
                    try:
                        raw = float(payload[key])
                        val = raw / 1_000_000 if raw > 10_000 else raw
                        self._cached_balance = val
                        self._last_balance_check = now
                        return val
                    except Exception:
                        continue
            return self._cached_balance
        except Exception as e:
            self.log.error("Failed to read balance: %r", e)
            return self._cached_balance

    def _resolve_token_ids(self) -> dict[str, list[str]]:
        resolved_tokens: dict[str, list[str]] = {}
        now_ts = int(time.time())
        current_bucket = now_ts - (now_ts % 300)
        
        for coin, conf in self.configs.items():
            slug = f"{conf['token_id']}-{current_bucket}".lower()
            self.log.info("Resolving market: %s", slug)
            try:
                # Try fetching by specific slug first
                r = requests.get(f"{GAMMA_EVENTS_URL}?slug={slug}", timeout=10)
                r.raise_for_status()
                data = r.json()
                
                # If not found, try searching for the coin's updown pattern
                if not data:
                    pattern = conf['token_id'].lower()
                    self.log.info("Slug %s not found, searching pattern %s...", slug, pattern)
                    r = requests.get(f"{GAMMA_EVENTS_URL}?active=true&closed=false&limit=100", timeout=10)
                    r.raise_for_status()
                    all_events = r.json()
                    data = [e for e in all_events if e.get("slug", "").lower() == slug]
                    if not data:
                        data = [e for e in all_events if pattern in e.get("slug", "").lower()]
                        data.sort(key=lambda e: e.get("slug", "").lower() != slug)

                if not data:
                    self.log.warning("No active market found for %s", coin)
                    continue

                ev = None
                for candidate in data:
                    if (candidate.get("slug") or "").lower() == slug:
                        ev = candidate
                        break
                if ev is None:
                    def _bucket_ts(event: dict) -> int:
                        try:
                            return int(str(event.get("slug", "")).rsplit("-", 1)[-1])
                        except Exception:
                            return -1
                    exact_pattern = [candidate for candidate in data if conf["token_id"].lower() in (candidate.get("slug") or "").lower()]
                    if exact_pattern:
                        ev = max(exact_pattern, key=_bucket_ts)
                    else:
                        ev = data[0]
                mkts = ev.get("markets") or []
                if not mkts:
                    continue
                
                m = mkts[0]
                # Token IDs are usually in 'clobTokenIds' JSON string
                clob_token_ids = m.get("clobTokenIds")
                if isinstance(clob_token_ids, str):
                    clob_token_ids = json.loads(clob_token_ids)
                
                if clob_token_ids and len(clob_token_ids) >= 2:
                    resolved_tokens[coin] = clob_token_ids[:2]
                    self.log.info("Resolved %s token IDs for slug %s: %s", coin, ev.get("slug"), clob_token_ids[:2])
            except Exception as e:
                self.log.error("Failed to resolve %s: %r", coin, e)
                
        return resolved_tokens

    def _coin_command(self, coin: str) -> list[str]:
        spec = BOT_SPECS[coin]
        cmd = [*spec["args"], "--config-file", str(self.config_path), "--log-dir", str(self.log_dir)]
        cmd.extend(self.child_extra_args)
        if self.max_session_loss_usd > 0:
            cmd.extend(["--max-session-loss-usd", str(self.max_session_loss_usd)])
        if self.target_balance is not None:
            cmd.extend(["--target-balance", str(self.target_balance)])
        if self.max_trades > 0:
            cmd.extend(["--max-trades", str(self.max_trades)])
        return cmd

    def _run_coin_thread(self, coin: str, argv: list[str]) -> None:
        module_path = ROOT_DIR / "all-in-one-bot" / "ws_trade_runner.py"
        mod = _load_module(f"ws_trade_runner_{coin.lower()}_{int(time.time() * 1000)}", module_path)
        try:
            mod.run_coin_with_shared_feeds(
                coin,
                self.pm_ws,
                self.price_feed,
                argv=argv,
                log_fn=lambda line, c=coin: self.log_ui(c, line),
                state_ref=self.positions[coin],
                trade_count_fn=self._increment_trade_count,
            )
        except Exception as exc:
            self.log.error("[%s] shared-feed runner failed: %r", coin, exc)
            self.log_ui(coin, f"Runner failed: {exc}")

    def _start_trading_children(self) -> None:
        for coin in self.configs.keys():
            spec = BOT_SPECS.get(coin)
            if not spec:
                self.log_ui(coin, "No live trader mapped.")
                continue
            argv = self._coin_command(coin)
            self.log_ui(coin, f"Starting trader: {spec['script'].name}")
            thread = threading.Thread(target=self._run_coin_thread, args=(coin, argv), daemon=True)
            thread.start()
            self.child_threads[coin] = thread

    def _poll_child_processes(self) -> None:
        for coin, thread in list(self.child_threads.items()):
            if thread.is_alive():
                continue
            self.log_ui(coin, "Trader exited")
            del self.child_threads[coin]

    def _increment_trade_count(self) -> None:
        self.trade_count += 1

    def _refresh_bucket_tokens_if_needed(self) -> None:
        bucket = int(time.time()) // 300
        if bucket == self.current_bucket:
            return
        self.current_bucket = bucket
        for coin in self.configs.keys():
            self._bucket_open_prices[coin] = None
            self._bucket_open_bucket[coin] = None
        self.log_ui("SYSTEM", "New bucket detected; refreshing token subscriptions.")
        self.coin_tokens = self._resolve_token_ids()
        self.tokens = [token for token_ids in self.coin_tokens.values() for token in token_ids]
        # ponytail: force-clear stale book cache + force resubscribe on the WS task
        try:
            self.pm_ws.refresh_subscriptions(self.tokens)
        except Exception as exc:
            self.log.warning("pm_ws refresh failed: %r", exc)

    async def _scan_loop(self):
        self.start_balance = self._get_balance(force=True)
        if self.start_balance is not None:
            self.positions["_meta"]["start_balance"] = self.start_balance
            self.log_ui("SYSTEM", f"Starting Balance: ${self.start_balance:.4f}")

        while not self._stopped:
            if self.max_trades > 0 and self.trade_count >= self.max_trades:
                self.log_ui("SYSTEM", f"Session stop: Max trades reached.")
                self.stop()
                break

            if self.session_pnl <= -self.max_session_loss_usd:
                self.log_ui("SYSTEM", f"Session stop: Max loss reached.")
                self.stop()
                break

            if self.target_balance is not None:
                current_bal = self._get_balance()
                if current_bal is not None and current_bal >= self.target_balance:
                    self.log_ui("SYSTEM", f"Session stop: Target balance reached.")
                    self.stop()
                    break

            self._poll_child_processes()
            self._refresh_bucket_tokens_if_needed()
            current_bal = self._get_balance()
            self.positions["_meta"]["current_balance"] = current_bal
            if self.start_balance is not None and current_bal is not None:
                self.positions["_meta"]["session_balance_pnl"] = current_bal - self.start_balance

            now_ts = int(time.time())
            bucket_secs = 300 - (now_ts % 300)
            bucket_open_ts_ms = (now_ts // 300) * 300 * 1000

            for coin, conf in self.configs.items():
                if coin not in self.positions:
                    continue
                pos = self.positions[coin]
                token_ids = self.coin_tokens.get(coin, [])

                bid = 0.0
                ask = 0.0
                up_ask = 0.0
                down_ask = 0.0
                live_books = 0
                for idx, tid in enumerate(token_ids):
                    book = self.pm_ws.get_book(tid)
                    if not book:
                        continue
                    live_books += 1
                    bid = max(bid, book.best_bid() or 0.0)
                    best_ask = book.best_ask() or 0.0
                    if best_ask > 0.0:
                        ask = best_ask if ask == 0.0 else min(ask, best_ask)
                        if idx == 0:
                            up_ask = best_ask
                        elif idx == 1:
                            down_ask = best_ask

                self.positions[coin]["bid"] = bid
                self.positions[coin]["ask"] = ask
                self.positions[coin]["up_ask"] = up_ask
                self.positions[coin]["down_ask"] = down_ask
                self.positions[coin]["secs"] = bucket_secs

                price = self.price_feed.get_price(coin)
                if self._bucket_open_bucket.get(coin) != (now_ts // 300):
                    self._bucket_open_bucket[coin] = now_ts // 300
                    self._bucket_open_prices[coin] = price
                elif self._bucket_open_prices.get(coin) is None and price is not None:
                    self._bucket_open_prices[coin] = price

                bucket_open_price = self._bucket_open_prices.get(coin)
                if price is not None and bucket_open_price is not None and bucket_open_price > 0:
                    self.positions[coin]["move"] = price - bucket_open_price
                else:
                    self.positions[coin]["move"] = None

                cur_status = str(pos.get("status", ""))
                if cur_status not in {"ENTERING", "IN_TRADE", "SELLING", "ERROR"}:
                    if live_books > 0:
                        self.positions[coin]["status"] = "SCANNING"
                    elif price is not None:
                        self.positions[coin]["status"] = "PRICE ONLY"
                    else:
                        self.positions[coin]["status"] = "WAITING"

                cur_shares = float(pos.get("shares", 0.0) or 0.0)

                if cur_shares > 0.0:
                    sell_now = cur_shares * bid
                    pnl = sell_now - (cur_shares * pos["avg_price"])
                    self.positions[coin]["pnl"] = pnl
                    self.positions[coin]["_last_pnl"] = pnl
                else:
                    self.positions[coin]["pnl"] = 0.0

                prev_shares = self._prev_shares.get(coin, 0.0)
                if prev_shares > 0.0 and cur_shares <= 0.0:
                    self.positions[coin]["session_pnl"] = float(pos.get("session_pnl", 0.0) or 0.0) + float(pos.get("_last_pnl", 0.0) or 0.0)
                    self.positions[coin]["_last_pnl"] = 0.0
                self._prev_shares[coin] = cur_shares

            if self.start_balance is not None and current_bal is not None:
                self.session_pnl = current_bal - self.start_balance
            await asyncio.sleep(0.5)

    async def run(self):
        self.log.info("Starting All-In-One Unified Bot")
        asyncio.create_task(self.pm_ws.run_forever(lambda: self.tokens))
        asyncio.create_task(self.price_feed.run_binance())
        asyncio.create_task(self.price_feed.run_bybit())
        self._start_trading_children()
        await self._scan_loop()

    def stop(self):
        self._stopped = True
        self.pm_ws.stop()
        self.price_feed.stop()
        self.child_stop.set()
        for coin, thread in list(self.child_threads.items()):
            self.log_ui(coin, "Stopping trader...")
            with contextlib.suppress(Exception):
                thread.join(timeout=1)

def main():
    bot = UnifiedBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        bot.stop()

if __name__ == "__main__":
    main()
