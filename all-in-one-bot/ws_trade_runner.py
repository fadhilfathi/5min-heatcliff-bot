#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgsV2,
    OrderArgsV2,
    OrderType,
)

import importlib.util

ROOT_DIR = Path(__file__).resolve().parents[1]
ALL_IN_ONE_DIR = ROOT_DIR / "all-in-one-bot"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


PriceFeed = _load_module("all_in_one_pricefeed", ALL_IN_ONE_DIR / "binance_ws.py").PriceFeed
MultiBookStream = _load_module("all_in_one_bookstream", ROOT_DIR / "ws-all-in-one-monitor" / "stream.py").MultiBookStream

UTC = dt.timezone.utc
LOCAL_TZ = dt.timezone(dt.timedelta(hours=7))
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_BASE_URL = "https://clob.polymarket.com"

DEFAULT_MIN_SHARES = 0.0
DEFAULT_MIN_BUY_USD = 1.0
DEFAULT_MAX_STAKE_USD = 1.0
DEFAULT_MAX_SPREAD = 0.05
DEFAULT_MIN_BOOK_DEPTH_USD = 1.0
DEFAULT_STOP_LOSS_BID = 0.05
DEFAULT_STOP_LOSS_PCT = 0.67
DEFAULT_TAKE_PROFIT_PCT = 0.07
DEFAULT_TAKE_PROFIT_USD = 1.05
DEFAULT_MAX_ENTRY_SECONDS_LEFT = 90
DEFAULT_MIN_ENTRY_SECONDS_LEFT = 30
DEFAULT_MAX_ENTRY_ASK = 0.95
DEFAULT_TIME_EXIT_MIN_SELL_USD = 1.0
DEFAULT_TRAIL_PCT = 0.03
DEFAULT_MAX_BID_EXIT = 0.99
DEFAULT_CLOSE_RETRY_MAX = 3
DEFAULT_CONFIRM_GAP_SEC = 0.10
DEFAULT_ENTRY_TIMEOUT_MIN = 60
DEFAULT_POLL_SEC = 0.2

LOG_FILE: Optional[Path] = None
VERBOSE = False
QUIET_TAPE = False
PM_WS: Optional[MultiBookStream] = None
PRICE_FEED: Optional[PriceFeed] = None
COIN_CFG: dict[str, Any] = {}
UI_LOG_FN = None
STATE_REF = None
TRADE_COUNT_FN = None
LAST_DIAG_MESSAGE: Optional[str] = None
LAST_DIAG_AT: float = 0.0
LAST_SCAN_LOG_AT: float = 0.0


def now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def ts_utc() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


def ts_local() -> str:
    return dt.datetime.now(LOCAL_TZ).strftime("%Y-%m-%d/%H:%M:%S")


def color_log(line: str) -> str:
    return line


def log(message: str) -> None:
    line = f"[{ts_local()}] {message}"
    if UI_LOG_FN:
        try:
            UI_LOG_FN(line)
        except Exception:
            pass
    if VERBOSE:
        try:
            print(color_log(line), flush=True)
        except OSError:
            pass
    if LOG_FILE:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def event(message: str) -> None:
    line = f"[{ts_local()}] {message}"
    if UI_LOG_FN:
        try:
            UI_LOG_FN(line)
        except Exception:
            pass
    if UI_LOG_FN is None:
        try:
            print(color_log(line), flush=True)
        except OSError:
            pass
    if LOG_FILE:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def tape(message: str = "") -> None:
    if QUIET_TAPE:
        return
    if UI_LOG_FN and message:
        try:
            UI_LOG_FN(message)
        except Exception:
            pass
    if UI_LOG_FN is None:
        try:
            print(message, flush=True)
        except OSError:
            pass
    if LOG_FILE:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(message + "\n")


def write_report(report: dict[str, Any]) -> None:
    return


def _preparse_path_arg(argv: Optional[list[str]], name: str, default: Path) -> Path:
    if not argv:
        return default
    try:
        idx = argv.index(name)
    except ValueError:
        return default
    if idx + 1 >= len(argv):
        return default
    return Path(argv[idx + 1])


def update_state(**fields) -> None:
    if STATE_REF is not None:
        STATE_REF.update(fields)


def load_env_file(path: Path) -> None:
    load_dotenv(path)


def public_client() -> ClobClient:
    return ClobClient(host=CLOB_BASE_URL, chain_id=137)


def auth_client() -> ClobClient:
    key = os.getenv("PM_PRIVATE_KEY") or ""
    funder = os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS") or None
    sig = int(os.getenv("PM_SIGNATURE_TYPE", "3") or "3")
    api_key = os.getenv("PM_API_KEY") or ""
    api_secret = os.getenv("PM_API_SECRET") or ""
    api_passphrase = os.getenv("PM_API_PASSPHRASE") or ""
    if not all([key, funder, api_key, api_secret, api_passphrase]):
        raise RuntimeError("missing live credentials in env")
    client = ClobClient(host=CLOB_BASE_URL, chain_id=137, key=key, signature_type=sig, funder=funder)
    client.set_api_creds(ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase))
    return client


def current_balance_usd(client: Optional[ClobClient] = None) -> Optional[float]:
    try:
        payload = (client or auth_client()).get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception as exc:
        log(f"balance unavailable: {exc}")
        return None
    for key in ("balance", "collateral", "available", "allowance"):
        if isinstance(payload, dict) and payload.get(key) is not None:
            try:
                value = float(payload[key])
                return value / 1_000_000 if value > 10_000 else value
            except Exception:
                pass
    return None


def _event_slug() -> str:
    return COIN_CFG["token_id"].lower()


def resolve_active_current_5m_market() -> Optional[dict[str, Any]]:
    now_ts = int(time.time())
    current_bucket = now_ts - (now_ts % 300)
    slug = f"{_event_slug()}-{current_bucket}"
    try:
        r = public_client()
        del r
        import requests
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=12)
        resp.raise_for_status()
        data = resp.json()
        return data[0] if data else None
    except Exception:
        return None


def market_side_prices(market: dict[str, Any]) -> tuple[Optional[float], Optional[float], str, str, str, str]:
    slug = market.get("slug") or ""
    end_iso = market.get("endDate") or market.get("end_date_iso") or market.get("end_date") or ""
    markets = market.get("markets") or []
    if not markets:
        raise RuntimeError("market has no outcomes")
    clob_token_ids = markets[0].get("clobTokenIds")
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)
    if not clob_token_ids or len(clob_token_ids) < 2:
        raise RuntimeError("market missing token ids")
    up_token, down_token = str(clob_token_ids[0]), str(clob_token_ids[1])
    books = clob_side_prices(up_token, down_token)
    return books["up"]["ask"], books["down"]["ask"], up_token, down_token, slug, end_iso


def _book_from_ws(token: str) -> dict[str, Any]:
    assert PM_WS is not None
    book = PM_WS.get_book(token)
    if book is None:
        return {"bid": None, "bid_size": 0.0, "ask": None, "ask_size": 0.0, "age_ms": -1}
    now = time.time()
    bid = book.best_bid()
    ask = book.best_ask()
    bid_size = float(book.bids.get(bid, 0.0)) if bid is not None else 0.0
    ask_size = float(book.asks.get(ask, 0.0)) if ask is not None else 0.0
    age_ms = int(max(0.0, now - book.updated_at) * 1000) if book.updated_at else -1
    return {"bid": bid, "bid_size": bid_size, "ask": ask, "ask_size": ask_size, "age_ms": age_ms}


def clob_side_prices(up_token: str, down_token: str) -> dict[str, Any]:
    return {"up": _book_from_ws(up_token), "down": _book_from_ws(down_token)}


def clob_token_prices(token: str) -> dict[str, Any]:
    return _book_from_ws(token)


def _asset_price(ts_ms: Optional[int] = None) -> Optional[float]:
    assert PRICE_FEED is not None
    coin = COIN_CFG["coin"]
    if ts_ms is None:
        return PRICE_FEED.get_price(coin)
    return PRICE_FEED.get_price_at_or_before(coin, ts_ms)


def asset_move(bucket_ts: int) -> Optional[float]:
    start = _asset_price(bucket_ts * 1000)
    current = _asset_price()
    if start is None or current is None:
        return None
    return current - start


def asset_velocity(window_sec: int) -> Optional[float]:
    now_ms = int(time.time() * 1000)
    start = _asset_price(now_ms - window_sec * 1000)
    current = _asset_price()
    if start is None or current is None:
        return None
    return current - start


def thesis_allows_exit(opened: dict[str, Any], threshold: float) -> bool:
    try:
        bucket_ts = int(str(opened.get("market_slug", "")).rsplit("-", 1)[-1])
    except Exception:
        return True
    current_move = asset_move(bucket_ts)
    if current_move is None:
        return True
    signed_in_direction = current_move if opened.get("side") == "UP" else -current_move
    return signed_in_direction < threshold


def env_health() -> dict[str, bool]:
    return {
        "PM_PRIVATE_KEY": bool(os.getenv("PM_PRIVATE_KEY")),
        "PM_API_KEY": bool(os.getenv("PM_API_KEY")),
        "PM_API_SECRET": bool(os.getenv("PM_API_SECRET")),
        "PM_API_PASSPHRASE": bool(os.getenv("PM_API_PASSPHRASE")),
        "PM_FUNDER_OR_ADDRESS": bool(os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS")),
    }


def pick_side(threshold: float, books: dict[str, Any], max_spread: float, min_book_depth_usd: float) -> tuple[Optional[tuple[str, float, float]], list[dict[str, Any]]]:
    candidates: list[tuple[str, float, float]] = []
    skips: list[dict[str, Any]] = []
    for side_name, payload in (("UP", books["up"]), ("DOWN", books["down"])):
        bid = payload.get("bid")
        ask = payload.get("ask")
        ask_size = float(payload.get("ask_size") or 0.0)
        spread = None if bid is None or ask is None else float(ask) - float(bid)
        ask_depth_usd = (float(ask) * ask_size) if ask is not None else 0.0
        reasons = []
        if ask is None:
            reasons.append("no_ask")
        elif float(ask) < threshold:
            reasons.append("below_threshold")
        if spread is None:
            reasons.append("no_spread")
        elif spread > max_spread:
            reasons.append("spread_too_wide")
        if ask_depth_usd < min_book_depth_usd:
            reasons.append("thin_book")
        if reasons:
            skips.append({"side": side_name, "bid": bid, "ask": ask, "spread": spread, "ask_depth_usd": round(ask_depth_usd, 6), "reasons": reasons})
            continue
        candidates.append((side_name, float(ask), ask_size))
    if not candidates:
        return None, skips
    return sorted(candidates, key=lambda item: (item[1], item[2]), reverse=True)[0], skips


def compute_order_size(target_usd: float, price: float, min_shares: float = DEFAULT_MIN_SHARES, min_buy_usd: float = DEFAULT_MIN_BUY_USD, max_stake_usd: float = DEFAULT_MAX_STAKE_USD) -> tuple[float, float, bool, bool]:
    stake_usd = min(max(target_usd, min_buy_usd), max_stake_usd)
    raw_shares = (stake_usd / price) if price > 0 else 0.0
    shares = max(min_shares, raw_shares)
    shares = round(shares, 4)
    notional = round(shares * price, 2)
    forced_minimum = shares > raw_shares + 1e-9
    capped_stake = stake_usd < target_usd
    return shares, notional, forced_minimum, capped_stake


def create_market_buy(client: ClobClient, token_id: str, amount_usd: float) -> dict[str, Any]:
    order = client.create_market_order(MarketOrderArgsV2(token_id=str(token_id), amount=float(amount_usd), side="BUY", order_type=OrderType.FOK))
    return client.post_order(order, OrderType.FOK)


def create_market_sell(client: ClobClient, token_id: str, amount_shares: float) -> dict[str, Any]:
    order = client.create_market_order(MarketOrderArgsV2(token_id=str(token_id), amount=float(amount_shares), side="SELL", order_type=OrderType.FOK))
    return client.post_order(order, OrderType.FOK)


def create_limit_sell_fak(client: ClobClient, token_id: str, amount_shares: float, price: float) -> dict[str, Any]:
    order = client.create_order(
        OrderArgsV2(token_id=str(token_id), price=float(price), size=float(amount_shares), side="SELL"),
        OrderType.FAK,
    )
    return client.post_order(order, OrderType.FAK)


def resolve_sell_shares(opened: dict[str, Any]) -> float:
    return round(float(opened.get("actual_shares") or opened.get("estimated_shares") or 0.0), 6)


def close_with_ladder(client: ClobClient, opened: dict[str, Any], retry_max: int) -> tuple[Optional[dict[str, Any]], str, float]:
    token_id = opened["token_id"]
    amount_shares = resolve_sell_shares(opened)
    last_err = "no_attempt"
    sell_shares = amount_shares
    for attempt in range(1, retry_max + 1):
        book = clob_token_prices(token_id)
        bid = book.get("bid")
        bid_size = float(book.get("bid_size") or 0.0)
        if bid is None:
            last_err = f"no_bid_attempt_{attempt}"
            time.sleep(0.3)
            continue
        chunk = round(min(sell_shares, bid_size if bid_size > 0 else sell_shares), 6)
        if chunk <= 0:
            last_err = f"zero_chunk_attempt_{attempt}"
            time.sleep(0.3)
            continue
        try:
            post = create_market_sell(client, token_id, chunk)
            event(f"market sell: token={token_id} chunk={chunk} filled_attempt_{attempt}")
            return post, f"filled_attempt_{attempt}_fok_chunk_{chunk}", sell_shares
        except Exception as exc:
            event(f"market sell failed: token={token_id} chunk={chunk} attempt={attempt}/{retry_max} error={exc}")
            last_err = f"attempt_{attempt}_fok_failed: {exc}"
            log(f"close ladder {attempt}/{retry_max} FOK failed: {exc}")
            # sell_shares = sell_shares / 2  # ponytail: removed halving to keep full order size
            time.sleep(0.3)
    return None, last_err, amount_shares


def settlement_close(closed_at: str, reason: str, shares: float, book: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    return {"closed_at": closed_at, "close_shares": 0.0, "remaining_shares": shares, "close_skipped": reason, "last_book": book}


def trade_block_header(slug: str, side: str) -> str:
    return f"[{dt.datetime.now(LOCAL_TZ):%H:%M}] round {slug.rsplit('-', 1)[-1]}   ->   {side}"


def diag(args: argparse.Namespace, message: str) -> None:
    global LAST_DIAG_MESSAGE, LAST_DIAG_AT
    now = time.time()
    if message.startswith("gate: seconds_left"): return # ponytail: silence seconds_left gate spam
    if message == LAST_DIAG_MESSAGE and (now - LAST_DIAG_AT) < 15.0:
        return
    LAST_DIAG_MESSAGE = message
    LAST_DIAG_AT = now
    log(message)


def scan_log(message: str, every_sec: float = 1.0) -> None:
    global LAST_SCAN_LOG_AT
    now = time.time()
    if (now - LAST_SCAN_LOG_AT) < every_sec:
        return
    LAST_SCAN_LOG_AT = now
    log(message)


def _cfg_threshold_value(key: str, threshold: float, default: float) -> float:
    table = COIN_CFG.get(key)
    if isinstance(table, dict):
        value = table.get(f"{round(float(threshold), 2):.2f}")
        if value is not None:
            return float(value)
        if table:
            try:
                nearest_key = min(table.keys(), key=lambda k: abs(float(k) - float(threshold)))
                return float(table[nearest_key])
            except Exception:
                pass
    fallback = COIN_CFG.get(key.replace("_by_threshold", ""))
    if fallback is not None:
        return float(fallback)
    return float(default)


def _exit_gate_value(key: str, default: float) -> float:
    return float(COIN_CFG.get(key, default))


def apply_profile(args: argparse.Namespace) -> argparse.Namespace:
    if args.threshold is None:
        args.threshold = float(COIN_CFG.get("threshold", 0.60))
    if args.stake_usd is None:
        args.stake_usd = 2.0
    if args.max_stake_usd is None:
        args.max_stake_usd = 2.0
    if args.max_spread is None:
        args.max_spread = _cfg_threshold_value("spread_by_threshold", args.threshold, DEFAULT_MAX_SPREAD)
    if args.min_book_depth_usd is None:
        args.min_book_depth_usd = float(COIN_CFG.get("min_book_depth_usd", DEFAULT_MIN_BOOK_DEPTH_USD))
    if getattr(args, "stop_loss_bid", None) is None:
        args.stop_loss_bid = float(COIN_CFG.get("stop_loss_bid", DEFAULT_STOP_LOSS_BID))
    if args.stop_loss_pct is None:
        args.stop_loss_pct = float(COIN_CFG.get("stop_loss_pct", DEFAULT_STOP_LOSS_PCT))
    if args.take_profit_pct is None:
        args.take_profit_pct = float(COIN_CFG.get("take_profit_pct", DEFAULT_TAKE_PROFIT_PCT))
    if args.take_profit_usd is None:
        args.take_profit_usd = DEFAULT_TAKE_PROFIT_USD
    if args.min_entry_seconds_left is None:
        args.min_entry_seconds_left = DEFAULT_MIN_ENTRY_SECONDS_LEFT
    if args.max_entry_seconds_left is None:
        args.max_entry_seconds_left = DEFAULT_MAX_ENTRY_SECONDS_LEFT
    if "min_entry_seconds_left" in COIN_CFG:
        args.min_entry_seconds_left = int(COIN_CFG["min_entry_seconds_left"])
    if "max_entry_seconds_left" in COIN_CFG:
        args.max_entry_seconds_left = int(COIN_CFG["max_entry_seconds_left"])
    if "profit_exit_seconds_left" in COIN_CFG:
        args.profit_exit_seconds_left = float(COIN_CFG["profit_exit_seconds_left"])
    if args.max_entry_ask is None:
        args.max_entry_ask = float(COIN_CFG.get("max_entry_ask", DEFAULT_MAX_ENTRY_ASK))
    if args.entry_timeout_min is None:
        args.entry_timeout_min = DEFAULT_ENTRY_TIMEOUT_MIN
    if args.poll_sec is None:
        args.poll_sec = DEFAULT_POLL_SEC
    if args.trail_pct is None:
        args.trail_pct = float(COIN_CFG.get("trail_pct", DEFAULT_TRAIL_PCT))
    if args.max_bid_exit is None:
        args.max_bid_exit = float(COIN_CFG.get("max_bid_exit", DEFAULT_MAX_BID_EXIT))
    if args.close_retry_max is None:
        args.close_retry_max = DEFAULT_CLOSE_RETRY_MAX
    if args.asset_move_usd_min is None:
        args.asset_move_usd_min = _cfg_threshold_value("move_min_by_threshold", args.threshold, 0.0)
    if args.asset_velocity_window_sec is None:
        args.asset_velocity_window_sec = int(COIN_CFG.get("vel_window_sec", 30))
    if args.asset_velocity_min_usd is None:
        args.asset_velocity_min_usd = _cfg_threshold_value("vel_min_by_threshold", args.threshold, float(COIN_CFG["vel_min"]))
    return args


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=None)
    ap.add_argument("--stake-usd", type=float, default=None)
    ap.add_argument("--max-stake-usd", type=float, default=None)
    ap.add_argument("--max-spread", type=float, default=None)
    ap.add_argument("--min-book-depth-usd", type=float, default=None)
    ap.add_argument("--stop-loss-bid", type=float, default=None)
    ap.add_argument("--stop-loss-pct", type=float, default=None)
    ap.add_argument("--take-profit-pct", type=float, default=None)
    ap.add_argument("--take-profit-usd", type=float, default=None)
    ap.add_argument("--stop-loss-usd", dest="stop_loss_pct", type=float, default=None, help=argparse.SUPPRESS)
    ap.add_argument("--time-exit-min-sell-usd", type=float, default=None)
    ap.add_argument("--asset-move-usd-min", type=float, default=None)
    ap.add_argument("--asset-velocity-window-sec", type=int, default=None)
    ap.add_argument("--asset-velocity-min-usd", type=float, default=None)
    ap.add_argument("--max-entry-seconds-left", type=int, default=None)
    ap.add_argument("--min-entry-seconds-left", type=int, default=None)
    ap.add_argument("--profit-exit-seconds-left", type=float, default=0.0)
    ap.add_argument("--max-entry-ask", type=float, default=None)
    ap.add_argument("--entry-timeout-min", type=int, default=None)
    ap.add_argument("--poll-sec", type=float, default=None)
    ap.add_argument("--trail-pct", type=float, default=None)
    ap.add_argument("--max-bid-exit", type=float, default=None)
    ap.add_argument("--close-retry-max", type=int, default=None)
    ap.add_argument("--env-file", default=str(ROOT_DIR / ".env"))
    ap.add_argument("--config-file", default=str(ALL_IN_ONE_DIR / "configs" / "coins.json"))
    ap.add_argument("--log-dir", default=str(ROOT_DIR / "logs-all-in-one"))
    ap.add_argument("--log-file", default=None)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--paper-trade", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--max-session-loss-usd", type=float, default=1.0)
    ap.add_argument("--target-balance", type=float, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    return apply_profile(args)


def run_cycle(args: argparse.Namespace, cycle_no: int, session_pnl: float = 0.0, start_balance: Optional[float] = None, traded_slugs: Optional[set] = None) -> dict[str, Any]:
    report: dict[str, Any] = {
        "started_at": ts_utc(),
        "cycle_no": cycle_no,
        "env_health": env_health(),
        "attempts": [],
        "coin": COIN_CFG["coin"],
        "book_source": "all-in-one-ws",
    }
    deadline = time.time() + args.entry_timeout_min * 60
    opened = None
    signal_side = None
    signal_polls = 0
    while time.time() < deadline:
        try:
            market = resolve_active_current_5m_market()
            if not market:
                report["attempts"].append({"ts": ts_utc(), "status": "heartbeat_no_current_market"})
                diag(args, f"gate: no_current_market slug_prefix={_event_slug()}")
                log(f"no current active {COIN_CFG['coin']} 5m market")
                update_state(status="WAITING")
                if args.once:
                    report["result"] = "no_current_market"
                    break
                time.sleep(args.poll_sec)
                continue
            _, _, up_token, down_token, slug, end_iso = market_side_prices(market)
            if traded_slugs and slug in traded_slugs:
                diag(args, f"gate: already_traded_this_bucket slug={slug}")
                update_state(status="WAITING")
                report["result"] = "already_traded_this_bucket"
                break
            end_ts = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
            seconds_left = max(0.0, end_ts - time.time())
            if seconds_left > args.max_entry_seconds_left or seconds_left < args.min_entry_seconds_left:
                signal_side = None
                signal_polls = 0
                update_state(status="WAITING", side="")
                diag(args, f"gate: seconds_left seconds_left={seconds_left:.1f} allowed={args.min_entry_seconds_left}..{args.max_entry_seconds_left}")
                time.sleep(args.poll_sec)
                continue
            books = clob_side_prices(up_token, down_token)
            current_move = asset_move(int(slug.rsplit("-", 1)[-1]))
            scan_log(
                "scan: "
                f"up_bid={'n/a' if books['up']['bid'] is None else f'{books['up']['bid']:.4f}'} "
                f"up_ask={'n/a' if books['up']['ask'] is None else f'{books['up']['ask']:.4f}'} "
                f"down_bid={'n/a' if books['down']['bid'] is None else f'{books['down']['bid']:.4f}'} "
                f"down_ask={'n/a' if books['down']['ask'] is None else f'{books['down']['ask']:.4f}'} "
                f"move={'n/a' if current_move is None else f'${current_move:+.7f}'} "
                f"seconds_left={seconds_left:.0f}"
            )
            update_state(
                up_ask=float(books["up"]["ask"] or 0.0),
                down_ask=float(books["down"]["ask"] or 0.0),
                bid=float(max(books["up"]["bid"] or 0.0, books["down"]["bid"] or 0.0)),
                ask=float(min(v for v in [books["up"]["ask"], books["down"]["ask"]] if v is not None) if any(v is not None for v in [books["up"]["ask"], books["down"]["ask"]]) else 0.0),
            )
            candidate, skip_reasons = pick_side(args.threshold, books, args.max_spread, args.min_book_depth_usd)
            if candidate is None:
                signal_side = None
                signal_polls = 0
                update_state(status="WAITING", side="")
                if all(payload.get("bid") is None and payload.get("ask") is None for payload in books.values()):
                    health = PM_WS.health() if PM_WS is not None else "NO_WS"
                    event_count = getattr(PM_WS, "event_count", -1) if PM_WS is not None else -1
                    diag(args, f"gate: empty_books ws_health={health} ws_events={event_count} skips={skip_reasons}")
                else:
                    diag(args, f"gate: no_candidate skips={skip_reasons}")
                report["attempts"].append({"ts": ts_utc(), "status": "no_candidate", "skips": skip_reasons})
                time.sleep(args.poll_sec)
                continue
            side, ask_price, ask_size = candidate
            if ask_price > args.max_entry_ask:
                signal_side = None
                signal_polls = 0
                update_state(status="WAITING", side=side)
                diag(args, f"gate: ask_too_high side={side} ask={ask_price:.4f} max={args.max_entry_ask:.4f}")
                time.sleep(args.poll_sec)
                continue
            bucket_ts = int(slug.rsplit("-", 1)[-1])
            move_usd = asset_move(bucket_ts)
            if move_usd is None:
                signal_side = None
                signal_polls = 0
                update_state(status="WAITING", side=side)
                diag(args, f"gate: move_missing side={side} bucket={bucket_ts}")
                time.sleep(args.poll_sec)
                continue
            signed_move = move_usd if side == "UP" else -move_usd
            velocity = asset_velocity(args.asset_velocity_window_sec)
            signed_velocity = None if velocity is None else (velocity if side == "UP" else -velocity)
            if signed_move < args.asset_move_usd_min or signed_velocity is None or signed_velocity < args.asset_velocity_min_usd:
                signal_side = None
                signal_polls = 0
                update_state(status="WAITING", side=side)
                diag(
                    args,
                    "gate: move_or_velocity "
                    f"side={side} signed_move={signed_move:.7f} need_move={args.asset_move_usd_min:.7f} "
                    f"signed_velocity={'n/a' if signed_velocity is None else f'{signed_velocity:.7f}'} "
                    f"need_velocity={args.asset_velocity_min_usd:.7f}",
                )
                time.sleep(args.poll_sec)
                continue
            signal_polls = signal_polls + 1 if signal_side == side else 1
            signal_side = side
            if signal_polls < 2:
                update_state(status="WAITING", side=side)
                diag(args, f"gate: confirming side={side} signal_polls={signal_polls}/2")
                time.sleep(args.poll_sec)
                continue
            shares, notional, forced_minimum, capped_stake = compute_order_size(args.stake_usd, ask_price, max_stake_usd=args.max_stake_usd)
            token_id = up_token if side == "UP" else down_token
            report["decision"] = {
                "ts": ts_utc(),
                "market_slug": slug,
                "side": side,
                "token_id": token_id,
                "entry_price": ask_price,
                "asset_move_usd": move_usd,
                "required_asset_move_usd": args.asset_move_usd_min,
                "signal_polls": signal_polls,
                "ask_size": ask_size,
                "estimated_shares": shares,
                "estimated_notional_usd": notional,
                "stake_capped": capped_stake,
                "exchange_minimum_forced": forced_minimum,
            }
            update_state(status="ENTERING", side=side)
            event(f"entry candidate: {side} ask={ask_price:.4f} spread={ask_price - (books['up']['bid'] if side == 'UP' else books['down']['bid'] or 0):.4f} {COIN_CFG['coin'].lower()}=${PRICE_FEED.get_price(COIN_CFG['coin']):.4f} buy=${notional:.4f}")
            report["tape_header"] = trade_block_header(slug, side)
            if args.paper_trade and not args.execute:
                actual_shares = shares
                fill_price = ask_price
                post = {"paper": True}
                diag(args, f"paper_trade: synthetic buy armed token={token_id} shares={shares} price={ask_price}")
            elif not args.execute:
                log("dry run: would buy, not submitting order")
                report["result"] = "dry_run_ready"
                break
            else:
                client = auth_client()
                post = None
                for buy_attempt in range(1, 4):
                    event(f"submitting BUY {side} token={token_id} amount=${notional:.4f}" + (f" (attempt {buy_attempt}/3)" if buy_attempt > 1 else ""))
                    try:
                        post = create_market_buy(client, token_id, notional)
                        break
                    except Exception as buy_exc:
                        event(f"buy attempt {buy_attempt}/3 failed: {buy_exc}")
                        if buy_attempt < 3:
                            time.sleep(1)
                if post is None:
                    raise RuntimeError("buy failed after 3 attempts")
                taking = float(post.get("takingAmount") or 0)
                making = float(post.get("makingAmount") or 0)
                actual_shares = taking or shares
                fill_price = round(taking / making, 6) if taking > 0 and making > 0 else ask_price
            opened = {
                "opened_at": ts_utc(),
                "market_slug": slug,
                "market_end_iso": end_iso,
                "side": side,
                "token_id": token_id,
                "entry_price": ask_price,
                "fill_price": fill_price,
                "actual_shares": actual_shares,
                "estimated_shares": shares,
                "estimated_notional_usd": notional,
                "order_post_result": post,
            }
            report["opened"] = opened
            update_state(status="IN_TRADE", side=side, shares=float(actual_shares), avg_price=float(fill_price or ask_price), pnl=0.0)
            event(f"BUY {side} @ ${ask_price:.4f} amount=${notional:.4f} shares={actual_shares:.6g} move=${move_usd:.7f}")
            log("buy submitted; monitoring exit")
            break
        except Exception as exc:
            report["attempts"].append({"ts": ts_utc(), "status": "error", "error": str(exc)})
            log(f"error: {exc}")
            if args.once:
                report["result"] = "error"
                break
            time.sleep(args.poll_sec)
    if not opened:
        report.setdefault("result", "no_entry_timeout")
        update_state(status="WAITING", shares=0.0, side="")
        log(f"finished: {report['result']}")
        write_report(report)
        return report
    end_ts = dt.datetime.fromisoformat(opened["market_end_iso"].replace("Z", "+00:00")).timestamp()
    take_profit_value = opened["estimated_notional_usd"] * (1.0 + args.take_profit_pct)
    stop_loss_value = opened["estimated_notional_usd"] * (1.0 - args.stop_loss_pct)
    close_reason = None
    ride_settlement = False
    latest_books = None
    peak_sell_now = 0.0
    trail_activated = False
    try:
        while True:
            now = time.time()
            seconds_left = max(0.0, end_ts - now)
            if now >= end_ts:
                close_reason = "settlement_wait_no_tp_or_sl"
                ride_settlement = True
                break
            latest_books = clob_token_prices(opened["token_id"])
            current_bid = latest_books["bid"]
            held_shares = opened["actual_shares"] or opened["estimated_shares"]
            sell_now_value = None if current_bid is None else held_shares * current_bid
            try:
                bucket_ts = int(str(opened.get("market_slug", "")).rsplit("-", 1)[-1])
            except Exception:
                bucket_ts = 0
            current_move = asset_move(bucket_ts) if bucket_ts else None
            if sell_now_value is not None and sell_now_value > peak_sell_now:
                peak_sell_now = sell_now_value
            log(
                f"monitor: bid={current_bid} "
                f"sell_now={'n/a' if sell_now_value is None else f'{sell_now_value:.4f}'} "
                f"peak={peak_sell_now:.4f} "
                f"move={'n/a' if current_move is None else f'${current_move:+.7f}'} "
                f"stop_usd=${stop_loss_value:.4f} tp_usd=${take_profit_value:.4f} "
                f"{seconds_left:.0f}s left"
            )
            if sell_now_value is not None:
                if args.max_bid_exit and current_bid is not None and current_bid >= args.max_bid_exit and sell_now_value > opened["estimated_notional_usd"]:
                    close_reason = f"max_bid_exit_bid_{current_bid:.4f}"
                    break
                if args.profit_exit_seconds_left and seconds_left <= args.profit_exit_seconds_left and sell_now_value > opened["estimated_notional_usd"]:
                    close_reason = f"profit_exit_{args.profit_exit_seconds_left:g}s_left"
                    break
                if sell_now_value >= take_profit_value:
                    trail_activated = True
                if trail_activated:
                    trail_floor = peak_sell_now * (1.0 - args.trail_pct)
                    if sell_now_value <= trail_floor:
                        time.sleep(DEFAULT_CONFIRM_GAP_SEC)
                        try:
                            fresh_books = clob_token_prices(opened["token_id"])
                        except Exception:
                            fresh_books = None
                        fresh_bid = fresh_books["bid"] if fresh_books else None
                        fresh_sell_now = None if fresh_bid is None else held_shares * fresh_bid
                        if fresh_sell_now is not None and fresh_sell_now > trail_floor:
                            event(f"trailing_tp rejected: stale tick (1st={sell_now_value:.4f} 2nd={fresh_sell_now:.4f} floor={trail_floor:.4f})")
                            time.sleep(args.poll_sec)
                            continue
                        if fresh_sell_now is not None and fresh_sell_now < opened["estimated_notional_usd"]:
                            latest_books = fresh_books or latest_books
                            log(f"trailing_tp book crashed in loss (sell_now={fresh_sell_now:.4f} floor={trail_floor:.4f}); continuing to monitor")
                            time.sleep(args.poll_sec)
                            continue
                        latest_books = fresh_books or latest_books
                        close_reason = f"trailing_tp_peak_{peak_sell_now:.4f}_floor_{trail_floor:.4f}"
                        break
                if sell_now_value <= stop_loss_value and seconds_left > 5:
                    time.sleep(DEFAULT_CONFIRM_GAP_SEC)
                    try:
                        fresh = clob_token_prices(opened["token_id"])
                    except Exception:
                        fresh = None
                    fresh_bid = fresh["bid"] if fresh else None
                    fresh_sell_now = None if fresh_bid is None else held_shares * fresh_bid
                    if fresh_sell_now is not None and fresh_sell_now > stop_loss_value:
                        event(f"stop_loss rejected: stale tick (1st={sell_now_value:.4f} 2nd={fresh_sell_now:.4f} stop={stop_loss_value:.4f})")
                        time.sleep(args.poll_sec)
                        continue
                    if fresh_bid is not None and fresh_bid < args.stop_loss_bid:
                        log(f"stop_loss skipped: bid {fresh_bid:.4f} below floor {args.stop_loss_bid:.4f}; continuing to monitor")
                        time.sleep(args.poll_sec)
                        continue
                    if not thesis_allows_exit(opened, _exit_gate_value("stop_loss_move_gate", 5.0)):
                        time.sleep(args.poll_sec)
                        continue
                    latest_books = fresh or latest_books
                    close_reason = f"stop_loss_sell_now_{stop_loss_value:g}"
                    break
            force_exit_seconds_left = int(COIN_CFG.get("force_exit_seconds_left", 5))
            if force_exit_seconds_left > 0 and seconds_left <= force_exit_seconds_left:
                if thesis_allows_exit(opened, _exit_gate_value("force_exit_move_gate", 5.0)):
                    close_reason = f"force_exit_in_loss_{force_exit_seconds_left}s_left"
                else:
                    close_reason = f"settlement_wait_thesis_alive_{force_exit_seconds_left}s"
                    ride_settlement = True
                break
            time.sleep(args.poll_sec)
    except KeyboardInterrupt:
        close_reason = "ctrl_c_emergency_exit"
    report["close_reason"] = close_reason
    update_state(status="SELLING", side=opened.get("side", ""))
    event(f"closing: {close_reason}")
    if ride_settlement:
        report["closed"] = settlement_close(ts_utc(), close_reason, opened["estimated_shares"], latest_books)
        report["result"] = "settlement_pending"
        wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
        losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
        update_state(
            status="WAITING",
            shares=0.0,
            pnl=0.0,
            side=opened.get("side", ""),
            wins=wins + 1,
            losses=losses,
        )
        write_report(report)
        return report
    if args.paper_trade and not args.execute:
        paper_bid = (latest_books or {}).get("bid")
        if paper_bid is None or paper_bid <= 0:
            report["closed"] = settlement_close(ts_utc(), "paper_no_bid_ride_settlement", opened["estimated_shares"], latest_books)
            report["result"] = "settlement_pending"
            wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
            losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
            update_state(status="WAITING", shares=0.0, pnl=0.0, side=opened.get("side", ""), wins=wins + 1, losses=losses)
            write_report(report)
            return report
        if paper_bid < args.stop_loss_bid:
            event(f"pre-close bid {paper_bid:.4f} below floor {args.stop_loss_bid:.4f}; riding settlement instead of selling into dead book")
            report["closed"] = settlement_close(ts_utc(), f"settlement_wait_pre_close_bid_floor_{paper_bid:.4f}", opened["estimated_shares"], latest_books)
            report["result"] = "settlement_pending"
            wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
            losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
            update_state(status="WAITING", shares=0.0, pnl=0.0, side=opened.get("side", ""), wins=wins + 1, losses=losses)
            write_report(report)
            return report
        taking_amount = round(float(opened["actual_shares"]) * float(paper_bid), 6)
        close_post = {"takingAmount": taking_amount, "paper": True}
        ladder_method = "paper_ws_bid"
        amount_shares = float(opened["actual_shares"])
    else:
        pre_close_bid = (latest_books or {}).get("bid")
        if pre_close_bid is not None and pre_close_bid < args.stop_loss_bid:
            event(f"pre-close bid {pre_close_bid:.4f} below floor {args.stop_loss_bid:.4f}; riding settlement instead of selling into dead book")
            report["closed"] = settlement_close(ts_utc(), f"settlement_wait_pre_close_bid_floor_{pre_close_bid:.4f}", opened["estimated_shares"], latest_books)
            report["result"] = "settlement_pending"
            wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
            losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
            update_state(status="WAITING", shares=0.0, pnl=0.0, side=opened.get("side", ""), wins=wins + 1, losses=losses)
            write_report(report)
            return report
        client = auth_client()
        close_post, ladder_method, amount_shares = close_with_ladder(client, opened, args.close_retry_max)
        if close_post is None:
            report["closed"] = settlement_close(ts_utc(), "ladder_exhausted_ride_settlement", amount_shares, latest_books)
            report["result"] = "settlement_pending"
            wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
            losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
            update_state(status="WAITING", shares=0.0, pnl=0.0, side=opened.get("side", ""), wins=wins + 1, losses=losses)
            write_report(report)
            return report
    report["closed"] = {"closed_at": ts_utc(), "close_shares": amount_shares, "remaining_shares": 0.0, "exit_book": latest_books, "close_post_result": close_post, "close_method": ladder_method, "peak_sell_now": peak_sell_now}
    report["result"] = "done"
    report["pnl_usd"] = round(float(close_post.get("takingAmount") or 0) - opened["estimated_notional_usd"], 6)
    pnl_usd = report["pnl_usd"]
    wins = int(STATE_REF.get("wins", 0)) if STATE_REF is not None else 0
    losses = int(STATE_REF.get("losses", 0)) if STATE_REF is not None else 0
    update_state(
        status="WAITING",
        shares=0.0,
        pnl=0.0,
        side=opened.get("side", ""),
        wins=wins + (1 if pnl_usd > 0 else 0),
        losses=losses + (1 if pnl_usd <= 0 else 0),
    )
    if TRADE_COUNT_FN:
        try:
            TRADE_COUNT_FN()
        except Exception:
            pass
    write_report(report)
    return report


def _load_coin_config(coin: str, argv: Optional[list[str]] = None) -> dict[str, Any]:
    base_path = ALL_IN_ONE_DIR / "configs" / "coins.json"
    config_path = _preparse_path_arg(argv, "--config-file", base_path)
    with base_path.open("r", encoding="utf-8") as fh:
        configs = json.load(fh)
    if config_path != base_path:
        with config_path.open("r", encoding="utf-8") as fh:
            overrides = json.load(fh)
        configs = {
            coin_key: {**configs.get(coin_key, {}), **override}
            for coin_key, override in overrides.items()
        }
    cfg = dict(configs[coin])
    cfg["coin"] = coin
    cfg["log_dir"] = str(ROOT_DIR / f"logs-{coin.lower()}") if coin != "BTC" else str(ROOT_DIR / "logs")
    return cfg


def _run_coin_internal(
    coin: str,
    argv: Optional[list[str]] = None,
    shared_pm_ws: Optional[MultiBookStream] = None,
    shared_price_feed: Optional[PriceFeed] = None,
    log_fn=None,
    state_ref=None,
    trade_count_fn=None,
) -> int:
    global COIN_CFG, LOG_FILE, VERBOSE, QUIET_TAPE, PM_WS, PRICE_FEED, UI_LOG_FN, STATE_REF, TRADE_COUNT_FN, LAST_DIAG_MESSAGE, LAST_DIAG_AT
    COIN_CFG = _load_coin_config(coin, argv)
    args = parse_args(argv)
    load_env_file(Path(args.env_file))
    VERBOSE = args.verbose
    UI_LOG_FN = log_fn
    STATE_REF = state_ref
    TRADE_COUNT_FN = trade_count_fn
    LAST_DIAG_MESSAGE = None
    LAST_DIAG_AT = 0.0
    log_dir = Path(args.log_dir)
    LOG_FILE = Path(args.log_file) if args.log_file else log_dir / f"{coin.lower()}_ws_{now_utc():%Y%m%d_%H%M%S}.log"
    log(f"started coin={coin} execute={args.execute} paper_trade={args.paper_trade} stake=${args.stake_usd:.4f} log_file={LOG_FILE}")
    own_feeds = shared_pm_ws is None or shared_price_feed is None
    PM_WS = shared_pm_ws or MultiBookStream()
    PRICE_FEED = shared_price_feed or PriceFeed()
    if own_feeds:
        market = resolve_active_current_5m_market()
        if market:
            _, _, up_token, down_token, _, _ = market_side_prices(market)
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.create_task(PM_WS.run_forever([up_token, down_token]))
            loop.create_task(PRICE_FEED.run_binance())
            loop.create_task(PRICE_FEED.run_bybit())
            thread = __import__("threading").Thread(target=loop.run_forever, daemon=True)
            thread.start()
            time.sleep(3)
        else:
            log("startup: no current market yet; runner will retry")
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.create_task(PM_WS.run_forever([]))
            loop.create_task(PRICE_FEED.run_binance())
            loop.create_task(PRICE_FEED.run_bybit())
            thread = __import__("threading").Thread(target=loop.run_forever, daemon=True)
            thread.start()
            time.sleep(3)
    total_pnl = 0.0
    start_balance = current_balance_usd() if args.execute else None
    trades = 0
    cycle_no = 0
    traded_slugs: set = set()
    while True:
        cycle_no += 1
        QUIET_TAPE = False
        report = run_cycle(args, cycle_no, total_pnl, start_balance, traded_slugs)
        traded_slug = (report.get("opened") or {}).get("market_slug")
        if traded_slug:
            traded_slugs.add(traded_slug)
        if report.get("result") == "done":
            trades += 1
            total_pnl += float(report.get("pnl_usd") or 0.0)
        elif report.get("result") == "dry_run_ready":
            tape(report.get("tape_header", "dry run candidate"))
            tape("        dry run: would fill, no order submitted")
            tape("")
        if args.once:
            break
        if args.paper_trade and report.get("result") in {"done", "settlement_pending"}:
            break
        if not args.execute and report.get("result") == "dry_run_ready":
            break
        if args.max_trades and trades >= args.max_trades:
            break
        if total_pnl <= -abs(args.max_session_loss_usd):
            tape(f"session stop: max loss ${abs(args.max_session_loss_usd):.4f} hit")
            break
        if report.get("result") == "open_but_close_failed":
            break
        QUIET_TAPE = True
        time.sleep(args.poll_sec)
    if own_feeds and PM_WS is not None:
        PM_WS.stop()
    if own_feeds and PRICE_FEED is not None:
        PRICE_FEED.stop()
    return 0


def run_coin(coin: str, argv: Optional[list[str]] = None) -> int:
    return _run_coin_internal(coin, argv)


def run_coin_with_shared_feeds(
    coin: str,
    pm_ws: MultiBookStream,
    price_feed: PriceFeed,
    argv: Optional[list[str]] = None,
    log_fn=None,
    state_ref=None,
    trade_count_fn=None,
) -> int:
    return _run_coin_internal(
        coin,
        argv,
        shared_pm_ws=pm_ws,
        shared_price_feed=price_feed,
        log_fn=log_fn,
        state_ref=state_ref,
        trade_count_fn=trade_count_fn,
    )
