#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import requests
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgsV2,
    OrderArgsV2,
    OrderType,
    PartialCreateOrderOptions,
)

UTC = dt.timezone.utc
LOCAL_TZ = dt.timezone(dt.timedelta(hours=7))
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_BASE_URL = "https://clob.polymarket.com"

# On-chain redemption (Polygon)
# ponytail: hardcoded Polymarket contract addresses — change only if Polymarket upgrades contracts
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPC_DEFAULT = "https://polygon-rpc.com"

CTF_ABI = [
    {"name": "isApprovedForAll", "type": "function", "stateMutability": "view",
     "inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}],
     "outputs": [{"name": "", "type": "bool"}]},
    {"name": "setApprovalForAll", "type": "function", "stateMutability": "nonethable",
     "inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "outputs": []},
]
ADAPTER_ABI = [
    {"name": "redeemPositions", "type": "function", "stateMutability": "nonethable",
     "inputs": [
         {"name": "collateralToken", "type": "address"},
         {"name": "parentCollectionId", "type": "bytes32"},
         {"name": "conditionId", "type": "bytes32"},
         {"name": "indexSets", "type": "uint256[]"},
     ],
     "outputs": []},
]
DEFAULT_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[1] / "logs-xrp"
DEFAULT_MIN_SHARES = 0.0
DEFAULT_MIN_BUY_USD = 1.0
DEFAULT_MAX_STAKE_USD = 1.0
DEFAULT_MAX_SPREAD = 0.08
DEFAULT_MIN_BOOK_DEPTH_USD = 1.0
DEFAULT_XRP_MOVE_USD_MIN = 0.00053
DEFAULT_MIN_SIGNAL_POLLS = 2
DEFAULT_STOP_LOSS_BID = 0.15
DEFAULT_STOP_LOSS_PCT = 0.67
DEFAULT_STOP_LOSS_MIN_SECONDS_LEFT = 45
DEFAULT_TAKE_PROFIT_PCT = 0.05
# sell_now-based exit/entry controls (sell_now = estimated_shares * current_bid)
DEFAULT_TAKE_PROFIT_USD = 1.07        # take profit once sell_now is up 7%
DEFAULT_STOP_LOSS_USD = 0.33          # stop loss once sell_now is down 70%
DEFAULT_MAX_ENTRY_SECONDS_LEFT = 120  # do not enter while more than 2m00s remain
DEFAULT_MAX_ENTRY_ASK = 0.95
DEFAULT_TIME_EXIT_MIN_SELL_USD = 1.0  # at time-exit, only sell if sell_now >= this, else ride settlement
# Trailing take-profit: once sell_now reaches +take_profit_pct, ride the winner and exit
# only when it falls trail_pct off its peak. Lets winners run instead of clipping +7%.
DEFAULT_TRAIL_PCT = 0.05
# Capture a near-certain win early: exit once best bid reaches this (payout maxes at $1.00).
DEFAULT_MAX_BID_EXIT = 0.99
# Entry freshness gate: XRP must keep moving in the trade direction over the recent window,
# filtering stale spikes that already stalled (prone to the reversals seen in logs).
DEFAULT_XRP_VELOCITY_WINDOW_SEC = 30
DEFAULT_XRP_VELOCITY_MIN_USD = 0.0003
# Exit execution ladder: FOK market sell -> FAK limit sell at best bid -> last-resort ride settlement.
DEFAULT_CLOSE_RETRY_MAX = 3
# Double-snapshot confirmation for SL/trailing-TP triggers. When a trigger
# is about to fire, read the book once more this many seconds later; only
# fire the exit if the second read agrees. Catches single-tick noise from
# the CLOB REST cache (see log: bid 0.77 entry -> 0.62 monitor 0s later).
DEFAULT_CONFIRM_GAP_SEC = 0.10

# Symbol prefix used everywhere we previously hardcoded "btc" — swap by changing this one constant.
ASSET_SYMBOL = "xrp"
BINANCE_SYMBOL = "XRPUSDT"
MARKET_SLUG_PREFIX = "xrp-updown-5m"


def now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def ts_utc() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


def now_local() -> dt.datetime:
    return dt.datetime.now(LOCAL_TZ)


def ts_local() -> str:
    return now_local().strftime("%Y-%m-%d/%H:%M:%S")


LOG_FILE: Optional[Path] = None
VERBOSE = False
QUIET_TAPE = False


def color_log(line: str) -> str:
    lower = line.lower()
    if "session pnl:" in lower or " pnl=" in lower:
        try:
            value = float(line.rsplit("PnL=", 1)[-1].replace("$", "").strip())
        except Exception:
            value = 0.0
        color = "\033[92m" if value > 0 else "\033[91m" if value < 0 else "\033[94m"
        return f"{color}{line}\033[0m"
    colors = (
        ("error", "\033[91m"),
        ("failed", "\033[91m"),
        ("monitor:", "\033[95m"),
        ("stop", "\033[91m"),
        ("skip:", "\033[91m"),
        ("closing:", "\033[93m"),
        ("entry candidate", "\033[96m"),
        ("submitting", "\033[96m"),
        ("buy ", "\033[92m"),
        ("close submitted", "\033[92m"),
        ("matched", "\033[92m"),
        ("done", "\033[92m"),
        ("settlement", "\033[95m"),
    )
    for needle, color in colors:
        if needle in lower:
            return f"{color}{line}\033[0m"
    return line


def _write_log(line: str) -> None:
    if not LOG_FILE:
        return
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError as e:
        print(f"[log write failed: {e}] {line}", flush=True)


def log(message: str) -> None:
    line = f"[{ts_local()}] {message}"
    if VERBOSE:
        print(color_log(line), flush=True)
    _write_log(line)


def event(message: str) -> None:
    line = f"[{ts_local()}] {message}"
    print(color_log(line), flush=True)
    _write_log(line)


def balance_event(start_balance: Optional[float], current_balance: Optional[float]) -> None:
    if start_balance is None or current_balance is None:
        log("balance: unavailable")
        return
    log(f"balance start=${start_balance:.2f} current=${current_balance:.2f} PnL={current_balance - start_balance:+.2f}")


def write_report(report: dict[str, Any]) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if VERBOSE:
        print(text)
    if LOG_FILE:
        try:
            LOG_FILE.with_suffix(".json").write_text(text + "\n", encoding="utf-8")
        except OSError as e:
            print(f"[report write failed: {e}]", flush=True)


def round_label(slug: str) -> str:
    try:
        return f"#{int(slug.rsplit('-', 1)[-1]) // 300}"
    except Exception:
        return slug or "unknown"


def tape(message: str = "") -> None:
    if QUIET_TAPE:
        return
    print(message, flush=True)
    _write_log(message)


def trade_block_header(slug: str, side: str) -> str:
    return f"[{now_local():%H:%M}] round {round_label(slug)}   ->   {side}"


def trade_block_fill(side: str, price: float, shares: float, close_post: Optional[dict[str, Any]], cost_usd: float = DEFAULT_MIN_BUY_USD) -> str:
    pnl = "n/a"
    if close_post:
        try:
            pnl_value = float(close_post.get("takingAmount") or 0) - cost_usd
            pnl = f"{pnl_value:+.2f}"
        except Exception:
            pass
    return f"        fill {side} @ ${price:.2f}   x{shares:g}    settles/exit submitted   {pnl}"


def trade_block_pnl(total_pnl: float) -> str:
    return f"        session PnL: {total_pnl:+.2f}"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def bucket_5m(ts: int) -> int:
    return ts - (ts % 300)


def fetch_event(slug: str) -> Optional[dict[str, Any]]:
    r = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=12)
    r.raise_for_status()
    arr = r.json()
    return arr[0] if arr else None


def resolve_active_current_5m_market() -> Optional[dict[str, Any]]:
    slug = f"{MARKET_SLUG_PREFIX}-{bucket_5m(int(time.time()))}"
    try:
        ev = fetch_event(slug)
    except Exception:
        return None
    if not ev:
        return None
    mkts = ev.get("markets") or []
    if not mkts:
        return None
    m = mkts[0]
    if m.get("closed") is True or m.get("active") is False:
        return None
    out = dict(m)
    out["_event_slug"] = slug
    return out


def parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def market_side_prices(market: dict[str, Any]) -> tuple[float, float, str, str, str, str]:
    outcomes = parse_json_field(market.get("outcomes")) or []
    prices = parse_json_field(market.get("outcomePrices")) or []
    token_ids = parse_json_field(market.get("clobTokenIds")) or []
    if len(prices) < 2 or len(token_ids) < 2:
        raise RuntimeError("missing outcomePrices/clobTokenIds")
    up_i, down_i = 0, 1
    labs = [str(x).lower() for x in outcomes[:2]] if isinstance(outcomes, list) else []
    if len(labs) >= 2 and ("up" in labs[1] or "yes" in labs[1]):
        up_i, down_i = 1, 0
    return (
        float(prices[up_i]),
        float(prices[down_i]),
        str(token_ids[up_i]),
        str(token_ids[down_i]),
        str(market.get("slug") or market.get("_event_slug") or ""),
        str(market.get("endDate") or market.get("endDateIso") or ""),
    )


def _best_bid_ask(book: Any) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if isinstance(book, dict):
        bids = book.get("bids") or []
        asks = book.get("asks") or []
    else:
        bids = getattr(book, "bids", []) or []
        asks = getattr(book, "asks", []) or []
    best_bid = best_bid_size = best_ask = best_ask_size = None
    for b in bids:
        price = float((b.get("price") if isinstance(b, dict) else getattr(b, "price", 0)) or 0)
        size = float((b.get("size") if isinstance(b, dict) else getattr(b, "size", 0)) or 0)
        if best_bid is None or price > best_bid:
            best_bid, best_bid_size = price, size
    for a in asks:
        price = float((a.get("price") if isinstance(a, dict) else getattr(a, "price", 0)) or 0)
        size = float((a.get("size") if isinstance(a, dict) else getattr(a, "size", 0)) or 0)
        if best_ask is None or price < best_ask:
            best_ask, best_ask_size = price, size
    return best_bid, best_bid_size, best_ask, best_ask_size


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
        payload = (client or auth_client()).get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
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


def clob_side_prices(up_token: str, down_token: str) -> dict[str, Any]:
    pub = public_client()
    up_book = pub.get_order_book(str(up_token))
    dn_book = pub.get_order_book(str(down_token))
    up_bid, up_bid_size, up_ask, up_ask_size = _best_bid_ask(up_book)
    dn_bid, dn_bid_size, dn_ask, dn_ask_size = _best_bid_ask(dn_book)
    return {
        "up": {"bid": up_bid, "bid_size": up_bid_size, "ask": up_ask, "ask_size": up_ask_size},
        "down": {"bid": dn_bid, "bid_size": dn_bid_size, "ask": dn_ask, "ask_size": dn_ask_size},
    }


def clob_token_prices(token: str) -> dict[str, Any]:
    bid, bid_size, ask, ask_size = _best_bid_ask(public_client().get_order_book(str(token)))
    return {"bid": bid, "bid_size": bid_size, "ask": ask, "ask_size": ask_size}


def _binance_price(ts_ms: Optional[int] = None) -> Optional[float]:
    """Binance XRPUSDT price at ts_ms (kline open), or spot now if ts_ms is None."""
    try:
        if ts_ms is None:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": BINANCE_SYMBOL},
                timeout=5,
            )
            return float(r.json()["price"]) if "price" in r.json() else None
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": BINANCE_SYMBOL, "interval": "1m", "startTime": ts_ms, "limit": 1},
            timeout=5,
        )
        arr = r.json()
        return float(arr[0][1]) if arr else None
    except Exception:
        return None


def xrp_move_usd(bucket_ts: int) -> Optional[float]:
    # ponytail: Binance spot is enough for a coarse 5m impulse gate; swap feed if basis matters.
    start = _binance_price(bucket_ts * 1000)
    current = _binance_price()
    if start is None or current is None:
        return None
    return current - start


def xrp_velocity_usd(window_sec: int) -> Optional[float]:
    # ponytail: short-window momentum. XRP change over the last `window_sec` seconds.
    # Used as a freshness gate so we don't enter on a spike that already stalled.
    now_ms = int(time.time() * 1000)
    start = _binance_price(now_ms - window_sec * 1000)
    current = _binance_price()
    if start is None or current is None:
        return None
    return current - start


def xrp_thesis_allows_exit(opened: dict[str, Any], threshold: float) -> bool:
    """
    Gate exits on the XRP move since bucket start (matches the 'move='
    field in the entry log, computed by xrp_move_usd(bucket_ts)).

    Returns True (allow the exit) if the move is < threshold in the trade
    direction OR in the opposite direction. Returns False (block the exit,
    caller should ride to settlement) if the move is >= threshold in the
    trade direction — i.e. the trade thesis is still alive.

    Returns True (allow exit) if XRP data is missing — never block on
    missing data, that's a separate failure mode.
    """
    side = opened.get("side")
    market_slug = opened.get("market_slug", "")
    if not side or not market_slug:
        return True
    # bucket_ts is the trailing number in "xrp-updown-5m-<bucket_ts>".
    try:
        bucket_ts = int(market_slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return True
    current_move = xrp_move_usd(bucket_ts)
    if current_move is None:
        return True
    # For UP: positive = in direction. For DOWN: negative = in direction.
    signed_in_direction = current_move if side == "UP" else -current_move
    return signed_in_direction < threshold


# Module-level cache for the monitor-line xrp_move display. Refreshes every
# 5 seconds to keep Binance REST load low at 0.1s poll cadence.
_xrp_move_display_cache = {"bucket_ts": None, "move": None, "fetched_at": 0.0}
_XRP_MOVE_DISPLAY_TTL_SEC = 5.0


def _get_xrp_move_for_display(bucket_ts: int) -> Optional[float]:
    """Cached xrp_move_usd for the monitor-line display. 5-second TTL."""
    now = time.time()
    if (_xrp_move_display_cache["bucket_ts"] == bucket_ts
            and now - _xrp_move_display_cache["fetched_at"] < _XRP_MOVE_DISPLAY_TTL_SEC):
        return _xrp_move_display_cache["move"]
    move = xrp_move_usd(bucket_ts)
    _xrp_move_display_cache["bucket_ts"] = bucket_ts
    _xrp_move_display_cache["move"] = move
    _xrp_move_display_cache["fetched_at"] = now
    return move


def env_health() -> dict[str, bool]:
    return {
        "PM_PRIVATE_KEY": bool(os.getenv("PM_PRIVATE_KEY")),
        "PM_API_KEY": bool(os.getenv("PM_API_KEY")),
        "PM_API_SECRET": bool(os.getenv("PM_API_SECRET")),
        "PM_API_PASSPHRASE": bool(os.getenv("PM_API_PASSPHRASE")),
        "PM_FUNDER_OR_ADDRESS": bool(os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS")),
    }


def pick_side(
    threshold: float,
    books: dict[str, Any],
    max_spread: float,
    min_book_depth_usd: float,
) -> tuple[Optional[tuple[str, float, float]], list[dict[str, Any]]]:
    candidates: list[tuple[str, float, float]] = []
    skips: list[dict[str, Any]] = []
    for side_name, payload in (("UP", books["up"]), ("DOWN", books["down"])):
        bid = payload.get("bid")
        ask = payload.get("ask")
        ask_size = float(payload.get("ask_size") or 0.0)
        spread = None if bid is None or ask is None else float(ask) - float(bid)
        ask_depth_usd = (float(ask) * ask_size) if ask is not None else 0.0
        skip_reasons = []
        if ask is None:
            skip_reasons.append("no_ask")
        elif float(ask) < threshold:
            skip_reasons.append("below_threshold")
        if spread is None:
            skip_reasons.append("no_spread")
        elif spread > max_spread:
            skip_reasons.append("spread_too_wide")
        if ask_depth_usd < min_book_depth_usd:
            skip_reasons.append("thin_book")
        if skip_reasons:
            skips.append(
                {
                    "side": side_name,
                    "bid": bid,
                    "ask": ask,
                    "spread": spread,
                    "ask_depth_usd": round(ask_depth_usd, 6),
                    "reasons": skip_reasons,
                }
            )
            continue
        candidates.append((side_name, float(ask), ask_size))
    if not candidates:
        return None, skips
    return sorted(candidates, key=lambda item: (item[1], item[2]), reverse=True)[0], skips


def compute_order_size(
    target_usd: float,
    price: float,
    min_shares: float = DEFAULT_MIN_SHARES,
    min_buy_usd: float = DEFAULT_MIN_BUY_USD,
    max_stake_usd: float = DEFAULT_MAX_STAKE_USD,
) -> tuple[float, float, bool, bool]:
    stake_usd = min(max(target_usd, min_buy_usd), max_stake_usd)
    raw_shares = (stake_usd / price) if price > 0 else 0.0
    shares = max(min_shares, raw_shares)
    shares = round(shares, 4)
    notional = round(shares * price, 2)
    forced_minimum = shares > raw_shares + 1e-9
    capped_stake = stake_usd < target_usd
    return shares, notional, forced_minimum, capped_stake


def create_market_buy(client: ClobClient, token_id: str, amount_usd: float) -> dict[str, Any]:
    order = client.create_market_order(
        MarketOrderArgsV2(
            token_id=str(token_id),
            amount=float(amount_usd),
            side="BUY",
            order_type=OrderType.FOK,
        )
    )
    return client.post_order(order, OrderType.FOK)


def find_last_trade_for_token(client: ClobClient, token_id: str) -> Optional[dict[str, Any]]:
    try:
        trades = client.get_trades()
    except Exception:
        return None
    for trade in trades:
        asset = str(trade.get("asset_id") or trade.get("token_id") or "")
        if asset == str(token_id):
            return trade
    return None


def create_market_sell(client: ClobClient, token_id: str, amount_shares: float) -> dict[str, Any]:
    order = client.create_market_order(
        MarketOrderArgsV2(
            token_id=str(token_id),
            amount=float(amount_shares),
            side="SELL",
            order_type=OrderType.FOK,
        )
    )
    return client.post_order(order, OrderType.FOK)


def _tick_for_price(price: float) -> str:
    # ponytail: Polymarket tick size bucket — pick the coarsest that fits the price.
    if price >= 0.1:
        return "0.01"
    if price >= 0.01:
        return "0.001"
    return "0.0001"


def create_limit_sell(
    client: ClobClient, token_id: str, amount_shares: float, price: float
) -> dict[str, Any]:
    # FAK (fill-and-kill): partial fills allowed, remainder cancelled. Survives thin books
    # where a pure FOK would die — exactly the failure mode seen in the logs.
    order = client.create_order(
        OrderArgsV2(token_id=str(token_id), price=float(price), size=float(amount_shares), side="SELL"),
        PartialCreateOrderOptions(tick_size=_tick_for_price(price), neg_risk=True),
    )
    return client.post_order(order, OrderType.FAK)


def resolve_sell_shares(client: ClobClient, opened: dict[str, Any]) -> float:
    # Use the actual filled size from the last trade if available; fall back to estimate.
    # Trust the local fill first; only fall back to estimate if no fill was recorded.
    amount = float(opened.get("actual_shares") or opened.get("estimated_shares") or 0.0)
    last_trade = find_last_trade_for_token(client, opened["token_id"])
    if last_trade:
        for key in ("size", "amount", "maker_orders_size", "taker_orders_size"):
            if key in last_trade and last_trade[key]:
                try:
                    remote = float(last_trade[key])
                    # Clamp to actual fill — never sell more than we have.
                    if opened.get("actual_shares"):
                        remote = min(remote, float(opened["actual_shares"]))
                    amount = remote
                    break
                except Exception:
                    pass
    return round(amount, 6)


def close_with_ladder(
    client: ClobClient,
    opened: dict[str, Any],
    retry_max: int,
) -> tuple[Optional[dict[str, Any]], str, float]:
    # Exit ladder: chunked FOKs only. FAK retries were dropped because
    # py-clob-client-v2 hits a "POLY_1271 signature does not match order
    # hash" error on FAK limit sells that follow a failed FOK (library
    # quirk, not our code). Halving the chunk on each retry works around
    # thin books at the top-of-book without needing the FAK fallback.
    token_id = opened["token_id"]
    amount_shares = resolve_sell_shares(client, opened)
    last_err = "no_attempt"
    sell_shares = amount_shares
    for attempt in range(1, retry_max + 1):
        book = clob_token_prices(token_id)
        bid = book.get("bid")
        bid_size = float(book.get("bid_size") or 0.0)
        if bid is None:
            last_err = f"no_bid_attempt_{attempt}"
            # sell_shares = sell_shares / 2
            time.sleep(0.3)
            continue
        # Cap chunk to current bid depth; shrinks on retry.
        chunk = round(min(sell_shares, bid_size if bid_size > 0 else sell_shares), 6)
        if chunk <= 0:
            last_err = f"zero_chunk_attempt_{attempt}"
            # sell_shares = sell_shares / 2
            time.sleep(0.3)
            continue
        try:
            post = create_market_sell(client, token_id, chunk)
            return post, f"filled_attempt_{attempt}_fok_chunk_{chunk}", sell_shares
        except Exception as exc:
            last_err = f"attempt_{attempt}_fok_failed: {exc}"
            log(f"close ladder {attempt}/{retry_max}: {exc}")
            # sell_shares = sell_shares / 2
            time.sleep(0.3)
    return None, last_err, amount_shares


def settlement_close(closed_at: str, reason: str, shares: float, book: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    return {
        "closed_at": closed_at,
        "close_shares": 0.0,
        "remaining_shares": shares,
        "close_skipped": reason,
        "last_book": book,
    }


def xrp_move_min_for_threshold(threshold: float) -> float:
    # ponytail: XRP scaled from BNB by price ratio using XRP=$1.0490 and BNB=$557.5.
    # Tune these numbers from your own logs; the table is the default you can edit.
    table = {
        0.60: 0.0032,
        0.61: 0.0032,
        0.62: 0.0032,
        0.63: 0.0030,
        0.64: 0.0030,
        0.65: 0.0030,
        0.66: 0.0028,
        0.67: 0.0028,
        0.68: 0.0028,
        0.69: 0.0026,
        0.70: 0.0026,
        0.71: 0.0026,
        0.72: 0.0024,
        0.73: 0.0024,
        0.74: 0.0024,
        0.75: 0.0022,
        0.76: 0.0022,
        0.77: 0.0022,
        0.78: 0.0020,
        0.79: 0.0020,
        0.80: 0.0020,
        0.81: 0.0018,
        0.82: 0.0018,
        0.83: 0.0018,
        0.84: 0.0016,
        0.85: 0.0016,
        0.86: 0.0016,
        0.87: 0.0014,
        0.88: 0.0014,
        0.89: 0.0014,
        0.90: 0.0012,
        0.91: 0.0012,
        0.92: 0.0012,
        0.93: 0.0010,
        0.94: 0.0010,
        0.95: 0.0010,
    }
    return table.get(round(float(threshold), 2), DEFAULT_XRP_MOVE_USD_MIN)


def xrp_move_min_for_price(price: float) -> float:
    for threshold in (0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.65, 0.60):
        if price >= threshold:
            return xrp_move_min_for_threshold(threshold)
    return DEFAULT_XRP_MOVE_USD_MIN


PROFILES: dict[str, dict[str, Any]] = {
    "conservative": {
        "threshold": 0.60,
        "stake_usd": 2.0,
        "max_stake_usd": 2.0,
        "max_spread": 0.05,
        "min_book_depth_usd": 1.0,
        "stop_loss_pct": DEFAULT_STOP_LOSS_PCT,
        "take_profit_pct": DEFAULT_TAKE_PROFIT_PCT,
        "take_profit_usd": DEFAULT_TAKE_PROFIT_USD,
        "stop_loss_usd": DEFAULT_STOP_LOSS_USD,
        "time_exit_min_sell_usd": DEFAULT_TIME_EXIT_MIN_SELL_USD,
        "min_signal_polls": DEFAULT_MIN_SIGNAL_POLLS,
        "stop_loss_bid": DEFAULT_STOP_LOSS_BID,
        "stop_loss_min_seconds_left": DEFAULT_STOP_LOSS_MIN_SECONDS_LEFT,
        "exit_before_sec": 20,
        "min_entry_seconds_left": 30,
        "max_entry_seconds_left": DEFAULT_MAX_ENTRY_SECONDS_LEFT,
        "max_entry_ask": DEFAULT_MAX_ENTRY_ASK,
        "entry_timeout_min": 60,
        "poll_sec": 1.0,
        "trail_pct": DEFAULT_TRAIL_PCT,
        "max_bid_exit": DEFAULT_MAX_BID_EXIT,
        "xrp_velocity_window_sec": DEFAULT_XRP_VELOCITY_WINDOW_SEC,
        "xrp_velocity_min_usd": DEFAULT_XRP_VELOCITY_MIN_USD,
        "close_retry_max": DEFAULT_CLOSE_RETRY_MAX,
    },
    "aggressive": {
        "threshold": 0.60,
        "stake_usd": 2.0,
        "max_stake_usd": 2.0,
        "max_spread": 0.05,
        "min_book_depth_usd": 1.0,
        "stop_loss_pct": DEFAULT_STOP_LOSS_PCT,
        "take_profit_pct": DEFAULT_TAKE_PROFIT_PCT,
        "take_profit_usd": DEFAULT_TAKE_PROFIT_USD,
        "stop_loss_usd": DEFAULT_STOP_LOSS_USD,
        "time_exit_min_sell_usd": DEFAULT_TIME_EXIT_MIN_SELL_USD,
        "min_signal_polls": DEFAULT_MIN_SIGNAL_POLLS,
        "stop_loss_bid": DEFAULT_STOP_LOSS_BID,
        "stop_loss_min_seconds_left": DEFAULT_STOP_LOSS_MIN_SECONDS_LEFT,
        "exit_before_sec": 20,
        "min_entry_seconds_left": 30,
        "max_entry_seconds_left": DEFAULT_MAX_ENTRY_SECONDS_LEFT,
        "max_entry_ask": DEFAULT_MAX_ENTRY_ASK,
        "entry_timeout_min": 60,
        "poll_sec": 1.0,
        "trail_pct": DEFAULT_TRAIL_PCT,
        "max_bid_exit": DEFAULT_MAX_BID_EXIT,
        "xrp_velocity_window_sec": DEFAULT_XRP_VELOCITY_WINDOW_SEC,
        "xrp_velocity_min_usd": DEFAULT_XRP_VELOCITY_MIN_USD,
        "close_retry_max": DEFAULT_CLOSE_RETRY_MAX,
    },
}


def apply_profile(args: argparse.Namespace) -> argparse.Namespace:
    prof = PROFILES.get(args.profile or "conservative", PROFILES["conservative"])
    for key, value in prof.items():
        attr = key.replace("-", "_")
        if getattr(args, attr, None) is None:
            setattr(args, attr, value)
    if args.xrp_move_usd_min is None:
        args.xrp_move_usd_min = xrp_move_min_for_threshold(args.threshold)
    return args


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=["conservative", "aggressive"], default="conservative")
    ap.add_argument("--threshold", type=float, default=None)
    ap.add_argument("--stake-usd", type=float, default=None)
    ap.add_argument("--max-stake-usd", type=float, default=None)
    ap.add_argument("--max-spread", type=float, default=None)
    ap.add_argument("--min-book-depth-usd", type=float, default=None)
    ap.add_argument("--stop-loss-pct", type=float, default=None)
    ap.add_argument("--take-profit-pct", type=float, default=None)
    ap.add_argument("--take-profit-usd", type=float, default=None, help="take profit when sell_now >= this")
    ap.add_argument("--stop-loss-usd", type=float, default=None, help="stop loss when sell_now <= this")
    ap.add_argument("--time-exit-min-sell-usd", type=float, default=None, help="at time-exit, only sell if sell_now >= this, else ride settlement")
    ap.add_argument("--xrp-move-usd-min", type=float, default=None)
    ap.add_argument("--min-signal-polls", type=int, default=None)
    ap.add_argument("--stop-loss-bid", type=float, default=None)
    ap.add_argument("--stop-loss-min-seconds-left", type=int, default=None)
    ap.add_argument("--exit-before-sec", type=int, default=None)
    ap.add_argument("--min-entry-seconds-left", type=int, default=None)
    ap.add_argument("--max-entry-seconds-left", type=int, default=None, help="do not enter while more seconds than this remain")
    ap.add_argument("--max-entry-ask", type=float, default=None, help="do not enter if best ask is above this")
    ap.add_argument("--entry-timeout-min", type=int, default=None)
    ap.add_argument("--poll-sec", type=float, default=None)
    ap.add_argument("--trail-pct", type=float, default=None,
                    help="once in profit, trail the winner and exit when sell_now falls this far off its peak")
    ap.add_argument("--max-bid-exit", type=float, default=None,
                    help="exit immediately once best bid reaches this (capture a near-$1 win)")
    ap.add_argument("--xrp-velocity-window-sec", type=int, default=None,
                    help="freshness-gate window for XRP momentum")
    ap.add_argument("--xrp-velocity-min-usd", type=float, default=None,
                    help="min XRP move required over the velocity window to enter")
    ap.add_argument("--close-retry-max", type=int, default=None,
                    help="max exit-ladder attempts (FOK then FAK at bid) before riding settlement")
    ap.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    ap.add_argument("--log-file", default=None)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--max-trades", type=int, default=0, help="0 means run until stopped")
    ap.add_argument("--max-session-loss-usd", type=float, default=1.0)
    ap.add_argument("--target-balance", type=float, default=None,
                    help="stop when current balance >= this USD value")
    ap.add_argument("--verbose", action="store_true")
    # Manual settlement (on-chain redeem after market resolution)
    ap.add_argument("--redeem-slug", default=None,
                    help="redeem a resolved market by event slug, e.g. xrp-updown-5m-1782654900")
    ap.add_argument("--redeem-condition-id", default=None,
                    help="redeem a resolved market directly by conditionId (0x...)")
    args = ap.parse_args()
    args.xrp_move_usd_min_user_set = args.xrp_move_usd_min is not None
    return apply_profile(args)


def run_cycle(
    args: argparse.Namespace,
    cycle_no: int,
    session_pnl: float = 0.0,
    start_balance: Optional[float] = None,
    traded_slugs: Optional[set] = None,  # per-bucket lockout: slugs already traded this session
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "started_at": ts_utc(),
        "cycle_no": cycle_no,
        "params": {
            "profile": args.profile,
            "threshold": args.threshold,
            "stake_usd": args.stake_usd,
            "min_buy_usd": DEFAULT_MIN_BUY_USD,
            "max_stake_usd": args.max_stake_usd,
            "max_spread": args.max_spread,
            "min_book_depth_usd": args.min_book_depth_usd,
            "stop_loss_pct": args.stop_loss_pct,
            "take_profit_pct": args.take_profit_pct,
            "take_profit_usd": args.take_profit_usd,
            "stop_loss_usd": args.stop_loss_usd,
            "time_exit_min_sell_usd": args.time_exit_min_sell_usd,
            "max_entry_seconds_left": args.max_entry_seconds_left,
            "max_entry_ask": args.max_entry_ask,
            "xrp_move_usd_min": args.xrp_move_usd_min,
            "min_signal_polls": args.min_signal_polls,
            "stop_loss_bid": args.stop_loss_bid,
            "stop_loss_min_seconds_left": args.stop_loss_min_seconds_left,
            "exit_before_sec": args.exit_before_sec,
            "min_entry_seconds_left": args.min_entry_seconds_left,
            "entry_timeout_min": args.entry_timeout_min,
            "poll_sec": args.poll_sec,
            "execute": args.execute,
            "once": args.once,
            "start_balance_usd": start_balance,
        },
        "env_health": env_health(),
        "attempts": [],
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
                log(f"no current active XRP 5m market")
                if args.once:
                    report["result"] = "no_current_market"
                    break
                time.sleep(args.poll_sec)
                continue

            gamma_up, gamma_down, up_token, down_token, slug, end_iso = market_side_prices(market)
            end_ts = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
            seconds_left = max(0.0, end_ts - time.time())
            books = clob_side_prices(up_token, down_token)
            log(
                f"{slug} {seconds_left:.0f}s left "
                f"gamma UP={gamma_up:.3f} DOWN={gamma_down:.3f} "
                f"book UP bid/ask={books['up']['bid']}/{books['up']['ask']} "
                f"DOWN bid/ask={books['down']['bid']}/{books['down']['ask']}"
            )
            current_balance = current_balance_usd() if args.execute else None
            balance_event(start_balance, current_balance)
            if args.target_balance is not None and current_balance is not None and current_balance >= args.target_balance:
                tape(f"session stop: target balance ${args.target_balance:.2f} reached (current ${current_balance:.2f})")
                report["result"] = "target_balance_reached"
                break
            report["attempts"].append(
                {
                    "ts": ts_utc(),
                    "slug": slug,
                    "status": "heartbeat",
                    "gamma_up": gamma_up,
                    "gamma_down": gamma_down,
                    "seconds_left": seconds_left,
                    "books": books,
                }
            )

            # Per-bucket lockout: one trade per 5m bucket
            if traded_slugs is not None and slug in traded_slugs:
                log(f"skip: {seconds_left:.0f}s left already traded slug={slug} this session")
                if args.once:
                    report["result"] = "bucket_lockout"
                    break
                time.sleep(args.poll_sec)
                continue

            if seconds_left > args.max_entry_seconds_left:
                log(f"wait: {seconds_left:.0f}s left too early to enter ({seconds_left:.0f}s > {args.max_entry_seconds_left}s)")
                if args.once:
                    report["result"] = "too_early_to_enter"
                    break
                time.sleep(args.poll_sec)
                continue

            if seconds_left < args.min_entry_seconds_left:
                log(f"skip: {seconds_left:.0f}s left too late to enter ({seconds_left:.0f}s < {args.min_entry_seconds_left}s)")
                if args.once:
                    report["result"] = "too_late_to_enter"
                    break
                time.sleep(args.poll_sec)
                continue

            pick, skip_reasons = pick_side(args.threshold, books, args.max_spread, args.min_book_depth_usd)
            if not pick:
                signal_side = None
                signal_polls = 0
                report["last_skip_reasons"] = skip_reasons
                reasons = "; ".join(
                    f"{item['side']}={','.join(item['reasons'])}" for item in skip_reasons
                )
                log(f"skip: {seconds_left:.0f}s left no safe entry ({reasons})")
                if args.once:
                    report["result"] = "no_safe_entry"
                    break
                time.sleep(args.poll_sec)
                continue

            side, ask_price, ask_size = pick
            if args.execute and session_pnl <= -abs(args.max_session_loss_usd):
                tape(f"session stop: max loss ${abs(args.max_session_loss_usd):.2f} hit before new entry")
                report["result"] = "max_session_loss_hit"
                break
            if args.max_entry_ask is not None and ask_price > args.max_entry_ask:
                signal_side = None
                signal_polls = 0
                log(f"skip: {seconds_left:.0f}s left ask {ask_price:.3f} above max_entry_ask {args.max_entry_ask:.3f}")
                if args.once:
                    report["result"] = "ask_above_max_entry"
                    break
                time.sleep(args.poll_sec)
                continue
            event_slug = str(market.get("_event_slug") or slug)
            try:
                market_bucket_ts = int(event_slug.rsplit("-", 1)[-1])
            except Exception:
                market_bucket_ts = bucket_5m(int(time.time()))
            move_usd = xrp_move_usd(market_bucket_ts)
            required_move_usd = (
                args.xrp_move_usd_min
                if getattr(args, "xrp_move_usd_min_user_set", False)
                else xrp_move_min_for_price(ask_price)
            )
            report["last_xrp_move_usd"] = move_usd
            report["last_required_xrp_move_usd"] = required_move_usd
            if move_usd is None or (side == "UP" and move_usd < required_move_usd) or (
                side == "DOWN" and move_usd > -required_move_usd
            ):
                signal_side = None
                signal_polls = 0
                log(f"skip: {seconds_left:.0f}s left XRP move mismatch side={side} move={move_usd} min={required_move_usd}")
                if args.once:
                    report["result"] = "xrp_move_mismatch"
                    break
                time.sleep(args.poll_sec)
                continue

            # Freshness gate: XRP must still be moving NOW, not just at bucket start
            if args.xrp_velocity_min_usd and args.xrp_velocity_min_usd > 0:
                vel = xrp_velocity_usd(args.xrp_velocity_window_sec)
                vel_ok = vel is None or (
                    (side == "UP" and vel >= args.xrp_velocity_min_usd) or
                    (side == "DOWN" and vel <= -args.xrp_velocity_min_usd)
                )
                if not vel_ok:
                    signal_side = None
                    signal_polls = 0
                    log(f"skip: {seconds_left:.0f}s left XRP velocity stale side={side} vel={vel} min=+-{args.xrp_velocity_min_usd}")
                    if args.once:
                        report["result"] = "xrp_velocity_stale"
                        break
                    time.sleep(args.poll_sec)
                    continue

            signal_polls = signal_polls + 1 if signal_side == side else 1
            signal_side = side
            if signal_polls < args.min_signal_polls:
                log(f"skip: {seconds_left:.0f}s left waiting signal confirmation {side} {signal_polls}/{args.min_signal_polls}")
                if args.once:
                    report["result"] = "signal_not_confirmed"
                    break
                time.sleep(args.poll_sec)
                continue

            token_id = up_token if side == "UP" else down_token
            shares, notional, forced_minimum, capped_stake = compute_order_size(
                args.stake_usd,
                ask_price,
                max_stake_usd=args.max_stake_usd,
            )
            # Capture entry-time context for post-trade analysis
            entry_side_book = books[side.lower()]
            entry_bid = entry_side_book.get("bid")
            entry_spread = round(ask_price - entry_bid, 4) if entry_bid is not None else None
            entry_bid_size = entry_side_book.get("bid_size")
            entry_ask_size = entry_side_book.get("ask_size")
            # XRP spot price at entry moment
            xrp_price_at_entry = _binance_price()
            report["decision"] = {
                "ts": ts_utc(),
                "market_slug": slug,
                "side": side,
                "token_id": token_id,
                "entry_price": ask_price,
                "xrp_move_usd": move_usd,
                "required_xrp_move_usd": required_move_usd,
                "signal_polls": signal_polls,
                "ask_size": ask_size,
                "bid_size_at_entry": entry_bid_size,
                "ask_size_at_entry": entry_ask_size,
                "spread_at_entry": entry_spread,
                "xrp_price_at_entry": xrp_price_at_entry,
                "target_stake_usd": args.stake_usd,
                "max_stake_usd": args.max_stake_usd,
                "estimated_shares": shares,
                "estimated_notional_usd": notional,
                "stake_capped": capped_stake,
                "exchange_minimum_forced": forced_minimum,
                "minimum_buy_usd": DEFAULT_MIN_BUY_USD,
                "skipped_sides": skip_reasons,
            }
            event(f"entry candidate: {side} ask={ask_price:.3f} spread={entry_spread} xrp=${xrp_price_at_entry} buy=${notional:.2f}")
            report["tape_header"] = trade_block_header(slug, side)

            if not args.execute:
                log("dry run: would buy, not submitting order")
                report["result"] = "dry_run_ready"
                break

            client = auth_client()
            post = None
            for buy_attempt in range(1, 4):
                event(f"submitting BUY {side} token={token_id} amount=${notional:.2f}" + (f" (attempt {buy_attempt}/3)" if buy_attempt > 1 else ""))
                try:
                    post = create_market_buy(client, token_id, notional)
                    break
                except Exception as buy_exc:
                    event(f"buy attempt {buy_attempt}/3 failed: {buy_exc}")
                    if buy_attempt < 3:
                        time.sleep(1)
            if post is None:
                raise RuntimeError(f"buy failed after 3 attempts")
            # Extract actual fill data from Polymarket buy response
            fill_price = None
            actual_shares = None
            try:
                taking = float(post.get("takingAmount") or 0)
                making = float(post.get("makingAmount") or 0)
                if taking > 0:
                    actual_shares = taking  # shares actually received from Polymarket
                if making > 0 and taking > 0:
                    fill_price = round(taking / making, 6)
            except Exception:
                pass
            opened = {
                "opened_at": ts_utc(),
                "market_slug": slug,
                "market_end_iso": end_iso,
                "side": side,
                "token_id": token_id,
                "entry_price": ask_price,
                "fill_price": fill_price,
                "spread_at_entry": entry_spread,
                "bid_size_at_entry": entry_bid_size,
                "ask_size_at_entry": entry_ask_size,
                "xrp_price_at_entry": xrp_price_at_entry,
                "actual_shares": actual_shares,
                "estimated_shares": shares,
                "estimated_notional_usd": notional,
                "order_post_result": post,
            }
            report["opened"] = opened
            event(f"BUY {side} @ ${ask_price:.2f} amount=${notional:.2f} shares={actual_shares or shares:.6g} move=${move_usd:.2f}")
            event("buy submitted; monitoring exit")
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
        report["finished_at"] = ts_utc()
        log(f"finished: {report['result']}")
        write_report(report)
        return report

    end_ts = dt.datetime.fromisoformat(opened["market_end_iso"].replace("Z", "+00:00")).timestamp()
    # all exit decisions compare sell_now = estimated_shares * current_bid against buy cost
    take_profit_value = opened["estimated_notional_usd"] * (1.0 + args.take_profit_pct)
    stop_loss_value = opened["estimated_notional_usd"] * (1.0 - args.stop_loss_pct)
    report["take_profit_value"] = take_profit_value
    report["stop_loss_value"] = stop_loss_value
    close_reason = None
    ride_settlement = False
    latest_books = None
    peak_sell_now = 0.0       # trailing TP: highest sell_now seen since entry
    trail_activated = False   # only arm trailing TP after first TP threshold breach
    try:
        while True:
            now = time.time()
            seconds_left = max(0.0, end_ts - now)
            if now >= end_ts:
                close_reason = "settlement_wait_no_tp_or_sl"
                ride_settlement = True
                break
            try:
                latest_books = clob_token_prices(opened["token_id"])
            except Exception as net_exc:
                event(f"monitor network error (retrying): {net_exc}")
                time.sleep(args.poll_sec)
                continue
            current_bid = latest_books["bid"]
            held_shares = opened["actual_shares"] or opened["estimated_shares"]
            sell_now_value = None if current_bid is None else held_shares * current_bid
            report["last_check_at"] = ts_utc()
            report["last_bid_price"] = current_bid
            report["last_sell_now_value"] = sell_now_value

            # Track peak for trailing TP and post-trade reporting
            if sell_now_value is not None and sell_now_value > peak_sell_now:
                peak_sell_now = sell_now_value
            report["peak_sell_now"] = peak_sell_now

            sn_str = f"{sell_now_value:.2f}" if sell_now_value is not None else "n/a"
            bkt_disp = int(opened.get("market_slug", "0").rsplit("-", 1)[-1] or 0)
            xrp_move_disp = _get_xrp_move_for_display(bkt_disp) if bkt_disp else None
            xrp_move_str = f"{xrp_move_disp:+.5f}" if xrp_move_disp is not None else "n/a"
            event(
                f"monitor: bid={current_bid} sell_now={sn_str} "
                f"peak={peak_sell_now:.2f} "
                f"stop_usd=${stop_loss_value:.2f} tp_usd=${take_profit_value:.2f} "
                f"{seconds_left:.0f}s left xrp_move={xrp_move_str}"
            )

            if sell_now_value is not None:
                # max_bid_exit: snap profits when bid is nearly $1 (almost certain win)
                if (args.max_bid_exit and current_bid is not None and current_bid >= args.max_bid_exit
                        and sell_now_value > opened["estimated_notional_usd"]):
                    close_reason = f"max_bid_exit_bid_{current_bid:.3f}"
                    break

                # Trailing TP: arm once we hit take_profit_value, exit when we drop trail_pct off peak
                if sell_now_value >= take_profit_value:
                    trail_activated = True
                if trail_activated:
                    trail_floor = peak_sell_now * (1.0 - args.trail_pct)
                    if sell_now_value <= trail_floor:
                        # Double-snapshot confirmation: gap + re-read. If the
                        # second read disagrees (we're back above the floor),
                        # treat the first read as a stale tick and keep polling.
                        time.sleep(DEFAULT_CONFIRM_GAP_SEC)
                        try:
                            fresh_books = clob_token_prices(opened["token_id"])
                        except Exception:
                            fresh_books = None
                        fresh_bid = fresh_books["bid"] if fresh_books else None
                        held_shares = opened["actual_shares"] or opened["estimated_shares"]
                        fresh_sell_now = None if fresh_bid is None else held_shares * fresh_bid
                        if fresh_sell_now is not None and fresh_sell_now > trail_floor:
                            event(f"trailing_tp rejected: stale tick (1st={sell_now_value:.2f} 2nd={fresh_sell_now:.2f} floor={trail_floor:.2f})")
                            time.sleep(args.poll_sec)
                            continue
                        if fresh_sell_now is not None and fresh_sell_now < trail_floor * 0.90:
                            latest_books = fresh_books
                            if fresh_sell_now > opened["estimated_notional_usd"]:
                                # Book crashed but still profitable — sell now rather than risk settlement loss
                                close_reason = f"trailing_tp_book_crashed_profitable_{fresh_sell_now:.2f}_floor_{trail_floor:.2f}"
                                break
                            # Book crashed and already in loss — keep monitoring for recovery
                            log(f"trailing_tp book crashed in loss (sell_now={fresh_sell_now:.2f} floor={trail_floor:.2f}); continuing to monitor")
                            time.sleep(args.poll_sec)
                            continue
                        # XRP thesis gate: only fire TP if XRP move is below the configured threshold in direction
                        # trade direction. Otherwise keep monitoring — all other gates remain active.
                        if not xrp_thesis_allows_exit(opened, 0.0005):
                            bkt = int(opened.get("market_slug", "0").rsplit("-", 1)[-1] or 0)
                            cm = xrp_move_usd(bkt) if bkt else None
                            cm_str = f"${cm:+.2f}" if cm is not None else "n/a"
                            log(f"trailing_tp blocked: XRP move still in trade direction (current={cm_str}, threshold=$0.0005); continuing to monitor")
                            time.sleep(args.poll_sec)
                            continue
                        close_reason = f"trailing_tp_peak_{peak_sell_now:.2f}_floor_{trail_floor:.2f}"
                        break

                # Hard stop loss — disabled inside last 5s, ride settlement instead.
                # Double-snapshot confirmation: same gate as the trailing-TP block.
                if sell_now_value <= stop_loss_value and seconds_left > 5:
                    time.sleep(DEFAULT_CONFIRM_GAP_SEC)
                    try:
                        fresh = clob_token_prices(opened["token_id"])
                    except Exception:
                        fresh = None
                    fresh_bid = fresh["bid"] if fresh else None
                    held = opened["actual_shares"] or opened["estimated_shares"] or 0
                    fresh_sell_now = None if fresh_bid is None else held * fresh_bid
                    if fresh_sell_now is not None and fresh_sell_now > stop_loss_value:
                        event(f"stop_loss rejected: stale tick (1st={sell_now_value:.2f} 2nd={fresh_sell_now:.2f} stop={stop_loss_value:.2f})")
                        time.sleep(args.poll_sec)
                        continue
                    # Bid floor: book too thin to sell into — keep monitoring for recovery
                    if fresh_bid is not None and fresh_bid < args.stop_loss_bid:
                        log(f"stop_loss skipped: bid {fresh_bid:.3f} below floor {args.stop_loss_bid:.3f}; continuing to monitor")
                        time.sleep(args.poll_sec)
                        continue
                    # XRP thesis gate on SL: block if XRP still moving in trade direction
                    if not xrp_thesis_allows_exit(opened, 0.0003):
                        bkt = int(opened.get("market_slug", "0").rsplit("-", 1)[-1] or 0)
                        cm = xrp_move_usd(bkt) if bkt else None
                        cm_str = f"${cm:+.2f}" if cm is not None else "n/a"
                        log(f"stop_loss blocked: XRP move still in trade direction (current={cm_str}, threshold=$0.0003); continuing to monitor")
                        time.sleep(args.poll_sec)
                        continue
                    close_reason = f"stop_loss_sell_now_{stop_loss_value:g}"
                    break

            # At ~15s, only force-exit winners near max payout; otherwise keep polling.
            if seconds_left <= 15 and not ride_settlement:
                if (args.max_bid_exit and sell_now_value is not None and current_bid is not None
                        and current_bid >= args.max_bid_exit
                        and sell_now_value > opened["estimated_notional_usd"]):
                    # XRP thesis gate: only fire 15s force-exit if XRP move is
                    # < $10 in trade direction or opposite. Otherwise ride.
                    if not xrp_thesis_allows_exit(opened, 0.0005):
                        bkt = int(opened.get("market_slug", "0").rsplit("-", 1)[-1] or 0)
                        cm = xrp_move_usd(bkt) if bkt else None
                        cm_str = f"${cm:+.2f}" if cm is not None else "n/a"
                        log(f"15s force-exit blocked: XRP move still in trade direction (current={cm_str}, threshold=$0.0005); continuing to monitor")
                        time.sleep(args.poll_sec)
                        continue
                    close_reason = f"force_exit_near_max_bid_{current_bid:.3f}_15s_left"
                    break
                if seconds_left <= 5:
                    # XRP thesis gate: only force-exit-in-loss if XRP move is
                    # < $5 in trade direction or opposite. Otherwise ride.
                    if not xrp_thesis_allows_exit(opened, 0.0003):
                        bkt = int(opened.get("market_slug", "0").rsplit("-", 1)[-1] or 0)
                        cm = xrp_move_usd(bkt) if bkt else None
                        cm_str = f"${cm:+.2f}" if cm is not None else "n/a"
                        event(f"force_exit_in_loss_5s blocked: XRP thesis alive at 5s (current={cm_str}, threshold=$0.0003); riding settlement")
                        close_reason = "settlement_wait_thesis_alive_5s"
                        ride_settlement = True
                        break
                    close_reason = "force_exit_in_loss_5s_left"
                    break

            if now >= end_ts:
                close_reason = "settlement_wait_no_tp_or_sl"
                ride_settlement = True
                break
            time.sleep(args.poll_sec)
    except KeyboardInterrupt:
        close_reason = "ctrl_c_emergency_exit"
        log("Ctrl+C received while holding; attempting emergency close")

    report["close_reason"] = close_reason
    event(f"closing: {close_reason}")
    if ride_settlement:
        report["closed"] = settlement_close(ts_utc(), close_reason, opened["estimated_shares"], latest_books)
        report["result"] = "settlement_pending"
        log("riding settlement: no take-profit or stop-loss close")
        write_report(report)
        return report
    try:
        # Final bid check before executing any sell — if book is dead, ride settlement
        pre_close_bid = (latest_books or {}).get("bid")
        if pre_close_bid is not None and pre_close_bid < args.stop_loss_bid:
            event(f"pre-close bid {pre_close_bid:.3f} below floor {args.stop_loss_bid:.3f}; riding settlement instead of selling into dead book")
            report["closed"] = settlement_close(ts_utc(), f"settlement_wait_pre_close_bid_floor_{pre_close_bid:.3f}", opened["estimated_shares"], latest_books)
            report["result"] = "settlement_pending"
            write_report(report)
            return report
        client = auth_client()
        close_post, ladder_method, amount_shares = close_with_ladder(client, opened, args.close_retry_max)
        if close_post is None:
            # All ladder steps exhausted — ride settlement
            report["closed"] = settlement_close(ts_utc(), "ladder_exhausted_ride_settlement", amount_shares, latest_books)
            report["result"] = "settlement_pending"
            log("close failed all ladder steps; riding settlement")
            write_report(report)
            return report
        exit_book = clob_token_prices(opened["token_id"])
        eth_price_at_close = _binance_price()
        eth_drift = (
            round(eth_price_at_close - opened["xrp_price_at_entry"], 2)
            if eth_price_at_close is not None and opened.get("xrp_price_at_entry") is not None
            else None
        )
        report["closed"] = {
            "closed_at": ts_utc(),
            "close_shares": amount_shares,
            "remaining_shares": 0.0,
            "exit_book": exit_book,
            "close_post_result": close_post,
            "close_method": ladder_method,
            "peak_sell_now": peak_sell_now,
            "eth_price_at_close": eth_price_at_close,
            "eth_drift_usd": eth_drift,
        }
        report["result"] = "done"
        report["pnl_usd"] = round(float(close_post.get("takingAmount") or 0) - opened["estimated_notional_usd"], 6)
        report["session_pnl_usd"] = round(session_pnl + report["pnl_usd"], 6)
        tape(report.get("tape_header", trade_block_header(opened["market_slug"], opened["side"])))
        tape(trade_block_fill(opened["side"], opened["entry_price"], amount_shares, close_post, opened["estimated_notional_usd"]))
        tape(trade_block_pnl(report["session_pnl_usd"]))
        tape("")
        event(f"close submitted via {ladder_method}")
    except Exception as exc:
        report["closed"] = settlement_close(ts_utc(), f"fok_close_failed_ride_settlement: {exc}", opened.get("estimated_shares", 0))
        report["result"] = "settlement_pending"
        log(f"close failed; riding settlement: {exc}")

    report["finished_at"] = ts_utc()
    event(f"finished: {report['result']}")
    write_report(report)
    return report


def fetch_condition_id(slug: str) -> Optional[str]:
    """Return conditionId for the first market of a Gamma event slug."""
    try:
        ev = fetch_event(slug)
    except Exception as exc:
        event(f"error fetching event for slug={slug}: {exc}")
        return None
    if not ev:
        return None
    mkts = ev.get("markets") or []
    return str(mkts[0]["conditionId"]) if mkts and mkts[0].get("conditionId") else None


def redeem_position(condition_id: str, rpc_url: str, private_key: str) -> str:
    """Call redeemPositions on-chain. Returns tx hash string. Raises on failure."""
    try:
        from web3 import Web3
    except ImportError:
        raise RuntimeError("web3 not installed — run: pip install web3")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError(f"cannot connect to Polygon RPC: {rpc_url}")

    account = w3.eth.account.from_key(private_key)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
    adapter_addr = Web3.to_checksum_address(CTF_ADAPTER_ADDRESS)

    # One-time ERC1155 approval so the adapter can burn our tokens
    if not ctf.functions.isApprovedForAll(account.address, adapter_addr).call():
        event("redeem: approving adapter for CTF tokens (one-time)...")
        approve_tx = ctf.functions.setApprovalForAll(adapter_addr, True).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": 100_000,
            "gasPrice": w3.eth.gas_price,
        })
        signed = account.sign_transaction(approve_tx)
        receipt = w3.eth.wait_for_transaction_receipt(w3.eth.send_raw_transaction(signed.raw_transaction))
        event(f"redeem: approval confirmed tx={receipt.transactionHash.hex()}")

    condition_bytes = bytes.fromhex(condition_id.removeprefix("0x").zfill(64))
    adapter = w3.eth.contract(address=adapter_addr, abi=ADAPTER_ABI)
    redeem_tx = adapter.functions.redeemPositions(
        Web3.to_checksum_address(PUSD_ADDRESS),
        b"\x00" * 32,  # parentCollectionId (root)
        condition_bytes,
        [1, 2],  # both index sets; losing tokens yield $0, winner pays out
    ).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 300_000,
        "gasPrice": w3.eth.gas_price,
    })
    signed = account.sign_transaction(redeem_tx)
    receipt = w3.eth.wait_for_transaction_receipt(w3.eth.send_raw_transaction(signed.raw_transaction))
    return receipt.transactionHash.hex()


def cmd_redeem(args: argparse.Namespace) -> int:
    load_env_file(Path(args.env_file))
    private_key = os.getenv("PM_PRIVATE_KEY") or ""
    if not private_key:
        event("error: PM_PRIVATE_KEY not set")
        return 1
    rpc_url = os.getenv("PM_RPC_URL") or POLYGON_RPC_DEFAULT

    condition_id = args.redeem_condition_id
    if not condition_id:
        slug = args.redeem_slug
        if not slug:
            event("error: provide --redeem-slug (e.g. xrp-updown-5m-1782654900) or --redeem-condition-id")
            return 1
        event(f"redeem: fetching conditionId for slug={slug}")
        condition_id = fetch_condition_id(slug)
        if not condition_id:
            event(f"error: conditionId not found for slug={slug} — market may not be on Gamma yet")
            return 1

    event(f"redeem: conditionId={condition_id} rpc={rpc_url}")
    try:
        tx = redeem_position(condition_id, rpc_url, private_key)
        event(f"redeem: success tx={tx}")
        event(f"redeem: verify on Polygonscan: https://polygonscan.com/tx/{tx}")
        return 0
    except Exception as exc:
        event(f"redeem failed: {exc}")
        return 1


def main() -> int:
    args = parse_args()

    # Redeem mode — skip trading entirely
    if args.redeem_slug or args.redeem_condition_id:
        return cmd_redeem(args)

    load_env_file(Path(args.env_file))

    global LOG_FILE, VERBOSE, QUIET_TAPE
    VERBOSE = args.verbose
    LOG_FILE = Path(args.log_file) if args.log_file else DEFAULT_LOG_DIR / f"xrp_5m_{now_utc():%Y%m%d_%H%M%S}.log"
    log(
        "started "
        f"profile={args.profile} execute={args.execute} stake=${args.stake_usd:.2f} "
        f"max_stake=${args.max_stake_usd:.2f} log_file={LOG_FILE}"
    )

    total_pnl = 0.0
    start_balance = current_balance_usd() if args.execute else None
    balance_event(start_balance, start_balance)
    if args.target_balance is not None and start_balance is not None and start_balance >= args.target_balance:
        tape(f"session stop: target balance ${args.target_balance:.2f} reached (current ${start_balance:.2f})")
        return 0
    trades = 0
    cycle_no = 0
    traded_slugs: set = set()  # per-bucket lockout: one trade per 5m bucket
    while True:
        cycle_no += 1
        QUIET_TAPE = False
        report = run_cycle(args, cycle_no, total_pnl, start_balance, traded_slugs)
        # Lock the slug after any trade (win, loss, or settlement ride)
        traded_slug = (report.get("opened") or {}).get("market_slug")
        if traded_slug:
            traded_slugs.add(traded_slug)
        if report.get("result") == "done":
            trades += 1
            total_pnl += float(report.get("pnl_usd") or 0.0)
            current_balance = current_balance_usd() if args.execute else None
            if start_balance is not None and current_balance is not None:
                total_pnl = current_balance - start_balance
                balance_event(start_balance, current_balance)
        elif report.get("result") == "dry_run_ready":
            tape(report.get("tape_header", "dry run candidate"))
            tape("        dry run: would fill, no order submitted")
            tape("")
        if args.once:
            break
        if not args.execute and report.get("result") == "dry_run_ready":
            break
        if args.max_trades and trades >= args.max_trades:
            break
        if total_pnl <= -abs(args.max_session_loss_usd):
            tape(f"session stop: max loss ${abs(args.max_session_loss_usd):.2f} hit")
            break
        if args.target_balance is not None and args.execute:
            current_balance = current_balance_usd()
            if current_balance is not None and current_balance >= args.target_balance:
                tape(f"session stop: target balance ${args.target_balance:.2f} reached (current ${current_balance:.2f})")
                break
        if report.get("result") == "open_but_close_failed":
            break
        QUIET_TAPE = True
        time.sleep(args.poll_sec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
