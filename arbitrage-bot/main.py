#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib.util
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds, AssetType, BalanceAllowanceParams

ROOT_DIR = Path(__file__).resolve().parents[1]
ARB_DIR = ROOT_DIR / "arbitrage-bot"
sys.path.insert(0, str(ARB_DIR))

from arb_logic import append_trade_log, execute_arb  # noqa: E402
from config import (  # noqa: E402
    ARB_THRESHOLD,
    CLOB_API_URL,
    COINS,
    GAMMA_EVENTS_URL,
    GTD_TIMEOUT_SEC,
    MIN_BOOK_DEPTH_USD,
    MIN_SECONDS_LEFT,
    STAKE_USD,
)
from tui import ArbUI  # noqa: E402

UTC = dt.timezone.utc
LOG = logging.getLogger("arb_main")


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


ArbBookStream = _load_module("arb_ws_stream", ROOT_DIR / "ws-arbitrage-bot" / "stream.py").ArbBookStream


def load_env_file(path: Path) -> None:
    load_dotenv(path)


def bucket_5m(ts: int) -> int:
    return ts - (ts % 300)


def public_client() -> ClobClient:
    return ClobClient(host=CLOB_API_URL, chain_id=137)


def auth_client() -> ClobClient:
    key = os.getenv("PM_PRIVATE_KEY") or ""
    funder = os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS") or None
    sig = int(os.getenv("PM_SIGNATURE_TYPE", "3") or "3")
    api_key = os.getenv("PM_API_KEY") or ""
    api_secret = os.getenv("PM_API_SECRET") or ""
    api_passphrase = os.getenv("PM_API_PASSPHRASE") or ""
    if not all([key, funder, api_key, api_secret, api_passphrase]):
        raise RuntimeError("missing live credentials in env")
    client = ClobClient(host=CLOB_API_URL, chain_id=137, key=key, signature_type=sig, funder=funder)
    client.set_api_creds(ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase))
    return client


def current_balance_usd(client: ClobClient | None = None) -> float | None:
    try:
        payload = (client or auth_client()).get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception:
        return None
    for key in ("balance", "collateral", "available", "allowance"):
        if isinstance(payload, dict) and payload.get(key) is not None:
            try:
                value = float(payload[key])
                return value / 1_000_000 if value > 10_000 else value
            except Exception:
                continue
    return None


def parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            import json

            return json.loads(value)
        except Exception:
            return value
    return value


def fetch_coin_market(coin: str, bucket_ts: int) -> dict[str, Any]:
    slug = f"{coin}-updown-5m-{bucket_ts}"
    resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=12)
    resp.raise_for_status()
    payload = resp.json()
    if not payload:
        raise RuntimeError(f"market not found for {slug}")
    event = payload[0]
    markets = event.get("markets") or []
    if not markets:
        raise RuntimeError(f"no markets in event {slug}")
    market = dict(markets[0])
    token_ids = parse_json_field(market.get("clobTokenIds")) or []
    if len(token_ids) < 2:
        raise RuntimeError(f"missing token ids for {slug}")
    outcomes = parse_json_field(market.get("outcomes")) or []
    up_idx, down_idx = resolve_outcome_indices(outcomes)
    end_iso = str(market.get("endDate") or event.get("endDate") or "")
    return {
        "coin": coin.upper(),
        "slug": str(market.get("slug") or event.get("slug") or slug),
        "up_token": str(token_ids[up_idx]),
        "down_token": str(token_ids[down_idx]),
        "end_iso": end_iso,
        "bucket_ts": bucket_ts,
    }


def resolve_outcome_indices(outcomes: Any) -> tuple[int, int]:
    labels = [str(item).lower() for item in outcomes[:2]] if isinstance(outcomes, list) else []
    if len(labels) >= 2 and ("up" in labels[1] or "yes" in labels[1]):
        return 1, 0
    return 0, 1


def seconds_left(end_iso: str) -> int:
    try:
        end_ts = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0
    return max(0, int(end_ts - time.time()))


def main() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file))
    (ROOT_DIR / "logs-arbitrage").mkdir(parents=True, exist_ok=True)
    state = build_state()
    control = {"paused": False, "quit": False}
    ui = ArbUI(COINS, state, control)
    state["_meta"]["threshold"] = args.threshold
    state["_meta"]["stake_usd"] = args.stake_usd
    state["_meta"]["shares_per_leg"] = 5
    state["_meta"]["balance"] = current_balance_usd() if not args.dry_run else None

    markets = refresh_markets(state, ui)
    token_map = build_token_map(markets)
    stream = ArbBookStream(list(token_map.keys()), lambda token_id, bid, ask, bid_size, ask_size: on_book_update(
        token_id=token_id,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        state=state,
        token_map=token_map,
    ))

    loop = asyncio.new_event_loop()
    loop_state: dict[str, Any] = {}
    loop_thread = threading.Thread(target=run_loop, args=(loop, stream, loop_state), daemon=True)
    loop_thread.start()

    ui_thread = threading.Thread(target=ui.start, daemon=True)
    ui_thread.start()

    client = None if args.dry_run else auth_client()
    last_bucket = markets[next(iter(markets))]["bucket_ts"] if markets else bucket_5m(int(time.time()))
    active_lock = threading.Lock()
    traded_coins_by_bucket: dict[int, set[str]] = {}
    log_dir = ROOT_DIR / "logs-arbitrage"
    event_log = log_dir / f"scan_{dt.datetime.now(UTC):%Y%m%d}.log"
    last_snapshot_at = 0.0

    write_event_log(
        event_log,
        {
            "event": "startup",
            "dry_run": args.dry_run,
            "threshold": args.threshold,
            "stake_usd": args.stake_usd,
            "coins": [coin.upper() for coin in COINS],
            "balance": state["_meta"].get("balance"),
        },
    )

    scan_loops = 0
    try:
        while not control.get("quit"):
            scan_loops += 1
            if control.get("paused"):
                time.sleep(0.25)
                continue
            current_bucket = bucket_5m(int(time.time()))
            if current_bucket != last_bucket:
                refreshed = refresh_markets(state, ui)
                if refreshed:
                    markets = refreshed
                    token_map = build_token_map(markets)
                    stream.replace_token_ids(list(token_map.keys()))
                    last_bucket = current_bucket
                    ui.add_log(f"Rotated bucket to {current_bucket}")
                    write_event_log(
                        event_log,
                        {
                            "event": "bucket_rotation",
                            "bucket_ts": current_bucket,
                            "coins": summarize_state(state),
                        },
                    )
                else:
                    state["_meta"]["last_message"] = "Gamma refresh failed; retrying"
                    time.sleep(1.0)
                    continue

            if not active_lock.acquire(blocking=False):
                time.sleep(0.05)
                continue
            try:
                if not markets:
                    time.sleep(0.25)
                    continue
                traded_coins = traded_coins_by_bucket.setdefault(current_bucket, set())
                candidate = find_candidate(markets, state, traded_coins, args.threshold)
                now = time.time()
                if now - last_snapshot_at >= 5.0:
                    write_event_log(
                        event_log,
                        {
                            "event": "scan_snapshot",
                            "bucket_ts": current_bucket,
                            "ws_event_count": stream.event_count,
                            "coins": summarize_state(state),
                        },
                    )
                    last_snapshot_at = now
                if not candidate:
                    if args.once and scan_loops >= 10:
                        control["quit"] = True
                    time.sleep(0.05)
                    continue
                coin = candidate["coin"]
                slug = candidate["slug"]
                state["coins"][coin]["status"] = "BUYING"
                ui.add_log(f"{coin} arb detected combined={candidate['combined_ask']:.4f}")
                write_event_log(
                    event_log,
                    {
                        "event": "candidate",
                        "coin": coin,
                        "slug": slug,
                        "combined_ask": candidate["combined_ask"],
                        "up_ask": candidate["up_ask"],
                        "down_ask": candidate["down_ask"],
                    },
                )
                result = execute_arb(
                    client=client,
                    up_token=candidate["up_token"],
                    down_token=candidate["down_token"],
                    ask_up=candidate["up_ask"],
                    ask_down=candidate["down_ask"],
                    stake_usd=args.stake_usd,
                    expiration_ts=_expiration_ts(candidate["end_iso"]),
                    dry_run=args.dry_run,
                )
                traded_coins.add(coin)
                apply_result(state, coin, result, args.dry_run)
                append_trade_log(log_dir, {"coin": coin, "slug": slug, **result})
                write_event_log(
                    event_log,
                    {
                        "event": "arb_result",
                        "coin": coin,
                        "slug": slug,
                        "result": result.get("result"),
                        "net_profit_usd": result.get("net_profit_usd"),
                        "expected_net_usd": result.get("expected_net_usd"),
                    },
                )
                ui.add_log(render_result_line(coin, result))
                if args.once:
                    control["quit"] = True
            except Exception as exc:
                state["coins"][coin]["status"] = "FAILED"
                state["_meta"]["last_message"] = f"{coin} execution failed: {exc}"
                write_event_log(
                    event_log,
                    {
                        "event": "arb_exception",
                        "coin": coin,
                        "slug": slug,
                        "error": str(exc),
                    },
                )
                ui.add_log(f"{coin} execution failed: {exc}")
            finally:
                active_lock.release()
            time.sleep(0.05)
    finally:
        control["quit"] = True
        stream.stop()
        shutdown_loop(loop, loop_state)
        ui.stop()
        loop_thread.join(timeout=2)
        ui_thread.join(timeout=2)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=str(ROOT_DIR / ".env"))
    parser.add_argument("--threshold", type=float, default=ARB_THRESHOLD)
    parser.add_argument("--stake-usd", type=float, default=STAKE_USD)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def build_state() -> dict[str, Any]:
    return {
        "coins": {
            coin.upper(): {
                "up_ask": 0.0,
                "down_ask": 0.0,
                "up_bid": 0.0,
                "down_bid": 0.0,
                "up_bid_size": 0.0,
                "down_bid_size": 0.0,
                "up_ask_size": 0.0,
                "down_ask_size": 0.0,
                "combined_ask": None,
                "status": "WAITING",
                "secs_left": 0,
            }
            for coin in COINS
        },
        "_meta": {"pnl_usd": 0.0, "arb_count": 0, "last_message": ""},
    }


def refresh_markets(state: dict[str, Any], ui: ArbUI) -> dict[str, dict[str, Any]]:
    bucket_ts = bucket_5m(int(time.time()))
    markets: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    pub = public_client()
    for coin in COINS:
        try:
            market = fetch_coin_market(coin, bucket_ts)
        except Exception as exc:
            errors.append(f"{coin.upper()}: {exc}")
            mark_coin_error(state["coins"][coin.upper()])
            continue
        markets[market["coin"]] = market
        row = state["coins"][market["coin"]]
        reset_coin_state(row, seconds_left(market["end_iso"]))
        seed_coin_books(row, pub, market["up_token"], market["down_token"])
    if errors:
        message = f"Gamma fetch failed for {len(errors)}/{len(COINS)} coins"
        state["_meta"]["last_message"] = message
        ui.add_log(message)
    if markets:
        ui.add_log(f"Subscribed bucket {bucket_ts} for {len(markets)} coins")
    else:
        ui.add_log("Gamma unavailable; no markets loaded")
    return markets


def build_token_map(markets: dict[str, dict[str, Any]]) -> dict[str, tuple[str, str]]:
    token_map: dict[str, tuple[str, str]] = {}
    for coin, market in markets.items():
        token_map[market["up_token"]] = (coin, "up")
        token_map[market["down_token"]] = (coin, "down")
    return token_map


def on_book_update(
    *,
    token_id: str,
    bid: float,
    ask: float,
    bid_size: float,
    ask_size: float,
    state: dict[str, Any],
    token_map: dict[str, tuple[str, str]],
) -> None:
    slot = token_map.get(token_id)
    if not slot:
        return
    coin, side = slot
    row = state["coins"][coin]
    row[f"{side}_bid"] = float(bid or 0.0)
    row[f"{side}_ask"] = float(ask or 0.0)
    row[f"{side}_bid_size"] = float(bid_size or 0.0)
    row[f"{side}_ask_size"] = float(ask_size or 0.0)
    up_ask = row.get("up_ask") or 0.0
    down_ask = row.get("down_ask") or 0.0
    if up_ask > 0 and down_ask > 0:
        row["combined_ask"] = round(up_ask + down_ask, 6)
    else:
        row["combined_ask"] = None


def find_candidate(
    markets: dict[str, dict[str, Any]],
    state: dict[str, Any],
    traded_coins: set[str],
    threshold: float,
) -> dict[str, Any] | None:
    for coin, market in markets.items():
        row = state["coins"][coin]
        row["secs_left"] = seconds_left(market["end_iso"])
        row["status"] = _candidate_status(row, coin, traded_coins, threshold)
        if row["status"] != "ARB!":
            continue
        combined = row.get("combined_ask")
        return {
            "coin": coin,
            "slug": market["slug"],
            "up_token": market["up_token"],
            "down_token": market["down_token"],
            "up_ask": float(row["up_ask"]),
            "down_ask": float(row["down_ask"]),
            "combined_ask": float(combined),
            "end_iso": market["end_iso"],
        }
    return None


def apply_result(state: dict[str, Any], coin: str, result: dict[str, Any], dry_run: bool) -> None:
    row = state["coins"][coin]
    meta = state["_meta"]
    outcome = str(result.get("result", "")).lower()
    if outcome == "success":
        row["status"] = "SUCCESS"
    elif outcome == "unwind":
        row["status"] = "UNWIND"
    elif outcome == "failed":
        row["status"] = "FAILED"
    elif outcome == "skipped":
        row["status"] = "SKIPPED"
    else:
        row["status"] = "DRY RUN" if dry_run else "DONE"
    pnl = float(result.get("net_profit_usd") or 0.0)
    if outcome == "unwind":
        pnl = float(((result.get("unwind") or {}).get("pnl_usd")) or 0.0)
    meta["pnl_usd"] = round(float(meta.get("pnl_usd", 0.0)) + pnl, 6)
    meta["arb_count"] = int(meta.get("arb_count", 0) or 0) + 1
    meta["last_message"] = render_result_line(coin, result)


def render_result_line(coin: str, result: dict[str, Any]) -> str:
    outcome = str(result.get("result", "")).upper()
    realized = result.get("net_profit_usd")
    expected = result.get("expected_net_usd")
    if outcome == "UNWIND":
        realized = ((result.get("unwind") or {}).get("pnl_usd"))
    if realized is not None:
        return f"{coin} {outcome} realized={float(realized or 0.0):+.4f}"
    if expected is not None:
        return f"{coin} {outcome} expected={float(expected or 0.0):+.4f}"
    return f"{coin} {outcome}"


def summarize_state(state: dict[str, Any]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for coin in COINS:
        row = state["coins"][coin.upper()]
        summary.append(
            {
                "coin": coin.upper(),
                "status": row.get("status"),
                "up_ask": row.get("up_ask"),
                "down_ask": row.get("down_ask"),
                "combined_ask": row.get("combined_ask"),
                "secs_left": row.get("secs_left"),
                "up_depth_usd": round(float(row.get("up_ask", 0.0) or 0.0) * float(row.get("up_ask_size", 0.0) or 0.0), 6),
                "down_depth_usd": round(float(row.get("down_ask", 0.0) or 0.0) * float(row.get("down_ask_size", 0.0) or 0.0), 6),
            }
        )
    return summary


def reset_coin_state(row: dict[str, Any], secs_left: int) -> None:
    row["up_ask"] = 0.0
    row["down_ask"] = 0.0
    row["up_bid"] = 0.0
    row["down_bid"] = 0.0
    row["up_bid_size"] = 0.0
    row["down_bid_size"] = 0.0
    row["up_ask_size"] = 0.0
    row["down_ask_size"] = 0.0
    row["combined_ask"] = None
    row["status"] = "NO ARB"
    row["secs_left"] = secs_left


def seed_coin_books(row: dict[str, Any], client: ClobClient, up_token: str, down_token: str) -> None:
    try:
        up_book = client.get_order_book(str(up_token))
        down_book = client.get_order_book(str(down_token))
    except Exception:
        return
    up_bid, up_bid_size, up_ask, up_ask_size = best_bid_ask(up_book)
    down_bid, down_bid_size, down_ask, down_ask_size = best_bid_ask(down_book)
    row["up_bid"] = up_bid
    row["up_bid_size"] = up_bid_size
    row["up_ask"] = up_ask
    row["up_ask_size"] = up_ask_size
    row["down_bid"] = down_bid
    row["down_bid_size"] = down_bid_size
    row["down_ask"] = down_ask
    row["down_ask_size"] = down_ask_size
    if up_ask > 0 and down_ask > 0:
        row["combined_ask"] = round(up_ask + down_ask, 6)
    else:
        row["combined_ask"] = None


def best_bid_ask(book: Any) -> tuple[float, float, float, float]:
    if isinstance(book, dict):
        bids = book.get("bids") or []
        asks = book.get("asks") or []
    else:
        bids = getattr(book, "bids", []) or []
        asks = getattr(book, "asks", []) or []
    best_bid = 0.0
    best_bid_size = 0.0
    best_ask = 0.0
    best_ask_size = 0.0
    for level in bids:
        price = safe_float(level.get("price") if isinstance(level, dict) else getattr(level, "price", 0.0))
        size = safe_float(level.get("size") if isinstance(level, dict) else getattr(level, "size", 0.0))
        if price > best_bid:
            best_bid = price
            best_bid_size = size
    for level in asks:
        price = safe_float(level.get("price") if isinstance(level, dict) else getattr(level, "price", 0.0))
        size = safe_float(level.get("size") if isinstance(level, dict) else getattr(level, "size", 0.0))
        if best_ask == 0.0 or (price > 0 and price < best_ask):
            best_ask = price
            best_ask_size = size
    return best_bid, best_bid_size, best_ask, best_ask_size


def safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def mark_coin_error(row: dict[str, Any]) -> None:
    row["up_ask"] = 0.0
    row["down_ask"] = 0.0
    row["up_bid"] = 0.0
    row["down_bid"] = 0.0
    row["up_bid_size"] = 0.0
    row["down_bid_size"] = 0.0
    row["up_ask_size"] = 0.0
    row["down_ask_size"] = 0.0
    row["combined_ask"] = None
    row["status"] = "ERROR"


def write_event_log(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"ts": dt.datetime.now(UTC).isoformat().replace("+00:00", "Z"), **payload}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=True) + "\n")


def _candidate_status(row: dict[str, Any], coin: str, traded_coins: set[str], threshold: float) -> str:
    if coin in traded_coins:
        return "DONE"
    if row["secs_left"] < MIN_SECONDS_LEFT:
        return "LATE"
    if _thin_book(row):
        return "THIN"
    if _should_fire_arb(row, threshold):
        return "ARB!"
    return "NO ARB"


def _should_fire_arb(row: dict[str, Any], threshold: float) -> bool:
    combined = row.get("combined_ask")
    locked = bool(row.get("status") == "BUYING")
    if locked:
        return False
    if _thin_book(row):
        return False
    return combined is not None and float(combined) < float(threshold)


def _thin_book(row: dict[str, Any]) -> bool:
    up_depth = float(row.get("up_ask", 0.0) or 0.0) * float(row.get("up_ask_size", 0.0) or 0.0)
    down_depth = float(row.get("down_ask", 0.0) or 0.0) * float(row.get("down_ask_size", 0.0) or 0.0)
    return up_depth < MIN_BOOK_DEPTH_USD or down_depth < MIN_BOOK_DEPTH_USD


def _expiration_ts(end_iso: str) -> int:
    try:
        return int(dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return int(time.time() + GTD_TIMEOUT_SEC)


def run_loop(loop: asyncio.AbstractEventLoop, stream: Any, loop_state: dict[str, Any]) -> None:
    asyncio.set_event_loop(loop)
    loop_state["task"] = loop.create_task(stream.run_forever())
    loop.run_forever()


def shutdown_loop(loop: asyncio.AbstractEventLoop, loop_state: dict[str, Any]) -> None:
    task = loop_state.get("task")
    if task is not None:
        def _cancel_task() -> None:
            if not task.done():
                task.cancel()
            loop.stop()

        loop.call_soon_threadsafe(_cancel_task)
        return
    loop.call_soon_threadsafe(loop.stop)


if __name__ == "__main__":
    raise SystemExit(main())
