#!/usr/bin/env python3
"""copy-trade-bot/main.py — Autonomous BTC 5m directional momentum bot.
Places GTD limit orders below ask when BTC moves since bucket open.
Scales in (up to 3 entries per bucket) if BTC keeps moving same direction.
Rides all positions to settlement. No sells."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
import queue
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
BOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BOT_DIR))

from dotenv import load_dotenv

from config import (
    BTC_MOVE_THRESHOLD,
    ENTRY_SHARES,
    FORCE_EXIT_MIN_PROFIT_PCT,
    FORCE_EXIT_SECONDS,
    INITIAL_ENTRY_MAX_ASK,
    INITIAL_ENTRY_MAX_SECS_LEFT,
    INITIAL_ENTRY_MIN_SECS_LEFT,
    LOG_DIR,
    MAX_CONCURRENT_BUCKETS,
    MAX_ENTRIES_PER_BUCKET,
    MAX_SESSION_LOSS_USD,
    TARGET_PROFIT_USD,
    MIN_SECONDS_LEFT,
    STAKE_USD_PER_ENTRY,
    TAKE_PROFIT_PCT,
    GTD_ENTRY_DELAY_SECONDS,
    FLIP_MOVE_THRESHOLD,
    HEDGE_OPPOSITE_ASK_THRESHOLD,
    HEDGE_OPPOSITE_MOVE_THRESHOLD,
    HEDGE2_ASK_THRESHOLD,
    HEDGE2_MOVE_THRESHOLD,
)
import poly_book_ws
from executor import (
    auth_client,
    cancel_order,
    fetch_condition_id,
    get_market_resolution,
    get_order_status,
    get_balance,
    get_tick_size,
    place_gtd_limit_order,
    redeem_position,
    apply_fee_cost_and_refresh_balance,
    get_fee_per_share,
    get_net_profit_per_share,
    estimate_fee_inclusive_buy_cost,
    place_limit_sell_fak,
    place_market_sell_fok,
    place_market_buy_fok,
)
from models import Entry
from price_feed import get_btc_price
from tui import CopyTradeUI

LOG = logging.getLogger("copy_trade")


def _bounded(fn, timeout=5, default=None):
    """Run fn in daemon thread, join(timeout). Return result or default on timeout."""
    q = queue.Queue()
    def _wrap():
        try:
            q.put(fn())
        except Exception as e:
            q.put(e)
    thread = threading.Thread(target=_wrap, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        LOG.error("[OUTAGE] timeout calling %r", fn.__name__)
        return default
    try:
        res = q.get_nowait()
        if isinstance(res, Exception):
            LOG.error("[OUTAGE] error in %r: %r", fn.__name__, res)
            return default
        return res
    except queue.Empty:
        return default


def _refresh_open_entry(client, entry: dict[str, Any]) -> None:
    order_id = str(entry.get("order_id") or "")
    if not order_id:
        return
    order_data = get_order_status(client, order_id)
    if not isinstance(order_data, dict):
        return
    status_text = str(order_data.get("status") or "").lower()
    matched = float(order_data.get("size_matched") or 0.0)
    original_size = float(order_data.get("original_size") or entry.get("shares") or 0.0)
    if matched > 0 or status_text in {"matched", "filled"}:
        entry["status"] = "FILLED"
        if matched > 0:
            entry["shares"] = matched
    elif status_text in {"canceled", "cancelled"}:
        entry["status"] = "CANCELLED"
    elif status_text in {"expired", "unmatched"}:
        entry["status"] = "EXPIRED"
    elif status_text == "live":
        entry["status"] = "RESTING"
    if entry.get("status") == "FILLED" and original_size > 0 and matched <= 0:
        entry["shares"] = original_size
    if entry.get("status") == "FILLED" and entry.get("token") and entry.get("limit_price"):
        raw_filled_cost = float(entry.get("shares", 0)) * float(entry.get("limit_price", 0))
        entry["cost"] = estimate_fee_inclusive_buy_cost(
            client,
            str(entry["token"]),
            float(entry["limit_price"]),
            float(entry.get("shares", 0)),
            raw_filled_cost,
        )


def _simple_open_position_token(pos: dict[str, Any]) -> str:
    direction = pos.get("direction", "")
    if direction == "UP":
        return str(pos.get("up_token") or "")
    if direction == "DOWN":
        return str(pos.get("down_token") or "")
    return ""


def _is_simple_open_position(pos: dict[str, Any]) -> bool:
    if pos.get("status") != "OPEN":
        return False
    entries = pos.get("entries", [])
    return len(entries) == 1


def _estimate_exit_profit(client, token: str, shares: float, total_cost: float) -> tuple[float | None, float | None, float, float]:
    bid, _ = poly_book_ws.get_best_prices(token)
    if bid <= 0 or shares <= 0 or total_cost <= 0:
        return None, None, bid, 0.0
    gross_value = shares * bid
    fee = shares * get_fee_per_share(client, token, bid) if client else 0.0
    net_value = max(gross_value - fee, 0.0)
    profit = net_value - total_cost
    profit_pct = profit / total_cost if total_cost > 0 else 0.0
    return profit, profit_pct, bid, net_value


_last_exit_error_ts: dict[int, float] = {}


def _close_simple_position(client, pos_ts: int, pos: dict[str, Any], reason: str, bid: float, ui: CopyTradeUI, target_price=None) -> bool:
    token = _simple_open_position_token(pos)
    if not token:
        return False
    entry = pos.get("entries", [{}])[0]
    shares = float(entry.get("shares") or 0.0)
    if shares <= 0:
        return False
    tick_size = get_tick_size(client, token)
    sell_bid = min(bid, 0.99)
    if target_price is not None:
        target_price = round(min(max(target_price, 0.01), 0.99), 4)
        close_entry, outcome = place_limit_sell_fak(client, pos_ts, token, shares, target_price)
        method = "fak_limit_sell_target"
        sell_bid = target_price
    else:
        close_entry, outcome = place_market_sell_fok(client, pos_ts, token, shares)
        method = "fok_market_sell"
        if close_entry is None:
            close_entry, outcome = place_limit_sell_fak(client, pos_ts, token, shares, sell_bid)
            method = "fak_limit_sell"
    if close_entry is None:
        now = time.time()
        if now - _last_exit_error_ts.get(pos_ts, 0) >= 5.0:
            LOG.error("[TRADE][EXIT] event=failed bucket=%s reason=%s token=%s sell_bid=%.4f shares=%.4f outcome=%s", pos_ts, reason, token[:8], sell_bid, shares, outcome)
            ui.add_log(f"exit failed: {reason} {outcome}")
            _last_exit_error_ts[pos_ts] = now
        return False
    buy_cost = float(entry.get("cost") or pos.get("total_cost") or 0.0)
    proceeds = float(close_entry.cost or 0.0)
    pnl = round(proceeds - buy_cost, 4)
    pos["status"] = "CLOSED"
    pos["closed_at"] = int(time.time())
    pos["close_reason"] = reason
    pos["close_method"] = method
    pos["exit_price"] = sell_bid
    pos["exit_shares"] = close_entry.shares
    pos["exit_order_id"] = close_entry.order_id
    pos["pnl"] = pnl
    bal_after = get_balance(client)
    if bal_after is not None:
        pos["balance_after"] = bal_after
    LOG.info("[TRADE][EXIT] event=closed bucket=%s reason=%s method=%s sell_bid=%.4f shares=%.4f proceeds=%.4f pnl=%+.4f order_id=%s", pos_ts, reason, method, sell_bid, close_entry.shares, proceeds, pnl, close_entry.order_id[:10])
    ui.add_log(f"exit {reason}: bid={sell_bid:.4f} sh={close_entry.shares:.4f} pnl=${pnl:+.4f}")
    return True


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=str(ROOT_DIR / ".env"))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--target-profit", type=float, default=None)
    return ap.parse_args()


def _setup_logging(verbose: bool) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / f"copy_trade_{int(time.time())}.log", encoding="utf-8")],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("py_clob_client_v2").setLevel(logging.WARNING)
    logging.getLogger("py_clob_client_v2.http_helpers.helpers").setLevel(logging.WARNING)


def _build_state() -> dict[str, Any]:
    return {
        "positions": {},
        "current_bucket": {
            "ts": 0,
            "btc_open": 0.0,
            "btc_now": 0.0,
            "move": 0.0,
            "direction": "",
            "entries": 0,
            "up_token": "",
            "down_token": "",
            "hedge_order_id": "",
            "hedge_placed_at": 0.0,
            "hedge_fok_fallback": False,
            "hedge2_order_id": "",
            "hedge2_placed_at": 0.0,
            "hedge2_fok_fallback": False,
        },
        "_meta": {
            "mode": "DRY",
            "balance": 0.0,
            "session_pnl": 0.0,
            "active_buckets": 0,
            "entry_count": 0,
            "poll_count": 0,
            "btc_price": 0.0,
        },
    }


def _seconds_left(bucket_ts: int) -> int:
    return max(0, (bucket_ts + 300) - int(time.time()))


def _get_btc_tokens() -> tuple[str, str]:
    import json, requests
    from config import GAMMA_EVENTS_URL
    now_ts = int(time.time())
    bucket = now_ts - (now_ts % 300)
    slug = f"btc-updown-5m-{bucket}"
    LOG.debug("[MARKET][TOKEN] event=resolve_start slug=%s", slug)
    try:
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data and data[0].get("markets"):
            token_ids = data[0]["markets"][0].get("clobTokenIds")
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if token_ids and len(token_ids) >= 2:
                LOG.info("[MARKET][TOKEN] event=resolve_ok slug=%s up_token=%s down_token=%s", slug, token_ids[0][:8], token_ids[1][:8])
                return token_ids[0], token_ids[1]
            else:
                LOG.warning("[MARKET][TOKEN] event=resolve_empty slug=%s reason=insufficient_token_ids", slug)
        else:
            LOG.warning("[MARKET][TOKEN] event=resolve_empty slug=%s reason=no_markets", slug)
    except Exception as exc:
        LOG.warning("[MARKET][TOKEN] event=resolve_failed slug=%s error=%r", slug, exc)
    return "", ""


def _sync_ws_subscriptions(state: dict[str, Any], cb: dict[str, Any]) -> None:
    token_ids: set[str] = set()
    up = cb.get("up_token", "")
    down = cb.get("down_token", "")
    if up:
        token_ids.add(up)
    if down:
        token_ids.add(down)
    for pos in state["positions"].values():
        if pos.get("status") == "OPEN":
            for t in (pos.get("up_token", ""), pos.get("down_token", "")):
                if t:
                    token_ids.add(t)
    poly_book_ws.set_subscriptions(token_ids)


def _hedge_conditions_still_met(cb: dict[str, Any]) -> bool:
    """Check if HEDGE thresholds still met (for FOK fallback)."""
    pos_dir = cb.get("hedge_pos_dir", "")
    if not pos_dir:
        return False
    opp_token = cb.get("hedge_token", "")
    if not opp_token:
        return False
    _, opp_ask = poly_book_ws.get_best_prices(opp_token)
    if opp_ask <= 0:
        return False
    move_flip_ready = (
        (pos_dir == "DOWN" and cb["move"] >= HEDGE_OPPOSITE_MOVE_THRESHOLD) or
        (pos_dir == "UP" and cb["move"] <= -HEDGE_OPPOSITE_MOVE_THRESHOLD)
    )
    return move_flip_ready and opp_ask >= HEDGE_OPPOSITE_ASK_THRESHOLD


def _hedge2_conditions_still_met(cb: dict[str, Any]) -> bool:
    """Check if HEDGE2 thresholds still met (for FOK fallback)."""
    orig_dir = cb.get("hedge2_orig_dir", "")
    if not orig_dir:
        return False
    token = cb.get("hedge2_token", "")
    if not token:
        return False
    _, ask = poly_book_ws.get_best_prices(token)
    if ask <= 0:
        return False
    move_to_orig = cb["move"] if orig_dir == "UP" else -cb["move"]
    return move_to_orig >= HEDGE2_MOVE_THRESHOLD and ask >= HEDGE2_ASK_THRESHOLD


def main() -> int:
    args = parse_args()
    load_dotenv(Path(args.env_file))
    _setup_logging(args.verbose)
    mode = "LIVE" if args.live else "DRY"
    target_profit = args.target_profit if args.target_profit is not None else TARGET_PROFIT_USD
    state = _build_state()
    state["_meta"]["mode"] = mode
    control: dict[str, Any] = {"paused": False, "quit": False}
    ui = CopyTradeUI(state, control)
    LOG.info("[BOOT] event=start mode=%s env_file=%s verbose=%s target_profit=%.2f", mode, args.env_file, args.verbose, target_profit)
    LOG.info(
        "[BOOT] event=config stake=%.2f entry_shares=%d move_threshold=%.2f flip_threshold=%.2f hedge_ask_threshold=%.2f hedge_move_threshold=%.2f min_seconds_left=%d max_entries=%d max_buckets=%d",
        STAKE_USD_PER_ENTRY,
        ENTRY_SHARES,
BTC_MOVE_THRESHOLD,
        FLIP_MOVE_THRESHOLD,
        HEDGE_OPPOSITE_ASK_THRESHOLD,
        HEDGE_OPPOSITE_MOVE_THRESHOLD,
        MIN_SECONDS_LEFT,
        MAX_ENTRIES_PER_BUCKET,
        MAX_CONCURRENT_BUCKETS,
    )
    ui.add_log("starting BTC directional momentum bot")
    ui.add_log(f"mode={mode} stake=${STAKE_USD_PER_ENTRY} shares={ENTRY_SHARES} threshold=${BTC_MOVE_THRESHOLD} max_entries={MAX_ENTRIES_PER_BUCKET}")

    client = None
    start_balance = None
    if args.live:
        try:
            client = auth_client()
            start_balance = get_balance(client)
            state["_meta"]["balance"] = start_balance or 0.0
            LOG.info("[AUTH] event=live_ready balance=%.4f", start_balance or 0.0)
            ui.add_log(f"live balance: ${start_balance:.4f}")
        except Exception as exc:
            ui.add_log(f"FATAL: auth failed: {exc}")
            LOG.error("[AUTH] event=failed error=%r", exc)
            return 1

    last_balance_check = 0.0
    net_ok = True
    net_down_at = 0.0

    import threading
    threading.Thread(target=ui.start, daemon=True).start()
    poly_book_ws.start()

    try:
        while not control.get("quit"):
            if control.get("paused"):
                LOG.debug("[CONTROL] event=loop_paused")
                time.sleep(0.05)
                continue

            # OUTAGE: skip all work, spin cheap, stay quit-able
            if not net_ok:
                time.sleep(0.05)
                continue

            state["_meta"]["poll_count"] += 1
            now_ts = int(time.time())

            if not args.live:
                cb = state.get("current_bucket", {})

            # --- HEDGE TIMEOUT CHECK ---
            if args.live and client is not None:
                cb = state.get("current_bucket", {})
                cb_secs_left = _seconds_left(cb["ts"]) if cb.get("ts") else 0

                if cb.get("hedge_order_id") and not cb.get("hedge_fok_fallback"):
                    order = _bounded(lambda oid=cb["hedge_order_id"]: get_order_status(client, oid), timeout=2, default=None)
                    if order and order.get("status") == "MATCHED":
                        cb["hedge_filled"] = True
                    elif 30 <= cb_secs_left < 60 and not cb.get("hedge_repriced"):
                        pos = state.get("positions", {}).get(cb.get("ts"), {})
                        if pos.get("status") == "OPEN" and _hedge_conditions_still_met(cb):
                            _, fresh_ask = poly_book_ws.get_best_prices(cb.get("hedge_token", ""))
                            if fresh_ask > 0 and fresh_ask < 0.95:
                                new_limit = round(min(fresh_ask + 0.01, 0.99), 4)
                                new_npps = max((1.0 - new_limit) * 0.93, 0.005)
                                first_trade_cost = float(pos.get("entries", [{}])[0].get("cost", 0.0) or 0.0)
                                new_shares = round(max((first_trade_cost * 1.01) / new_npps, 1.0), 4)
                                new_cost = round(new_shares * new_limit, 4)
                                bal = get_balance(client)
                                if bal is not None and bal < new_cost:
                                    affordable_budget = round(max(bal * 0.98, 0.0), 4)
                                    affordable_shares = round(max(affordable_budget / new_limit, 0.0), 4)
                                    if affordable_shares >= 1.0:
                                        new_shares = affordable_shares
                                        new_cost = round(new_shares * new_limit, 4)
                                    else:
                                        new_cost = 0.0
                                if new_cost >= 1.0:
                                    LOG.warning(
                                        "[TRADE][HEDGE] event=repricing bucket=%s old_order=%s secs_left=%d old_limit=%.4f new_limit=%.4f new_shares=%.4f new_cost=%.4f",
                                        cb["ts"], cb["hedge_order_id"][:10], cb_secs_left, float(order.get("price", 0) or 0), new_limit, new_shares, new_cost
                                    )
                                    _bounded(lambda oid=cb["hedge_order_id"]: cancel_order(client, oid), timeout=2, default=False)
                                    tick_size = get_tick_size(client, cb["hedge_token"])
                                    entry, outcome = place_gtd_limit_order(client, cb["ts"], cb["hedge_token"], fresh_ask, tick_size, shares=new_shares, limit_price=new_limit)
                                    if outcome and entry:
                                        cb["hedge_order_id"] = entry.order_id
                                        cb["hedge_placed_at"] = now_ts
                                        cb["hedge_repriced"] = True
                                        cb["hedge_cost"] = new_cost
                                        for _e in pos.get("entries", []):
                                            if _e.get("token") == cb["hedge_token"]:
                                                _e["order_id"] = entry.order_id
                                                _e["status"] = "OPEN"
                                                _e["shares"] = new_shares
                                                _e["cost"] = new_cost
                                                _e["limit_price"] = new_limit
                                                break
                                    else:
                                        LOG.warning("[TRADE][HEDGE] event=repricing_failed bucket=%s reason=order_failed", cb["ts"])
                                else:
                                    LOG.warning("[TRADE][HEDGE] event=repricing_skip bucket=%s reason=insufficient_balance", cb["ts"])
                            else:
                                LOG.warning("[TRADE][HEDGE] event=repricing_skip bucket=%s reason=ask_too_high fresh_ask=%.4f", cb["ts"], fresh_ask)
                    elif cb_secs_left < 30:
                        pos = state.get("positions", {}).get(cb.get("ts"), {})
                        LOG.warning("[TRADE][HEDGE] event=timeout_cancel bucket=%s order_id=%s secs_left=%d", cb["ts"], cb["hedge_order_id"][:10], cb_secs_left)
                        _bounded(lambda oid=cb["hedge_order_id"]: cancel_order(client, oid), timeout=2, default=False)
                        cb["hedge_fok_fallback"] = True
                        if pos.get("status") == "OPEN" and _hedge_conditions_still_met(cb):
                            _, opp_ask = poly_book_ws.get_best_prices(cb.get("hedge_token", ""))
                            if opp_ask > 0 and opp_ask < 0.95:
                                fok_limit_price = round(min(opp_ask, 0.99), 4)
                                npps = max((1.0 - fok_limit_price) * 0.93, 0.005)
                                first_trade_cost = float(pos.get("entries", [{}])[0].get("cost", 0.0) or 0.0)
                                target_recovery = first_trade_cost * 1.01
                                required_shares = round(max(target_recovery / npps, 1.0), 4)
                                fok_amount = round(required_shares * fok_limit_price, 4)
                                bal = get_balance(client)
                                if bal is not None and bal < fok_amount:
                                    affordable_budget = round(max(bal * 0.98, 0.0), 4)
                                    affordable_shares = round(max(affordable_budget / fok_limit_price, 0.0), 4)
                                    if affordable_shares >= 1.0:
                                        fok_amount = round(affordable_shares * fok_limit_price, 4)
                                    else:
                                        fok_amount = 0.0
                                if fok_amount >= 1.0:
                                    entry, outcome = place_market_buy_fok(client, cb["ts"], cb["hedge_token"], fok_amount)
                                    if outcome and entry:
                                        cb["hedge_order_id"] = entry.order_id
                                        for _e in pos.get("entries", []):
                                            if _e.get("token") == cb["hedge_token"]:
                                                _e["order_id"] = entry.order_id
                                                _e["status"] = "FILLED"
                                                _e["shares"] = entry.shares
                                                _e["cost"] = entry.cost
                                                _e["limit_price"] = entry.limit_price
                                                break
                                else:
                                    LOG.warning("[TRADE][HEDGE] event=timeout_skip bucket=%s reason=insufficient_balance", cb["ts"])
                            else:
                                LOG.warning("[TRADE][HEDGE] event=timeout_skip bucket=%s reason=ask_too_high opp_ask=%.4f", cb["ts"], opp_ask)

                if cb.get("hedge2_order_id") and not cb.get("hedge2_fok_fallback"):
                    order = _bounded(lambda oid=cb["hedge2_order_id"]: get_order_status(client, oid), timeout=2, default=None)
                    if order and order.get("status") == "MATCHED":
                        cb["hedge2_filled"] = True
                    elif cb_secs_left <= 11:
                        pos = state.get("positions", {}).get(cb.get("ts"), {})
                        LOG.warning("[TRADE][HEDGE2] event=timeout_cancel bucket=%s order_id=%s secs_left=%d", cb["ts"], cb["hedge2_order_id"][:10], cb_secs_left)
                        _bounded(lambda oid=cb["hedge2_order_id"]: cancel_order(client, oid), timeout=2, default=False)
                        cb["hedge2_fok_fallback"] = True
                        if pos.get("status") == "OPEN" and _hedge2_conditions_still_met(cb):
                            _, orig_ask = poly_book_ws.get_best_prices(cb.get("hedge2_token", ""))
                            if orig_ask > 0 and orig_ask < 0.95:
                                fok_limit_price = round(min(orig_ask, 0.99), 4)
                                npps2 = max((1.0 - fok_limit_price) * 0.93, 0.005)
                                first_entry = pos.get("entries", [{}])[0]
                                second_entry = pos.get("entries", [{}, {}])[1] if len(pos.get("entries", [])) > 1 else {}
                                first_trade_cost = float(first_entry.get("cost", 0.0) or 0.0)
                                first_trade_shares = float(first_entry.get("shares", 0.0) or 0.0)
                                first_hedge_cost = float(second_entry.get("cost", 0.0) or 0.0)
                                target_recovery = (1.01 * (first_trade_cost + first_hedge_cost)) - first_trade_shares
                                required_shares = round(max(target_recovery / npps2, 1.0), 4)
                                fok_amount = round(required_shares * fok_limit_price, 4)
                                bal = get_balance(client)
                                if bal is not None and bal < fok_amount:
                                    affordable_budget = round(max(bal * 0.98, 0.0), 4)
                                    affordable_shares = round(max(affordable_budget / fok_limit_price, 0.0), 4)
                                    if affordable_shares >= 1.0:
                                        fok_amount = round(affordable_shares * fok_limit_price, 4)
                                    else:
                                        fok_amount = 0.0
                                if fok_amount >= 1.0:
                                    entry, outcome = place_market_buy_fok(client, cb["ts"], cb["hedge2_token"], fok_amount)
                                    if outcome and entry:
                                        cb["hedge2_order_id"] = entry.order_id
                                        for _e in pos.get("entries", []):
                                            if _e.get("token") == cb["hedge2_token"]:
                                                _e["order_id"] = entry.order_id
                                                _e["status"] = "FILLED"
                                                _e["shares"] = entry.shares
                                                _e["cost"] = entry.cost
                                                _e["limit_price"] = entry.limit_price
                                                break
                                else:
                                    LOG.warning("[TRADE][HEDGE2] event=timeout_skip bucket=%s reason=insufficient_balance", cb["ts"])
                            else:
                                LOG.warning("[TRADE][HEDGE2] event=timeout_skip bucket=%s reason=ask_too_high orig_ask=%.4f", cb["ts"], orig_ask)

            if now_ts - last_balance_check > 10 and args.live and client is not None:
                bal = _bounded(lambda: get_balance(client), timeout=3)
                if bal is not None:
                    state["_meta"]["balance"] = bal
                    pnl = 0.0
                    if start_balance is not None:
                        pnl = round(bal - start_balance, 4)
                        state["_meta"]["session_pnl"] = pnl
                    LOG.debug("[RISK] event=balance_update balance=%.4f session_pnl=%.4f", bal, pnl)
                    if pnl <= -MAX_SESSION_LOSS_USD:
                        LOG.error("[RISK] event=session_stop balance=%.4f session_pnl=%.4f max_loss=%.4f", bal, pnl, -MAX_SESSION_LOSS_USD)
                        ui.add_log(f"SESSION STOP: max loss ${MAX_SESSION_LOSS_USD} hit")
                        control["quit"] = True
                    elif target_profit is not None and start_balance is not None and pnl >= target_profit:
                        LOG.error("[RISK] event=session_stop balance=%.4f session_pnl=%.4f target_profit=%.4f", bal, pnl, target_profit)
                        ui.add_log(f"SESSION STOP: target profit ${target_profit} hit")
                        control["quit"] = True
                else:
                    LOG.warning("[RISK] event=balance_update_failed")

                last_balance_check = now_ts

            current_bucket = now_ts - (now_ts % 300)
            cb = state["current_bucket"]

            for pos_ts in list(state["positions"].keys()):
                pos = state["positions"][pos_ts]
                if pos.get("status") != "OPEN":
                    continue
                secs = _seconds_left(pos_ts)
                pos["secs_left"] = secs
                if secs <= 0:
                    pos["status"] = "SETTLED"
                    pos["closed_at"] = now_ts
                    pos["monitor_at"] = now_ts + 30
                    pos["monitored"] = False
                    if args.live and client is not None:
                        for entry in pos.get("entries", []):
                            if entry.get("status") == "RESTING" and entry.get("order_id"):
                                ok = _bounded(lambda oid=str(entry["order_id"]): cancel_order(client, oid), timeout=3, default=False)
                                if ok:
                                    entry["status"] = "CANCELLED"
                    LOG.info(
                        "[SETTLE] event=settled bucket=%s dir=%s entries=%d total_cost=%.4f closed_at=%d",
                        pos_ts,
                        pos.get("direction"),
                        len(pos.get("entries", [])),
                        pos.get("total_cost", 0),
                        now_ts,
                    )
                    ui.add_log(f"settled bucket={pos_ts} side={pos['direction']} entries={len(pos['entries'])}")
                    _sync_ws_subscriptions(state, cb)

            if args.live and client is not None:
                closed_any = False
                for pos_ts in list(state["positions"].keys()):
                    pos = state["positions"][pos_ts]
                    if not _is_simple_open_position(pos):
                        continue
                    entry = pos.get("entries", [{}])[0]
                    # _refresh_open_entry(client, entry)
                    if entry.get("status") != "FILLED":
                        continue
                    pos["total_cost"] = float(entry.get("cost") or pos.get("total_cost") or 0.0)
                    shares = float(entry.get("shares") or 0.0)
                    total_cost = float(entry.get("cost") or pos.get("total_cost") or 0.0)
                    token = _simple_open_position_token(pos)
                    profit, profit_pct, bid, _ = _bounded(lambda t=token, s=shares, c=total_cost: _estimate_exit_profit(client, t, s, c), timeout=3, default=(None, None, None, None))
                    if profit is None or profit_pct is None:
                        continue
                    pos_secs_left = _seconds_left(pos_ts)
                    if profit_pct >= TAKE_PROFIT_PCT:
                        cost_per_share = total_cost / shares if shares > 0 else 0.0
                        target_price = round(cost_per_share * (1.0 + TAKE_PROFIT_PCT), 4)
                        LOG.debug("[TRADE][EXIT] event=trigger bucket=%s reason=take_profit bid=%.4f target=%.4f profit=%+.4f profit_pct=%.4f secs_left=%d", pos_ts, bid, target_price, profit, profit_pct, pos_secs_left)
                        closed = _bounded(lambda tp=target_price: _close_simple_position(client, pos_ts, pos, "take_profit", bid, ui, target_price=tp), timeout=5, default=False)
                        if closed:
                            closed_any = True
                            continue
                    if pos_secs_left <= FORCE_EXIT_SECONDS and profit_pct > FORCE_EXIT_MIN_PROFIT_PCT:
                        LOG.debug("[TRADE][EXIT] event=trigger bucket=%s reason=force_profit_exit bid=%.4f profit=%+.4f profit_pct=%.4f secs_left=%d", pos_ts, bid, profit, profit_pct, pos_secs_left)
                        closed = _bounded(lambda: _close_simple_position(client, pos_ts, pos, "force_profit_exit", bid, ui), timeout=5, default=False)
                        if closed:
                            closed_any = True
                if closed_any:
                    _sync_ws_subscriptions(state, cb)

            for pos_ts in list(state["positions"].keys()):
                pos = state["positions"][pos_ts]
                if pos.get("status") != "SETTLED" or pos.get("monitored"):
                    continue
                if now_ts < int(pos.get("monitor_at") or 0):
                    continue

                resolved_side = _bounded(lambda: get_market_resolution(pos_ts), timeout=3, default=None)
                filled_entries = 0
                total_entries = len(pos.get("entries", []))
                total_cost = 0.0
                winning_shares = 0.0

                for entry in pos.get("entries", []):
                    order_id = str(entry.get("order_id") or "")
                    if args.live and client is not None and order_id:
                        order_data = get_order_status(client, order_id)
                        if isinstance(order_data, dict):
                            status_text = str(order_data.get("status") or "").lower()
                            matched = float(order_data.get("size_matched") or 0.0)
                            original_size = float(order_data.get("original_size") or entry.get("shares") or 0.0)
                            if matched > 0 or status_text in {"matched", "filled"}:
                                entry["status"] = "FILLED"
                                if matched > 0:
                                    entry["shares"] = matched
                            elif status_text in {"canceled", "cancelled"}:
                                entry["status"] = "CANCELLED"
                            elif status_text in {"expired", "unmatched"}:
                                entry["status"] = "EXPIRED"
                            elif status_text == "live":
                                entry["status"] = "RESTING"
                            if entry.get("status") == "FILLED" and original_size > 0 and matched <= 0:
                                entry["shares"] = original_size

                            if entry.get("status") == "FILLED" and entry.get("token") and entry.get("limit_price"):
                                raw_filled_cost = float(entry.get("shares", 0)) * float(entry.get("limit_price", 0))
                                entry["cost"] = estimate_fee_inclusive_buy_cost(
                                    client, str(entry["token"]), float(entry["limit_price"]),
                                    float(entry.get("shares", 0)), raw_filled_cost,
                                )

                    if entry.get("status") == "FILLED":
                        filled_entries += 1
                        entry_cost = float(entry.get("cost") or 0.0)
                        total_cost += entry_cost
                        if resolved_side and entry.get("side") == resolved_side:
                            winning_shares += float(entry.get("shares") or 0.0)

                pos["total_cost"] = round(total_cost, 6)

                pnl = None
                if resolved_side:
                    pnl = round(winning_shares - total_cost, 4)
                    pos["resolved_side"] = resolved_side
                    pos["pnl"] = pnl
                    pos["monitored"] = True
                else:
                    pos["monitor_at"] = int(time.time()) + 10

                LOG.info(
                    "[MONITOR] event=post_settle bucket=%s resolved_side=%s filled_entries=%d total_entries=%d winning_shares=%.4f total_cost=%.4f pnl=%s",
                    pos_ts,
                    resolved_side or "unknown",
                    filled_entries,
                    total_entries,
                    winning_shares,
                    total_cost,
                    f"{pnl:+.4f}" if pnl is not None else "unknown",
                )
                if pnl is not None:
                    ui.add_log(f"monitor bucket={pos_ts}: {resolved_side} pnl=${pnl:+.4f} filled={filled_entries}/{total_entries}")
                elif not pos.get("monitored"):
                    last_log = pos.get("last_pending_log_at", 0)
                    if now_ts - last_log >= 60:
                        ui.add_log(f"monitor bucket={pos_ts}: resolution pending, filled={filled_entries}/{total_entries}")
                        pos["last_pending_log_at"] = now_ts

            btc_price = get_btc_price()
            if btc_price is None:
                LOG.debug("[MARKET][PRICE] event=skip_loop reason=price_unavailable")
                time.sleep(0.05)
                continue
            state["_meta"]["btc_price"] = btc_price

            if cb["ts"] != current_bucket:
                cb["ts"] = current_bucket
                cb["btc_open"] = btc_price
                cb["btc_now"] = btc_price
                cb["move"] = 0.0
                cb["direction"] = ""
                cb["entries"] = 0
                cb["best_abs_move"] = 0.0
                cb["hedge_count"] = 0
                cb["last_logged_dir"] = ""
                up_token, down_token = _bounded(_get_btc_tokens, timeout=3, default=(None, None))
                if not up_token or not down_token:
                    LOG.warning("[STATE] event=bucket_open_missing_tokens bucket=%s btc_open=%.2f", current_bucket, btc_price)
                    time.sleep(0.05)
                    continue
                cb["up_token"] = up_token
                cb["down_token"] = down_token
                _sync_ws_subscriptions(state, cb)
                LOG.info(
                    "[STATE] event=bucket_open bucket=%s btc_open=%.2f up_token=%s down_token=%s",
                    current_bucket, btc_price, up_token[:8], down_token[:8]
                )
                ui.add_log(f"new bucket {current_bucket} BTC=${btc_price:.2f}")

            cb["btc_now"] = btc_price
            cb["move"] = btc_price - cb["btc_open"]
            move_abs = abs(cb["move"])

            secs_left = _seconds_left(current_bucket)

            if secs_left < MIN_SECONDS_LEFT:
                LOG.debug("[SIGNAL] event=skip bucket=%s reason=too_late secs_left=%d min_seconds_left=%d", current_bucket, secs_left, MIN_SECONDS_LEFT)
                time.sleep(0.05)
                continue

            # HEDGE: if opposite ask is rich or BTC has flipped far enough, buy opposite to recover.
            if (cb["entries"] > 0
                    and cb["ts"] in state["positions"]
                    and state["positions"][cb["ts"]].get("status") == "OPEN"
                    and cb.get("hedge_count", 0) == 0):
                pos_dir = state["positions"][cb["ts"]].get("direction", "")
                if pos_dir:
                    opp_dir = "UP" if pos_dir == "DOWN" else "DOWN"
                    opp_token = cb["up_token"] if opp_dir == "UP" else cb["down_token"]
                    if not opp_token:
                        LOG.warning("[TRADE][HEDGE] event=skip bucket=%s reason=missing_opp_token from=%s to=%s", cb["ts"], pos_dir, opp_dir)
                        time.sleep(0.05)
                        continue

                    _, opp_ask = poly_book_ws.get_best_prices(opp_token)
                    if opp_ask <= 0:
                        LOG.debug("[TRADE][HEDGE] event=skip bucket=%s reason=no_opp_ask opp_token=%s ws_status=%s", cb["ts"], opp_token[:8], poly_book_ws.get_status(opp_token))
                        time.sleep(0.05)
                        continue

                    move_flip_ready = (
                        (pos_dir == "DOWN" and cb["move"] >= HEDGE_OPPOSITE_MOVE_THRESHOLD) or
                        (pos_dir == "UP" and cb["move"] <= -HEDGE_OPPOSITE_MOVE_THRESHOLD)
                    )
                    ask_ready = opp_ask >= HEDGE_OPPOSITE_ASK_THRESHOLD
                    LOG.debug(
                        "[TRADE][HEDGE] event=check bucket=%s from=%s to=%s opp_ask=%.4f ask_ready=%s(>=%.2f) move=%+.2f move_ready=%s(>=%.1f) secs_left=%d",
                        current_bucket, pos_dir, opp_dir, opp_ask, ask_ready, HEDGE_OPPOSITE_ASK_THRESHOLD, cb["move"], move_flip_ready, HEDGE_OPPOSITE_MOVE_THRESHOLD, secs_left,
                    )
                    if ask_ready and move_flip_ready:
                        LOG.debug(
                            "[TRADE][HEDGE] event=trigger bucket=%s from=%s to=%s opp_ask=%.4f ask_ready=%s move=%+.2f move_ready=%s secs_left=%d",
                            current_bucket, pos_dir, opp_dir, opp_ask, ask_ready, cb["move"], move_flip_ready, secs_left
                        )
                        if secs_left >= 60:
                            hedge_limit_price = opp_ask - 0.01
                        else:
                            hedge_limit_price = opp_ask + 0.01
                        hedge_limit_price = round(min(max(hedge_limit_price, 0.01), 0.99), 4)

                        net_profit_per_share = get_net_profit_per_share(client, opp_token, hedge_limit_price) if client else (1.0 - hedge_limit_price) * 0.93
                        net_profit_per_share = max(net_profit_per_share, 0.005)
                        if net_profit_per_share < 0.005:
                            LOG.debug("[TRADE][HEDGE] event=skip bucket=%s reason=low_profit_per_share limit=%.4f npps=%.6f", current_bucket, hedge_limit_price, net_profit_per_share)
                            time.sleep(0.05)
                            continue

                        first_trade_cost = float(state["positions"][cb["ts"]]["entries"][0].get("cost", 0.0) or 0.0)
                        hedge_shares = round(max((first_trade_cost * 1.01) / net_profit_per_share, 1.0), 4)
                        hedge_cost = round(hedge_shares * hedge_limit_price, 4)

                        cb["direction"] = opp_dir
                        cb["hedge_pos_dir"] = pos_dir
                        cb["hedge_token"] = opp_token
                        cb["hedge_cost"] = hedge_cost
                        entry_number = cb["entries"] + 1

                        LOG.debug(
                            "[TRADE][HEDGE] event=sizing bucket=%s dir=%s first_cost=%.4f limit=%.4f npps=%.6f required_shares=%.4f required_cost=%.4f",
                            current_bucket, opp_dir, first_trade_cost, hedge_limit_price, net_profit_per_share, hedge_shares, hedge_cost,
                        )
                        ui.add_log(f"#{entry_number} HEDGE {opp_dir}: opp_ask={opp_ask:.4f} BTC_move=${cb['move']:+.2f} secs_left={secs_left}")
                        if not args.live:
                            LOG.info(
                                "[TRADE][HEDGE] event=dry_run bucket=%s dir=%s entry=%d ask=%.4f limit=%.4f shares=%.4f cost=%.4f",
                                current_bucket, opp_dir, entry_number, opp_ask, hedge_limit_price, hedge_shares, hedge_cost
                            )
                            cb["entries"] += 1
                            cb["hedge_count"] = 1
                            state["_meta"]["entry_count"] += 1
                            cb_ts = cb["ts"]
                            state["positions"][cb_ts]["entries"].append({"side": opp_dir, "move": cb["move"], "ask": opp_ask, "limit_price": hedge_limit_price, "shares": hedge_shares, "cost": hedge_cost, "status": "DRY_SKIP"})
                            state["positions"][cb_ts]["total_cost"] += hedge_cost
                            state["positions"][cb_ts]["total_shares"] += hedge_shares
                            time.sleep(GTD_ENTRY_DELAY_SECONDS)
                            continue
                        if client is None:
                            LOG.error("[ERROR] event=invariant_failed reason=missing_client mode=LIVE")
                            time.sleep(0.05)
                            continue
                        bal = get_balance(client)
                        if bal is not None and bal < hedge_cost:
                            affordable_budget = round(max(bal * 0.98, 0.0), 4)
                            affordable_shares = round(max(affordable_budget / hedge_limit_price, 0.0), 4)
                            if affordable_shares < 1.0:
                                LOG.debug("[RISK] event=hedge_skip bucket=%s reason=insufficient_balance balance=%.4f limit=%.4f", current_bucket, bal, hedge_limit_price)
                                ui.add_log(f"skip hedge: balance ${bal:.2f} < 1 share @ ${hedge_limit_price:.2f}")
                                time.sleep(0.05)
                                continue
                            hedge_shares = affordable_shares
                            hedge_cost = round(hedge_shares * hedge_limit_price, 4)
                            LOG.debug(
                                "[RISK] event=hedge_resize bucket=%s balance=%.4f budget=%.4f shares=%.4f cost=%.4f target_1pct_not_guaranteed=true",
                                current_bucket, bal, affordable_budget, hedge_shares, hedge_cost
                            )
                            ui.add_log(f"partial hedge: 1% target not guaranteed, balance cap sh={hedge_shares:.4f} cost=${hedge_cost:.4f}")
                        tick_size = get_tick_size(client, opp_token)
                        if secs_left < 30:
                            entry, outcome = place_market_buy_fok(client, current_bucket, opp_token, hedge_cost)
                        else:
                            entry, outcome = place_gtd_limit_order(client, current_bucket, opp_token, opp_ask, tick_size, shares=hedge_shares, limit_price=hedge_limit_price)
                            cb["hedge_order_id"] = entry.order_id if outcome and entry else ""
                            cb["hedge_placed_at"] = now_ts
                            cb["hedge_fok_fallback"] = False
                            cb["hedge_repriced"] = secs_left < 60
                            cb["hedge_cost"] = hedge_cost
                            cb["hedge_token"] = opp_token
                        if entry is None:
                            LOG.error(
                                "[TRADE][HEDGE] event=failed bucket=%s dir=%s ask=%.4f limit=%.4f shares=%.4f cost=%.4f outcome=%s",
                                current_bucket, opp_dir, opp_ask, hedge_limit_price, hedge_shares, hedge_cost, outcome
                            )
                            ui.add_log(f"hedge failed: {outcome}")
                            time.sleep(0.05)
                            continue
                        bal_after = apply_fee_cost_and_refresh_balance(client, bal, entry)
                        if bal_after is not None:
                            state["_meta"]["balance"] = bal_after
                        cb["entries"] += 1
                        cb["hedge_count"] = 1
                        state["_meta"]["entry_count"] += 1
                        cb_ts = cb["ts"]
                        state["positions"][cb_ts]["entries"].append({"side": opp_dir, "move": cb["move"], "ask": opp_ask, "limit_price": entry.limit_price, "shares": entry.shares, "cost": entry.cost, "status": entry.status, "order_id": entry.order_id})
                        state["positions"][cb_ts]["total_cost"] += entry.cost
                        state["positions"][cb_ts]["total_shares"] += entry.shares
                        LOG.debug(
                            "[TRADE][HEDGE] event=placed bucket=%s dir=%s entry=%d status=%s limit=%.4f shares=%.4f cost=%.4f order_id=%s",
                            current_bucket, opp_dir, entry_number, entry.status, entry.limit_price, entry.shares, entry.cost, entry.order_id[:10]
                        )
                        if entry.status == "RESTING":
                            LOG.debug("[MONITOR] event=watch_order bucket=%s order_id=%s status=RESTING kind=hedge", current_bucket, entry.order_id[:10])
                        ui.add_log(f"#{entry_number} HEDGE {entry.status}: {opp_dir} sh={entry.shares:.4f} limit={entry.limit_price:.4f} cost=${entry.cost:.4f} order={entry.order_id[:10]}")
                        time.sleep(GTD_ENTRY_DELAY_SECONDS)
                        continue
                    else:
                        LOG.debug(
                            "[TRADE][HEDGE] event=skip bucket=%s reason=conditions_not_met opp_ask=%.4f ask_ready=%s move=%+.2f move_ready=%s secs_left=%d",
                            current_bucket, opp_ask, ask_ready, cb["move"], move_flip_ready, secs_left,
                        )

            # HEDGE2: if market flips back to original entry side, buy that side again to recover hedge loss plus 1%.
            if (cb["entries"] > 1
                    and cb["ts"] in state["positions"]
                    and state["positions"][cb["ts"]].get("status") == "OPEN"
                    and cb.get("hedge_count", 0) == 1):
                pos = state["positions"][cb["ts"]]
                orig_dir = pos.get("direction", "")
                if orig_dir:
                    hedge2_token = cb["up_token"] if orig_dir == "UP" else cb["down_token"]
                    if not hedge2_token:
                        LOG.warning("[TRADE][HEDGE2] event=skip bucket=%s reason=missing_token dir=%s", cb["ts"], orig_dir)
                        time.sleep(0.05)
                        continue

                    _, orig_ask = poly_book_ws.get_best_prices(hedge2_token)
                    if orig_ask <= 0:
                        LOG.debug("[TRADE][HEDGE2] event=skip bucket=%s reason=no_ask token=%s ws_status=%s", cb["ts"], hedge2_token[:8], poly_book_ws.get_status(hedge2_token))
                        time.sleep(0.05)
                        continue

                    move_to_orig = cb["move"] if orig_dir == "UP" else -cb["move"]
                    move2_ready = move_to_orig >= HEDGE2_MOVE_THRESHOLD
                    ask2_ready = orig_ask >= HEDGE2_ASK_THRESHOLD
                    if move2_ready and ask2_ready:
                        LOG.debug(
                            "[TRADE][HEDGE2] event=trigger bucket=%s to=%s ask=%.4f ask_ready=%s move=%+.2f move_to_orig=%.2f move_ready=%s secs_left=%d",
                            current_bucket, orig_dir, orig_ask, ask2_ready, cb["move"], move_to_orig, move2_ready, secs_left
                        )
                        cb["direction"] = orig_dir
                        entry_number = cb["entries"] + 1
                        if secs_left >= 60:
                            hedge2_limit_price = orig_ask - 0.01
                        else:
                            hedge2_limit_price = orig_ask + 0.01
                        hedge2_limit_price = round(min(max(hedge2_limit_price, 0.01), 0.99), 4)

                        net_profit_per_share2 = get_net_profit_per_share(client, hedge2_token, hedge2_limit_price) if client else (1.0 - hedge2_limit_price) * 0.93
                        if net_profit_per_share2 < 0.005:
                            LOG.debug("[TRADE][HEDGE2] event=skip bucket=%s reason=low_profit_per_share limit=%.4f npps=%.6f", current_bucket, hedge2_limit_price, net_profit_per_share2)
                            time.sleep(0.05)
                            continue

                        first_entry = pos.get("entries", [{}])[0]
                        second_entry = pos.get("entries", [{}, {}])[1] if len(pos.get("entries", [])) > 1 else {}
                        first_trade_cost = float(first_entry.get("cost", 0.0) or 0.0)
                        first_trade_shares = float(first_entry.get("shares", 0.0) or 0.0)
                        first_hedge_cost = float(second_entry.get("cost", 0.0) or 0.0)

                        target_recovery = (1.01 * (first_trade_cost + first_hedge_cost)) - first_trade_shares
                        hedge2_shares = round(max(target_recovery / net_profit_per_share2, 1.0), 4)
                        hedge2_cost = round(hedge2_shares * hedge2_limit_price, 4)
                        LOG.debug(
                            "[TRADE][HEDGE2] event=sizing bucket=%s dir=%s first_cost=%.4f hedge1_cost=%.4f first_shares=%.4f limit=%.4f npps=%.6f target_recovery=%.4f required_shares=%.4f required_cost=%.4f",
                            current_bucket, orig_dir, first_trade_cost, first_hedge_cost, first_trade_shares, hedge2_limit_price, net_profit_per_share2, target_recovery, hedge2_shares, hedge2_cost,
                        )
                        ui.add_log(f"#{entry_number} HEDGE2 {orig_dir}: ask={orig_ask:.4f} BTC_move=${cb['move']:+.2f} secs_left={secs_left}")

                        cb["hedge2_orig_dir"] = orig_dir
                        cb["hedge2_token"] = hedge2_token
                        cb["hedge2_cost"] = hedge2_cost

                        if not args.live:
                            LOG.info(
                                "[TRADE][HEDGE2] event=dry_run bucket=%s dir=%s entry=%d ask=%.4f limit=%.4f shares=%.4f cost=%.4f",
                                current_bucket, orig_dir, entry_number, orig_ask, hedge2_limit_price, hedge2_shares, hedge2_cost
                            )
                            cb["entries"] += 1
                            cb["hedge_count"] = 2
                            state["_meta"]["entry_count"] += 1
                            cb_ts = cb["ts"]
                            state["positions"][cb_ts]["entries"].append({"side": orig_dir, "move": cb["move"], "ask": orig_ask, "limit_price": hedge2_limit_price, "shares": hedge2_shares, "cost": hedge2_cost, "status": "DRY_SKIP"})
                            state["positions"][cb_ts]["total_cost"] += hedge2_cost
                            state["positions"][cb_ts]["total_shares"] += hedge2_shares
                            time.sleep(GTD_ENTRY_DELAY_SECONDS)
                            continue

                        if client is None:
                            LOG.error("[ERROR] event=invariant_failed reason=missing_client mode=LIVE")
                            time.sleep(0.05)
                            continue

                        bal = get_balance(client)
                        if bal is not None and bal < hedge2_cost:
                            affordable_budget = round(max(bal * 0.98, 0.0), 4)
                            affordable_shares = round(max(affordable_budget / hedge2_limit_price, 0.0), 4)
                            if affordable_shares < 1.0:
                                LOG.debug("[RISK] event=hedge2_skip bucket=%s reason=insufficient_balance balance=%.4f limit=%.4f", current_bucket, bal, hedge2_limit_price)
                                ui.add_log(f"skip hedge2: balance ${bal:.2f} < 1 share @ ${hedge2_limit_price:.2f}")
                                time.sleep(0.05)
                                continue
                            hedge2_shares = affordable_shares
                            hedge2_cost = round(hedge2_shares * hedge2_limit_price, 4)
                            LOG.debug(
                                "[RISK] event=hedge2_resize bucket=%s balance=%.4f budget=%.4f shares=%.4f cost=%.4f target_1pct_not_guaranteed=true",
                                current_bucket, bal, affordable_budget, hedge2_shares, hedge2_cost
                            )
                            ui.add_log(f"partial hedge2: 1% target not guaranteed, balance cap sh={hedge2_shares:.4f} cost=${hedge2_cost:.4f}")

                        tick_size = get_tick_size(client, hedge2_token)
                        if secs_left < 30:
                            entry, outcome = place_market_buy_fok(client, current_bucket, hedge2_token, hedge2_cost)
                        else:
                            entry, outcome = place_gtd_limit_order(client, current_bucket, hedge2_token, orig_ask, tick_size, shares=hedge2_shares, limit_price=hedge2_limit_price)
                            cb["hedge2_order_id"] = entry.order_id if outcome and entry else ""
                            cb["hedge2_placed_at"] = now_ts
                            cb["hedge2_fok_fallback"] = False
                        if entry is None:
                            LOG.error(
                                "[TRADE][HEDGE2] event=failed bucket=%s dir=%s ask=%.4f limit=%.4f shares=%.4f cost=%.4f outcome=%s",
                                current_bucket, orig_dir, orig_ask, hedge2_limit_price, hedge2_shares, hedge2_cost, outcome
                            )
                            ui.add_log(f"hedge2 failed: {outcome}")
                            time.sleep(0.05)
                            continue

                        bal_after = apply_fee_cost_and_refresh_balance(client, bal, entry)
                        if bal_after is not None:
                            state["_meta"]["balance"] = bal_after

                        cb["entries"] += 1
                        cb["hedge_count"] = 2
                        state["_meta"]["entry_count"] += 1
                        cb_ts = cb["ts"]
                        state["positions"][cb_ts]["entries"].append({"side": orig_dir, "move": cb["move"], "ask": orig_ask, "limit_price": entry.limit_price, "shares": entry.shares, "cost": entry.cost, "status": entry.status, "order_id": entry.order_id})
                        state["positions"][cb_ts]["total_cost"] += entry.cost
                        state["positions"][cb_ts]["total_shares"] += entry.shares
                        LOG.debug(
                            "[TRADE][HEDGE2] event=placed bucket=%s dir=%s entry=%d status=%s limit=%.4f shares=%.4f cost=%.4f order_id=%s",
                            current_bucket, orig_dir, entry_number, entry.status, entry.limit_price, entry.shares, entry.cost, entry.order_id[:10]
                        )
                        if entry.status == "RESTING":
                            LOG.debug("[MONITOR] event=watch_order bucket=%s order_id=%s status=RESTING kind=hedge2", current_bucket, entry.order_id[:10])
                        ui.add_log(f"#{entry_number} HEDGE2 {entry.status}: {orig_dir} sh={entry.shares:.4f} limit={entry.limit_price:.4f} cost=${entry.cost:.4f} order={entry.order_id[:10]}")
                        time.sleep(GTD_ENTRY_DELAY_SECONDS)
                        continue

            if move_abs < BTC_MOVE_THRESHOLD:
                LOG.debug("[SIGNAL] event=below_threshold bucket=%s move=%+.2f move_abs=%.2f threshold=%.2f", current_bucket, cb["move"], move_abs, BTC_MOVE_THRESHOLD)
                time.sleep(0.05)
                continue

            cb["direction"] = "UP" if cb["move"] > 0 else "DOWN"
            if cb["direction"] != cb.get("last_logged_dir"):
                LOG.debug("[SIGNAL] event=threshold_cross bucket=%s move=%+.2f move_abs=%.2f threshold=%.2f dir=%s", current_bucket, cb["move"], move_abs, BTC_MOVE_THRESHOLD, cb["direction"])
                ui.add_log(f"BTC moved ${cb['move']:+.2f} → direction {cb['direction']}")
                cb["last_logged_dir"] = cb["direction"]

            if cb["entries"] >= MAX_ENTRIES_PER_BUCKET:
                LOG.debug("[RISK] event=skip bucket=%s reason=max_entries entries=%d max_entries=%d", current_bucket, cb["entries"], MAX_ENTRIES_PER_BUCKET)
                time.sleep(0.05)
                continue

            if cb["entries"] == 0 and secs_left > INITIAL_ENTRY_MAX_SECS_LEFT:
                LOG.debug("[RISK] event=skip bucket=%s reason=too_early_initial secs_left=%d max_secs_left=%d", current_bucket, secs_left, INITIAL_ENTRY_MAX_SECS_LEFT)
                time.sleep(0.05)
                continue

            if cb["entries"] == 0 and secs_left <= INITIAL_ENTRY_MIN_SECS_LEFT:
                LOG.debug("[RISK] event=skip bucket=%s reason=too_late_initial secs_left=%d min_secs_left=%d", current_bucket, secs_left, INITIAL_ENTRY_MIN_SECS_LEFT)
                time.sleep(0.05)
                continue

            active_buckets = sum(1 for ts, p in state["positions"].items() if p.get("status") == "OPEN")
            if active_buckets >= MAX_CONCURRENT_BUCKETS:
                LOG.debug("[RISK] event=skip bucket=%s reason=max_concurrent active_buckets=%d max_buckets=%d", current_bucket, active_buckets, MAX_CONCURRENT_BUCKETS)
                time.sleep(0.05)
                continue

            token = cb["up_token"] if cb["direction"] == "UP" else cb["down_token"]
            if not token:
                LOG.warning("[SIGNAL] event=skip bucket=%s dir=%s reason=missing_token", current_bucket, cb["direction"])
                time.sleep(0.05)
                continue

            _, ask = poly_book_ws.get_best_prices(token)
            if ask <= 0:
                LOG.debug("[MARKET][BOOK] event=skip bucket=%s token=%s reason=no_ask ws_status=%s", current_bucket, token[:8], poly_book_ws.get_status(token))
                time.sleep(0.05)
                continue

            if cb["entries"] == 0 and ask >= INITIAL_ENTRY_MAX_ASK:
                LOG.debug("[RISK] event=skip bucket=%s reason=ask_too_high token=%s ask=%.4f max_ask=%.4f", current_bucket, token[:8], ask, INITIAL_ENTRY_MAX_ASK)
                time.sleep(0.05)
                continue

            # check for flip
            is_flip = False
            if cb["entries"] > 0 and cb["ts"] in state["positions"]:
                 pos_dir = state["positions"][cb["ts"]].get("direction", "")
                 if pos_dir and cb["direction"] != pos_dir:
                     is_flip = True
                     LOG.debug("[SIGNAL] event=flip_detected bucket=%s prev_dir=%s dir=%s move=%+.2f", current_bucket, pos_dir, cb["direction"], cb["move"])

            threshold = FLIP_MOVE_THRESHOLD if is_flip else BTC_MOVE_THRESHOLD
            
            if cb["entries"] > 0 and not is_flip and move_abs <= cb.get("best_abs_move", 0.0):
                LOG.debug("[SIGNAL] event=skip bucket=%s reason=not_extending move=%+.2f move_abs=%.2f best_abs_move=%.2f", current_bucket, cb["move"], move_abs, cb.get('best_abs_move', 0.0))
                # ui.add_log suppressed for not_extending (too noisy)
                time.sleep(0.05)
                continue
            
            if is_flip and move_abs < threshold:
                LOG.debug("[SIGNAL] event=skip bucket=%s reason=flip_below_threshold move=%+.2f move_abs=%.2f threshold=%.2f", current_bucket, cb["move"], move_abs, threshold)
                # ui.add_log suppressed for flip_below_threshold (too noisy)
                time.sleep(0.05)
                continue

            entry_number = cb["entries"] + 1
            ui.add_log(
                f"#{entry_number} {cb['direction']} entry: BTC_move=${cb['move']:+.2f} "
                f"ask={ask:.4f} secs_left={secs_left}"
            )

            if not args.live:
                dry_limit = round(max(ask + 0.01, 0.01), 4)
                dry_shares = float(ENTRY_SHARES)
                dry_cost = round(dry_shares * dry_limit, 4)
                LOG.info(
                    "[TRADE][ENTRY] event=dry_run bucket=%s dir=%s entry=%d token=%s ask=%.4f limit=%.4f shares=%.4f cost=%.4f",
                    current_bucket, cb["direction"], entry_number, token[:8], ask, dry_limit, dry_shares, dry_cost
                )
                cb["entries"] += 1
                state["_meta"]["entry_count"] += 1
                cb_ts = cb["ts"]
                if cb_ts not in state["positions"]:
                    state["positions"][cb_ts] = {
                        "bucket_ts": cb_ts,
                        "direction": cb["direction"],
                        "entries": [],
                        "total_cost": 0.0,
                        "total_shares": 0.0,
                        "status": "OPEN",
                        "secs_left": secs_left,
                        "btc_open": cb["btc_open"],
                        "btc_now": cb["btc_now"],
                        "up_token": cb.get("up_token", ""),
                        "down_token": cb.get("down_token", ""),
                    }
                state["positions"][cb_ts]["entries"].append({
                    "side": cb["direction"],
                    "ask": ask,
                    "limit_price": dry_limit,
                    "shares": dry_shares,
                    "cost": dry_cost,
                    "status": "DRY_SKIP",
                })
                state["positions"][cb_ts]["total_cost"] += dry_cost
                time.sleep(0.05)
                continue

            if client is None:
                LOG.error("[ERROR] event=invariant_failed reason=missing_client mode=LIVE")
                time.sleep(0.05)
                continue

            bal = get_balance(client)
            entry_cost_estimate = ENTRY_SHARES * round(ask + 0.01, 4)
            if bal is not None and bal < entry_cost_estimate:
                LOG.debug("[RISK] event=skip bucket=%s reason=insufficient_balance balance=%.4f required=%.4f", current_bucket, bal, entry_cost_estimate)
                ui.add_log(f"skip: balance ${bal:.2f} < ${entry_cost_estimate:.2f}")
                time.sleep(0.05)
                continue

            tick_size = get_tick_size(client, token)
            LOG.info("[TRADE][ENTRY] event=submit bucket=%s dir=%s entry=%d token=%s ask=%.4f secs_left=%d mode=LIVE", current_bucket, cb["direction"], entry_number, token[:8], ask, secs_left)
            entry, outcome = place_gtd_limit_order(
                client, current_bucket, token, ask, tick_size, shares=ENTRY_SHARES, limit_price=round(ask + 0.01, 4)
            )
            if entry is None and "invalid amount for a marketable BUY order" in outcome and "min size: $1" in outcome:
                retry_amount_usd = max(STAKE_USD_PER_ENTRY, 1.0)
                LOG.debug("[TRADE][ENTRY] event=retry_marketable_min bucket=%s dir=%s entry=%d token=%s type=FOK amount_usd=%.4f", current_bucket, cb["direction"], entry_number, token[:8], retry_amount_usd)
                ui.add_log(f"retry entry: FOK ${retry_amount_usd:.2f} min notional")
                entry, outcome = place_market_buy_fok(client, current_bucket, token, retry_amount_usd)
            if entry is None:
                LOG.error("[TRADE][ENTRY] event=failed bucket=%s dir=%s entry=%d token=%s outcome=%s", current_bucket, cb["direction"], entry_number, token[:8], outcome)
                ui.add_log(f"order failed: {outcome}")
                time.sleep(0.05)
                continue

            bal_after = apply_fee_cost_and_refresh_balance(client, bal, entry)
            if bal_after is not None:
                state["_meta"]["balance"] = bal_after

            cb["entries"] += 1
            state["_meta"]["entry_count"] += 1
            cb_ts = cb["ts"]
            if cb_ts not in state["positions"]:
                state["positions"][cb_ts] = {
                    "bucket_ts": cb_ts,
                    "direction": cb["direction"],
                    "entries": [],
                    "total_cost": 0.0,
                    "total_shares": 0.0,
                    "status": "OPEN",
                    "secs_left": secs_left,
                    "btc_open": cb["btc_open"],
                    "btc_now": cb["btc_now"],
                    "up_token": cb.get("up_token", ""),
                    "down_token": cb.get("down_token", ""),
                }
            state["positions"][cb_ts]["entries"].append({
                "side": cb["direction"],
                "move": cb["move"],
                "ask": ask,
                "limit_price": entry.limit_price,
                "shares": entry.shares,
                "cost": entry.cost,
                "status": entry.status,
                "order_id": entry.order_id,
            })
            state["positions"][cb_ts]["total_cost"] += entry.cost
            state["positions"][cb_ts]["total_shares"] += entry.shares
            cb["best_abs_move"] = move_abs
            LOG.info(
                "[TRADE][ENTRY] event=placed bucket=%s dir=%s entry=%d status=%s limit=%.4f shares=%.4f cost=%.4f order_id=%s",
                current_bucket, cb["direction"], entry_number, entry.status, entry.limit_price, entry.shares, entry.cost, entry.order_id[:10]
            )
            if entry.status == "RESTING":
                LOG.debug("[MONITOR] event=watch_order bucket=%s order_id=%s status=RESTING kind=entry", current_bucket, entry.order_id[:10])
            ui.add_log(
                f"#{entry_number} {entry.status}: {cb['direction']} sh={entry.shares:.4f} "
                f"limit={entry.limit_price:.4f} cost=${entry.cost:.4f} order={entry.order_id[:10]}"
            )
            time.sleep(GTD_ENTRY_DELAY_SECONDS)

            time.sleep(0.05)

            if cb["ts"] in state["positions"]:
                cb_pos = state["positions"][cb["ts"]]
                if cb_pos.get("status") == "SETTLED":
                    cb["entries"] = 0
                    cb["direction"] = ""
                    cb["best_abs_move"] = 0.0

            state["_meta"]["active_buckets"] = sum(1 for p in state["positions"].values() if p.get("status") == "OPEN")

    except KeyboardInterrupt:
        ui.add_log("interrupted")
        LOG.warning("BOT STOPPED: keyboard interrupt")
    except Exception as exc:
        ui.add_log(f"BOT CRASH: {exc}")
        LOG.critical("BOT CRASH: %r", exc, exc_info=True)
        raise
    finally:
        control["quit"] = True
        ui.stop()
        LOG.info("BOT EXITED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


