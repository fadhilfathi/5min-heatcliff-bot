#!/usr/bin/env python3
"""copy-trade-bot/main.py — Autonomous BTC 5m directional momentum bot.
Places GTD limit orders below ask when BTC moves $30+ since bucket open.
Scales in (up to 3 entries per bucket) if BTC keeps moving same direction.
Rides all positions to settlement. No sells."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
BOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BOT_DIR))

from dotenv import load_dotenv

from config import (
    BTC_MOVE_THRESHOLD,
    LOG_DIR,
    MAX_CONCURRENT_BUCKETS,
    MAX_ENTRIES_PER_BUCKET,
    MAX_SESSION_LOSS_USD,
    MIN_SECONDS_LEFT,
    STAKE_USD_PER_ENTRY,
    GTD_ENTRY_DELAY_SECONDS,
    FLIP_MOVE_THRESHOLD,
    HEDGE_OPPOSITE_ASK_THRESHOLD,
    HEDGE_OPPOSITE_MOVE_THRESHOLD,
)
from book import fetch_best_prices
from executor import (
    auth_client,
    cancel_order,
    fetch_condition_id,
    get_balance,
    get_tick_size,
    place_gtd_limit_order,
    redeem_position,
)
from models import Entry
from price_feed import get_btc_price
from tui import CopyTradeUI

LOG = logging.getLogger("copy_trade")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=str(ROOT_DIR / ".env"))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


def _setup_logging(verbose: bool) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / f"copy_trade_{int(time.time())}.log", encoding="utf-8")],
    )


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
    try:
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data and data[0].get("markets"):
            token_ids = data[0]["markets"][0].get("clobTokenIds")
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if token_ids and len(token_ids) >= 2:
                return token_ids[0], token_ids[1]
    except Exception as exc:
        LOG.warning("token resolution failed: %r", exc)
    return "", ""


def main() -> int:
    args = parse_args()
    load_dotenv(Path(args.env_file))
    _setup_logging(args.verbose)
    mode = "LIVE" if args.live else "DRY"
    state = _build_state()
    state["_meta"]["mode"] = mode
    control: dict[str, Any] = {"paused": False, "quit": False}
    ui = CopyTradeUI(state, control)
    ui.add_log("starting BTC directional momentum bot")
    ui.add_log(f"mode={mode} stake=${STAKE_USD_PER_ENTRY} threshold=${BTC_MOVE_THRESHOLD} max_entries={MAX_ENTRIES_PER_BUCKET}")

    client = None
    start_balance = None
    if args.live:
        try:
            client = auth_client()
            start_balance = get_balance(client)
            state["_meta"]["balance"] = start_balance or 0.0
            ui.add_log(f"live balance: ${start_balance:.4f}")
        except Exception as exc:
            ui.add_log(f"FATAL: auth failed: {exc}")
            LOG.error("auth failed: %r", exc)
            return 1

    last_balance_check = 0.0

    import threading
    threading.Thread(target=ui.start, daemon=True).start()

    try:
        while not control.get("quit"):
            if control.get("paused"):
                time.sleep(0.05)
                continue

            state["_meta"]["poll_count"] += 1
            now_ts = int(time.time())
            current_bucket = now_ts - (now_ts % 300)
            cb = state["current_bucket"]

            if now_ts - last_balance_check > 10 and args.live and client is not None:
                bal = get_balance(client)
                if bal is not None:
                    state["_meta"]["balance"] = bal
                    if start_balance is not None:
                        state["_meta"]["session_pnl"] = round(bal - start_balance, 4)
                    if state["_meta"]["session_pnl"] <= -MAX_SESSION_LOSS_USD:
                        ui.add_log(f"SESSION STOP: max loss ${MAX_SESSION_LOSS_USD} hit")
                        control["quit"] = True
                last_balance_check = now_ts

            for pos_ts in list(state["positions"].keys()):
                pos = state["positions"][pos_ts]
                if pos.get("status") != "OPEN":
                    continue
                secs = _seconds_left(pos_ts)
                pos["secs_left"] = secs
                if secs <= 0:
                    pos["status"] = "SETTLED"
                    pos["closed_at"] = now_ts
                    ui.add_log(f"settled bucket={pos_ts} side={pos['direction']} entries={len(pos['entries'])}")
                    LOG.info("SETTLED: bucket=%s side=%s entries=%d cost=%.4f", pos_ts, pos['direction'], len(pos['entries']), pos.get('total_cost', 0))

            btc_price = get_btc_price()
            if btc_price is None:
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
                up_token, down_token = _get_btc_tokens()
                cb["up_token"] = up_token
                cb["down_token"] = down_token
                ui.add_log(f"new bucket {current_bucket} BTC=${btc_price:.2f}")
                LOG.info("new bucket %s BTC=%.2f up=%s down=%s", current_bucket, btc_price, up_token[:8], down_token[:8])

            cb["btc_now"] = btc_price
            cb["move"] = btc_price - cb["btc_open"]
            move_abs = abs(cb["move"])

            secs_left = _seconds_left(current_bucket)

            if secs_left < MIN_SECONDS_LEFT:
                time.sleep(0.05)
                continue

            # HEDGE: if opposite ask is rich or BTC has flipped far enough, buy opposite to recover.
            if (cb["entries"] > 0
                    and cb["ts"] in state["positions"]):
                pos_dir = state["positions"][cb["ts"]].get("direction", "")
                if pos_dir:
                    opp_dir = "UP" if pos_dir == "DOWN" else "DOWN"
                    opp_token = cb["up_token"] if opp_dir == "UP" else cb["down_token"]
                    if opp_token:
                        _, opp_ask = fetch_best_prices(opp_token)
                        move_flip_ready = (
                            (pos_dir == "DOWN" and cb["move"] >= HEDGE_OPPOSITE_MOVE_THRESHOLD) or
                            (pos_dir == "UP" and cb["move"] <= -HEDGE_OPPOSITE_MOVE_THRESHOLD)
                        )
                        ask_ready = opp_ask >= HEDGE_OPPOSITE_ASK_THRESHOLD
                        if ask_ready or move_flip_ready:
                            cb["direction"] = opp_dir
                            entry_number = cb["entries"] + 1
                            hedge_limit_price = round(max(opp_ask - 0.01, 0.01), 4)
                            hedge_profit_per_share = max(1.0 - hedge_limit_price, 0.01)
                            first_trade_cost = float(state["positions"][cb["ts"]]["entries"][0].get("cost", 0.0) or 0.0)
                            hedge_shares = round(max(first_trade_cost / hedge_profit_per_share, 1.0), 4)
                            hedge_cost = round(hedge_shares * hedge_limit_price, 4)
                            ui.add_log(f"#{entry_number} HEDGE {opp_dir}: opp_ask={opp_ask:.4f} BTC_move=${cb['move']:+.2f} secs_left={secs_left}")
                            LOG.info("HEDGE: bucket=%s from=%s to=%s opp_ask=%.4f ask_ready=%s move=%+.2f move_ready=%s first_cost=%.4f hedge_shares=%.4f hedge_cost=%.4f", current_bucket, pos_dir, opp_dir, opp_ask, ask_ready, cb["move"], move_flip_ready, first_trade_cost, hedge_shares, hedge_cost)
                            if not args.live:
                                cb["entries"] += 1
                                state["_meta"]["entry_count"] += 1
                                cb_ts = cb["ts"]
                                state["positions"][cb_ts]["entries"].append({"side": opp_dir, "move": cb["move"], "ask": opp_ask, "limit_price": hedge_limit_price, "shares": hedge_shares, "cost": hedge_cost, "status": "DRY_SKIP"})
                                state["positions"][cb_ts]["total_cost"] += hedge_cost
                                state["positions"][cb_ts]["total_shares"] += hedge_shares
                                time.sleep(GTD_ENTRY_DELAY_SECONDS)
                                continue
                            if client is None:
                                time.sleep(0.05)
                                continue
                            bal = get_balance(client)
                            if bal is not None and bal < hedge_cost:
                                affordable_budget = round(max(bal * 0.98, 0.0), 4)
                                affordable_shares = round(max(affordable_budget / hedge_limit_price, 0.0), 4)
                                if affordable_shares < 1.0:
                                    ui.add_log(f"skip hedge: balance ${bal:.2f} < 1 share @ ${hedge_limit_price:.2f}")
                                    LOG.info("HEDGE SKIP: bucket=%s balance=%.4f limit=%.4f need_at_least=1 share", current_bucket, bal, hedge_limit_price)
                                    time.sleep(0.05)
                                    continue
                                hedge_shares = affordable_shares
                                hedge_cost = round(hedge_shares * hedge_limit_price, 4)
                                ui.add_log(f"partial hedge: balance cap sh={hedge_shares:.4f} cost=${hedge_cost:.4f}")
                                LOG.info("HEDGE PARTIAL: bucket=%s balance=%.4f budget=%.4f shares=%.4f cost=%.4f", current_bucket, bal, affordable_budget, hedge_shares, hedge_cost)
                            tick_size = get_tick_size(client, opp_token)
                            entry, outcome = place_gtd_limit_order(client, current_bucket, opp_token, opp_ask, tick_size, shares=hedge_shares)
                            if entry is None:
                                ui.add_log(f"hedge failed: {outcome}")
                                LOG.error("HEDGE FAILED: bucket=%s dir=%s ask=%.4f limit=%.4f shares=%.4f cost=%.4f outcome=%s", current_bucket, opp_dir, opp_ask, hedge_limit_price, hedge_shares, hedge_cost, outcome)
                                time.sleep(0.05)
                                continue
                            cb["entries"] += 1
                            state["_meta"]["entry_count"] += 1
                            cb_ts = cb["ts"]
                            state["positions"][cb_ts]["entries"].append({"side": entry.side, "move": cb["move"], "ask": opp_ask, "limit_price": entry.limit_price, "shares": entry.shares, "cost": entry.cost, "status": entry.status, "order_id": entry.order_id})
                            state["positions"][cb_ts]["total_cost"] += entry.cost
                            state["positions"][cb_ts]["total_shares"] += entry.shares
                            state["_meta"]["balance"] = bal if bal is not None else state["_meta"]["balance"]
                            ui.add_log(f"#{entry_number} HEDGE {entry.status}: {entry.side} sh={entry.shares:.4f} limit={entry.limit_price:.4f} cost=${entry.cost:.4f} order={entry.order_id[:10]}")
                            time.sleep(GTD_ENTRY_DELAY_SECONDS)
                            continue

            if move_abs < BTC_MOVE_THRESHOLD:
                time.sleep(0.05)
                continue

            cb["direction"] = "UP" if cb["move"] > 0 else "DOWN"
            ui.add_log(f"BTC moved ${cb['move']:+.2f} → direction {cb['direction']}")

            if cb["entries"] >= MAX_ENTRIES_PER_BUCKET:
                time.sleep(0.05)
                continue

            active_buckets = sum(1 for ts, p in state["positions"].items() if p.get("status") == "OPEN")
            if active_buckets >= MAX_CONCURRENT_BUCKETS:
                time.sleep(0.05)
                continue

            token = cb["up_token"] if cb["direction"] == "UP" else cb["down_token"]
            if not token:
                time.sleep(0.05)
                continue

            _, ask = fetch_best_prices(token)
            if ask <= 0:
                time.sleep(0.05)
                continue

            # check for flip
            is_flip = cb["entries"] > 0 and (
                (cb["direction"] == "UP" and state["positions"][cb_ts]["direction"] == "DOWN") or
                (cb["direction"] == "DOWN" and state["positions"][cb_ts]["direction"] == "UP")
            )
            
            threshold = FLIP_MOVE_THRESHOLD if is_flip else BTC_MOVE_THRESHOLD
            
            if cb["entries"] > 0 and not is_flip and move_abs <= cb.get("best_abs_move", 0.0):
                ui.add_log(f"skip #{cb['entries']+1}: move ${cb['move']:+.2f} not extending best ${cb.get('best_abs_move', 0.0):+.2f}")
                time.sleep(0.05)
                continue
            
            if is_flip and move_abs < threshold:
                ui.add_log(f"skip flip #{cb['entries']+1}: move ${cb['move']:+.2f} < threshold ${threshold}")
                time.sleep(0.05)
                continue

            entry_number = cb["entries"] + 1
            ui.add_log(
                f"#{entry_number} {cb['direction']} entry: BTC_move=${cb['move']:+.2f} "
                f"ask={ask:.4f} secs_left={secs_left}"
            )

            if not args.live:
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
                    }
                state["positions"][cb_ts]["entries"].append({
                    "side": cb["direction"],
                    "ask": ask,
                    "limit_price": round(max(ask - 0.01, 0.01), 4),
                    "shares": round(STAKE_USD_PER_ENTRY / max(ask - 0.01, 0.01), 4),
                    "cost": STAKE_USD_PER_ENTRY,
                    "status": "DRY_SKIP",
                })
                state["positions"][cb_ts]["total_cost"] += STAKE_USD_PER_ENTRY
                time.sleep(0.05)
                continue

            if client is None:
                time.sleep(0.05)
                continue

            bal = get_balance(client)
            if bal is not None and bal < STAKE_USD_PER_ENTRY:
                ui.add_log(f"skip: balance ${bal:.2f} < ${STAKE_USD_PER_ENTRY}")
                time.sleep(0.05)
                continue

            tick_size = get_tick_size(client, token)
            entry, outcome = place_gtd_limit_order(
                client, current_bucket, token, ask, tick_size
            )
            if entry is None:
                ui.add_log(f"order failed: {outcome}")
                time.sleep(0.05)
                continue

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
                }
            state["positions"][cb_ts]["entries"].append({
                "side": entry.side,
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
            state["_meta"]["balance"] = bal if bal is not None else state["_meta"]["balance"]
            ui.add_log(
                f"#{entry_number} {entry.status}: {entry.side} sh={entry.shares:.4f} "
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


