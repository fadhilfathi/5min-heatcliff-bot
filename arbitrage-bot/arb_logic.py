from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any

from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderArgsV2, OrderPayload, OrderType

from py_clob_client_v2.client import ClobClient

from config import ARB_SHARES_PER_LEG, GTD_TIMEOUT_SEC, MIN_ORDER_SHARES, TAKER_FEE


def place_fok_limit_buy(client: ClobClient, token_id: str, price: float, size: float, expiration_ts: int) -> tuple[str, dict[str, Any]]:
    order = client.create_order(
        OrderArgsV2(
            token_id=str(token_id),
            price=float(price),
            size=float(size),
            side="BUY",
            expiration=int(expiration_ts),
        )
    )
    post = client.post_order(order, OrderType.FOK)
    order_id = str(post.get("orderID") or post.get("id") or "")
    if not order_id:
        raise RuntimeError(f"missing order id in FOK response: {post}")
    return order_id, post


def check_order_status(client: ClobClient, order_id: str) -> tuple[str, float, dict[str, Any]]:
    payload = client.get_order(str(order_id))
    status = str(payload.get("status") or payload.get("state") or "UNKNOWN").upper()
    filled_size = _to_float(
        payload.get("filledSize"),
        payload.get("filled_size"),
        payload.get("matched"),
        payload.get("sizeMatched"),
        payload.get("size_matched"),
    )
    return status, filled_size, payload


def cancel_order_if_open(client: ClobClient, order_id: str) -> dict[str, Any] | None:
    try:
        status, _, payload = check_order_status(client, order_id)
    except Exception:
        status = ""
        payload = {}
    if status in {"FILLED", "CANCELLED", "CANCELED"}:
        return payload
    return client.cancel_order(OrderPayload(orderID=str(order_id)))


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


def execute_arb(
    *,
    client: ClobClient | None,
    up_token: str,
    down_token: str,
    ask_up: float,
    ask_down: float,
    stake_usd: float,
    expiration_ts: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    n_shares = round(float(ARB_SHARES_PER_LEG), 4)
    gross_cost = round(n_shares * (float(ask_up) + float(ask_down)), 6)
    estimated_fee = round(gross_cost * TAKER_FEE, 6)
    expected_net = round((n_shares * 1.0) - gross_cost - estimated_fee, 6)
    result: dict[str, Any] = {
        "up_token": up_token,
        "down_token": down_token,
        "ask_up": round(float(ask_up), 6),
        "ask_down": round(float(ask_down), 6),
        "shares": n_shares,
        "stake_usd_requested": float(stake_usd),
        "gross_cost_usd": gross_cost,
        "estimated_fee_usd": estimated_fee,
        "expected_net_usd": expected_net,
        "started_at": _ts_utc(),
    }
    if n_shares < MIN_ORDER_SHARES:
        result.update(
            {
                "result": "skipped",
                "reason": "min_order_size",
                "min_order_shares": MIN_ORDER_SHARES,
                "finished_at": _ts_utc(),
            }
        )
        return result
    if dry_run:
        result.update(
            {
                "result": "dry_run",
                "status_up": "DRY_RUN",
                "status_down": "DRY_RUN",
                "filled_up": n_shares,
                "filled_down": n_shares,
                "net_profit_usd": expected_net,
            }
        )
        return result
    if client is None:
        raise RuntimeError("live client required when dry_run is false")

    post_up: dict[str, Any] = {}
    post_down: dict[str, Any] = {}
    up_error = ""
    down_error = ""
    try:
        order_id_up, post_up = place_fok_limit_buy(client, up_token, ask_up, n_shares, expiration_ts)
        result["order_id_up"] = order_id_up
        result["post_up"] = post_up
        last_up = check_order_status(client, order_id_up)
    except Exception as exc:
        last_up = ("FAILED", 0.0, post_up)
        up_error = str(exc)

    try:
        order_id_down, post_down = place_fok_limit_buy(client, down_token, ask_down, n_shares, expiration_ts)
        result["order_id_down"] = order_id_down
        result["post_down"] = post_down
        last_down = check_order_status(client, order_id_down)
    except Exception as exc:
        last_down = ("FAILED", 0.0, post_down)
        down_error = str(exc)

    up_status, up_fill, up_payload = last_up
    down_status, down_fill, down_payload = last_down
    result.update(
        {
            "status_up": up_status,
            "status_down": down_status,
            "filled_up": up_fill,
            "filled_down": down_fill,
            "order_up": up_payload,
            "order_down": down_payload,
        }
    )

    if up_error:
        result["error_up"] = up_error
    if down_error:
        result["error_down"] = down_error

    if _filled_enough(up_status, up_fill, n_shares) and _filled_enough(down_status, down_fill, n_shares):
        actual_fee = round((up_fill * ask_up + down_fill * ask_down) * TAKER_FEE, 6)
        result.update(
            {
                "result": "success",
                "actual_fee_usd": actual_fee,
                "net_profit_usd": round((min(up_fill, down_fill) * 1.0) - ((up_fill * ask_up) + (down_fill * ask_down)) - actual_fee, 6),
                "finished_at": _ts_utc(),
            }
        )
        return result

    if up_fill <= 0 and down_fill <= 0:
        result.update({"result": "failed", "reason": up_error or down_error or "no_fill", "finished_at": _ts_utc()})
        return result

    unwind: dict[str, Any] = {}
    if up_fill > 0 and down_fill <= 0:
        unwind = _unwind_leg(client, up_token, up_fill, ask_up, "UP")
    elif down_fill > 0 and up_fill <= 0:
        unwind = _unwind_leg(client, down_token, down_fill, ask_down, "DOWN")
    else:
        if up_fill > down_fill:
            unwind = _unwind_leg(client, up_token, up_fill - down_fill, ask_up, "UP")
        elif down_fill > up_fill:
            unwind = _unwind_leg(client, down_token, down_fill - up_fill, ask_down, "DOWN")

    result.update({"result": "unwind", "unwind": unwind, "finished_at": _ts_utc()})
    return result


def append_trade_log(log_dir: Path, payload: dict[str, Any]) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"arb_{dt.datetime.now(dt.timezone.utc):%Y%m%d}.log"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
    return path


def _unwind_leg(client: ClobClient, token_id: str, filled_size: float, entry_price: float, leg_name: str) -> dict[str, Any]:
    sell_post = create_market_sell(client, token_id, filled_size)
    taking = _to_float(sell_post.get("takingAmount"))
    fee = round((filled_size * entry_price) * TAKER_FEE, 6)
    pnl = round(taking - (filled_size * entry_price) - fee, 6)
    return {
        "token_id": token_id,
        "leg": leg_name,
        "filled_size": round(filled_size, 6),
        "sell_post": sell_post,
        "fee_usd": fee,
        "pnl_usd": pnl,
    }


def _filled_enough(status: str, filled_size: float, wanted_size: float) -> bool:
    return status == "FILLED" or filled_size >= max(wanted_size - 0.0001, 0.0)


def _to_float(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _ts_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
