from __future__ import annotations

import logging
import os
import time
from typing import Optional

import requests

from config import CLOB_BASE_URL, GAMMA_EVENTS_URL, GTD_ENTRY_DELAY_SECONDS, GTD_EXPIRY_OFFSET, STAKE_USD_PER_ENTRY
from models import Entry

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgsV2,
    OrderArgsV2,
    OrderPayload,
    OrderType,
)

# On-chain redemption (Polygon)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPC_DEFAULT = "https://polygon-rpc.com"
CTF_ABI = [
    {"name": "isApprovedForAll", "type": "function", "stateMutability": "view",
     "inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}],
     "outputs": [{"name": "", "type": "bool"}]},
    {"name": "setApprovalForAll", "type": "function", "stateMutability": "nonpayable",
     "inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "outputs": []},
]
ADAPTER_ABI = [
    {"name": "redeemPositions", "type": "function", "stateMutability": "nonpayable",
     "inputs": [
         {"name": "collateralToken", "type": "address"},
         {"name": "parentCollectionId", "type": "bytes32"},
         {"name": "conditionId", "type": "bytes32"},
         {"name": "indexSets", "type": "uint256[]"},
     ],
     "outputs": []},
]

LOG = logging.getLogger("copy_executor")


def fetch_condition_id(slug: str) -> Optional[str]:
    LOG.debug("[MARKET][TOKEN] event=condition_id_start slug=%s", slug)
    import json, requests
    from config import GAMMA_EVENTS_URL
    try:
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=12)
        resp.raise_for_status()
        ev = resp.json()
        if not ev:
            LOG.warning("[MARKET][TOKEN] event=condition_id_empty slug=%s reason=no_events", slug)
            return None
        mkts = ev[0].get("markets") or []
        if not mkts or not mkts[0].get("conditionId"):
            LOG.warning("[MARKET][TOKEN] event=condition_id_empty slug=%s reason=no_condition_id", slug)
            return None
        LOG.info("[MARKET][TOKEN] event=condition_id_ok slug=%s condition_id=%s", slug, mkts[0]["conditionId"][:16])
        return str(mkts[0]["conditionId"])
    except Exception as exc:
        LOG.warning("[MARKET][TOKEN] event=condition_id_failed slug=%s error=%r", slug, exc)
        return None


def redeem_position(condition_id: str, rpc_url: str, private_key: str) -> str:
    """Call redeemPositions on-chain. Returns tx hash string. Raises on failure."""
    LOG.info("[SETTLE] event=redeem_start condition_id=%s rpc=%s", condition_id[:16], rpc_url)
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
    if not ctf.functions.isApprovedForAll(account.address, adapter_addr).call():
        LOG.info("[SETTLE] event=approval_needed operator=%s", adapter_addr[:10])
        approve_tx = ctf.functions.setApprovalForAll(adapter_addr, True).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": 100_000,
            "gasPrice": w3.eth.gas_price,
        })
        signed = account.sign_transaction(approve_tx)
        receipt = w3.eth.wait_for_transaction_receipt(w3.eth.send_raw_transaction(signed.raw_transaction))
        LOG.info("[SETTLE] event=approval_confirmed tx=%s", receipt.transactionHash.hex())
    else:
        LOG.debug("[SETTLE] event=approval_present operator=%s", adapter_addr[:10])
    condition_bytes = bytes.fromhex(condition_id.removeprefix("0x").zfill(64))
    adapter = w3.eth.contract(address=adapter_addr, abi=ADAPTER_ABI)
    redeem_tx = adapter.functions.redeemPositions(
        Web3.to_checksum_address(PUSD_ADDRESS),
        b"\x00" * 32,
        condition_bytes,
        [1, 2],
    ).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 300_000,
        "gasPrice": w3.eth.gas_price,
    })
    signed = account.sign_transaction(redeem_tx)
    receipt = w3.eth.wait_for_transaction_receipt(w3.eth.send_raw_transaction(signed.raw_transaction))
    LOG.info("[SETTLE] event=redeem_confirmed tx=%s", receipt.transactionHash.hex())
    return receipt.transactionHash.hex()


def auth_client() -> ClobClient:
    key = os.getenv("PM_PRIVATE_KEY", "")
    funder = os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS") or ""
    api_key = os.getenv("PM_API_KEY", "")
    api_secret = os.getenv("PM_API_SECRET", "")
    api_pass = os.getenv("PM_API_PASSPHRASE", "")
    creds = {
        "PM_PRIVATE_KEY": key,
        "PM_FUNDER": funder,
        "PM_API_KEY": api_key,
        "PM_API_SECRET": api_secret,
        "PM_API_PASSPHRASE": api_pass,
    }
    missing = [name for name, value in creds.items() if not value]
    if missing:
        LOG.error("[AUTH] event=failed reason=missing_credentials missing=%s", missing)
        raise RuntimeError(f"missing credentials: {', '.join(missing)} — check .env")
    c = ClobClient(
        host=CLOB_BASE_URL,
        chain_id=137,
        key=key,
        signature_type=int(os.getenv("PM_SIGNATURE_TYPE", "2")),
        funder=funder,
    )
    c.set_api_creds(ApiCreds(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_pass,
    ))
    LOG.info("[AUTH] event=ready funder=%s", funder[:8] if funder else "none")
    return c


def get_balance(client: ClobClient) -> Optional[float]:
    try:
        payload = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        for k in ("balance", "collateral", "available", "allowance"):
            if isinstance(payload, dict) and payload.get(k) is not None:
                raw = float(payload[k])
                bal = raw / 1_000_000 if raw > 10_000 else raw
                LOG.debug("[RISK] event=balance_update balance=%.4f (raw=%s key=%s)", bal, raw, k)
                return bal
    except Exception as exc:
        LOG.warning("[RISK] event=balance_update_failed error=%r", exc)
    return None



def _human(raw) -> float:
    try:
        val = float(raw or 0)
    except (TypeError, ValueError):
        return 0.0
    return val / 1_000_000 if val > 10_000 else val


def _is_filled(res: dict) -> bool:
    if not res:
        return False
    if isinstance(res, dict) and res.get("success"):
        return True
    return False


def get_tick_size(client: ClobClient, token_id: str) -> float:
    try:
        ts = client.get_tick_size(token_id)
        return float(ts) if ts else 0.01
    except Exception as exc:
        LOG.warning("[ORDER] event=tick_size_fallback token=%s fallback=0.01 error=%r", token_id[:8], exc)
        return 0.01


def place_gtd_limit_order(
    client: ClobClient,
    bucket_ts: int,
    token: str,
    ask_price: float,
    tick_size: float,
    expiration_offset: int | None = None,
    shares: float = 1.0,
    limit_price: float | None = None,
) -> tuple[Optional[Entry], str]:
    """Place GTD limit BUY below ask (not marketable → no $1 minimum)."""
    if ask_price <= 0:
        LOG.info("[ORDER] event=skip bucket=%s reason=no_ask token=%s", bucket_ts, token[:8])
        return None, "no_ask"

    if limit_price is None:
        limit_price = ask_price - max(tick_size, 0.01)
    limit_price = round(max(limit_price, 0.01), 4)
    shares = round(max(shares, 1.0), 4)
    cost = round(shares * limit_price, 6)
    if expiration_offset is None:
        expiration_offset = GTD_EXPIRY_OFFSET
    expiration_ts = max(
        bucket_ts + 300 + expiration_offset + 60,
        int(time.time()) + 181,
    )

    LOG.info("[ORDER] event=submit bucket=%s token=%s side=BUY ask=%.4f limit=%.4f shares=%.4f expiration=%d", bucket_ts, token[:8], ask_price, limit_price, shares, expiration_ts)

    try:
        order = client.create_order(
            OrderArgsV2(
                token_id=token,
                price=limit_price,
                size=shares,
                side="BUY",
                expiration=expiration_ts,
            )
        )
        res = client.post_order(order, OrderType.GTD)
    except Exception as exc:
        err_text = str(exc)
        LOG.error(
            "[ORDER] event=failed bucket=%s token=%s reason=%s limit=%.4f shares=%.4f expiration=%d error=%r",
            bucket_ts, token[:8], "timeout" if "timeout" in err_text.lower() else "post_error", limit_price, shares, expiration_ts, exc,
        )
        return None, f"order_error: {exc}"

    if not isinstance(res, dict) or not res.get("success"):
        err = res.get("errorMsg", "") if isinstance(res, dict) else ""
        LOG.warning("[ORDER] event=rejected bucket=%s token=%s reason=not_accepted limit=%.4f shares=%.4f error=%s", bucket_ts, token[:8], limit_price, shares, err)
        return None, f"not_accepted: {err}"

    order_id = res.get("orderID", "")
    taking_amount = _human(res.get("takingAmount"))
    making_amount = _human(res.get("makingAmount"))
    filled_shares = taking_amount if taking_amount > 0 else shares
    filled_cost = making_amount if making_amount > 0 else cost
    status = "FILLED" if taking_amount > 0 else "RESTING"

    entry = Entry(
        coin="BTC",
        bucket_ts=bucket_ts,
        side="UP" if "up" in token.lower() else "DOWN",
        token=token,
        shares=filled_shares,
        limit_price=limit_price,
        cost=filled_cost,
        placed_at=time.time(),
        order_id=order_id,
        status=status,
    )
    LOG.info(
        "[ORDER] event=accepted bucket=%s token=%s status=%s order_id=%s limit=%.4f shares=%.4f cost=%.4f",
        bucket_ts, token[:8], status, order_id[:10], entry.limit_price, entry.shares, entry.cost
    )
    return entry, "placed"


def place_market_sell_fok(
    client: ClobClient,
    bucket_ts: int,
    token: str,
    shares: float,
) -> tuple[Optional[Entry], str]:
    shares = round(max(shares, 0.0), 4)
    if shares <= 0:
        LOG.info("[ORDER] event=skip bucket=%s reason=no_shares token=%s side=SELL", bucket_ts, token[:8])
        return None, "no_shares"
    LOG.info("[ORDER] event=submit bucket=%s token=%s side=SELL type=FOK shares=%.4f", bucket_ts, token[:8], shares)
    try:
        order = client.create_market_order(MarketOrderArgsV2(token_id=str(token), amount=float(shares), side="SELL", order_type=OrderType.FOK))
        res = client.post_order(order, OrderType.FOK)
    except Exception as exc:
        LOG.error("[ORDER] event=failed bucket=%s token=%s side=SELL type=FOK shares=%.4f error=%r", bucket_ts, token[:8], shares, exc)
        return None, f"order_error: {exc}"
    if not isinstance(res, dict) or not res.get("success"):
        err = res.get("errorMsg", "") if isinstance(res, dict) else ""
        LOG.warning("[ORDER] event=rejected bucket=%s token=%s side=SELL type=FOK shares=%.4f error=%s", bucket_ts, token[:8], shares, err)
        return None, f"not_accepted: {err}"
    order_id = res.get("orderID", "")
    taking_amount = _human(res.get("takingAmount"))
    making_amount = _human(res.get("makingAmount"))
    filled_shares = shares if making_amount <= 0 else making_amount
    proceeds = taking_amount if taking_amount > 0 else 0.0
    entry = Entry(
        coin="BTC",
        bucket_ts=bucket_ts,
        side="SELL",
        token=token,
        shares=filled_shares,
        limit_price=0.0,
        cost=proceeds,
        placed_at=time.time(),
        order_id=order_id,
        status="FILLED" if proceeds > 0 else "RESTING",
    )
    LOG.info("[ORDER] event=accepted bucket=%s token=%s side=SELL type=FOK status=%s order_id=%s shares=%.4f proceeds=%.4f", bucket_ts, token[:8], entry.status, order_id[:10], entry.shares, entry.cost)
    return entry, "placed"


def place_limit_sell_fak(
    client: ClobClient,
    bucket_ts: int,
    token: str,
    shares: float,
    price: float,
) -> tuple[Optional[Entry], str]:
    shares = round(max(shares, 0.0), 4)
    price = round(max(price, 0.01), 4)
    if shares <= 0:
        LOG.info("[ORDER] event=skip bucket=%s reason=no_shares token=%s side=SELL", bucket_ts, token[:8])
        return None, "no_shares"
    LOG.info("[ORDER] event=submit bucket=%s token=%s side=SELL type=FAK limit=%.4f shares=%.4f", bucket_ts, token[:8], price, shares)
    try:
        order = client.create_order(OrderArgsV2(token_id=str(token), price=float(price), size=float(shares), side="SELL"), OrderType.FAK)
        res = client.post_order(order, OrderType.FAK)
    except Exception as exc:
        LOG.error("[ORDER] event=failed bucket=%s token=%s side=SELL type=FAK limit=%.4f shares=%.4f error=%r", bucket_ts, token[:8], price, shares, exc)
        return None, f"order_error: {exc}"
    if not isinstance(res, dict) or not res.get("success"):
        err = res.get("errorMsg", "") if isinstance(res, dict) else ""
        LOG.warning("[ORDER] event=rejected bucket=%s token=%s side=SELL type=FAK limit=%.4f shares=%.4f error=%s", bucket_ts, token[:8], price, shares, err)
        return None, f"not_accepted: {err}"
    order_id = res.get("orderID", "")
    taking_amount = _human(res.get("takingAmount"))
    making_amount = _human(res.get("makingAmount"))
    filled_shares = shares if making_amount <= 0 else making_amount
    proceeds = taking_amount if taking_amount > 0 else round(filled_shares * price, 6)
    entry = Entry(
        coin="BTC",
        bucket_ts=bucket_ts,
        side="SELL",
        token=token,
        shares=filled_shares,
        limit_price=price,
        cost=proceeds,
        placed_at=time.time(),
        order_id=order_id,
        status="FILLED" if proceeds > 0 else "RESTING",
    )
    LOG.info("[ORDER] event=accepted bucket=%s token=%s side=SELL type=FAK status=%s order_id=%s limit=%.4f shares=%.4f proceeds=%.4f", bucket_ts, token[:8], entry.status, order_id[:10], entry.limit_price, entry.shares, entry.cost)
    return entry, "placed"


def get_fee_per_share(client: ClobClient, token_id: str, price: float) -> float:
    """Return Polymarket taker fee per share at given price. Falls back to 0.07 * p*(1-p)."""
    try:
        fee_rate = client.get_fee_rate_bps(token_id) / 10000.0
        fee_exp = client.get_fee_exponent(token_id)
        return fee_rate * (price * (1.0 - price)) ** fee_exp
    except Exception:
        return 0.07 * (price * (1.0 - price))


def get_net_profit_per_share(client: ClobClient, token_id: str, price: float) -> float:
    """Return profit per share after Polymarket taker fee: (1 - price) - fee."""
    return (1.0 - price) - get_fee_per_share(client, token_id, price)


def estimate_fee_inclusive_buy_cost(
    client: ClobClient,
    token_id: str,
    price: float,
    shares: float,
    raw_cost: float,
) -> float:
    """Return raw_cost + estimated Polymarket taker fee."""
    try:
        fee = shares * get_fee_per_share(client, token_id, price)
        return round(raw_cost + fee, 6)
    except Exception:
        return round(raw_cost * 1.015, 6)


def apply_fee_cost_and_refresh_balance(client: ClobClient, bal_before: Optional[float], entry) -> Optional[float]:
    """Estimate fee-inclusive cost for FILLED entry, then refresh balance snapshot."""
    if entry is None or entry.status != "FILLED" or bal_before is None:
        return None
    try:
        real_cost = estimate_fee_inclusive_buy_cost(client, entry.token, entry.limit_price, entry.shares, entry.cost)
        if real_cost > 0:
            entry.cost = real_cost
        bal_after = get_balance(client)
        return bal_after if bal_after is not None else round(max(bal_before - real_cost, 0.0), 6)
    except Exception:
        pass
    return None


def cancel_order(client: ClobClient, order_id: str) -> bool:
    if not order_id:
        return False
    LOG.info("[ORDER] event=cancel_request order_id=%s", order_id[:10])
    try:
        client.cancel_order(OrderPayload(orderID=order_id))
        LOG.info("[ORDER] event=cancelled order_id=%s", order_id[:10])
        return True
    except Exception as exc:
        LOG.warning("[ORDER] event=cancel_failed order_id=%s error=%r", order_id[:10], exc)
        return False


def get_order_status(client: ClobClient, order_id: str) -> Optional[dict]:
    if not order_id:
        return None
    try:
        return client.get_order(order_id)
    except Exception as exc:
        LOG.warning("[ORDER] event=status_failed order_id=%s error=%r", order_id[:10], exc)
        return None


def get_market_resolution(bucket_ts: int) -> Optional[str]:
    slug = f"btc-updown-5m-{bucket_ts}"
    try:
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data or not data[0].get("markets"):
            return None
        market = data[0]["markets"][0]
        if not bool(market.get("closed")):
            return None
        winner = str(market.get("winner") or market.get("winningOutcome") or "").upper()
        if winner in {"UP", "DOWN"}:
            return winner
        outcome_prices = market.get("outcomePrices") or []
        outcomes = market.get("outcomes") or []
        if isinstance(outcomes, str):
            import json
            outcomes = json.loads(outcomes)
        if isinstance(outcome_prices, str):
            import json
            outcome_prices = json.loads(outcome_prices)
        if len(outcomes) >= 2 and len(outcome_prices) >= 2:
            try:
                prices = [float(x) for x in outcome_prices[:2]]
                idx = -1
                if prices[0] == 1.0 and prices[1] == 0.0:
                    idx = 0
                elif prices[1] == 1.0 and prices[0] == 0.0:
                    idx = 1
                if idx >= 0:
                    guess = str(outcomes[idx]).upper()
                    if guess in {"UP", "DOWN"}:
                        return guess
            except (TypeError, ValueError):
                return None
    except Exception as exc:
        LOG.warning("[SETTLE] event=resolution_failed bucket=%s error=%r", bucket_ts, exc)
    return None
