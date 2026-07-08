from __future__ import annotations

import logging
import os
import time
from typing import Optional

from config import CLOB_BASE_URL, STAKE_USD_PER_ENTRY, GTD_ENTRY_DELAY_SECONDS
from models import Entry

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgsV2,
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
    import json, requests
    from config import GAMMA_EVENTS_URL
    try:
        resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=12)
        resp.raise_for_status()
        ev = resp.json()
        if not ev:
            return None
        mkts = ev[0].get("markets") or []
        return str(mkts[0]["conditionId"]) if mkts and mkts[0].get("conditionId") else None
    except Exception:
        return None


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
    if not ctf.functions.isApprovedForAll(account.address, adapter_addr).call():
        LOG.info("redeem: approving adapter for CTF tokens (one-time)...")
        approve_tx = ctf.functions.setApprovalForAll(adapter_addr, True).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": 100_000,
            "gasPrice": w3.eth.gas_price,
        })
        signed = account.sign_transaction(approve_tx)
        receipt = w3.eth.wait_for_transaction_receipt(w3.eth.send_raw_transaction(signed.raw_transaction))
        LOG.info("redeem: approval confirmed tx=%s", receipt.transactionHash.hex())
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
    return receipt.transactionHash.hex()


def auth_client() -> ClobClient:
    key = os.getenv("PM_PRIVATE_KEY", "")
    funder = os.getenv("PM_FUNDER") or os.getenv("PM_ADDRESS") or ""
    api_key = os.getenv("PM_API_KEY", "")
    api_secret = os.getenv("PM_API_SECRET", "")
    api_pass = os.getenv("PM_API_PASSPHRASE", "")
    if not all([key, funder, api_key, api_secret, api_pass]):
        raise RuntimeError("missing credentials — check .env")
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
    return c


def get_balance(client: ClobClient) -> Optional[float]:
    try:
        payload = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        for k in ("balance", "collateral", "available", "allowance"):
            if isinstance(payload, dict) and payload.get(k) is not None:
                raw = float(payload[k])
                return raw / 1_000_000 if raw > 10_000 else raw
    except Exception as exc:
        LOG.warning("balance fetch failed: %r", exc)
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
    except Exception:
        return 0.01


def place_gtd_limit_order(
    client: ClobClient,
    bucket_ts: int,
    token: str,
    ask_price: float,
    tick_size: float,
    expiration_offset: int = 60,
    shares: float = 1.0,
) -> tuple[Optional[Entry], str]:
    """Place GTD limit BUY below ask (not marketable → no $1 minimum)."""
    if ask_price <= 0:
        return None, "no_ask"

    limit_price = round(max(ask_price - max(tick_size, 0.01), 0.01), 4)
    shares = round(max(shares, 1.0), 4)
    cost = round(shares * limit_price, 6)
    expiration_ts = int(time.time()) + 600

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
        if "timed out" in err_text.lower() or "timeout" in err_text.lower():
            LOG.error(
                "GTD order timeout token=%s ask=%.4f limit=%.4f shares=%.4f expiration=%s error=%r",
                token[:16], ask_price, limit_price, shares, expiration_ts, exc,
            )
            return None, f"order_timeout: {exc}"
        LOG.error(
            "GTD order failed token=%s ask=%.4f limit=%.4f shares=%.4f expiration=%s error=%r",
            token[:16], ask_price, limit_price, shares, expiration_ts, exc,
        )
        return None, f"order_error: {exc}"

    if not isinstance(res, dict) or not res.get("success"):
        err = res.get("errorMsg", "") if isinstance(res, dict) else ""
        LOG.info("GTD not accepted: %s (price=%.4f, shares=%.4f)", err, limit_price, shares)
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
    LOG.info("GTD %s token=%s limit=%.4f shares=%.4f cost=%.4f status=%s",
             entry.side, token[:16], entry.limit_price, entry.shares, entry.cost, status)
    return entry, "placed"


def cancel_order(client: ClobClient, order_id: str) -> bool:
    if not order_id:
        return False
    try:
        client.cancel_order(order_id)
        LOG.info("cancelled order %s", order_id[:16])
        return True
    except Exception as exc:
        LOG.warning("cancel failed %s: %r", order_id[:16], exc)
        return False
