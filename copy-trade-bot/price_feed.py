from __future__ import annotations

import asyncio
import json
import logging
import threading
import time

import requests
import websockets
from websockets.exceptions import ConnectionClosed

from config import BINANCE_API, BINANCE_WS_URL, ETH_SYMBOL, COINBASE_PRODUCT, COINBASE_WS_URL

LOG = logging.getLogger("price_feed")
_PING_INTERVAL = 20
_BACKOFF = [1, 2, 4, 8, 16, 32, 60]
_STALE_SECONDS = 30


class _LatestPrice:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._price: float | None = None
        self._updated_at = 0.0

    def set(self, price: float) -> None:
        with self._lock:
            self._price = price
            self._updated_at = time.time()

    def get(self) -> float | None:
        with self._lock:
            if self._price is None:
                return None
            if time.time() - self._updated_at > _STALE_SECONDS:
                return None
            return self._price


_latest_price = _LatestPrice()
_latest_binance_price = _LatestPrice()
_start_lock = threading.Lock()
_started = False


def get_eth_price_binance() -> float | None:
    try:
        resp = requests.get(
            f"{BINANCE_API}/api/v3/ticker/price",
            params={"symbol": ETH_SYMBOL},
            timeout=3,
        )
        resp.raise_for_status()
        data = resp.json()
        price = float(data.get("price") or 0.0)
        if price <= 0:
            LOG.warning("[MARKET][PRICE] event=fetch_failed source=binance reason=non_positive price=%.2f payload_keys=%s", price, list(data.keys()))
            return None
        return price
    except requests.exceptions.Timeout:
        LOG.warning("[MARKET][PRICE] event=fetch_failed source=binance reason=timeout symbol=%s timeout_s=3", ETH_SYMBOL)
        return None
    except requests.exceptions.HTTPError as exc:
        LOG.warning("[MARKET][PRICE] event=fetch_failed source=binance reason=http status=%s symbol=%s error=%r", exc.response.status_code if exc.response else None, ETH_SYMBOL, exc)
        return None
    except Exception as exc:
        LOG.warning("[MARKET][PRICE] event=fetch_failed source=binance reason=unknown error=%r", exc)
        return None


async def _coinbase_ws_loop() -> None:
    subscribe = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": [COINBASE_PRODUCT]}],
    }
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL, ping_interval=_PING_INTERVAL, ping_timeout=20, open_timeout=10) as ws:
                await ws.send(json.dumps(subscribe))
                LOG.info("[MARKET][PRICE] event=ws_connected source=coinbase product=%s", COINBASE_PRODUCT)
                backoff_idx = 0
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(raw)
                    if msg.get("type") != "ticker":
                        continue
                    if msg.get("product_id") != COINBASE_PRODUCT:
                        continue
                    price = float(msg.get("price") or 0.0)
                    if price > 0:
                        _latest_price.set(price)
        except asyncio.TimeoutError:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=coinbase reason=timeout product=%s", COINBASE_PRODUCT)
        except ConnectionClosed as exc:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=coinbase reason=closed product=%s error=%r", COINBASE_PRODUCT, exc)
        except Exception as exc:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=coinbase reason=unknown product=%s error=%r", COINBASE_PRODUCT, exc)

        delay = _BACKOFF[min(backoff_idx, len(_BACKOFF) - 1)]
        backoff_idx += 1
        await asyncio.sleep(delay)


async def _binance_ws_loop() -> None:
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL, ping_interval=_PING_INTERVAL, ping_timeout=20, open_timeout=10) as ws:
                LOG.info("[MARKET][PRICE] event=ws_connected source=binance symbol=%s", ETH_SYMBOL)
                backoff_idx = 0
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(raw)
                    price = float(msg.get("p") or 0.0)
                    if price > 0:
                        _latest_binance_price.set(price)
        except asyncio.TimeoutError:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=binance reason=timeout symbol=%s", ETH_SYMBOL)
        except ConnectionClosed as exc:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=binance reason=closed symbol=%s error=%r", ETH_SYMBOL, exc)
        except Exception as exc:
            LOG.warning("[MARKET][PRICE] event=ws_reconnect source=binance reason=unknown symbol=%s error=%r", ETH_SYMBOL, exc)

        delay = _BACKOFF[min(backoff_idx, len(_BACKOFF) - 1)]
        backoff_idx += 1
        await asyncio.sleep(delay)


def _thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_coinbase_ws_loop())


def _binance_thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_binance_ws_loop())


def _ensure_started() -> None:
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        threading.Thread(target=_thread_target, daemon=True, name="coinbase-price-feed").start()
        threading.Thread(target=_binance_thread_target, daemon=True, name="binance-price-feed").start()
        _started = True


def get_eth_price() -> float | None:
    _ensure_started()
    price = _latest_price.get()
    if price is None:
        LOG.debug("[MARKET][PRICE] event=skip_loop source=coinbase reason=unavailable")
    return price
