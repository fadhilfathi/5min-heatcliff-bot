from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed

from config import POLY_WS_URL, POLY_WS_STALE_SECONDS

LOG = logging.getLogger("poly_book_ws")

_BACKOFF = [1, 2, 4, 8, 16, 32, 60]
_PING_INTERVAL = 20


class _BookCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, float]] = {}

    def update(self, token_id: str, best_bid: float, best_ask: float) -> None:
        with self._lock:
            self._data[token_id] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "updated_at": time.time(),
            }

    def get(self, token_id: str) -> tuple[float, float]:
        with self._lock:
            entry = self._data.get(token_id)
            if entry is None:
                return 0.0, 0.0
            if time.time() - entry["updated_at"] > POLY_WS_STALE_SECONDS:
                return 0.0, 0.0
            return entry["best_bid"], entry["best_ask"]


_cache = _BookCache()
_sub_lock = threading.Lock()
_subscribed_ids: set[str] = set()
_desired_ids: set[str] = set()
_sub_dirty = threading.Event()
_connected = threading.Event()
_started = False
_start_lock = threading.Lock()


def get_best_prices(token_id: str) -> tuple[float, float]:
    return _cache.get(token_id)


def is_connected() -> bool:
    return _connected.is_set()


def set_subscriptions(token_ids: set[str]) -> None:
    global _desired_ids
    with _sub_lock:
        if token_ids == _desired_ids:
            return
        _desired_ids = set(token_ids)
        _sub_dirty.set()


async def _ws_loop() -> None:
    global _subscribed_ids
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(
                POLY_WS_URL,
                ping_interval=_PING_INTERVAL,
                ping_timeout=20,
                open_timeout=10,
            ) as ws:
                _connected.set()
                backoff_idx = 0
                LOG.info("[POLY_WS] event=connected url=%s", POLY_WS_URL)

                with _sub_lock:
                    current_desired = set(_desired_ids)
                if current_desired:
                    msg = json.dumps({
                        "assets_ids": list(current_desired),
                        "type": "market",
                        "custom_feature_enabled": True,
                    })
                    await ws.send(msg)
                    _subscribed_ids = set(current_desired)
                    _sub_dirty.clear()
                    LOG.info("[POLY_WS] event=subscribed tokens=%d", len(current_desired))

                while True:
                    if _sub_dirty.is_set():
                        with _sub_lock:
                            current_desired = set(_desired_ids)
                        new_ids = current_desired - _subscribed_ids
                        remove_ids = _subscribed_ids - current_desired
                        if new_ids or remove_ids:
                            if new_ids:
                                msg = json.dumps({
                                    "assets_ids": list(new_ids),
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                })
                                await ws.send(msg)
                            if remove_ids:
                                msg = json.dumps({
                                    "assets_ids": list(remove_ids),
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                    "unsubscribe": True,
                                })
                                await ws.send(msg)
                            _subscribed_ids = set(current_desired)
                            LOG.info("[POLY_WS] event=resubscribed new=%d removed=%d total=%d", len(new_ids), len(remove_ids), len(_subscribed_ids))
                        _sub_dirty.clear()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    except asyncio.TimeoutError:
                        continue

                    msg = json.loads(raw)
                    messages = msg if isinstance(msg, list) else [msg]
                    for item in messages:
                        if not isinstance(item, dict):
                            continue
                        event_type = item.get("event_type", "")

                        if event_type == "best_bid_ask":
                            asset_id = item.get("asset_id", "")
                            best_bid = float(item.get("best_bid") or 0.0)
                            best_ask = float(item.get("best_ask") or 0.0)
                            if asset_id and (best_bid > 0 or best_ask > 0):
                                _cache.update(asset_id, best_bid, best_ask)

        except asyncio.TimeoutError:
            _connected.clear()
            LOG.warning("[POLY_WS] event=reconnect reason=timeout")
        except ConnectionClosed as exc:
            _connected.clear()
            LOG.warning("[POLY_WS] event=reconnect reason=closed error=%r", exc)
        except Exception as exc:
            _connected.clear()
            LOG.warning("[POLY_WS] event=reconnect reason=unknown error=%r", exc)

        delay = _BACKOFF[min(backoff_idx, len(_BACKOFF) - 1)]
        backoff_idx += 1
        await asyncio.sleep(delay)


def _thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_loop())


def start() -> None:
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        threading.Thread(target=_thread_target, daemon=True, name="poly-book-ws").start()
        _started = True
        LOG.info("[POLY_WS] event=start url=%s stale_seconds=%d", POLY_WS_URL, POLY_WS_STALE_SECONDS)
