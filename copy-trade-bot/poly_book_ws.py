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
_HEALTH_LOG_INTERVAL = 30


class _BookCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, float]] = {}
        self._seen_tokens: set[str] = set()

    def update(self, token_id: str, best_bid: float, best_ask: float) -> None:
        with self._lock:
            is_first = token_id not in self._data
            self._data[token_id] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "updated_at": time.time(),
            }
            if is_first:
                self._seen_tokens.add(token_id)
        if is_first:
            LOG.info("[POLY_WS] event=first_quote token=%s bid=%.4f ask=%.4f", token_id[:12], best_bid, best_ask)
        else:
            LOG.debug("[POLY_WS] event=quote token=%s bid=%.4f ask=%.4f", token_id[:12], best_bid, best_ask)

    def get(self, token_id: str) -> tuple[float, float]:
        with self._lock:
            entry = self._data.get(token_id)
            if entry is None:
                return 0.0, 0.0
            if time.time() - entry["updated_at"] > POLY_WS_STALE_SECONDS:
                return 0.0, 0.0
            return entry["best_bid"], entry["best_ask"]

    def get_status(self, token_id: str) -> str:
        with self._lock:
            entry = self._data.get(token_id)
            if entry is None:
                return "missing"
            age = time.time() - entry["updated_at"]
            if age > POLY_WS_STALE_SECONDS:
                return f"stale({age:.1f}s)"
            return f"fresh({age:.1f}s)"

    def prune(self, keep: set[str]) -> None:
        with self._lock:
            for token_id in list(self._data.keys()):
                if token_id not in keep:
                    del self._data[token_id]

    def stats(self) -> dict:
        with self._lock:
            now = time.time()
            result = {}
            for token_id, entry in self._data.items():
                age = now - entry["updated_at"]
                result[token_id[:12]] = {
                    "bid": entry["best_bid"],
                    "ask": entry["best_ask"],
                    "age_s": round(age, 1),
                    "stale": age > POLY_WS_STALE_SECONDS,
                }
            return result


_cache = _BookCache()
_sub_lock = threading.Lock()
_subscribed_ids: set[str] = set()
_desired_ids: set[str] = set()
_sub_dirty = threading.Event()
_connected = threading.Event()
_started = False
_start_lock = threading.Lock()
_msg_count = 0
_event_counts: dict[str, int] = {}
_connect_time = 0.0


def get_best_prices(token_id: str) -> tuple[float, float]:
    return _cache.get(token_id)


def get_status(token_id: str) -> str:
    return _cache.get_status(token_id)


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
    global _subscribed_ids, _msg_count, _event_counts, _connect_time
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
                _connect_time = time.time()
                _msg_count = 0
                _event_counts = {}
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
                    _cache.prune(current_desired)
                    _sub_dirty.clear()
                    LOG.info("[POLY_WS] event=subscribed tokens=%d", len(current_desired))

                last_health_log = time.time()

                while True:
                    if _sub_dirty.is_set():
                        _sub_dirty.clear()
                        with _sub_lock:
                            current_desired = set(_desired_ids)
                        if current_desired != _subscribed_ids:
                            LOG.info("[POLY_WS] event=force_reconnect reason=subscription_change tokens=%d", len(current_desired))
                            break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    except asyncio.TimeoutError:
                        now = time.time()
                        if now - last_health_log > _HEALTH_LOG_INTERVAL:
                            _log_health()
                            last_health_log = now
                        continue

                    _msg_count += 1

                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        LOG.debug("[POLY_WS] event=non_json_msg raw_len=%d raw_preview=%r", len(raw), raw[:80] if isinstance(raw, str) else type(raw).__name__)
                        continue

                    messages = msg if isinstance(msg, list) else [msg]
                    for item in messages:
                        if not isinstance(item, dict):
                            continue
                        event_type = item.get("event_type", "")
                        if event_type:
                            _event_counts[event_type] = _event_counts.get(event_type, 0) + 1

                        if event_type == "best_bid_ask":
                            asset_id = item.get("asset_id", "")
                            best_bid = float(item.get("best_bid") or 0.0)
                            best_ask = float(item.get("best_ask") or 0.0)
                            if asset_id and (best_bid > 0 or best_ask > 0):
                                _cache.update(asset_id, best_bid, best_ask)

                    now = time.time()
                    if now - last_health_log > _HEALTH_LOG_INTERVAL:
                        _log_health()
                        last_health_log = now

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


def _log_health() -> None:
    uptime = time.time() - _connect_time if _connect_time else 0
    cache_stats = _cache.stats()
    event_summary = ", ".join(f"{k}={v}" for k, v in sorted(_event_counts.items())) or "none"
    cache_summary = "; ".join(f"{tok}={s['bid']:.4f}/{s['ask']:.4f}({s['age_s']}s{'*' if s['stale'] else ''})" for tok, s in cache_stats.items()) or "empty"
    LOG.info(
        "[POLY_WS] event=health uptime=%.0fs msgs=%d events=[%s] cache=[%s]",
        uptime, _msg_count, event_summary, cache_summary,
    )


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
