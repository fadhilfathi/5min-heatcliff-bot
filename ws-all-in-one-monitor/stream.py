"""
ws_all_in_one — Multi-token Polymarket CLOB WebSocket monitor.
Improves on ws-monitor/ by subscribing to ALL tokens upfront
on a single WS connection, with better reconnect + fallback.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("ws_all_in_one")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_PING_INTERVAL = 10
_BACKOFF = [1, 2, 4, 8, 16, 32, 60]
_MAX_FAILURES = 5
_STALE_SEC = 5.0
_HEALTH_SEC = 15.0


class BookState:
    __slots__ = ("bids", "asks", "updated_at")
    def __init__(self):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.updated_at: float = 0.0

    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None

    def is_stale(self, now: float) -> bool:
        if self.updated_at == 0.0:
            return True
        return (now - self.updated_at) > _STALE_SEC

    def apply_snapshot(self, bids: list, asks: list) -> None:
        self.bids = {float(b["price"]): float(b["size"]) for b in bids}
        self.asks = {float(a["price"]): float(a["size"]) for a in asks}
        self.updated_at = time.time()

    def apply_change(self, side: str, price: float, size: float) -> None:
        book = self.bids if side == "BUY" else self.asks if side == "SELL" else None
        if book is None:
            return
        if size == 0.0:
            book.pop(price, None)
        else:
            book[price] = size
        self.updated_at = time.time()


class MultiBookStream:
    """Single WS connection subscribing to multiple tokens."""

    def __init__(self):
        self._books: dict[str, BookState] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._ws: Optional = None
        self._backoff_idx: int = 0
        self._fail_count: int = 0
        self._connected_since: Optional[float] = None
        self._stopped: bool = False
        self._subscribed: set[str] = set()
        self._last_event_at: float = 0.0
        self.gave_up: bool = False
        self.event_count: int = 0

    def _ensure_lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()

    def _ws_is_open(self) -> bool:
        if self._ws is None:
            return False
        state = getattr(self._ws, "state", None)
        if state is not None:
            return int(state) == 1
        closed = getattr(self._ws, "closed", None)
        return closed is False

    def get_book(self, token_id: str) -> Optional[BookState]:
        return self._books.get(token_id)

    def get_all_books(self) -> dict[str, BookState]:
        return dict(self._books)

    async def ensure_subscribed(self, token_ids: list[str]) -> None:
        if not self._ws_is_open():
            await self._connect()
        missing = [tid for tid in token_ids if tid and tid not in self._subscribed]
        if missing:
            await self._subscribe_many(missing)

    async def _connect(self) -> None:
        self._subscribed.clear()
        backoff = _BACKOFF[min(self._backoff_idx, len(_BACKOFF) - 1)] if self._fail_count > 0 else 0
        jitter = backoff * random.uniform(-0.2, 0.2) if backoff > 0 else 0
        if backoff > 0:
            await asyncio.sleep(backoff + jitter)
        try:
            log.info("WS connecting to %s", WS_URL)
            self._ws = await websockets.connect(
                WS_URL,
                ping_interval=_PING_INTERVAL,
                ping_timeout=_PING_INTERVAL,
                open_timeout=10,
                max_size=2**20,
            )
            self._connected_since = time.time()
            self._last_event_at = time.time()
            self._fail_count = 0
            self._backoff_idx = 0
            self.gave_up = False
            log.info("WS connected")
        except Exception as exc:
            log.warning("WS connect failed: %r", exc)
            self._fail_count += 1
            if self._fail_count >= _MAX_FAILURES:
                log.error("WS gave up after %d failures", _MAX_FAILURES)
                self.gave_up = True
                return
            self._backoff_idx = min(self._backoff_idx + 1, len(_BACKOFF) - 1)
            raise

    async def _subscribe(self, token_id: str) -> None:
        await self._subscribe_many([token_id])

    async def _subscribe_many(self, token_ids: list[str]) -> None:
        if self._ws is None:
            return
        token_ids = [str(tid) for tid in token_ids if tid]
        if not token_ids:
            return
        for token_id in token_ids:
            self._books.setdefault(token_id, BookState())
        msg = {"assets_ids": token_ids, "type": "market"}
        await self._ws.send(json.dumps(msg))
        self._subscribed.update(token_ids)
        log.info("Subscribed to %d tokens", len(token_ids))

    async def _listen_once(self, timeout: float = 1.0) -> None:
        assert self._ws is not None
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            return
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            log.debug("Non-JSON WS frame ignored")
            return
        if isinstance(msg, list):
            for sub in msg:
                if isinstance(sub, dict):
                    await self._handle_message(sub)
        elif isinstance(msg, dict):
            await self._handle_message(msg)

    async def run_forever(self, token_ids) -> None:
        self._ensure_lock()
        while not self._stopped:
            if self.gave_up:
                await asyncio.sleep(5)
                continue
            try:
                current_token_ids = token_ids() if callable(token_ids) else token_ids
                await self.ensure_subscribed(list(current_token_ids))
                if not self._ws_is_open():
                    continue
                await self._listen_once(timeout=1.0)
                now = time.time()
                if now - self._last_event_at > _HEALTH_SEC:
                    log.warning("WS silent for %.0fs; reconnecting", now - self._last_event_at)
                    await self._reconnect()
            except ConnectionClosed as exc:
                log.warning("WS connection closed: %r; reconnecting", exc)
                await self._reconnect()
            except Exception as exc:
                log.error("WS error: %r; reconnecting", exc)
                await self._reconnect()

    async def _reconnect(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        self._subscribed.clear()
        self._backoff_idx = min(self._backoff_idx + 1, len(_BACKOFF) - 1)
        self._fail_count += 1
        if self._fail_count >= _MAX_FAILURES:
            log.error("WS reconnect gave up after %d failures", _MAX_FAILURES)
            self.gave_up = True
        await self._connect()

    async def _handle_message(self, msg: dict) -> None:
        self._last_event_at = time.time()
        self.event_count += 1
        event_type = msg.get("event_type", "")

        if event_type == "book":
            asset_id = msg.get("asset_id", "")
            if not asset_id:
                return
            if asset_id not in self._books:
                self._books[asset_id] = BookState()
            self._books[asset_id].apply_snapshot(
                msg.get("bids", []),
                msg.get("asks", []),
            )
            log.debug("Book update for %s: bids=%d asks=%d", asset_id, len(msg.get("bids", [])), len(msg.get("asks", [])))
        elif event_type == "price_change":
            for change in msg.get("price_changes", []) or []:
                asset_id = change.get("asset_id", "")
                if not asset_id:
                    continue
                if asset_id not in self._books:
                    self._books[asset_id] = BookState()
                side = change.get("side", "")
                try:
                    price = float(change.get("price", 0))
                    size = float(change.get("size", 0))
                except (TypeError, ValueError):
                    continue
                self._books[asset_id].apply_change(side, price, size)

    def refresh_subscriptions(self, token_ids: list[str]) -> None:
        """Force full resubscription with new IDs; clears old books + state."""
        self._subscribed.clear()
        # ponytail: drop stale books so the TUI does not read dead-bucket data
        self._books.clear()
        # schedule ensure_subscribed on the event loop via a fire-and-forget
        token_ids = [str(tid) for tid in token_ids if tid]
        if not token_ids:
            return
        for token_id in token_ids:
            self._books[token_id] = BookState()
        asyncio.ensure_future(self._subscribe_many(token_ids))

    def stop(self) -> None:
        self._stopped = True

    def health(self) -> str:
        if self.gave_up:
            return "GAVE UP"
        if self._ws_is_open():
            return "CONNECTED"
        return "DISCONNECTED"
