from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("arb_ws")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10
BACKOFF_STEPS = [1, 2, 4, 8, 16, 32, 60]
MAX_FAILURES = 5
HEALTH_SEC = 15.0


class BookState:
    __slots__ = ("bids", "asks", "updated_at")

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.updated_at: float = 0.0

    def apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        self.bids = {
            float(level["price"]): float(level["size"])
            for level in bids
            if _valid_level(level)
        }
        self.asks = {
            float(level["price"]): float(level["size"])
            for level in asks
            if _valid_level(level)
        }
        self.updated_at = time.time()

    def apply_change(self, side: str, price: float, size: float) -> None:
        book = self.bids if side == "BUY" else self.asks if side == "SELL" else None
        if book is None:
            return
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size
        self.updated_at = time.time()

    def best_bid(self) -> tuple[float, float]:
        if not self.bids:
            return 0.0, 0.0
        price = max(self.bids)
        return price, float(self.bids.get(price) or 0.0)

    def best_ask(self) -> tuple[float, float]:
        if not self.asks:
            return 0.0, 0.0
        price = min(self.asks)
        return price, float(self.asks.get(price) or 0.0)


class ArbBookStream:
    def __init__(self, token_ids: list[str], on_book_update: Callable[[str, float, float, float, float], None]):
        self._token_ids: list[str] = list(token_ids)
        self._on_book_update = on_book_update
        self._books: dict[str, BookState] = {}
        self._ws = None
        self._stopped = False
        self._subscribed: set[str] = set()
        self._last_event_at = 0.0
        self._backoff_idx = 0
        self._fail_count = 0
        self.gave_up = False
        self.event_count = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None

    def token_ids(self) -> list[str]:
        return list(self._token_ids)

    def replace_token_ids(self, token_ids: list[str]) -> None:
        self._token_ids = list(token_ids)
        self._subscribed.clear()
        self._books = {token_id: self._books.get(token_id, BookState()) for token_id in self._token_ids}
        self._request_reconnect()

    def stop(self) -> None:
        self._stopped = True

    async def run_forever(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        while not self._stopped:
            if self.gave_up:
                await asyncio.sleep(5)
                continue
            try:
                await self._ensure_connected()
                if not self._ws_is_open():
                    continue
                await self._ensure_subscribed(self._token_ids)
                await asyncio.wait_for(self._listen_once(), timeout=30)
            except asyncio.TimeoutError:
                now = time.time()
                if now - self._last_event_at > HEALTH_SEC:
                    log.warning("arb ws silent for %.0fs; reconnecting", now - self._last_event_at)
                    await self._reconnect()
            except ConnectionClosed as exc:
                log.warning("arb ws closed: %r", exc)
                await self._reconnect()
            except Exception as exc:
                log.warning("arb ws error: %r", exc)
                await self._reconnect()

    async def _ensure_connected(self) -> None:
        if self._ws_is_open():
            return
        backoff = BACKOFF_STEPS[min(self._backoff_idx, len(BACKOFF_STEPS) - 1)] if self._fail_count > 0 else 0
        jitter = backoff * random.uniform(-0.2, 0.2) if backoff > 0 else 0
        if backoff > 0:
            await asyncio.sleep(backoff + jitter)
        self._ws = await websockets.connect(
            WS_URL,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_INTERVAL,
            open_timeout=10,
            max_size=2**20,
        )
        self._fail_count = 0
        self._backoff_idx = 0
        self._last_event_at = time.time()
        self._subscribed.clear()
        self.gave_up = False
        log.info("arb ws connected")

    async def _reconnect(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        self._subscribed.clear()
        self._backoff_idx = min(self._backoff_idx + 1, len(BACKOFF_STEPS) - 1)
        self._fail_count += 1
        if self._fail_count >= MAX_FAILURES:
            self.gave_up = True
            log.error("arb ws gave up after %d failures", MAX_FAILURES)

    async def _ensure_subscribed(self, token_ids: list[str]) -> None:
        missing = [token_id for token_id in token_ids if token_id and token_id not in self._subscribed]
        if not missing or self._ws is None:
            return
        for token_id in missing:
            self._books.setdefault(token_id, BookState())
        payload = {"assets_ids": [str(token_id) for token_id in missing], "type": "market"}
        await self._ws.send(json.dumps(payload))
        self._subscribed.update(missing)
        log.info("arb ws subscribed count=%d", len(missing))

    async def _listen_once(self) -> None:
        assert self._ws is not None
        async for raw in self._ws:
            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if isinstance(msg, list):
                for item in msg:
                    if isinstance(item, dict):
                        await self._handle_message(item)
            elif isinstance(msg, dict):
                await self._handle_message(msg)

    async def _handle_message(self, msg: dict) -> None:
        self._last_event_at = time.time()
        self.event_count += 1
        event_type = msg.get("event_type", "")
        if event_type == "book":
            token_id = str(msg.get("asset_id") or "")
            if not token_id:
                return
            book = self._books.setdefault(token_id, BookState())
            book.apply_snapshot(msg.get("bids") or [], msg.get("asks") or [])
            best_bid, bid_size = book.best_bid()
            best_ask, ask_size = book.best_ask()
            self._on_book_update(token_id, best_bid, best_ask, bid_size, ask_size)
            return
        if event_type == "price_change":
            for change in msg.get("price_changes", []) or []:
                token_id = str(change.get("asset_id") or "")
                if not token_id:
                    continue
                try:
                    price = float(change.get("price", 0.0) or 0.0)
                    size = float(change.get("size", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                side = str(change.get("side") or "")
                book = self._books.setdefault(token_id, BookState())
                book.apply_change(side, price, size)
                best_bid, bid_size = book.best_bid()
                best_ask, ask_size = book.best_ask()
                self._on_book_update(token_id, best_bid, best_ask, bid_size, ask_size)

    def _ws_is_open(self) -> bool:
        if self._ws is None:
            return False
        state = getattr(self._ws, "state", None)
        if state is not None:
            return int(state) == 1
        closed = getattr(self._ws, "closed", None)
        return closed is False

    def _request_reconnect(self) -> None:
        if self._loop is None or self._ws is None:
            return
        if self._loop_thread_id == threading.get_ident():
            self._ws.close()
            return
        self._loop.call_soon_threadsafe(self._ws.close)


def _valid_level(level: dict) -> bool:
    try:
        float(level["price"])
        float(level["size"])
        return True
    except (KeyError, TypeError, ValueError):
        return False
