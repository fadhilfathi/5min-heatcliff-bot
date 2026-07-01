#!/usr/bin/env python3
"""
binance_ws.py - Multi-coin price feed via Binance/Bybit WebSocket.

BTC, ETH, SOL, BNB, XRP, DOGE from Binance combined stream.
HYPE from Bybit v5 public stream.

Stores latest live trade/ticker price per symbol.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("binance_ws")

BINANCE_STREAMS_URL = "wss://stream.binance.com:9443/stream?streams="
BYBIT_STREAMS_URL = "wss://stream.bybit.com/v5/public/linear"

# Map coin names to Binance lowercase streams
BINANCE_SYMBOLS = {
    "BTC": "btcusdt@trade",
    "ETH": "ethusdt@trade",
    "SOL": "solusdt@trade",
    "BNB": "bnbusdt@trade",
    "XRP": "xrpusdt@trade",
    "DOGE": "dogeusdt@trade",
}
BYBIT_SYMBOL = "HYPE"
BYBIT_TOPIC = "tickers."

_PING_INTERVAL = 20
_MAX_FAILURES = 5
_BACKOFF = [1, 2, 4, 8, 16, 32, 60]


class PriceFeed:
    """Maintains latest prices for all coins from Binance/Bybit WS."""

    def __init__(self):
        self._prices: dict[str, float] = {}
        self._klines: dict[str, deque[dict]] = {}
        self._ws_binance: Optional = None
        self._ws_bybit: Optional = None
        self._stopped: bool = False
        self._fail_count: int = 0
        self._last_event_at: float = 0.0

    def get_price(self, coin: str) -> Optional[float]:
        return self._prices.get(coin)

    def get_klines(self, coin: str) -> list[dict]:
        return list(self._klines.get(coin, ()))

    def get_price_at_or_before(self, coin: str, ts_ms: int) -> Optional[float]:
        klines = self._klines.get(coin)
        if not klines:
            return self._prices.get(coin)
        best = None
        for item in klines:
            if int(item.get("ts_ms", 0)) <= ts_ms:
                best = item
            else:
                break
        if best is not None:
            return float(best.get("price", 0.0)) or None
        return float(klines[0].get("price", 0.0)) if klines else self._prices.get(coin)

    def _record_price(self, coin: str, price: float, ts_ms: int) -> None:
        self._prices[coin] = price
        self._last_event_at = time.time()
        series = self._klines.setdefault(coin, deque(maxlen=256))
        if series and int(series[-1].get("ts_ms", -1)) == ts_ms:
            series[-1]["price"] = price
        else:
            series.append({"ts_ms": int(ts_ms), "price": float(price)})

    def health(self) -> str:
        if self._fail_count >= _MAX_FAILURES:
            return "DOWN"
        if self._last_event_at == 0.0:
            return "CONNECTING"
        if time.time() - self._last_event_at > 30:
            return "STALE"
        return "OK"

    async def run_binance(self) -> None:
        streams = "/".join(BINANCE_SYMBOLS.values())
        url = f"{BINANCE_STREAMS_URL}{streams}"
        backoff_idx = 0
        while not self._stopped:
            try:
                async with websockets.connect(url, ping_interval=_PING_INTERVAL, ping_timeout=20, open_timeout=10) as ws:
                    log.info("Binance WS connected")
                    self._fail_count = 0
                    backoff_idx = 0
                    while not self._stopped:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        msg = json.loads(raw)
                        data = msg.get("data", {})
                        symbol = data.get("s", "")
                        if not symbol:
                            continue
                        # Find coin name from symbol
                        for coin, stream in BINANCE_SYMBOLS.items():
                            if stream.startswith(symbol.lower()):
                                price = float(data.get("p", 0))
                                if price > 0:
                                    self._record_price(coin, price, int(data.get("T", 0) or time.time() * 1000))
                                break
            except asyncio.TimeoutError:
                now = time.time()
                if now - self._last_event_at > 60 and self._last_event_at > 0:
                    log.warning("Binance WS silent for 60s, reconnecting")
                    self._fail_count += 1
            except ConnectionClosed as exc:
                log.warning("Binance WS closed: %r", exc)
                self._fail_count += 1
            except Exception as exc:
                log.error("Binance WS error: %r", exc)
                self._fail_count += 1

            if self._fail_count >= _MAX_FAILURES:
                log.error("Binance WS gave up")
                break
            backoff_idx = min(backoff_idx + 1, len(_BACKOFF) - 1)
            delay = _BACKOFF[backoff_idx]
            log.info("Binance WS reconnecting in %ds", delay)
            await asyncio.sleep(delay)

    async def run_bybit(self) -> None:
        url = BYBIT_STREAMS_URL
        backoff_idx = 0
        subscribe_msg = json.dumps({"op": "subscribe", "args": [f"{BYBIT_TOPIC}{BYBIT_SYMBOL}USDT"]})
        while not self._stopped:
            try:
                async with websockets.connect(url, ping_interval=_PING_INTERVAL, ping_timeout=20, open_timeout=10) as ws:
                    log.info("Bybit WS connected")
                    await ws.send(subscribe_msg)
                    self._fail_count = 0
                    backoff_idx = 0
                    while not self._stopped:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        msg = json.loads(raw)
                        topic = msg.get("topic", "")
                        data = msg.get("data", {})
                        if "tickers" in topic and data:
                            price = float(data.get("lastPrice", 0))
                            if price > 0:
                                self._record_price(BYBIT_SYMBOL, price, int(msg.get("ts", 0) or time.time() * 1000))
            except asyncio.TimeoutError:
                now = time.time()
                if now - self._last_event_at > 60 and self._last_event_at > 0:
                    log.warning("Bybit WS silent for 60s, reconnecting")
                    self._fail_count += 1
            except ConnectionClosed as exc:
                log.warning("Bybit WS closed: %r", exc)
                self._fail_count += 1
            except Exception as exc:
                log.error("Bybit WS error: %r", exc)
                self._fail_count += 1

            if self._fail_count >= _MAX_FAILURES:
                log.error("Bybit WS gave up")
                break
            backoff_idx = min(backoff_idx + 1, len(_BACKOFF) - 1)
            delay = _BACKOFF[backoff_idx]
            log.info("Bybit WS reconnecting in %ds", delay)
            await asyncio.sleep(delay)

    def stop(self) -> None:
        self._stopped = True
