from __future__ import annotations

import logging
import time

import requests

from config import BINANCE_API, BTC_SYMBOL

LOG = logging.getLogger("price_feed")


def get_btc_price() -> float | None:
    try:
        resp = requests.get(
            f"{BINANCE_API}/api/v3/ticker/price",
            params={"symbol": BTC_SYMBOL},
            timeout=3,
        )
        resp.raise_for_status()
        data = resp.json()
        return float(data.get("price") or 0.0)
    except Exception as exc:
        LOG.warning("btc price fetch failed: %r", exc)
        return None
