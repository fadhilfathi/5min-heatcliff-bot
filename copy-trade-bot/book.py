from __future__ import annotations

import logging
from typing import Optional

import requests

from config import CLOB_URL

LOG = logging.getLogger("copy_book")


def fetch_best_prices(token_id: str) -> tuple[float, float]:
    """Returns (best_bid, best_ask). Both 0.0 if unavailable."""
    try:
        resp = requests.get(
            f"{CLOB_URL}/book",
            params={"token_id": token_id},
            timeout=3,
        )
        resp.raise_for_status()
        data = resp.json()
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        bid_prices = [float(b["price"]) for b in bids if b.get("price")]
        ask_prices = [float(a["price"]) for a in asks if a.get("price")]
        best_bid = max(bid_prices) if bid_prices else 0.0
        best_ask = min(ask_prices) if ask_prices else 0.0
        return best_bid, best_ask
    except Exception as exc:
        LOG.warning("book fetch failed for %s: %r", token_id[:16], exc)
        return 0.0, 0.0
