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
        if not bid_prices:
            LOG.debug("[MARKET][BOOK] event=fetch_ok token=%s bid=none ask=%.4f", token_id[:8], best_ask)
        elif not ask_prices:
            LOG.debug("[MARKET][BOOK] event=fetch_ok token=%s bid=%.4f ask=none", token_id[:8], best_bid)
        elif best_bid > best_ask:
            LOG.debug("[MARKET][BOOK] event=anomaly token=%s bid=%.4f ask=%.4f reason=bid_gt_ask", token_id[:8], best_bid, best_ask)
        return best_bid, best_ask
    except requests.exceptions.Timeout:
        LOG.warning("[MARKET][BOOK] event=fetch_failed token=%s reason=timeout timeout_s=3", token_id[:8])
        return 0.0, 0.0
    except Exception as exc:
        LOG.warning("[MARKET][BOOK] event=fetch_failed token=%s error=%r", token_id[:8], exc)
        return 0.0, 0.0
