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
        price = float(data.get("price") or 0.0)
        if price <= 0:
            LOG.warning("[MARKET][PRICE] event=fetch_failed reason=non_positive price=%.2f payload_keys=%s", price, list(data.keys()))
            return None
        return price
    except requests.exceptions.Timeout:
        LOG.warning("[MARKET][PRICE] event=fetch_failed reason=timeout symbol=%s timeout_s=3", BTC_SYMBOL)
        return None
    except requests.exceptions.HTTPError as exc:
        LOG.warning("[MARKET][PRICE] event=fetch_failed reason=http status=%s symbol=%s error=%r", exc.response.status_code if exc.response else None, BTC_SYMBOL, exc)
        return None
    except Exception as exc:
        LOG.warning("[MARKET][PRICE] event=fetch_failed reason=unknown error=%r", exc)
        return None
