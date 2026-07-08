from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Entry:
    coin: str
    bucket_ts: int
    side: str
    token: str
    shares: float
    limit_price: float
    cost: float
    placed_at: float
    order_id: str = ""
    status: str = "PENDING"
