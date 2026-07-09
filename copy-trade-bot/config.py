from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT_DIR / "logs-copy-trade"

BTC_MOVE_THRESHOLD = 37.0
SCALE_MOVE_THRESHOLD = 30.0
STAKE_USD_PER_ENTRY = 0.50
FLIP_MOVE_THRESHOLD = 20.0
HEDGE_OPPOSITE_ASK_THRESHOLD = 0.87
HEDGE_OPPOSITE_MOVE_THRESHOLD = 13.0
GTD_ENTRY_DELAY_SECONDS = 15
MAX_ENTRIES_PER_BUCKET = 1
MAX_CONCURRENT_BUCKETS = 2
MAX_SESSION_LOSS_USD = 99999.0
MIN_SECONDS_LEFT = 5
GTD_EXPIRY_OFFSET = 60

CLOB_URL = "https://clob.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com"
BTC_SYMBOL = "BTCUSDT"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
