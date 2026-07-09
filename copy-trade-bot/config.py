from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT_DIR / "logs-copy-trade"

BTC_MOVE_THRESHOLD = 50.0
SCALE_MOVE_THRESHOLD = 30.0
STAKE_USD_PER_ENTRY = 0.50
FLIP_MOVE_THRESHOLD = 20.0
HEDGE_OPPOSITE_ASK_THRESHOLD = 0.87
HEDGE_OPPOSITE_MOVE_THRESHOLD = 17.5
HEDGE2_MOVE_THRESHOLD = 20.0
HEDGE2_ASK_THRESHOLD = 0.88
GTD_ENTRY_DELAY_SECONDS = 15
MAX_ENTRIES_PER_BUCKET = 1
MAX_CONCURRENT_BUCKETS = 2
MAX_SESSION_LOSS_USD = 99999.0
MIN_SECONDS_LEFT = 5
# seconds after bucket close that GTD effectively dies; Polymarket subtracts 60s internally
GTD_EXPIRY_OFFSET = 30

CLOB_URL = "https://clob.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com"
BTC_SYMBOL = "BTCUSDT"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_PRODUCT = "BTC-USD"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
