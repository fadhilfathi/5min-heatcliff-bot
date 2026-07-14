from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT_DIR / "logs-copy-trade"

ETH_MOVE_THRESHOLD = 0.33
SCALE_MOVE_THRESHOLD = 30.0
STAKE_USD_PER_ENTRY = 0.50
ENTRY_SHARES = 1
TAKE_PROFIT_PCT = 0.01
FORCE_EXIT_SECONDS = 15
FORCE_EXIT_MIN_PROFIT_PCT = 0.0
INITIAL_ENTRY_MAX_SECS_LEFT = 240
INITIAL_ENTRY_MIN_SECS_LEFT = 15
INITIAL_ENTRY_MAX_ASK = 0.99
FLIP_MOVE_THRESHOLD = 0.15
HEDGE_OPPOSITE_ASK_THRESHOLD = 0.88
HEDGE_OPPOSITE_MOVE_THRESHOLD = 0.25
HEDGE2_MOVE_THRESHOLD = 0.20
HEDGE2_ASK_THRESHOLD = 0.87
GTD_ENTRY_DELAY_SECONDS = 15
MAX_ENTRIES_PER_BUCKET = 1
MAX_CONCURRENT_BUCKETS = 2
MAX_SESSION_LOSS_USD = 99999.0
TARGET_PROFIT_USD = 99999.0
MIN_SECONDS_LEFT = 5
# seconds after bucket close that GTD effectively dies; Polymarket subtracts 60s internally
GTD_EXPIRY_OFFSET = 30

CLOB_URL = "https://clob.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com"
ETH_SYMBOL = "ETHUSDT"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/ethusdt@trade"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_PRODUCT = "ETH-USD"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLY_WS_STALE_SECONDS = 10
