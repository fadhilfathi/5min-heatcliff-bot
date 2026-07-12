# 5-Minute ETH Directional Momentum Bot

Autonomous directional momentum bot for the 5-minute ETH Up/Down prediction markets on Polymarket. Buys when ETH moves beyond a threshold from the bucket's opening price, takes profit at 7%+, and hedges if the direction flips.

## Strategy

1. **Entry**: When ETH price moves more than `$0.50` (`ETH_MOVE_THRESHOLD`) from the bucket open, the bot places a GTD limit buy order on the winning side. If rejected for being marketable, retries as FOK with $1 minimum.
2. **Take Profit**: Exits positions when profit reaches 7%+ (`TAKE_PROFIT_PCT`). Sells via FOK market sell, falling back to FAK limit sell (clamped to $0.99 max).
3. **Force Exit**: In the last 15 seconds (`FORCE_EXIT_SECONDS`), force-sells any position in profit.
4. **Hedge**: If ETH flips direction against the entry, buys the opposite side to recover losses when the opposite ask is favorable (`HEDGE_OPPOSITE_ASK_THRESHOLD = 0.88`).
5. **Hedge 2**: If ETH flips back to the original entry side after hedging, buys that side again to recover hedge loss + 1%.
6. **Settlement**: Holds remaining positions until the 5-minute bucket resolves. Polymarket auto-redeems winning shares.

## Key Components

| File | Purpose |
|------|---------|
| `main.py` | Main loop, strategy logic, TUI integration |
| `executor.py` | Polymarket CLOB API: orders (GTD, FOK, FAK), balance, resolution |
| `tui.py` | Rich terminal UI with colored table, logs, header |
| `config.py` | All trading parameters and API URLs |
| `price_feed.py` | Real-time ETH price from Binance REST/WS + Coinbase WS |
| `poly_book_ws.py` | Polymarket order book WebSocket for live bid/ask |
| `models.py` | `Entry` dataclass |

## Configuration (`config.py`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ETH_MOVE_THRESHOLD` | `0.50` | Min ETH move from open to trigger entry |
| `STAKE_USD_PER_ENTRY` | `0.50` | USD amount per entry order |
| `TAKE_PROFIT_PCT` | `0.07` | Profit % to trigger exit |
| `FORCE_EXIT_SECONDS` | `15` | Force sell in last N seconds if profitable |
| `INITIAL_ENTRY_MAX_SECS_LEFT` | `210` | Max seconds left to place first entry |
| `INITIAL_ENTRY_MIN_SECS_LEFT` | `15` | Min seconds left to place first entry |
| `FLIP_MOVE_THRESHOLD` | `0.15` | ETH move to trigger flip entry |
| `HEDGE_OPPOSITE_ASK_THRESHOLD` | `0.88` | Opposite ask price to trigger hedge |
| `HEDGE_OPPOSITE_MOVE_THRESHOLD` | `0.25` | ETH move against position to trigger hedge |
| `MAX_ENTRIES_PER_BUCKET` | `1` | Max entries per 5-minute bucket |
| `MAX_CONCURRENT_BUCKETS` | `2` | Max open positions across buckets |

## Running

```bash
# Live trading
run_copy_trade_bot.bat

# Or directly
python copy-trade-bot\main.py --live

# With verbose logging
python copy-trade-bot\main.py --live --verbose
```

## Environment Variables (`.env`)

```
PM_PRIVATE_KEY=0x...
PM_FUNDER=0x...
PM_API_KEY=...
PM_API_SECRET=...
PM_API_PASSPHRASE=...
PM_SIGNATURE_TYPE=2
```

## TUI

- **Header**: Mode, ETH price, move, direction, balance, PnL, entry count
- **Table**: Bucket history (newest first) with direction, entries, move, limit, shares, cost, PnL, status, seconds left
- **Footer**: Live event log with runtime and poll count
- **Controls**: `q` = quit, `p` = pause/resume
- Colors: green = profit/up, red = loss/down, cyan = open positions, blue = runtime

## Logs

Session logs are written to `logs-copy-trade/` with timestamped filenames. Use `--verbose` to include DEBUG-level messages.
