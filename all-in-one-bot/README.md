# All-In-One Bot

Shared multi-coin Polymarket 5-minute UP/DOWN binary prediction market bot.

## Requirements

Python 3.11+

```
rich==15.0.0
websockets==15.0.1
requests==2.34.2
aiohttp==3.14.1
python-dotenv==1.2.2
py-clob-client-v2==1.0.1
```

Install:
```
pip install rich==15.0.0 websockets==15.0.1 requests==2.34.2 aiohttp==3.14.1 python-dotenv==1.2.2 py-clob-client-v2==1.0.1
```

## Architecture

| File | Role |
|---|---|
| `launcher.py` | Entry point. Starts `UnifiedBot` in a background thread, runs the Rich TUI in the main thread. |
| `unified_bot.py` | Resolves token IDs from Gamma API, subscribes to the shared WS book stream, starts one thread per coin, runs the `_scan_loop` that feeds TUI state. |
| `ws_trade_runner.py` | Per-coin strategy: entry gates, buy order, exit monitor. All coins share this file; coin-specific values come from `COIN_CFG` loaded from the chosen config JSON. |
| `binance_ws.py` | Live price feed (Binance trade stream + Bybit ticker). Maintains per-coin price history for move/velocity calculation. |
| `ws-all-in-one-monitor/stream.py` | Polymarket order book WS. Single connection subscribing to all active UP/DOWN token IDs, one per message (Polymarket requires individual subscription messages, not bulk). |
| `configs/coins.json` | Main strategy config (ETH, BNB, HYPE, XRP, DOGE). |
| `configs/coins_late_entry.json` | Late-entry strategy config (BTC, ETH, SOL, XRP, DOGE). |
| `ui.py` | Rich TUI layout, keyboard thread, state rendering. |

## Launchers

### Main bot
```
run_all_in_one_bot.bat
python all-in-one-bot\launcher.py --max-loss 5 --target-balance 10 --max-trades 0
```
Coins: ETH, BNB, HYPE, XRP, DOGE
Entry window: 30–90s left (defaults from `ws_trade_runner.py`)
Logs to: `logs-all-in-one/`

### Late-entry bot
```
run_all_in_one_late_entry_bot.bat
python all-in-one-bot\launcher.py --max-loss 5 --target-balance 10 --max-trades 0 --config all-in-one-bot\configs\coins_late_entry.json --log-dir logs-all-in-one-late --min-entry-seconds-left 15 --max-entry-seconds-left 35 --profit-exit-seconds-left 10
```
Coins: BTC, ETH, SOL, XRP, DOGE
Entry window: 15–35s left (per CLI default, overridden per coin in config)
Logs to: `logs-all-in-one-late/`

Note: per-coin config values always override CLI defaults. Both configs currently set `min_entry_seconds_left` and `max_entry_seconds_left` explicitly per coin.

## Current Coin Sets

### Main config (`configs/coins.json`)

| Coin | Entry Threshold | Max Ask | Spread | Move Range (0.60→0.95) | Vel Min | SL% | Exit Gates |
|---|---|---|---|---|---|---|---|
| ETH | 0.60 | 0.95 | 0.10 | $3.00 → $0.80 | $0.20/30s | 40% | trail=0.40, sl=0.20, force=0.60 |
| BNB | 0.60 | 0.95 | 0.10 | $0.26 → $0.15 | $0.015/30s | 40% | trail=0.07, sl=0.07, force=0.10 |
| HYPE | 0.60 | 0.95 | 0.10 | $0.039 → $0.027 | $0.00025/30s | 40% | trail=0.005, sl=0.010, force=0.010 |
| XRP | 0.60 | 0.95 | 0.10 | $0.0032 → $0.0010 | $0.00015/30s | 40% | trail=0.00033, sl=0.00033, force=0.0005 |
| DOGE | 0.60 | 0.95 | 0.10 | $0.0000575 → $0.0000306 | $0.00000403/30s | 40% | trail=0.0000061, sl=0.0000061, force=0.0000122 |

`stop_loss_pct = 0.40` means: stop fires when `sell_now < notional × (1 - 0.40)` = when sell_now drops below $0.60 on a $1 stake.

### Late-entry config (`configs/coins_late_entry.json`)

| Coin | Entry Window | Profit Exit | Max Ask | Spread | Move Min (0.90→0.99) | Vel Min | SL% | Force Exit Gate |
|---|---|---|---|---|---|---|---|---|
| BTC | 15–35s | 10s | 0.98 | 0.15 | $27.0 → $23.0 | $3.30/30s | disabled | 99999 |
| ETH | 15–35s | 10s | 0.98 | 0.15 | $1.0 → $0.6 | $0.10/30s | disabled | 99999 |
| SOL | 15–35s | 10s | 0.98 | 0.15 | $0.12 → $0.06 | $0.0033/30s | disabled | 99999 |
| XRP | 15–35s | 10s | 0.99 | 0.15 | $0.0012 → $0.0008 | $0.0001/30s | disabled | 99999 |
| DOGE | 15–35s | 10s | 0.98 | 0.15 | $0.000080 → $0.000020 | $0.0000050/30s | disabled | 99999 |

`stop_loss_pct = 1` → `stop_loss_value = notional × (1 - 1) = $0.00` → SL never fires.
`force_exit_move_gate = 99999` → thesis gate never blocks at 5s → always force-closes, never rides settlement.

## Price Feed Sources

| Coin | Source | Stream |
|---|---|---|
| BTC | Binance | `btcusdt@trade` |
| ETH | Binance | `ethusdt@trade` |
| SOL | Binance | `solusdt@trade` |
| BNB | Binance | `bnbusdt@trade` |
| XRP | Binance | `xrpusdt@trade` |
| DOGE | Binance | `dogeusdt@trade` |
| HYPE | Bybit | `tickers.HYPEUSDT` |

`Move$` in TUI and logs is calculated from these live feeds, not from Polymarket book prices.
Move/velocity uses 7 decimal places in logs and TUI.

## Config Precedence

Values resolve in this order (first match wins):
1. Per-coin key in chosen config JSON
2. CLI argument
3. Shared default constant in `ws_trade_runner.py`

### Shared defaults in `ws_trade_runner.py`

| Constant | Value | Description |
|---|---|---|
| `DEFAULT_MIN_BUY_USD` | 1.0 | Minimum stake per trade |
| `DEFAULT_MAX_STAKE_USD` | 1.0 | Maximum stake per trade |
| `DEFAULT_MAX_SPREAD` | 0.05 | Max bid-ask spread (overridden by config `spread`) |
| `DEFAULT_MIN_BOOK_DEPTH_USD` | 1.0 | Minimum ask-side depth to enter |
| `DEFAULT_STOP_LOSS_BID` | 0.05 | Skip SL sell if current bid is below this floor (book too dead to sell into) |
| `DEFAULT_STOP_LOSS_PCT` | 0.67 | Fallback SL percent if not in config |
| `DEFAULT_TAKE_PROFIT_PCT` | 0.07 | Trailing TP arms at `notional × 1.07` |
| `DEFAULT_MAX_ENTRY_SECONDS_LEFT` | 90 | Latest entry cutoff |
| `DEFAULT_MIN_ENTRY_SECONDS_LEFT` | 30 | Earliest entry cutoff |
| `DEFAULT_MAX_ENTRY_ASK` | 0.95 | Reject entry if ask is above this |
| `DEFAULT_TIME_EXIT_MIN_SELL_USD` | 1.0 | Min sell value for timed exit |
| `DEFAULT_TRAIL_PCT` | 0.03 | Trailing TP retracement: exit if sell_now drops 3% from peak |
| `DEFAULT_MAX_BID_EXIT` | 0.99 | Exit immediately if bid hits this value and trade is profitable |
| `DEFAULT_CLOSE_RETRY_MAX` | 3 | Number of FOK sell attempts in close ladder |
| `DEFAULT_CONFIRM_GAP_SEC` | 0.10 | Sleep between double-snapshot confirmation ticks |
| `DEFAULT_ENTRY_TIMEOUT_MIN` | 60 | Give up scanning for entry after this many minutes |
| `DEFAULT_POLL_SEC` | 0.2 | Sleep between scan/monitor ticks |

## Main Strategy Flow (per coin, per bucket)

```
1. Resolve current 5m market slug from Gamma API
2. Check not already traded this bucket
3. Wait until inside entry window (seconds_left between min and max)
4. Check books are not empty (WS has data)
5. Find candidate side (UP or DOWN) passing:
   - move_min_by_threshold: ask-price-tiered move minimum
   - spread <= configured spread limit
   - ask_depth_usd >= min_book_depth_usd
   - ask <= max_entry_ask
6. Confirm move and velocity:
   - move >= move_min for current ask tier
   - velocity (price change over vel_window_sec) >= vel_min
7. Double-confirm: same side must pass on two consecutive polls
8. Submit FOK buy order (up to 3 attempts)
9. Enter exit monitor loop
```

## Entry Gate Log Messages

| Log | Meaning |
|---|---|
| `gate: no_current_market` | No active 5m market found for this bucket |
| `gate: already_traded_this_bucket` | Already bought in this 5m window |
| `gate: seconds_left seconds_left=X allowed=Y..Z` | Outside the entry time window |
| `gate: empty_books ws_health=DISCONNECTED` | WS not connected or no book data yet |
| `gate: no_candidate` | No side passed spread/depth/move/ask checks |
| `gate: ask_too_high` | Best ask above `max_entry_ask` |
| `gate: move_missing` | Asset move could not be calculated (price feed not ready) |
| `gate: move_or_velocity` | Move or velocity below threshold |
| `gate: confirming side=... signal_polls=1/2` | First confirmation passed, waiting for second |

## Exit Logic

### Exit monitor tracks each poll:
- `current_bid` — best bid for held token
- `sell_now_value` = `held_shares × current_bid`
- `peak_sell_now` — highest sell_now ever seen this trade
- `current_move` — asset move since bucket open
- `seconds_left` — time remaining in bucket
- `take_profit_value` = `notional × (1 + take_profit_pct)` (e.g., $1.07 on $1 stake)
- `stop_loss_value` = `notional × (1 - stop_loss_pct)` (e.g., $0.60 on $1 stake with 40% SL)

### Exit paths (checked in order each poll):

**1. Max bid exit** — fires immediately if `bid >= max_bid_exit` and trade is profitable. Default 0.99, meaning this only fires if bid goes near $1 (near-certain win).

**2. Profit exit** (late-entry only) — fires when `seconds_left <= profit_exit_seconds_left` AND `sell_now > notional`. Exits at 10s left if any profit exists. Primary exit path for late-entry bot.

**3. Trailing take profit** — arms when `sell_now >= take_profit_value`. Once armed, exits if `sell_now <= peak × (1 - trail_pct)` (i.e., if sell_now drops 3% from peak after passing TP level). Double-snapshot confirms (rejects stale ticks). Blocked by `trail_exit_move_gate` thesis check.

**4. Stop loss** — fires when `sell_now <= stop_loss_value` AND `seconds_left > 5`. Double-snapshot confirms. Skipped if `current_bid < stop_loss_bid` (book too dead, ride settlement instead). Blocked by `stop_loss_move_gate` thesis check. **Disabled in late-entry config** (`stop_loss_pct = 1`).

**5. Force exit at 5s** — fires when `seconds_left <= 5`. If `thesis_allows_exit(force_exit_move_gate)` is True, closes with reason `force_exit_in_loss_5s_left`. If False (coin still moving hard in trade direction), closes with `settlement_wait_thesis_alive_5s` and **rides settlement**. In late-entry config, `force_exit_move_gate = 99999` so this always force-closes and never rides settlement.

**6. Settlement ride** — position held until bucket expiry. Shares settle at $1.00 (correct direction) or $0.00 (wrong direction). TUI counts this as a win.

### Thesis gate (`thesis_allows_exit`)

Used by trailing TP, SL, and force exit. Returns `True` (allow exit) when the asset is NOT strongly moving in the trade direction. Returns `False` (block exit) when:
- Trade is UP and asset move is strongly positive (>= gate value)
- Trade is DOWN and asset move is strongly negative (>= gate value in absolute terms)

Gate values per coin are configured separately for each exit type:
- `trail_exit_move_gate` — blocks trailing TP exits
- `stop_loss_move_gate` — blocks SL exits
- `force_exit_move_gate` — blocks 5s force exit (set to 99999 in late-entry = never blocks)

### Close ladder (FOK sell)

On exit, `close_with_ladder` attempts up to `close_retry_max` (default 3) FOK sell orders:
- Each attempt reads current bid and bid_size from CLOB
- Chunks the sell to what bid_size can absorb
- If FOK fails, halves the chunk and retries after 0.3s
- If all attempts fail: falls back to `settlement_ride` (`ladder_exhausted_ride_settlement`)
- If bid is below `stop_loss_bid` before close: skips the ladder entirely and rides settlement

## Strategy Differences: Main vs Late-Entry

| Aspect | Main Bot | Late-Entry Bot |
|---|---|---|
| Entry window | 30–90s left | 15–35s left |
| Entry ask range | 0.60–0.95 (any skew) | 0.90–0.99 (strong skew only) |
| Move requirement at 0.95 ask | ETH $0.80, BNB $0.15 | ETH $0.80, BTC $25 |
| Stop loss | Active (40% loss = trigger) | Disabled (pct=1) |
| Primary exit | Trailing TP or 5s settlement ride | Profit exit at 10s left |
| Settlement ride | Common (86% of exits in typical session) | Never (force_exit_move_gate=99999) |
| Thesis gate at 5s | Blocks exit if still in-direction | Disabled (threshold 99999) |

## TUI

### Columns

| Column | Description |
|---|---|
| `Coin` | Coin name. Bold when in a trade. Dim when WAITING/no data. |
| `Move$` | Asset price change since bucket open. Green = up, Red = down. 7 decimal places. |
| `UP Ask` | Current best ask for the UP token |
| `DOWN Ask` | Current best ask for the DOWN token |
| `Side` | UP or DOWN when in a trade, blank otherwise |
| `Status` | SCANNING (yellow), IN_TRADE/SELLING (bold cyan), WAITING/PRICE ONLY (dim), ERROR (bold red) |
| `W/L` | Per-coin wins / losses this session |
| `Secs` | Seconds left in current bucket. Red ≤10s, yellow ≤30s |

### Header

- Session PnL (from start balance to current balance via `_meta.session_balance_pnl`)
- Total trade count and W/L across all coins

### Footer

- `Copyright (c) Heatcliff`
- Current wallet balance (polled from Polymarket CLOB every 60s)
- Session runtime (HH:MM:SS)

### Keyboard shortcuts

| Key | Action |
|---|---|
| `q` | Quit |
| `p` | Pause/unpause trading |
| `v` | Toggle verbose log output |
| `a` | Show all coins in log panel |
| `b` | Filter logs to BTC |
| `e` | Filter logs to ETH |
| `s` | Filter logs to SOL (no dedicated key — SOL is not in main config) |
| `n` | Filter logs to BNB |
| `h` | Filter logs to HYPE |
| `x` | Filter logs to XRP |
| `d` | Filter logs to DOGE |

## W/L Counting Behavior

- Trade closes with positive PnL → win
- Trade closes with negative PnL → loss
- `settlement_pending` paths (settlement ride) → counted as win in TUI at close time (actual outcome unknown until settlement)
- `already_traded_this_bucket` → no W/L change
- `ladder_exhausted_ride_settlement` → counted as win (settlement outcome unknown)

## Common Log Patterns

```
scan: up_bid=0.73 up_ask=0.74 down_bid=0.26 down_ask=0.27 move=$+0.3000 seconds_left=87
gate: no_candidate skips=[{side: UP, reasons: [spread_too_wide]}, ...]
gate: confirming side=DOWN signal_polls=1/2
entry candidate: DOWN ask=0.68 spread=0.05 eth=$2847.50 buy=$1.0000
submitting BUY DOWN token=... amount=$1.0000
BUY DOWN @ $0.6800 amount=$1.0000 shares=1.47059 move=$-1.2000
monitor: bid=0.71 sell_now=1.0441 peak=1.0441 move=$-1.3000 stop_usd=$0.6000 tp_usd=$1.0700 92s left
trailing_tp book crashed in loss (sell_now=0.9500 floor=1.0100); continuing to monitor
stop_loss rejected: stale tick (1st=0.5900 2nd=0.6100 stop=0.6000)
stop_loss skipped: bid 0.04 below floor 0.05; continuing to monitor
closing: trailing_tp_peak_1.0786_floor_1.0463
closing: profit_exit_10s_left
closing: stop_loss_sell_now_0.6
closing: settlement_wait_thesis_alive_5s
closing: force_exit_in_loss_5s_left
closing: ladder_exhausted_ride_settlement
finished: no_entry_timeout
finished: already_traded_this_bucket
```

## Report Files

JSON report writing is currently disabled. `write_report(...)` is a no-op. All output is plain text log files only.

## Files To Check First

If behavior looks wrong, check in this order:

1. `all-in-one-bot/configs/coins.json` — main strategy values
2. `all-in-one-bot/configs/coins_late_entry.json` — late-entry strategy values
3. `all-in-one-bot/ws_trade_runner.py` — all gate logic, exit logic, defaults
4. `all-in-one-bot/unified_bot.py` — token resolution, WS subscription, TUI state feed
5. `all-in-one-bot/binance_ws.py` — price feed and move/velocity calculation
6. `logs-all-in-one/` or `logs-all-in-one-late/` — live run output

## Known Limits

- `Move$` is based on external exchange prices (Binance/Bybit), not Polymarket or Chainlink oracle prices. SOL and others can diverge from Polymarket's settlement source.
- Polymarket WS requires one subscription message per token (not bulk). Sending all tokens in one message causes the server to close the connection immediately.
- `take_profit_usd` and `time_exit_min_sell_usd` still exist as args/defaults but are not the primary active close path.
- SOL has no dedicated keyboard filter key in `ui.py` (SOL is only in the late-entry config).
- `_meta` state key (balance, session PnL) is set by `unified_bot.py`; if it is not present, TUI header shows `n/a`.
