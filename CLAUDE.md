# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- **Run Bot**: `run_copy_trade_bot.bat` or `python copy-trade-bot/main.py --live`
- **TUI**: Bot runs in TUI (Rich). `q` to quit, `p` to pause/resume.
- **Config**: Edit `copy-trade-bot/config.py` (thresholds, URLs).
- **Logs**: Located in `logs-copy-trade/`.

## Architecture

1. **Orchestrator**: `main.py` runs event loop (~50ms poll), state machine, bucket tracking.
2. **Trade Execution**: `executor.py` wraps CLOB SDK for orders (GTD/FOK/FAK), balance, redemption.
3. **Data/Feed**: `price_feed.py` (WS: Coinbase/Binance), `poly_book_ws.py` (WS: Polymarket orderbook).
4. **Resilience**: `_bounded()` wrapper (timeout/daemon thread) for all network I/O.
5. **Config**: All strategy knobs are in `copy-trade-bot/config.py`.

## Important
- **Market**: Repository targets BTC. Ensure slugs (`btc-updown-5m`) and price feed logic match.
- **Minimum Order**: CLOB enforces $1 min notional. Bot auto-retries GTD failure as FOK market buy at $1.
- **Hedging**: Enabled. `HEDGE_OPPOSITE_MOVE_THRESHOLD` (default 25), `HEDGE2_MOVE_THRESHOLD` (default 20) control hedging behavior.

## Development Rules
- **Commits**: Do not include "CLAUDE" in commit messages or as a contributor/co-author.
