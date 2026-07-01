# 5min Heatcliff Bot

A suite of automated crypto trading bots and monitoring tools designed for 5-minute timeframe strategies on Polymarket and various exchanges.

## Overview

This project provides tools for:
- **All-In-One Bot**: Executes multi-coin strategies with shared websocket monitoring.
- **Arbitrage Bot**: Detects and captures market inefficiencies across different pairs.
- **Diagnostics & Management**: CLI tools and scripts to test strategies, manage bot sessions, and aggregate performance reports.

## Architecture

- `all-in-one-bot/`: Orchestrator for parallel coin trading.
- `arbitrage-bot/`: Real-time arbitrage identification and execution.
- `ws-all-in-one-monitor/`: Shared websocket stream for efficient order book tracking.
- `scripts/`: Diagnostic utilities and management scripts for individual strategies.

## Requirements

- Python 3.11 or higher
- Required packages (install via `pip`):
  - `asyncio`
  - `rich` (for TUI monitoring)
  - `websockets`
  - `requests`
  - `python-dotenv`
  - `py-clob-client-v2` (for Polymarket API integration)

## Setup

1. Clone this repository.
2. Configure credentials in the `.env` file (copy from `.env.example`):
   ```env
   PM_PRIVATE_KEY=your_private_key
   PM_API_KEY=your_api_key
   PM_API_SECRET=your_api_secret
   PM_API_PASSPHRASE=your_api_passphrase
   PM_FUNDER=your_funder_address
   PM_SIGNATURE_TYPE=2
   ```

## Running the Bots

Use the provided `.bat` files for easy execution:

### All-In-One Late-Entry Bot
Optimized for late-session entries (optimized entry/exit windows):
```cmd
run_all_in_one_late_entry_bot.bat
```

### Arbitrage Bot
```cmd
run_arbitrage_bot.bat
```

### Specific Coin Bots
Launch specific coin bots if not using the all-in-one orchestrator:
```cmd
run_btc_bot.bat
run_eth_bot.bat
```

## Strategy Customization
Most strategy parameters (entry/exit thresholds, windows) are configured in `all-in-one-bot/configs/coins_late_entry.json`.
