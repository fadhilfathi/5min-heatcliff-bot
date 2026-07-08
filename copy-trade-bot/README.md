# 5-Minute BTC Directional Momentum Bot

This bot executes an autonomous directional momentum strategy for the 5-minute BTC price movement markets on Polymarket.

## Strategy

1.  **Entry Trigger**: When the BTC price moves more than `$60.00` (configurable `BTC_MOVE_THRESHOLD`) from the 5-minute bucket's opening price, the bot places a Good-Til-Cancelled (GTD) limit order.
2.  **Order Sizing**: The bot calculates the number of shares to buy based on a fixed USD stake (`STAKE_USD_PER_ENTRY`), currently set to `$0.50`.
3.  **Hedging**:
    *   If the opposite side's ask price becomes highly favorable (e.g., >= `$0.88`), the bot will place a hedge order to cover potential losses.
    *   It will also hedge if the BTC price flips direction by more than `$10.00` against the initial entry.
4.  **Settlement**: The bot holds positions until the market resolves and relies on Polymarket's automatic redemption of winning shares.

## Key Components

*   `main.py`: The main application loop, handling state, strategy logic, and TUI updates.
*   `executor.py`: Handles all interactions with the Polymarket CLOB API, including placing orders and fetching balances.
*   `tui.py`: A terminal-based user interface for monitoring the bot's status, open positions, and PnL.
*   `config.py`: Centralized configuration for all trading parameters.
*   `price_feed.py`: Fetches real-time BTC price from Binance.
*   `book.py`: Fetches order book data from Polymarket.
