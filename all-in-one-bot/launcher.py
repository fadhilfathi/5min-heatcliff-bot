#!/usr/bin/env python3
"""
launcher.py — UI wrapper for the All-In-One Unified Bot.
Starts the unified_bot in a background thread and runs the Rich TUI in the main thread.
"""
import json
import logging
import sys
import threading
import time
from pathlib import Path
import importlib.util
import asyncio

ROOT = Path(__file__).resolve().parent


def load_module(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


BotUI = load_module("ui_mod", "ui.py").BotUI
UnifiedBot = load_module("bot_mod", "unified_bot.py").UnifiedBot

# Silence websockets and other library logs so they don't break the UI
logging.getLogger("websockets").setLevel(logging.ERROR)
logging.getLogger("binance_ws").setLevel(logging.ERROR)
logging.getLogger("ws_all_in_one").setLevel(logging.INFO)

def run_bot_thread(bot):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(bot.run())
    except Exception as e:
        pass
    finally:
        loop.close()

import argparse

def main():
    parser = argparse.ArgumentParser(description="All-In-One 5M Bot")
    parser.add_argument("--max-loss", type=float, default=1.0, help="Max session loss in USD")
    parser.add_argument("--target-balance", type=float, default=None, help="Stop when balance reaches this value")
    parser.add_argument("--max-trades", type=int, default=0, help="Stop after this many trades (0=unlimited)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--config", default=str(ROOT / "configs" / "coins.json"), help="Coin config file")
    parser.add_argument("--log-dir", default=str(ROOT.parent / "logs-all-in-one"), help="Log directory")
    args, child_args = parser.parse_known_args()

    root_dir = Path(__file__).resolve().parent
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: {config_path} missing.")
        sys.exit(1)
        
    with config_path.open("r", encoding="utf-8") as f:
        coins = json.load(f)
        
    ui = BotUI(list(coins.keys()))
    bot = UnifiedBot(
        ui_queue=ui.logs,
        max_session_loss_usd=args.max_loss,
        target_balance=args.target_balance,
        max_trades=args.max_trades,
        verbose_logging=args.verbose,
        config_path=config_path,
        log_dir=Path(args.log_dir),
        child_extra_args=child_args,
    )
    
    # Inject shared state dict reference so UI can read bot's live state
    ui.state = bot.positions
    
    t = threading.Thread(target=run_bot_thread, args=(bot,), daemon=True)
    t.start()
    
    ui.add_log("SYSTEM", "All-In-One Bot Started.")
    
    try:
        ui.start()
    except KeyboardInterrupt:
        bot.stop()
        print("\nShutting down safely...")

if __name__ == "__main__":
    main()
