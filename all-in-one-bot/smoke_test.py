#!/usr/bin/env python3
"""
smoke_test.py — Dry-run validation test for all-in-one-bot and modules.
Verifies structure, configs, imports, and UI layout cleanly without placing live trades.
"""
import asyncio
import json
import sys
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir))

def test_imports_and_configs():
    print("[1/3] Testing imports and configuration load...")
    config_path = root_dir / "all-in-one-bot" / "configs" / "coins.json"
    assert config_path.exists(), "all-in-one-bot/configs/coins.json missing"
    
    with config_path.open("r") as f:
        coins = json.load(f)
    
    expected_coins = ["BTC", "ETH", "DOGE", "HYPE", "BNB"]
    for c in expected_coins:
        assert c in coins, f"Missing coin {c} in config"
        assert "move_min" in coins[c]
        assert "vel_min" in coins[c]
        assert "spread" in coins[c]
        assert "symbol" in coins[c]
        assert "token_id" in coins[c]
    
    print(" -> Configs loaded successfully and validated!")

def _load_module(name: str, path: Path):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_ui_render():
    print("[2/3] Testing UI Render layout engine...")
    BotUI = _load_module("ui_mod", root_dir / "all-in-one-bot" / "ui.py").BotUI
    ui = BotUI(["BTC", "ETH", "DOGE", "HYPE", "BNB"])
    ui.add_log("TEST", "Dry run smoke test active.")
    layout = ui.generate_layout()
    assert layout is not None
    print(" -> Rich TUI layout rendering operational!")

async def test_ws_streams():
    print("[3/3] Testing WebSocket engine initializations...")
    MultiBookStream = _load_module("ws_mod", root_dir / "ws-all-in-one-monitor" / "stream.py").MultiBookStream
    PriceFeed = _load_module("price_mod", root_dir / "all-in-one-bot" / "binance_ws.py").PriceFeed
    
    mb = MultiBookStream()
    pf = PriceFeed()
    
    assert mb.health() == "DISCONNECTED"
    assert pf.health() == "CONNECTING"
    print(" -> WebSocket modules loaded & initialized operational state!")

def main():
    print("=== RUNNING ALL-IN-ONE BOT SMOKE TEST (DRY RUN) ===")
    test_imports_and_configs()
    test_ui_render()
    asyncio.run(test_ws_streams())
    print("=====================================================")
    print("ALL SMOKE TESTS PASSED PERFECTLY!")

if __name__ == "__main__":
    main()
