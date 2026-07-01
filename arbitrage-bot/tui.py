from __future__ import annotations

import threading
import time
from queue import Queue
from typing import Any

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.table import Table as RichTable


class ArbUI:
    def __init__(self, coins: list[str], state: dict[str, Any], control: dict[str, Any]):
        self.coins = [coin.upper() for coin in coins]
        self.state = state
        self.control = control
        self.logs: Queue[str] = Queue(maxsize=20)
        self.console = Console()
        self.started_at = time.time()
        self._stopped = False

    def add_log(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        if self.logs.full():
            self.logs.get()
        self.logs.put(line)

    def stop(self) -> None:
        self._stopped = True

    def start(self) -> None:
        threading.Thread(target=self._kb_thread, daemon=True).start()
        with Live(self._render(), refresh_per_second=4, screen=True) as live:
            while not self._stopped and not self.control.get("quit"):
                time.sleep(0.25)
                live.update(self._render())
        self.control["quit"] = True

    def _kb_thread(self) -> None:
        try:
            import msvcrt

            while not self._stopped and not self.control.get("quit"):
                if msvcrt.kbhit():
                    ch = msvcrt.getch().decode("utf-8", errors="ignore").lower()
                    if ch == "q":
                        self.control["quit"] = True
                    elif ch == "p":
                        self.control["paused"] = not bool(self.control.get("paused"))
                time.sleep(0.05)
        except Exception:
            return

    def _render(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=4),
        )

        meta = self.state.get("_meta", {})
        total_pnl = float(meta.get("pnl_usd", 0.0) or 0.0)
        total_arbs = int(meta.get("arb_count", 0) or 0)
        active = "PAUSED" if self.control.get("paused") else "ACTIVE"
        active_style = "bold red" if self.control.get("paused") else "bold green"
        layout["header"].update(
            Panel(
                f"[bold green]Polymarket Arbitrage Bot[/bold green]  "
                f"PnL: [bold {'green' if total_pnl >= 0 else 'red'}]${total_pnl:+.4f}[/bold {'green' if total_pnl >= 0 else 'red'}]  "
                f"Arbs: [cyan]{total_arbs}[/cyan]  "
                f"[{active_style}][{active}][/{active_style}]  "
                f"[dim]q=quit  p=pause  Threshold: {meta.get('threshold', 'n/a')}  Shares: {meta.get('shares_per_leg', 5)}[/dim]"
            )
        )

        table = Table(expand=True, show_lines=False, padding=(0, 1))
        table.add_column("Coin", width=6)
        table.add_column("UP Ask", justify="right", width=8)
        table.add_column("DN Ask", justify="right", width=8)
        table.add_column("Combined", justify="right", width=10)
        table.add_column("Gap%", justify="right", width=7)
        table.add_column("Status", width=10)
        table.add_column("Secs", justify="right", width=6)

        rows = self.state.get("coins", {})
        for coin in self.coins:
            row = rows.get(coin, {})
            up_bid = float(row.get("up_ask", 0.0) or 0.0)
            down_bid = float(row.get("down_ask", 0.0) or 0.0)
            combined = row.get("combined_ask")
            secs = int(row.get("secs_left", 0) or 0)
            status = str(row.get("status", "WAITING"))
            gap = None if combined is None or combined <= 0 else (1.0 - float(combined)) * 100.0
            coin_style = "bright_green" if combined is not None and combined < float(meta.get("threshold", 0.98) or 0.98) else "white"
            status_style = _status_style(status)
            secs_style = "red" if secs < 60 else "yellow" if secs < 120 else "white"
            table.add_row(
                f"[{coin_style}]{coin}[/{coin_style}]",
                f"{up_bid:.4f}" if up_bid > 0 else "-",
                f"{down_bid:.4f}" if down_bid > 0 else "-",
                f"{float(combined):.4f}" if combined is not None else "-",
                f"{gap:.2f}" if gap is not None else "-",
                f"[{status_style}]{status}[/{status_style}]",
                f"[{secs_style}]{secs}[/{secs_style}]",
            )

        layout["body"].update(table)
        last_message = meta.get("last_message") or "No trades yet"
        elapsed = max(0, int(time.time() - self.started_at))
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        balance = meta.get("balance")
        balance_text = "n/a" if balance is None else f"${float(balance):.4f}"
        footer = RichTable.grid(expand=True)
        footer.add_column(justify="left")
        footer.add_column(justify="right")
        footer.add_row(
            "[bright_cyan]Copyright (c) Heatcliff[/bright_cyan]",
            f"Current Balance: [bright_cyan]{balance_text}[/bright_cyan]  Runtime [bright_cyan]{runtime}[/bright_cyan]",
        )
        layout["footer"].update(
            Panel(footer, subtitle=last_message)
        )
        return layout


def _status_style(status: str) -> str:
    if status == "ARB!":
        return "bright_green"
    if status == "BUYING":
        return "yellow"
    if status == "SKIPPED":
        return "yellow"
    if status in {"UNWIND", "FAILED"}:
        return "red"
    if status == "SUCCESS":
        return "bright_cyan"
    return "dim"
