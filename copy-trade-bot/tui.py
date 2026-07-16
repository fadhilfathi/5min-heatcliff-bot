from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

LOG = logging.getLogger("copy_tui")


class CopyTradeUI:
    def __init__(self, state: dict[str, Any], control: dict[str, Any]):
        self.state = state
        self.control = control
        self.console = Console()
        self.logs: deque[str] = deque(maxlen=18)
        self.started_at = time.time()
        self._stopped = False

    def add_log(self, message: str) -> None:
        self.logs.appendleft(f"[{time.strftime('%H:%M:%S')}] {message}")

    def stop(self) -> None:
        LOG.info("[UI] event=stop")
        self._stopped = True

    def start(self) -> None:
        LOG.info("[UI] event=start")
        threading.Thread(target=self._kb_thread, daemon=True).start()
        with Live(self._render(), refresh_per_second=4, screen=True) as live:
            while not self._stopped and not self.control.get("quit"):
                time.sleep(0.25)
                live.update(self._render())

    def _kb_thread(self) -> None:
        LOG.debug("[UI] event=keyboard_thread_start")
        try:
            import msvcrt
            while not self._stopped and not self.control.get("quit"):
                if msvcrt.kbhit():
                    ch = msvcrt.getch().decode("utf-8", errors="ignore").lower()
                    if ch == "q":
                        LOG.warning("[CONTROL] event=quit_requested source=keyboard")
                        self.control["quit"] = True
                    elif ch == "p":
                        new_state = not bool(self.control.get("paused"))
                        LOG.info("[CONTROL] event=pause state=%s", str(new_state).lower())
                        self.control["paused"] = new_state
                time.sleep(0.05)
        except Exception as exc:
            LOG.warning("[ERROR] event=ui_thread_error error=%r", exc)
            return

    def _render(self):
        meta = self.state["_meta"]
        cb = self.state.get("current_bucket", {})
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=10),
        )
        pnl = meta.get("session_pnl", 0.0)
        pnl_color = "green" if pnl >= 0 else "red"
        elapsed = max(0, int(time.time() - self.started_at))
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        btc_price = meta.get("btc_price", 0.0)
        btc_open = cb.get("btc_open", 0.0)
        move = cb.get("move", 0.0)
        move_color = "green" if move > 0 else "red" if move < 0 else "white"
        move_str = f"${move:+.2f}" if btc_open > 0 else "—"
        direction = cb.get("direction", "—") or "—"

        layout["header"].update(
            Panel(
                f"Mode=[cyan]{meta['mode']}[/cyan] "
                f"BTC=[cyan]${btc_price:,.2f}[/cyan] "
                f"Move=[{move_color}]{move_str}[/{move_color}] "
                f"Direction=[cyan]{direction}[/cyan] "
                f"Best=[cyan]${cb.get('best_abs_move', 0):,.2f}[/cyan] "
                f"Balance=[cyan]${meta.get('balance', 0):.2f}[/cyan] "
                f"PnL=[{pnl_color}]${pnl:+.4f}[/{pnl_color}] "
                f"Entry=[cyan]{meta.get('entry_count', 0)}[/cyan] "
                f"[dim]q=quit p=pause[/dim]"
            )
        )
        table = Table(expand=True, padding=(0, 1))
        for col, width, justify in (
            ("Bucket", 11, "right"),
            ("Dir", 5, "center"),
            ("Entries", 7, "right"),
            ("Move$", 8, "right"),
            ("Limit$", 7, "right"),
            ("Shares", 7, "right"),
            ("Cost", 7, "right"),
            ("PnL", 7, "right"),
            ("Status", 12, "left"),
            ("Secs", 5, "right"),
        ):
            table.add_column(col, width=width, justify=justify)
        for pos_ts in sorted(self.state["positions"].keys(), reverse=True):
            pos = self.state["positions"][pos_ts]
            entries = pos.get("entries", [])
            if not entries:
                continue
            last = entries[-1]
            status_color = "bright_cyan" if pos.get("status") == "OPEN" else "dim"
            move_val = last.get("move")
            move_color = "green" if move_val and move_val > 0 else "red" if move_val and move_val < 0 else "dim"
            pnl_val = pos.get("pnl")
            pnl_color = "green" if pnl_val and pnl_val > 0 else "red" if pnl_val and pnl_val < 0 else "dim"
            table.add_row(
                str(pos_ts),
                pos.get("direction", ""),
                str(len(entries)),
                f"[{move_color}]{_fmt(move_val, 4)}[/{move_color}]",
                _fmt(last.get("limit_price")),
                _fmt(pos.get("total_shares")),
                _fmt(pos.get("total_cost")),
                f"[{pnl_color}]{_fmt(pnl_val, 4)}[/{pnl_color}]",
                f"[{status_color}]{pos.get('status','')}[/{status_color}]",
                str(pos.get("secs_left", "-")),
            )
        layout["body"].update(table)
        logs = "\n".join(self.logs) if self.logs else "No events yet"
        layout["footer"].update(Panel(logs, title=f"Runtime=[blue]{runtime}[/blue]  Poll=[cyan]{meta.get('poll_count', 0)}[/cyan]"))
        return layout


def _fmt(value: Any, decimals: int = 4) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        if value == 0.0:
            return "-"
        return f"{value:.{decimals}f}"
    return str(value)
