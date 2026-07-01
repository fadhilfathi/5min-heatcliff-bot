import time
import threading
from queue import Queue
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.console import Console
from rich.table import Table as RichTable

COIN_KEYS = {"b": "BTC", "e": "ETH", "s": "SOL", "n": "BNB", "h": "HYPE", "x": "XRP", "d": "DOGE"}


class BotUI:
    def __init__(self, coins):
        self.coins = coins
        self.state = {coin: {
            "bid": 0.0, "ask": 0.0, "status": "SCANNING", "pnl": 0.0,
            "up_ask": 0.0, "down_ask": 0.0,
            "shares": 0.0, "move": None, "secs": 0, "side": "", "wins": 0, "losses": 0,
            "session_pnl": 0.0,
        } for coin in coins}
        self.logs = Queue(maxsize=20)
        self.console = Console()
        self.paused = False
        self.verbose = False
        self.filter_coin = None
        self._stopped = False
        self.started_at = time.time()

    def add_log(self, coin, msg):
        timestamp = time.strftime("%H:%M:%S")
        entry = f"[{timestamp}] [bold blue]{coin}[/bold blue] {msg}"
        if self.logs.full():
            self.logs.get()
        self.logs.put(entry)

    def _handle_key(self, ch):
        if ch == "q":
            self._stopped = True
        elif ch == "p":
            self.paused = not self.paused
        elif ch == "v":
            self.verbose = not self.verbose
        elif ch == "a":
            self.filter_coin = None
        elif ch in COIN_KEYS:
            coin = COIN_KEYS[ch]
            self.filter_coin = None if self.filter_coin == coin else coin

    def _kb_thread(self):
        try:
            import msvcrt
            while not self._stopped:
                if msvcrt.kbhit():
                    ch = msvcrt.getch().decode("utf-8", errors="ignore").lower()
                    self._handle_key(ch)
                time.sleep(0.05)
        except Exception:
            pass

    def generate_layout(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )

        meta = self.state.get("_meta", {})
        total_pnl = float(meta.get("session_balance_pnl", 0.0) or 0.0)
        current_balance = meta.get("current_balance")
        total_wins = sum(d.get("wins", 0) for d in self.state.values())
        total_losses = sum(d.get("losses", 0) for d in self.state.values())
        elapsed = max(0, int(time.time() - self.started_at))
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        balance_str = "n/a" if current_balance is None else f"${float(current_balance):.4f}"
        balance_move_str = f"(+${total_pnl:.4f})" if total_pnl > 0 else f"(-${abs(total_pnl):.4f})" if total_pnl < 0 else "($0.0000)"
        pnl_color = "green" if total_pnl > 0 else "red" if total_pnl < 0 else "bright_black"
        pnl_style = f"bold {pnl_color}" if total_pnl != 0 else f"dim {pnl_color}"
        paused_tag = "  [bold red][PAUSED][/bold red]" if self.paused else ""
        filter_tag = f"  [bold yellow][{self.filter_coin}][/bold yellow]" if self.filter_coin else ""

        layout["header"].update(Panel(
            f"[bold green]Heatcliff All-In-One[/bold green]  "
            f"PnL: [bold {pnl_color}]${total_pnl:+.2f}[/bold {pnl_color}]  "
            f"Trades: [cyan]{total_wins + total_losses}[/cyan] "
            f"([green]{total_wins}W[/green]/[red]{total_losses}L[/red])"
            f"{paused_tag}{filter_tag}  "
            f"[dim]q=quit  p=pause  v=verbose  b/e/s/n/h/x/d=filter  a=all[/dim]"
        ))

        table = Table(expand=True, show_lines=False, padding=(0, 1))
        table.add_column("Coin", style="cyan", width=6)
        table.add_column("Move$", justify="right", width=13)
        table.add_column("UP Ask", justify="right", width=7)
        table.add_column("DOWN Ask", justify="right", width=8)
        table.add_column("Side", width=5)
        table.add_column("Status", width=12)
        table.add_column("W/L", justify="center", width=7)
        table.add_column("Secs", justify="right", width=5)

        for coin in self.coins:
            data = self.state.get(coin, {})
            status = data.get("status", "SCANNING")
            shares = data.get("shares", 0.0)
            up_ask = data.get("up_ask", 0.0)
            down_ask = data.get("down_ask", 0.0)
            move = data.get("move")
            secs = data.get("secs", 0)
            side = data.get("side", "")
            wins = data.get("wins", 0)
            losses = data.get("losses", 0)

            move_str = f"${move:+.7f}" if move is not None else "—"
            move_color = "green" if move and move > 0 else "red" if move and move < 0 else "white"
            secs_color = "red" if secs <= 10 else "yellow" if secs <= 30 else "white"
            wl_str = f"[green]{wins}[/green]/[red]{losses}[/red]"

            if status in ("WAITING", "PRICE ONLY"):
                status_cell = f"[dim]{status}[/dim]"
                coin_cell = f"[dim]{coin}[/dim]"
            elif shares > 0.0 or status in ("IN_TRADE", "SELLING"):
                status_cell = f"[bold bright_cyan]{status}[/bold bright_cyan]"
                coin_cell = f"[bold]{coin}[/bold]"
            elif status == "ERROR":
                status_cell = f"[bold red]{status}[/bold red]"
                coin_cell = coin
            else:
                status_cell = f"[yellow]{status}[/yellow]"
                coin_cell = coin

            table.add_row(
                coin_cell,
                f"[{move_color}]{move_str}[/{move_color}]",
                f"{up_ask:.2f}",
                f"{down_ask:.2f}",
                side,
                status_cell,
                wl_str,
                f"[{secs_color}]{secs}[/{secs_color}]",
            )

        layout["body"].update(table)
        footer = RichTable.grid(expand=True)
        footer.add_column(justify="left")
        footer.add_column(justify="right")
        footer.add_row(
            "[bright_cyan]Copyright (c) Heatcliff[/bright_cyan]",
            f"Current Balance: [bright_cyan]{balance_str}[/bright_cyan] "
            f"[{pnl_style}]{balance_move_str}[/{pnl_style}]  "
            f"Runtime [bright_cyan]{runtime}[/bright_cyan]",
        )
        layout["footer"].update(
            Panel(footer)
        )

        return layout

    def start(self):
        threading.Thread(target=self._kb_thread, daemon=True).start()
        with Live(self.generate_layout(), refresh_per_second=4, screen=True) as live:
            while not self._stopped:
                time.sleep(0.25)
                live.update(self.generate_layout())
