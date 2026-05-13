"""
Polymarket Whale Convergence Analyzer

Fetches the top 50 wallets by weekly profit from the Polymarket leaderboard,
collects each wallet's open positions, then groups by market to surface the
top 10 markets where the most whales are simultaneously positioned.

Usage
-----
# Live mode (requires network access):
    python -m src.tools.polymarket_whales

# Demo mode (synthetic data, no network required):
    python -m src.tools.polymarket_whales --demo

API endpoints used
------------------
  Leaderboard : https://data-api.polymarket.com/v1/leaderboard?timePeriod=WEEK&orderBy=PNL&limit=50
  Positions   : https://data-api.polymarket.com/positions?user=<address>&sortBy=CASHPNL&limit=500
  Market meta : https://gamma-api.polymarket.com/markets?condition_ids=<id1,id2,...>
"""

from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from typing import Any

import httpx
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
POSITIONS_URL = "https://data-api.polymarket.com/positions"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://polymarket.com/",
    "Origin": "https://polymarket.com",
}

# Concurrency cap — polite API usage
MAX_CONCURRENT = 10

console = Console()


# ---------------------------------------------------------------------------
# Demo / synthetic data
# ---------------------------------------------------------------------------

DEMO_MARKETS: list[dict] = [
    {"conditionId": "cond_trump_2028",      "title": "Will Donald Trump win the 2028 US Presidential Election?"},
    {"conditionId": "cond_btc_100k_q3",     "title": "Will Bitcoin exceed $100k before end of Q3 2026?"},
    {"conditionId": "cond_fed_rate_cut_jul", "title": "Will the Fed cut rates at its July 2026 meeting?"},
    {"conditionId": "cond_eth_merge_v2",     "title": "Will Ethereum hit $10k by December 2026?"},
    {"conditionId": "cond_recession_2026",   "title": "Will the US enter recession in 2026?"},
    {"conditionId": "cond_nvidia_1t",        "title": "Will Nvidia market cap exceed $5 trillion in 2026?"},
    {"conditionId": "cond_sol_200",          "title": "Will Solana exceed $200 before July 2026?"},
    {"conditionId": "cond_doge_act",         "title": "Will DOGE (govt dept) be officially disbanded by 2027?"},
    {"conditionId": "cond_apple_ai",         "title": "Will Apple release an on-device LLM model by end of 2026?"},
    {"conditionId": "cond_xrp_sec",          "title": "Will XRP be formally classified as NOT a security by end of 2026?"},
    {"conditionId": "cond_china_taiwan",     "title": "Will China conduct a military blockade of Taiwan in 2026?"},
    {"conditionId": "cond_agi_2026",         "title": "Will any AI lab claim AGI by end of 2026?"},
    {"conditionId": "cond_sp500_6500",       "title": "Will the S&P 500 close above 6,500 by end of 2026?"},
    {"conditionId": "cond_musk_twitter_ceo", "title": "Will Elon Musk step down as X CEO before 2027?"},
    {"conditionId": "cond_iran_deal",        "title": "Will the US sign a nuclear deal with Iran in 2026?"},
]

def _build_demo_data(n_wallets: int = 50) -> tuple[list[str], dict[str, list[dict]]]:
    """
    Generates synthetic whale wallets and positions that mimic realistic
    Polymarket distributions: a few markets attract many whales, most attract few.
    """
    import hashlib
    import random

    rng = random.Random(42)  # deterministic for reproducibility

    # Generate wallet addresses
    wallets: list[str] = []
    for i in range(n_wallets):
        seed = f"whale_{i:04d}"
        addr = "0x" + hashlib.sha256(seed.encode()).hexdigest()[:40]
        wallets.append(addr)

    # Assign positions: markets have power-law popularity among whales
    # Top markets: 35, 28, 22, 19, 15, 12, 10, 8, 7, 6, 5, 4, 3, 3, 2 whales
    popularity = [35, 28, 22, 19, 15, 12, 10, 8, 7, 6, 5, 4, 3, 3, 2]

    positions_by_wallet: dict[str, list[dict]] = {w: [] for w in wallets}
    for market, n_holders in zip(DEMO_MARKETS, popularity):
        holders = rng.sample(wallets, min(n_holders, len(wallets)))
        for wallet in holders:
            size = round(rng.uniform(500, 150_000), 2)
            price = round(rng.uniform(0.05, 0.95), 3)
            positions_by_wallet[wallet].append({
                "conditionId": market["conditionId"],
                "title": market["title"],
                "outcome": rng.choice(["Yes", "No"]),
                "size": size,
                "avgPrice": price,
                "currentValue": round(size * price, 2),
            })

    return wallets, positions_by_wallet


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_wallets(leaderboard: list | dict) -> list[str]:
    rows: list[dict] = leaderboard if isinstance(leaderboard, list) else leaderboard.get("data", [])
    wallets: list[str] = []
    for row in rows:
        addr = (
            row.get("proxyWallet")
            or row.get("proxy_wallet")
            or row.get("address")
            or row.get("wallet")
        )
        if addr:
            wallets.append(addr.lower())
    return wallets


def _extract_positions(raw: list | dict) -> list[dict]:
    if isinstance(raw, list):
        return raw
    return raw.get("data", raw.get("positions", []))


def _condition_id(pos: dict) -> str | None:
    return (
        pos.get("conditionId")
        or pos.get("condition_id")
        or pos.get("market")
        or pos.get("marketId")
    )


def _market_title(pos: dict) -> str:
    return (
        pos.get("title")
        or pos.get("question")
        or pos.get("marketQuestion")
        or pos.get("conditionId")
        or pos.get("market")
        or "Unknown"
    )


def _is_open(pos: dict) -> bool:
    size = pos.get("size") or pos.get("currentTokens") or pos.get("quantity") or 0
    try:
        return float(size) > 0.01
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Async live fetchers
# ---------------------------------------------------------------------------

async def fetch_leaderboard(client: httpx.AsyncClient, limit: int = 50) -> list[str]:
    resp = await client.get(
        LEADERBOARD_URL,
        params={"timePeriod": "WEEK", "orderBy": "PNL", "limit": min(limit, 50)},
        timeout=30,
    )
    resp.raise_for_status()
    return _extract_wallets(resp.json())


async def fetch_positions_for_wallet(
    client: httpx.AsyncClient,
    address: str,
    sem: asyncio.Semaphore,
) -> tuple[str, list[dict]]:
    async with sem:
        try:
            resp = await client.get(
                POSITIONS_URL,
                params={
                    "user": address,
                    "sizeThreshold": "0.01",
                    "sortBy": "CASHPNL",
                    "sortDirection": "DESC",
                    "limit": 500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            positions = _extract_positions(resp.json())
            return address, [p for p in positions if _is_open(p)]
        except httpx.HTTPStatusError as exc:
            console.print(f"[dim red]  HTTP {exc.response.status_code} for {address[:10]}…[/dim red]")
            return address, []
        except Exception as exc:  # noqa: BLE001
            console.print(f"[dim red]  Error fetching {address[:10]}…: {exc}[/dim red]")
            return address, []


async def enrich_market_titles(
    client: httpx.AsyncClient,
    condition_ids: list[str],
    existing_titles: dict[str, str],
) -> dict[str, str]:
    missing = [cid for cid in condition_ids if existing_titles.get(cid, cid) == cid]
    if not missing:
        return existing_titles

    titles = dict(existing_titles)
    for i in range(0, len(missing), 20):
        batch = missing[i : i + 20]
        try:
            resp = await client.get(
                GAMMA_MARKETS_URL,
                params={"condition_ids": ",".join(batch)},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            markets: list[dict] = data if isinstance(data, list) else data.get("data", [])
            for mkt in markets:
                cid = mkt.get("conditionId") or mkt.get("condition_id")
                title = mkt.get("question") or mkt.get("title") or cid
                if cid:
                    titles[cid] = title
        except Exception:  # noqa: BLE001
            pass
    return titles


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def _render_results(
    ranked: list[tuple[str, set[str]]],
    market_titles: dict[str, str],
    total_wallets: int,
    top_markets: int,
    demo: bool,
) -> None:
    mode_tag = " [bold yellow](DEMO DATA)[/bold yellow]" if demo else ""
    table = Table(
        title=(
            f"[bold]Top {top_markets} Markets by Whale Convergence[/bold]"
            f"  [dim](top {total_wallets} weekly-profit wallets)[/dim]{mode_tag}"
        ),
        show_lines=True,
        header_style="bold magenta",
    )
    table.add_column("#", style="cyan", width=3, justify="right")
    table.add_column("Market Question", style="white", min_width=50, max_width=62)
    table.add_column("Whales", style="bold green", justify="center", width=8)
    table.add_column("% of Top 50", style="yellow", justify="center", width=12)
    table.add_column("Sample Wallets", style="dim", max_width=38)

    for rank, (cid, whales) in enumerate(ranked, 1):
        title = market_titles.get(cid, cid)
        display_title = title if len(title) <= 62 else title[:59] + "…"
        pct = f"{100 * len(whales) / total_wallets:.1f}%"
        sample = sorted(whales)[:3]
        wallet_str = "  ".join(f"{w[:8]}…" for w in sample)
        if len(whales) > 3:
            wallet_str += f"  [dim]+{len(whales)-3}[/dim]"
        table.add_row(str(rank), display_title, str(len(whales)), pct, wallet_str)

    console.print(table)


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

async def run_live(top_n: int, top_markets: int) -> None:
    async with httpx.AsyncClient(headers=BROWSER_HEADERS, follow_redirects=True) as client:

        console.print(f"\n[bold]Step 1[/bold]  Fetching top {top_n} wallets by weekly profit…")
        wallets = await fetch_leaderboard(client, top_n)
        if not wallets:
            console.print("[red]No wallets returned from leaderboard.[/red]")
            sys.exit(1)
        console.print(f"  [green]✓[/green] {len(wallets)} whale wallets retrieved")

        console.print(f"\n[bold]Step 2[/bold]  Fetching open positions (concurrent, cap={MAX_CONCURRENT})…")
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        tasks = [fetch_positions_for_wallet(client, addr, sem) for addr in wallets]

        market_whales: dict[str, set[str]] = defaultdict(set)
        market_titles: dict[str, str] = {}
        total_positions = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Wallets processed", total=len(tasks))
            for coro in asyncio.as_completed(tasks):
                address, positions = await coro
                progress.advance(task_id)
                total_positions += len(positions)
                for pos in positions:
                    cid = _condition_id(pos)
                    if not cid:
                        continue
                    market_whales[cid].add(address)
                    if cid not in market_titles:
                        market_titles[cid] = _market_title(pos)

        console.print(
            f"  [green]✓[/green] {total_positions} open positions "
            f"across {len(market_whales)} unique markets"
        )

        if not market_whales:
            console.print("[yellow]No open positions found.[/yellow]")
            sys.exit(0)

        console.print("\n[bold]Step 3[/bold]  Enriching market titles via Gamma API…")
        top_cids = [
            cid for cid, _ in sorted(
                market_whales.items(), key=lambda x: len(x[1]), reverse=True
            )[:top_markets * 2]
        ]
        market_titles = await enrich_market_titles(client, top_cids, market_titles)
        console.print("  [green]✓[/green] Done")

    ranked = sorted(market_whales.items(), key=lambda x: len(x[1]), reverse=True)[:top_markets]
    console.print()
    _render_results(ranked, market_titles, len(wallets), top_markets, demo=False)


async def run_demo(top_n: int, top_markets: int) -> None:
    console.print("\n[bold yellow]Running in demo mode — no network calls are made.[/bold yellow]")
    console.print(
        "[dim]The data below uses realistic synthetic wallets and market distributions\n"
        "to illustrate what the live analysis would produce.[/dim]\n"
    )

    console.print(f"[bold]Step 1[/bold]  Generating {top_n} synthetic whale wallets…")
    wallets, positions_by_wallet = _build_demo_data(top_n)
    console.print(f"  [green]✓[/green] {len(wallets)} wallets ready")

    console.print(f"\n[bold]Step 2[/bold]  Aggregating open positions…")
    market_whales: dict[str, set[str]] = defaultdict(set)
    market_titles: dict[str, str] = {}
    total_positions = 0

    for wallet, positions in positions_by_wallet.items():
        for pos in positions:
            cid = _condition_id(pos)
            if not cid:
                continue
            market_whales[cid].add(wallet)
            total_positions += 1
            if cid not in market_titles:
                market_titles[cid] = _market_title(pos)

    console.print(
        f"  [green]✓[/green] {total_positions} open positions "
        f"across {len(market_whales)} unique markets"
    )

    ranked = sorted(market_whales.items(), key=lambda x: len(x[1]), reverse=True)[:top_markets]
    console.print()
    _render_results(ranked, market_titles, len(wallets), top_markets, demo=True)


async def run_analysis(top_n: int = 50, top_markets: int = 10, demo: bool = False) -> None:
    console.rule("[bold cyan]Polymarket Whale Convergence Analyzer[/bold cyan]")
    if demo:
        await run_demo(top_n, top_markets)
    else:
        await run_live(top_n, top_markets)

    console.print()
    console.print(
        "[dim]'Whales' = count of top-50 weekly-profit wallets holding an open position "
        "in that market.  '% of Top 50' = share of those 50 wallets converged on the market.[/dim]"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    demo = "--demo" in sys.argv
    asyncio.run(run_analysis(top_n=50, top_markets=10, demo=demo))


if __name__ == "__main__":
    main()
