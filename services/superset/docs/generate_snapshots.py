"""generate_snapshots.py — Generate realistic Superset dashboard preview images.

Produces five PNG files that illustrate the dashboards built in Phase 10.
Run from: services/superset/
    python docs/generate_snapshots.py
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np

OUT = Path(__file__).parent / "snapshots"
OUT.mkdir(parents=True, exist_ok=True)

# ── Shared style ──────────────────────────────────────────────────────────────
BG      = "#1a1a2e"
PANEL   = "#16213e"
ACCENT  = "#0f3460"
GREEN   = "#00c896"
RED     = "#e94560"
BLUE    = "#4fc3f7"
YELLOW  = "#ffd54f"
PURPLE  = "#ce93d8"
ORANGE  = "#ffb74d"
TEXT    = "#e0e0e0"
SUBTEXT = "#9e9e9e"
GRID    = "#2a2a4a"

plt.rcParams.update({
    "figure.facecolor":  BG,
    "axes.facecolor":    PANEL,
    "axes.edgecolor":    GRID,
    "axes.labelcolor":   TEXT,
    "axes.titlecolor":   TEXT,
    "xtick.color":       SUBTEXT,
    "ytick.color":       SUBTEXT,
    "grid.color":        GRID,
    "grid.linewidth":    0.6,
    "text.color":        TEXT,
    "font.family":       "sans-serif",
    "font.size":         9,
})

random.seed(2024)
rng = np.random.default_rng(2024)

SYMBOLS   = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "JPM"]
SECTORS   = ["Technology", "Consumer Disc.", "Financials"]
TODAY     = date(2025, 2, 14)
DAYS      = 30
DATES     = [TODAY - timedelta(days=DAYS - i - 1) for i in range(DAYS)]
DATE_LBLS = [d.strftime("%b %d") for d in DATES]


def _ohlcv(seed_price: float, n: int = DAYS):
    prices, vols = [seed_price], []
    for _ in range(n):
        prices.append(prices[-1] * rng.uniform(0.985, 1.015))
        vols.append(int(rng.uniform(500_000, 3_000_000)))
    opens, closes, highs, lows = [], [], [], []
    for i in range(n):
        o = prices[i]
        c = prices[i + 1]
        h = max(o, c) * rng.uniform(1.001, 1.012)
        l = min(o, c) * rng.uniform(0.988, 0.999)
        opens.append(o)
        closes.append(c)
        highs.append(h)
        lows.append(l)
    return np.array(opens), np.array(closes), np.array(highs), np.array(lows), np.array(vols)


def _sma(arr, n):
    return np.convolve(arr, np.ones(n) / n, mode="valid")


def _rsi(closes, period=14):
    diffs = np.diff(closes)
    gains = np.where(diffs > 0, diffs, 0.0)
    losses = np.where(diffs < 0, -diffs, 0.0)
    avg_g = np.convolve(gains, np.ones(period) / period, mode="valid")
    avg_l = np.convolve(losses, np.ones(period) / period, mode="valid")
    rs = avg_g / np.where(avg_l == 0, 1e-9, avg_l)
    return 100 - 100 / (1 + rs)


def add_title_bar(fig, title: str, subtitle: str = "") -> None:
    fig.text(0.015, 0.965, title,   fontsize=14, fontweight="bold", color=TEXT, va="top")
    fig.text(0.015, 0.945, subtitle, fontsize=8.5, color=SUBTEXT,  va="top")
    fig.text(0.99,  0.965, "Apache Superset  •  Phase 10", fontsize=8,
             color=SUBTEXT, va="top", ha="right")


def kpi_box(ax, value: str, label: str, color: str = GREEN) -> None:
    ax.set_facecolor(ACCENT)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis("off")
    ax.text(0.5, 0.65, value, ha="center", va="center",
            fontsize=18, fontweight="bold", color=color)
    ax.text(0.5, 0.22, label, ha="center", va="center",
            fontsize=8, color=SUBTEXT)
    for spine in ax.spines.values():
        spine.set_visible(False)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Executive Market Overview
# ─────────────────────────────────────────────────────────────────────────────
def dash_executive():
    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(BG)
    add_title_bar(fig, "Executive Market Overview",
                  f"Real-time aggregated market metrics  •  {TODAY.strftime('%B %d, %Y')}")

    gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.55, wspace=0.35,
                           left=0.05, right=0.97, top=0.90, bottom=0.06)

    # KPI row
    kpi_data = [
        ("$4.82B",  "Total Market Value",  GREEN),
        ("184.3M",  "Total Volume",        BLUE),
        ("32,914",  "Total Trades",        YELLOW),
        ("6 / 2",   "Advance / Decline",   GREEN),
    ]
    for col, (val, lbl, col_) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, col])
        kpi_box(ax, val, lbl, col_)

    # Market volume bar chart
    ax_vol = fig.add_subplot(gs[1:, :2])
    sym_vols = {s: int(rng.uniform(10, 80) * 1e6) for s in SYMBOLS}
    bars = ax_vol.barh(list(sym_vols.keys()), list(sym_vols.values()),
                       color=[GREEN if v > 40e6 else BLUE for v in sym_vols.values()],
                       height=0.6)
    ax_vol.set_title("Volume by Symbol", pad=8, fontsize=10, color=TEXT)
    ax_vol.set_xlabel("Volume", color=SUBTEXT)
    ax_vol.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.0f}M"))
    ax_vol.grid(axis="x"); ax_vol.set_axisbelow(True)
    for bar, (sym, vol) in zip(bars, sym_vols.items()):
        pct = rng.choice([-1, 1]) * rng.uniform(0.3, 4.5)
        clr = GREEN if pct >= 0 else RED
        sign = "+" if pct >= 0 else ""
        ax_vol.text(bar.get_width() + 0.3e6, bar.get_y() + bar.get_height() / 2,
                    f"{sign}{pct:.1f}%", va="center", fontsize=8, color=clr)

    # Sector pie
    ax_pie = fig.add_subplot(gs[1, 2])
    sector_vals = [55, 30, 15]
    colors_pie  = [BLUE, GREEN, PURPLE]
    wedges, texts, autotexts = ax_pie.pie(
        sector_vals, labels=SECTORS, colors=colors_pie,
        autopct="%1.0f%%", startangle=90,
        textprops={"color": TEXT, "fontsize": 7.5},
        wedgeprops={"linewidth": 0.5, "edgecolor": BG},
    )
    for at in autotexts:
        at.set_color(BG); at.set_fontsize(7)
    ax_pie.set_title("Sector Mix (Value)", pad=8, fontsize=10, color=TEXT)

    # Top movers table
    ax_tbl = fig.add_subplot(gs[1, 3])
    ax_tbl.axis("off")
    ax_tbl.set_title("Top Movers", pad=8, fontsize=10, color=TEXT)
    movers = [
        ("NVDA", "+5.2%"), ("TSLA", "+3.8%"), ("META", "+2.1%"),
        ("AMZN", "-1.4%"), ("JPM",  "-2.7%"),
    ]
    for i, (sym, chg) in enumerate(movers):
        clr = GREEN if chg.startswith("+") else RED
        y = 0.85 - i * 0.18
        ax_tbl.text(0.05, y, sym, fontsize=9, fontweight="bold", color=TEXT, transform=ax_tbl.transAxes)
        ax_tbl.text(0.7,  y, chg, fontsize=9, color=clr, transform=ax_tbl.transAxes, ha="right")
        ax_tbl.axhline(y * ax_tbl.get_ylim()[1] if ax_tbl.get_ylim()[1] else 1,
                       color=GRID, linewidth=0.4, xmin=0.02, xmax=0.98)

    # Market value line
    ax_line = fig.add_subplot(gs[2, 2:])
    mv = rng.uniform(3.5, 5.2, DAYS) * 1e9
    ax_line.fill_between(range(DAYS), mv, alpha=0.15, color=BLUE)
    ax_line.plot(range(DAYS), mv, color=BLUE, linewidth=1.5)
    ax_line.set_title("Market Value (30-day)", pad=8, fontsize=10, color=TEXT)
    ax_line.set_xticks([0, 7, 14, 21, 29])
    ax_line.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]], fontsize=7)
    ax_line.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x/1e9:.1f}B"))
    ax_line.grid(True)

    plt.savefig(OUT / "01_executive_market_overview.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  01_executive_market_overview.png")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Symbol Deep Dive — AAPL (candlestick + volume + RSI)
# ─────────────────────────────────────────────────────────────────────────────
def dash_symbol_deep_dive():
    sym = "AAPL"
    o, c, h, l, vol = _ohlcv(182.0, DAYS)
    sma20 = _sma(c, 20)
    sma50 = _sma(c, min(DAYS, 50)) if DAYS >= 50 else _sma(c, DAYS // 2)
    rsi   = _rsi(c, 14)

    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(BG)
    add_title_bar(fig, f"Symbol Deep Dive — {sym}",
                  f"OHLCV  •  SMA-20 / SMA-50  •  RSI-14  •  {TODAY.strftime('%B %d, %Y')}")

    gs = gridspec.GridSpec(4, 4, figure=fig, hspace=0.45, wspace=0.3,
                           left=0.05, right=0.97, top=0.89, bottom=0.06,
                           height_ratios=[1, 3, 1.2, 1.5])

    # KPI row
    kpi_data = [
        (f"${c[-1]:.2f}",          "Last Price",    GREEN if c[-1] >= o[-1] else RED),
        (f"${h.max():.2f}",        "52-wk High",    BLUE),
        (f"${l.min():.2f}",        "52-wk Low",     ORANGE),
        (f"{vol[-1]/1e6:.1f}M",    "Volume",        YELLOW),
    ]
    for col, (val, lbl, clr) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, col])
        kpi_box(ax, val, lbl, clr)

    # Candlestick + SMA
    ax_c = fig.add_subplot(gs[1, :])
    xs = np.arange(DAYS)
    width = 0.6
    for i in xs:
        color = GREEN if c[i] >= o[i] else RED
        # body
        ax_c.bar(i, abs(c[i] - o[i]), bottom=min(o[i], c[i]),
                 width=width, color=color, alpha=0.9)
        # wick
        ax_c.plot([i, i], [l[i], h[i]], color=color, linewidth=0.8)

    offset20 = DAYS - len(sma20)
    offset50 = DAYS - len(sma50)
    ax_c.plot(xs[offset20:], sma20, color=YELLOW,  linewidth=1.3, label="SMA-20", alpha=0.9)
    ax_c.plot(xs[offset50:], sma50, color=PURPLE,  linewidth=1.3, label="SMA-50", alpha=0.9)
    ax_c.legend(loc="upper left", framealpha=0.3, fontsize=8)
    ax_c.set_title(f"{sym} — Candlestick (30 days)", pad=8, fontsize=10, color=TEXT)
    ax_c.set_xticks([0, 7, 14, 21, 29])
    ax_c.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_c.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax_c.grid(True)

    # Volume bars
    ax_v = fig.add_subplot(gs[2, :])
    bar_clrs = [GREEN if c[i] >= o[i] else RED for i in range(DAYS)]
    ax_v.bar(xs, vol, color=bar_clrs, alpha=0.7, width=0.8)
    ax_v.set_ylabel("Volume", color=SUBTEXT, fontsize=8)
    ax_v.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.0f}M"))
    ax_v.set_xticks([])
    ax_v.grid(axis="y")

    # RSI panel
    ax_r = fig.add_subplot(gs[3, :])
    rsi_x = xs[DAYS - len(rsi):]
    ax_r.plot(rsi_x, rsi, color=BLUE, linewidth=1.4)
    ax_r.axhline(70, color=RED,   linewidth=0.8, linestyle="--", alpha=0.7)
    ax_r.axhline(30, color=GREEN, linewidth=0.8, linestyle="--", alpha=0.7)
    ax_r.fill_between(rsi_x, rsi, 70, where=rsi > 70, alpha=0.15, color=RED)
    ax_r.fill_between(rsi_x, rsi, 30, where=rsi < 30, alpha=0.15, color=GREEN)
    ax_r.set_ylabel("RSI-14", color=SUBTEXT, fontsize=8)
    ax_r.set_ylim(0, 100)
    ax_r.set_xticks([0, 7, 14, 21, 29])
    ax_r.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_r.text(0.99, 0.82, "Overbought (70)", transform=ax_r.transAxes,
              ha="right", fontsize=7, color=RED, alpha=0.8)
    ax_r.text(0.99, 0.12, "Oversold (30)", transform=ax_r.transAxes,
              ha="right", fontsize=7, color=GREEN, alpha=0.8)
    ax_r.grid(True)

    plt.savefig(OUT / "02_symbol_deep_dive.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  02_symbol_deep_dive.png")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Trader Analytics
# ─────────────────────────────────────────────────────────────────────────────
def dash_trader_analytics():
    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(BG)
    add_title_bar(fig, "Trader Analytics",
                  f"Activity distribution, top traders, symbol exposure  •  {TODAY.strftime('%B %d, %Y')}")

    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.55, wspace=0.35,
                           left=0.05, right=0.97, top=0.89, bottom=0.06)

    # KPI row
    kpi_data = [
        ("2,847",  "Active Traders",  BLUE),
        ("11.6",   "Avg Trades/Trader", YELLOW),
        ("$163K",  "Avg Trade Value", GREEN),
    ]
    for col, (val, lbl, clr) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, col])
        kpi_box(ax, val, lbl, clr)

    # Top 10 traders by volume (horizontal bar)
    ax_top = fig.add_subplot(gs[1:, :2])
    traders = [f"ACC-{1000 + i:04d}" for i in range(10)]
    t_vals  = sorted(rng.integers(5_000, 80_000, 10), reverse=True)
    colors  = [GREEN if v > np.median(t_vals) else BLUE for v in t_vals]
    ax_top.barh(traders[::-1], t_vals[::-1], color=colors[::-1], height=0.6)
    ax_top.set_title("Top 10 Traders by Total Volume", pad=8, fontsize=10, color=TEXT)
    ax_top.set_xlabel("Total Quantity Traded", color=SUBTEXT)
    ax_top.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
    ax_top.grid(axis="x"); ax_top.set_axisbelow(True)

    # Buy/sell donut
    ax_donut = fig.add_subplot(gs[1, 2])
    buy_pct, sell_pct = 54, 46
    ax_donut.pie([buy_pct, sell_pct], labels=["BUY", "SELL"],
                 colors=[GREEN, RED], startangle=90,
                 wedgeprops={"width": 0.45, "linewidth": 0.5, "edgecolor": BG},
                 autopct="%1.0f%%",
                 textprops={"color": TEXT, "fontsize": 8})
    ax_donut.text(0, 0, f"{buy_pct}%\nBUY", ha="center", va="center",
                  fontsize=9, color=GREEN, fontweight="bold")
    ax_donut.set_title("Buy vs Sell Split", pad=8, fontsize=10, color=TEXT)

    # Symbol exposure heatmap (traders × symbols)
    ax_hm = fig.add_subplot(gs[2, 2])
    n_t, n_s = 8, len(SYMBOLS)
    hm_data = rng.integers(0, 200, (n_t, n_s)).astype(float)
    im = ax_hm.imshow(hm_data, aspect="auto",
                      cmap="YlGn", interpolation="nearest")
    ax_hm.set_xticks(range(n_s))
    ax_hm.set_xticklabels(SYMBOLS, fontsize=6.5, rotation=45, ha="right")
    ax_hm.set_yticks(range(n_t))
    ax_hm.set_yticklabels([f"ACC-{1000+i}" for i in range(n_t)], fontsize=6.5)
    ax_hm.set_title("Trader × Symbol Exposure", pad=8, fontsize=10, color=TEXT)
    plt.colorbar(im, ax=ax_hm, shrink=0.8, pad=0.02,
                 label="Qty Traded").ax.yaxis.set_tick_params(color=SUBTEXT)

    plt.savefig(OUT / "03_trader_analytics.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  03_trader_analytics.png")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Data Pipeline Health
# ─────────────────────────────────────────────────────────────────────────────
def dash_pipeline_health():
    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(BG)
    add_title_bar(fig, "Data Pipeline Health",
                  f"Dagster run status, latency, data freshness  •  {TODAY.strftime('%B %d, %Y')}")

    gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.55, wspace=0.35,
                           left=0.05, right=0.97, top=0.89, bottom=0.06)

    # KPI row
    kpi_data = [
        ("99.4%", "Pipeline Uptime",   GREEN),
        ("1.8s",  "Avg Ingest Lag",    YELLOW),
        ("0",     "Failed Runs (24h)", GREEN),
        ("2m ago","Data Freshness",    GREEN),
    ]
    for col, (val, lbl, clr) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, col])
        kpi_box(ax, val, lbl, clr)

    # Trade count over time (multi-line)
    ax_tc = fig.add_subplot(gs[1, :2])
    for sym, base in zip(["AAPL", "MSFT", "TSLA"], [300, 250, 200]):
        counts = base + rng.integers(-50, 80, DAYS).cumsum() % 200 + base // 2
        ax_tc.plot(range(DAYS), counts, linewidth=1.5, label=sym)
    ax_tc.set_title("Trade Count per Symbol (30-day)", pad=8, fontsize=10, color=TEXT)
    ax_tc.set_xticks([0, 7, 14, 21, 29])
    ax_tc.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_tc.legend(loc="upper left", framealpha=0.3, fontsize=8)
    ax_tc.grid(True)

    # Ingestion lag line
    ax_lag = fig.add_subplot(gs[1, 2:])
    lag = 1.5 + rng.uniform(-0.3, 0.6, DAYS)
    ax_lag.fill_between(range(DAYS), lag, alpha=0.2, color=YELLOW)
    ax_lag.plot(range(DAYS), lag, color=YELLOW, linewidth=1.5)
    ax_lag.axhline(3.0, color=RED, linewidth=0.9, linestyle="--", alpha=0.7,
                   label="SLA limit (3s)")
    ax_lag.set_title("Kafka → Iceberg Ingest Lag (seconds)", pad=8, fontsize=10, color=TEXT)
    ax_lag.set_xticks([0, 7, 14, 21, 29])
    ax_lag.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_lag.set_ylabel("Seconds", color=SUBTEXT, fontsize=8)
    ax_lag.legend(loc="upper right", framealpha=0.3, fontsize=8)
    ax_lag.grid(True)

    # Dagster run status stacked bar
    ax_runs = fig.add_subplot(gs[2, :2])
    weeks = ["W-4", "W-3", "W-2", "W-1", "This Week"]
    success = [42, 45, 43, 47, 12]
    failed  = [1,  0,  2,  0,  0]
    skipped = [2,  1,  1,  1,  1]
    ax_runs.bar(weeks, success, label="Success", color=GREEN, alpha=0.85)
    ax_runs.bar(weeks, failed,  bottom=success, label="Failed", color=RED, alpha=0.85)
    ax_runs.bar(weeks, skipped, bottom=[s+f for s,f in zip(success,failed)],
                label="Skipped", color=YELLOW, alpha=0.7)
    ax_runs.set_title("Dagster Run Status by Week", pad=8, fontsize=10, color=TEXT)
    ax_runs.legend(loc="upper left", framealpha=0.3, fontsize=8)
    ax_runs.grid(axis="y"); ax_runs.set_axisbelow(True)

    # Layer row count
    ax_layer = fig.add_subplot(gs[2, 2:])
    layers = ["Bronze\n(Raw)", "Silver\n(Cleaned)", "Gold\n(Aggregated)"]
    row_counts = [1_842_000, 1_810_000, 24_500]
    bar_clrs = [ORANGE, BLUE, GREEN]
    bars = ax_layer.bar(layers, row_counts, color=bar_clrs, width=0.5, alpha=0.85)
    for bar, val in zip(bars, row_counts):
        ax_layer.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.02,
                      f"{val/1e3:.0f}K" if val < 1e6 else f"{val/1e6:.2f}M",
                      ha="center", fontsize=9, color=TEXT)
    ax_layer.set_title("Iceberg Layer Row Counts", pad=8, fontsize=10, color=TEXT)
    ax_layer.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax_layer.grid(axis="y"); ax_layer.set_axisbelow(True)

    plt.savefig(OUT / "04_pipeline_health.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  04_pipeline_health.png")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Risk & Compliance
# ─────────────────────────────────────────────────────────────────────────────
def dash_risk_compliance():
    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(BG)
    add_title_bar(fig, "Risk & Compliance",
                  f"Unusual volume, wash-trade signals, sector concentration  •  {TODAY.strftime('%B %d, %Y')}")

    gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.55, wspace=0.35,
                           left=0.05, right=0.97, top=0.89, bottom=0.06)

    # KPI row
    kpi_data = [
        ("3",    "Volume Anomalies",   RED),
        ("1",    "Wash-Trade Signals", RED),
        ("0",    "PII Violations",     GREEN),
        ("94.2%","Compliance Score",   GREEN),
    ]
    for col, (val, lbl, clr) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, col])
        kpi_box(ax, val, lbl, clr)

    # Unusual volume: z-score bar
    ax_zvol = fig.add_subplot(gs[1, :2])
    z_scores = rng.uniform(-1.5, 3.8, len(SYMBOLS))
    colors_z = [RED if z > 2.5 else YELLOW if z > 1.5 else BLUE for z in z_scores]
    bars = ax_zvol.bar(SYMBOLS, z_scores, color=colors_z, alpha=0.85)
    ax_zvol.axhline(2.5, color=RED, linewidth=1.0, linestyle="--", alpha=0.8,
                    label="Alert threshold (2.5σ)")
    ax_zvol.axhline(0,   color=SUBTEXT, linewidth=0.5)
    ax_zvol.set_title("Volume Z-Score by Symbol", pad=8, fontsize=10, color=TEXT)
    ax_zvol.set_ylabel("Z-Score (σ)", color=SUBTEXT, fontsize=8)
    ax_zvol.legend(loc="upper right", framealpha=0.3, fontsize=8)
    ax_zvol.grid(axis="y"); ax_zvol.set_axisbelow(True)
    for bar, z in zip(bars, z_scores):
        ax_zvol.text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + 0.05 if z >= 0 else bar.get_height() - 0.25,
                     f"{z:.1f}", ha="center", fontsize=7.5,
                     color=RED if z > 2.5 else TEXT)

    # Wash-trade timeline scatter
    ax_wt = fig.add_subplot(gs[1, 2:])
    wt_days   = rng.integers(0, DAYS, 6)
    wt_syms   = rng.integers(0, len(SYMBOLS), 6)
    wt_scores = rng.uniform(0.5, 1.0, 6)
    sc = ax_wt.scatter(wt_days, wt_syms, s=wt_scores * 200,
                       c=wt_scores, cmap="RdYlGn_r", vmin=0, vmax=1,
                       alpha=0.85, edgecolors=BG, linewidths=0.5)
    plt.colorbar(sc, ax=ax_wt, label="Signal Strength").ax.yaxis.set_tick_params(color=SUBTEXT)
    ax_wt.set_yticks(range(len(SYMBOLS)))
    ax_wt.set_yticklabels(SYMBOLS, fontsize=8)
    ax_wt.set_xticks([0, 7, 14, 21, 29])
    ax_wt.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_wt.set_title("Wash-Trade Signal Timeline", pad=8, fontsize=10, color=TEXT)
    ax_wt.grid(True)

    # Sector concentration risk gauge (horizontal bar)
    ax_conc = fig.add_subplot(gs[2, :2])
    sectors_full = ["Technology", "Financials", "Consumer Disc.", "Healthcare", "Energy"]
    conc = [62, 18, 10, 6, 4]
    bar_c = [RED if v > 50 else YELLOW if v > 25 else GREEN for v in conc]
    bars2 = ax_conc.barh(sectors_full[::-1], conc[::-1], color=bar_c[::-1], height=0.5)
    ax_conc.axvline(50, color=RED, linewidth=0.9, linestyle="--", alpha=0.7,
                    label="Concentration limit (50%)")
    ax_conc.set_xlabel("Portfolio Weight %", color=SUBTEXT, fontsize=8)
    ax_conc.set_title("Sector Concentration Risk", pad=8, fontsize=10, color=TEXT)
    ax_conc.legend(loc="lower right", framealpha=0.3, fontsize=8)
    ax_conc.grid(axis="x"); ax_conc.set_axisbelow(True)
    for bar, val in zip(bars2, conc[::-1]):
        ax_conc.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                     f"{val}%", va="center", fontsize=8, color=TEXT)

    # PII audit log timeline
    ax_pii = fig.add_subplot(gs[2, 2:])
    pii_days   = range(DAYS)
    pii_grants = 40 + rng.integers(-5, 15, DAYS)
    pii_denials = rng.integers(0, 4, DAYS)
    ax_pii.fill_between(pii_days, pii_grants, alpha=0.2, color=GREEN)
    ax_pii.plot(pii_days, pii_grants,  color=GREEN, linewidth=1.5, label="Granted")
    ax_pii.bar(pii_days, pii_denials, bottom=0, color=RED, alpha=0.7, width=0.8, label="Denied")
    ax_pii.set_title("PII Access Audit Log (30-day)", pad=8, fontsize=10, color=TEXT)
    ax_pii.set_xticks([0, 7, 14, 21, 29])
    ax_pii.set_xticklabels([DATE_LBLS[i] for i in [0, 7, 14, 21, 29]])
    ax_pii.set_ylabel("Events", color=SUBTEXT, fontsize=8)
    ax_pii.legend(loc="upper right", framealpha=0.3, fontsize=8)
    ax_pii.grid(True)

    plt.savefig(OUT / "05_risk_compliance.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  05_risk_compliance.png")


# ─────────────────────────────────────────────────────────────────────────────
# 6. Order Book Depth chart (custom plugin preview)
# ─────────────────────────────────────────────────────────────────────────────
def dash_order_book_depth():
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PANEL)

    mid = 182.50
    bid_prices = np.linspace(mid * 0.97, mid - 0.01, 40)
    ask_prices = np.linspace(mid + 0.01, mid * 1.03, 40)

    bid_qty = rng.uniform(50, 800, 40)
    bid_qty = np.sort(bid_qty)[::-1]
    bid_cum = np.cumsum(bid_qty)

    ask_qty = rng.uniform(50, 800, 40)
    ask_qty = np.sort(ask_qty)
    ask_cum = np.cumsum(ask_qty)

    ax.fill_betweenx(bid_prices, 0, bid_cum, alpha=0.4, color=GREEN)
    ax.plot(bid_cum, bid_prices, color=GREEN, linewidth=1.8, label="Bid (cumulative)")

    ax.fill_betweenx(ask_prices, 0, ask_cum, alpha=0.4, color=RED)
    ax.plot(ask_cum, ask_prices, color=RED, linewidth=1.8, label="Ask (cumulative)")

    ax.axhline(mid, color=YELLOW, linewidth=1.0, linestyle="--", alpha=0.7,
               label=f"Mid ${mid:.2f}")

    ax.set_xlabel("Cumulative Volume", color=SUBTEXT)
    ax.set_ylabel("Price ($)", color=SUBTEXT)
    ax.set_title("AAPL — Order Book Depth  (custom ECharts plugin)",
                 fontsize=12, pad=10, color=TEXT)
    ax.legend(loc="lower right", framealpha=0.3, fontsize=9)
    ax.grid(True)
    ax.spines[:].set_color(GRID)

    fig.text(0.98, 0.02, "Apache Superset  •  Phase 10  •  echarts_depth plugin",
             fontsize=7, color=SUBTEXT, ha="right")

    plt.tight_layout()
    plt.savefig(OUT / "06_order_book_depth.png", dpi=130, bbox_inches="tight",
                facecolor=BG)
    plt.close()
    print("  ✓  06_order_book_depth.png")


if __name__ == "__main__":
    print(f"Generating dashboard snapshots → {OUT}/")
    dash_executive()
    dash_symbol_deep_dive()
    dash_trader_analytics()
    dash_pipeline_health()
    dash_risk_compliance()
    dash_order_book_depth()
    print("Done.")
