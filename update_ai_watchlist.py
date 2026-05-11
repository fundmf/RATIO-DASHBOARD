#!/usr/bin/env python3
"""
update_ai_watchlist.py — Tracks weekly Friday-close prices for an AI-affected stock
watchlist. Appends a new data point every Friday after US market close.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW TO ADD A TICKER                                                       ║
║  ──────────────────                                                        ║
║  1. Add one line to the TICKERS dict below:                                ║
║         "YAHOO_SYMBOL": "DISPLAY_LABEL",                                   ║
║     e.g.  "NVDA": "NVDA",   or   "^GSPC": "S&P500"                         ║
║  2. Commit + push. Next workflow run will:                                 ║
║       (a) BACKFILL the new ticker's Friday close for every week already    ║
║           recorded — so the chart's baseline still works                   ║
║       (b) Include it in all future weekly captures                         ║
║  3. The frontend reads tickers + labels from ai_watchlist.json, so the     ║
║     chart and snapshot card update automatically — no JS edits needed.     ║
║                                                                            ║
║  HOW TO REMOVE A TICKER                                                    ║
║  ─────────────────────                                                     ║
║  Just delete the line from TICKERS. Historical data stays in the JSON      ║
║  but the frontend filters by data.tickers, so removed tickers disappear    ║
║  from the chart on next save. To purge data entirely, edit the JSON.       ║
╚════════════════════════════════════════════════════════════════════════════╝

Logic:
  - Determine the most recent completed US Friday market close
    (using 21:00 UTC as a DST-safe cutoff).
  - Backfill: for any ticker in TICKERS missing from past weeks, fetch its
    historical close and patch it in.
  - Append: if the latest target Friday is not yet recorded, capture closes
    for all tickers and add a new week entry.
  - Idempotent — safe to run multiple times.

Run manually:           python update_ai_watchlist.py
Run via GitHub Actions: .github/workflows/update-ai-watchlist.yml (weekly Sat 02:00 UTC)
"""

import json
import math
import sys
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
import yfinance as yf

DATA_FILE = Path("ai_watchlist.json")

# ┌──────────────────────────────────────────────────────────────────────────┐
# │  TICKER LIST — edit this dict to add/remove tickers.                     │
# │  Key   = Yahoo Finance symbol (use ^PREFIX for indices)                  │
# │  Value = display label shown in chart legend + snapshot card             │
# └──────────────────────────────────────────────────────────────────────────┘
TICKERS = {
    "GTLB": "GTLB",
    "CDW":  "CDW",
    "ADBE": "ADBE",
    "EXLS": "EXLS",
    "ADP":  "ADP",
    "^NDX": "NASDAQ100",
}


def load_data():
    if not DATA_FILE.exists():
        return {
            "last_updated": "",
            "tickers": list(TICKERS.keys()),
            "labels":  TICKERS,
            "baseline_date": "",
            "weeks": [],
        }
    with open(DATA_FILE, "r") as f:
        d = json.load(f)
    # Migrate old shape if needed
    d.setdefault("tickers", list(TICKERS.keys()))
    d.setdefault("labels", TICKERS)
    d.setdefault("weeks", [])
    d.setdefault("baseline_date", "")
    return d


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"Saved {len(data['weeks'])} weekly records to {DATA_FILE}")


def last_completed_friday_close():
    """
    Returns the date of the most recent Friday whose US market close (4pm ET)
    has already passed. Uses 21:00 UTC as the safe cutoff (4pm EST = 21:00 UTC,
    4pm EDT = 20:00 UTC — we use 21:00 UTC to be conservative across DST).
    """
    now = datetime.now(timezone.utc)
    # weekday: Mon=0 ... Sun=6  → Friday=4
    # Days since last Friday (0 if today is Friday)
    days_since_fri = (now.weekday() - 4) % 7
    candidate = now.date() - timedelta(days=days_since_fri)

    # If today IS Friday, check whether market close has already passed
    if days_since_fri == 0:
        # Cutoff: 21:00 UTC on this Friday
        cutoff = datetime.combine(candidate, datetime.min.time(), tzinfo=timezone.utc).replace(hour=21)
        if now < cutoff:
            # Market not yet closed today — use previous Friday
            candidate = candidate - timedelta(days=7)
    return candidate


def fetch_close_on(ticker, friday_date):
    """
    Fetch the close price for a single ticker on a specific Friday date.
    Returns float close or None if unavailable.
    """
    start = (friday_date - timedelta(days=10)).strftime("%Y-%m-%d")
    end   = (friday_date + timedelta(days=4)).strftime("%Y-%m-%d")
    target = friday_date.strftime("%Y-%m-%d")
    try:
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return None
        if hasattr(df.columns, "levels") and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)
        df_dates = [idx.strftime("%Y-%m-%d") for idx in df.index]
        if target not in df_dates:
            # Walk backwards up to 3 days in case Friday was a holiday
            cand = friday_date
            for _ in range(3):
                cand = cand - timedelta(days=1)
                cs = cand.strftime("%Y-%m-%d")
                if cs in df_dates:
                    target = cs
                    break
            else:
                return None
        row = df.loc[df.index[df_dates.index(target)]]
        close = row["Close"]
        if hasattr(close, "item"):
            close = close.item()
        close_f = float(close)
        if math.isnan(close_f) or math.isinf(close_f):
            return None
        return round(close_f, 4)
    except Exception as e:
        print(f"    {ticker} fetch error: {e}")
        return None


def fetch_friday_closes(friday_date):
    """Fetch close prices for ALL TICKERS on `friday_date`."""
    closes = {}
    for ticker in TICKERS:
        c = fetch_close_on(ticker, friday_date)
        if c is not None:
            closes[ticker] = c
            print(f"  {ticker}: {c:.4f}")
        else:
            print(f"  {ticker}: no close on {friday_date}")
    return closes


def backfill_missing_tickers(data):
    """
    For each ticker in TICKERS that is missing from any existing week,
    fetch its historical close and patch the week in-place.
    Returns number of patches applied.
    """
    patches = 0
    for week in data["weeks"]:
        week_date = datetime.strptime(week["date"], "%Y-%m-%d").date()
        for ticker in TICKERS:
            if ticker in week.get("closes", {}):
                continue
            print(f"  Backfilling {ticker} for {week['date']}...")
            c = fetch_close_on(ticker, week_date)
            if c is not None:
                week.setdefault("closes", {})[ticker] = c
                print(f"    -> {c:.4f}")
                patches += 1
            else:
                print(f"    -> no data available")
    return patches


def main():
    print(f"=== AI Watchlist updater — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} ===")
    data = load_data()
    target_friday = last_completed_friday_close()
    target_str = target_friday.strftime("%Y-%m-%d")

    print(f"Target Friday close: {target_str}")
    print(f"Existing weeks:      {len(data['weeks'])}")
    print(f"Configured tickers:  {list(TICKERS.keys())}")

    changed = False

    # ── Step 1: backfill any newly-added tickers into existing weeks ──
    if data["weeks"]:
        print("\n[Step 1/2] Checking for new tickers needing historical backfill...")
        patches = backfill_missing_tickers(data)
        if patches:
            print(f"Backfilled {patches} historical data point(s).")
            changed = True
        else:
            print("All existing weeks already cover every configured ticker.")

    # ── Step 2: append latest Friday if not already recorded ──
    print(f"\n[Step 2/2] Checking for new Friday close ({target_str})...")
    existing_dates = {w["date"] for w in data["weeks"]}
    if target_str in existing_dates:
        print(f"Already have {target_str} — no new week to add.")
    else:
        print(f"Fetching Friday close prices for {target_str}...")
        closes = fetch_friday_closes(target_friday)
        if closes:
            data["weeks"].append({"date": target_str, "closes": closes})
            data["weeks"].sort(key=lambda w: w["date"])
            if not data["baseline_date"]:
                data["baseline_date"] = target_str
            print(f"Added week {target_str}: {len(closes)} tickers")
            changed = True
        else:
            print("No data fetched — skipping new week.")

    # Refresh metadata + save
    if changed:
        data["tickers"] = list(TICKERS.keys())
        data["labels"]  = TICKERS
        data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        save_data(data)
    else:
        # Still refresh tickers/labels metadata if the dict changed (e.g. ticker removed)
        if data.get("tickers") != list(TICKERS.keys()) or data.get("labels") != TICKERS:
            data["tickers"] = list(TICKERS.keys())
            data["labels"]  = TICKERS
            save_data(data)
            print("\nUpdated ticker metadata in JSON.")
        else:
            print("\nNothing changed.")


if __name__ == "__main__":
    main()
