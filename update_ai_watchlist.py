#!/usr/bin/env python3
"""
update_ai_watchlist.py — Tracks weekly Friday-close prices for an AI-affected stock
watchlist. Appends a new data point every Friday after US market close.

Tickers tracked:
  GTLB, CDW, ADBE, EXLS, ADP, ^NDX (NASDAQ 100 index)

Logic:
  - On each run, determine the most recent completed US Friday market close
    (Friday 4:00pm ET = 20:00 UTC during DST, 21:00 UTC outside DST).
  - If that Friday is not yet in ai_watchlist.json, fetch close prices for all
    tickers via yfinance and append.
  - Idempotent — safe to run multiple times.

Run manually:  python update_ai_watchlist.py
Run via GitHub Actions: included in update.yml (runs hourly)
"""

import json
import math
import sys
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
import yfinance as yf

DATA_FILE = Path("ai_watchlist.json")

# Yahoo Finance tickers → display labels
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


def fetch_friday_closes(friday_date):
    """
    For each ticker, fetch the close price on `friday_date` via yfinance.
    Returns dict {ticker: close_price}. Missing tickers are omitted.
    """
    closes = {}
    # Pull ~10 days of history to ensure the Friday close is included
    start = (friday_date - timedelta(days=10)).strftime("%Y-%m-%d")
    end   = (friday_date + timedelta(days=2)).strftime("%Y-%m-%d")
    target = friday_date.strftime("%Y-%m-%d")

    for ticker in TICKERS:
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if df.empty:
                print(f"  {ticker}: no data returned")
                continue
            # Flatten multi-level columns
            if hasattr(df.columns, "levels") and df.columns.nlevels > 1:
                df.columns = df.columns.get_level_values(0)

            # Match row for target Friday — yfinance index is a Timestamp
            df_dates = [idx.strftime("%Y-%m-%d") for idx in df.index]
            if target not in df_dates:
                print(f"  {ticker}: no close on {target} (available: {df_dates[-3:] if df_dates else 'none'})")
                continue
            row = df.loc[df.index[df_dates.index(target)]]
            close = row["Close"]
            if hasattr(close, "item"):
                close = close.item()
            close_f = float(close)
            if math.isnan(close_f) or math.isinf(close_f):
                print(f"  {ticker}: invalid close value")
                continue
            closes[ticker] = round(close_f, 4)
            print(f"  {ticker}: {close_f:.4f}")
        except Exception as e:
            print(f"  {ticker} error: {e}")
    return closes


def main():
    print(f"=== AI Watchlist updater — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} ===")
    data = load_data()
    target_friday = last_completed_friday_close()
    target_str = target_friday.strftime("%Y-%m-%d")

    print(f"Target Friday close: {target_str}")
    print(f"Existing weeks: {len(data['weeks'])}")

    # Check if we already have this Friday
    existing_dates = {w["date"] for w in data["weeks"]}
    if target_str in existing_dates:
        print(f"Already have {target_str} — nothing to do.")
        return

    print(f"\nFetching Friday close prices for {target_str}...")
    closes = fetch_friday_closes(target_friday)
    if not closes:
        print("No data fetched — aborting.")
        return

    # Append new week
    data["weeks"].append({
        "date": target_str,
        "closes": closes,
    })
    data["weeks"].sort(key=lambda w: w["date"])
    data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Set baseline_date on first record
    if not data["baseline_date"]:
        data["baseline_date"] = target_str

    # Always refresh tickers + labels in case they changed
    data["tickers"] = list(TICKERS.keys())
    data["labels"]  = TICKERS

    save_data(data)
    print(f"\nAdded week {target_str}: {len(closes)} tickers")


if __name__ == "__main__":
    main()
