#!/usr/bin/env python3
"""
backfill_liquidity.py — Fetches daily BTC price and total crypto market volume
from CoinGecko free API (max 365 days lookback on free tier).

Generates: liquidity_daily.json

Uses CoinGecko /coins/bitcoin/market_chart?days=365 endpoint which returns daily
data points. Merges with existing data to preserve historical records.

Run locally or via GitHub Actions:
    pip install requests
    python backfill_liquidity.py
"""

import json
import requests
from datetime import datetime, timezone

OUTPUT_FILE = "liquidity_daily.json"
MIN_DATE = "2025-01-01"  # Earliest date we want to keep


def fetch_daily():
    """Fetch up to 365 days of daily BTC price + volume."""
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": "365"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    prices = {}
    for ts, price in data.get("prices", []):
        key = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        prices[key] = price

    volumes = {}
    for ts, vol in data.get("total_volumes", []):
        key = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        volumes[key] = vol

    return prices, volumes


def load_existing():
    """Load existing JSON data."""
    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
            days = {d["date"]: d for d in data.get("days", [])}
            print(f"  Loaded {len(days)} existing records from {OUTPUT_FILE}")
            return days
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"  No existing {OUTPUT_FILE} found, starting fresh")
        return {}


def main():
    print("=== Liquidity Data Backfill ===\n")

    print("Fetching BTC daily price + volume (up to 365 days)...")
    prices, volumes = fetch_daily()
    print(f"  Got {len(prices)} price points, {len(volumes)} volume points\n")

    # Merge with existing data (preserves older records beyond 365-day window)
    print(f"Building {OUTPUT_FILE}...")
    existing = load_existing()

    new_count = 0
    all_dates = sorted(set(prices.keys()) & set(volumes.keys()))
    for date in all_dates:
        if date < MIN_DATE:
            continue
        if date not in existing:
            new_count += 1
        existing[date] = {
            "date": date,
            "btc_price": round(prices[date], 2),
            "volume": round(volumes[date], 2),
        }

    # Sort and save
    days = sorted(existing.values(), key=lambda x: x["date"])
    output = {
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"  Saved {len(days)} records ({new_count} new)")
    if days:
        print(f"  Range: {days[0]['date']} to {days[-1]['date']}\n")

    print("Done!")


if __name__ == "__main__":
    main()
