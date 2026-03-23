#!/usr/bin/env python3
"""
backfill_liquidity.py — Fetches daily BTC and FARTCOIN price + volume
from CoinGecko free API (max 365 days lookback on free tier).

Generates: liquidity_daily.json, fartcoin_liquidity_daily.json

Uses CoinGecko /coins/{id}/market_chart?days=365 endpoint which returns daily
data points. Merges with existing data to preserve historical records.

Run locally or via GitHub Actions:
    pip install requests
    python backfill_liquidity.py
"""

import json
import time
import requests
from datetime import datetime, timezone

BTC_OUTPUT = "liquidity_daily.json"
FART_OUTPUT = "fartcoin_liquidity_daily.json"
MIN_DATE = "2025-01-01"  # Earliest date we want to keep


def fetch_daily(coin_id):
    """Fetch up to 365 days of daily price + volume for a coin."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
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


def load_existing(output_file):
    """Load existing JSON data."""
    try:
        with open(output_file, "r") as f:
            data = json.load(f)
            days = {d["date"]: d for d in data.get("days", [])}
            print(f"  Loaded {len(days)} existing records from {output_file}")
            return days
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"  No existing {output_file} found, starting fresh")
        return {}


def build_and_save(coin_id, price_key, output_file):
    """Fetch, merge, and save daily data for a coin."""
    print(f"\nFetching {coin_id} daily price + volume (up to 365 days)...")
    prices, volumes = fetch_daily(coin_id)
    print(f"  Got {len(prices)} price points, {len(volumes)} volume points")

    print(f"Building {output_file}...")
    existing = load_existing(output_file)

    new_count = 0
    all_dates = sorted(set(prices.keys()) & set(volumes.keys()))
    for date in all_dates:
        if date < MIN_DATE:
            continue
        if date not in existing:
            new_count += 1
        existing[date] = {
            "date": date,
            price_key: round(prices[date], 6 if prices[date] < 1 else 2),
            "volume": round(volumes[date], 2),
        }

    # Sort and save
    days = sorted(existing.values(), key=lambda x: x["date"])
    output = {
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
    }
    with open(output_file, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"  Saved {len(days)} records ({new_count} new)")
    if days:
        print(f"  Range: {days[0]['date']} to {days[-1]['date']}")


def main():
    print("=== Liquidity Data Backfill ===")

    # BTC
    build_and_save("bitcoin", "btc_price", BTC_OUTPUT)

    # Rate limit pause
    print("\nWaiting 6s for rate limit...")
    time.sleep(6)

    # FARTCOIN
    build_and_save("fartcoin", "fart_price", FART_OUTPUT)

    print("\nDone!")


if __name__ == "__main__":
    main()
