#!/usr/bin/env python3
"""
backfill_fartcoin.py — One-time script to fetch the last 90 days of hourly
BTC and FARTCOIN data from CoinGecko free API.

Run this ONCE locally after setting up the repo:
    pip install requests --break-system-packages
    python backfill_fartcoin.py

Then upload the generated fartcoin_hourly.json to your GitHub repo.
After that, the nightly GitHub Actions will keep it updated automatically.
"""

import json
import time
import requests
from datetime import datetime, timezone

OUTPUT_FILE = "fartcoin_hourly.json"

def fetch_hourly(coin_id, days=90):
    """Fetch hourly price data from CoinGecko free API."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": str(days), "interval": "hourly"}
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    # prices = [[timestamp_ms, price], ...]
    return {
        datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:00:00Z"): price
        for ts, price in data.get("prices", [])
    }

def main():
    print("Fetching BTC hourly data (last 90 days)...")
    btc_prices = fetch_hourly("bitcoin", days=90)
    print(f"  Got {len(btc_prices)} BTC hourly candles")

    print("Waiting 10s to respect CoinGecko free rate limit...")
    time.sleep(10)

    print("Fetching FARTCOIN hourly data (last 90 days)...")
    fart_prices = fetch_hourly("fartcoin", days=90)
    print(f"  Got {len(fart_prices)} FARTCOIN hourly candles")

    # Merge on matching timestamps
    all_timestamps = sorted(set(btc_prices.keys()) & set(fart_prices.keys()))
    print(f"  Matched timestamps: {len(all_timestamps)}")

    hours = []
    for ts in all_timestamps:
        btc = btc_prices.get(ts)
        fart = fart_prices.get(ts)
        if btc and fart:
            hours.append({
                "t": ts,
                "btc": round(btc, 2),
                "fart": round(fart, 6)
            })

    output = {
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hours": hours
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"\n✓ Saved {len(hours)} hourly records to {OUTPUT_FILE}")
    print("  Upload this file to your GitHub repo root.")
    print("  After that, the nightly GitHub Action will keep it updated.")

if __name__ == "__main__":
    main()
