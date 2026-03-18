#!/usr/bin/env python3
"""
backfill_fartcoin.py — Fetches the last 90 days of hourly BTC and FARTCOIN
data from CoinGecko free API in small chunks to stay within rate limits.

Run this ONCE locally:
    pip install requests
    python backfill_fartcoin.py

Then push the generated fartcoin_hourly.json to your GitHub repo.
After that, nightly GitHub Actions will keep it updated automatically.
"""

import json
import time
import requests
from datetime import datetime, timezone, timedelta

OUTPUT_FILE = "fartcoin_hourly.json"
DAYS_TOTAL = 90
CHUNK_DAYS = 7

def fetch_hourly_chunk(coin_id, from_ts, to_ts):
    """Fetch hourly price data for a specific time range using /range endpoint."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": str(from_ts), "to": str(to_ts)}
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    result = {}
    for ts, price in data.get("prices", []):
        key = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
        result[key] = price
    return result

def fetch_hourly_all(coin_id, days=DAYS_TOTAL):
    """Fetch hourly data in 7-day chunks to stay within free tier limits."""
    now = datetime.now(tz=timezone.utc)
    all_prices = {}
    chunks_needed = (days + CHUNK_DAYS - 1) // CHUNK_DAYS

    for i in range(chunks_needed):
        chunk_end = now - timedelta(days=i * CHUNK_DAYS)
        chunk_start = now - timedelta(days=(i + 1) * CHUNK_DAYS)
        if chunk_start < now - timedelta(days=days):
            chunk_start = now - timedelta(days=days)

        from_ts = int(chunk_start.timestamp())
        to_ts = int(chunk_end.timestamp())

        print(f"  Chunk {i+1}/{chunks_needed}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}...", end=" ")

        try:
            prices = fetch_hourly_chunk(coin_id, from_ts, to_ts)
            all_prices.update(prices)
            print(f"got {len(prices)} candles")
        except Exception as e:
            print(f"ERROR: {e}")

        if i < chunks_needed - 1:
            time.sleep(6)

    return all_prices

def main():
    print(f"=== FARTCOIN Hourly Backfill ({DAYS_TOTAL} days in {CHUNK_DAYS}-day chunks) ===\n")

    existing_hours = {}
    try:
        with open(OUTPUT_FILE, "r") as f:
            existing = json.load(f)
            for h in existing.get("hours", []):
                existing_hours[h["t"]] = h
            print(f"Loaded {len(existing_hours)} existing records from {OUTPUT_FILE}")
    except FileNotFoundError:
        print(f"No existing {OUTPUT_FILE} found, starting fresh")

    print(f"\nFetching BTC hourly data...")
    btc_prices = fetch_hourly_all("bitcoin", days=DAYS_TOTAL)
    print(f"  Total BTC candles: {len(btc_prices)}")

    print(f"\nWaiting 10s to respect rate limits...\n")
    time.sleep(10)

    print(f"Fetching FARTCOIN hourly data...")
    fart_prices = fetch_hourly_all("fartcoin", days=DAYS_TOTAL)
    print(f"  Total FARTCOIN candles: {len(fart_prices)}")

    all_timestamps = sorted(set(btc_prices.keys()) & set(fart_prices.keys()))
    print(f"\nMatched timestamps: {len(all_timestamps)}")

    new_count = 0
    for ts in all_timestamps:
        btc = btc_prices.get(ts)
        fart = fart_prices.get(ts)
        if btc and fart:
            if ts not in existing_hours:
                new_count += 1
            existing_hours[ts] = {"t": ts, "btc": round(btc, 2), "fart": round(fart, 6)}

    hours = sorted(existing_hours.values(), key=lambda x: x["t"])
    output = {
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hours": hours
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"\n Done! Saved {len(hours)} total hourly records to {OUTPUT_FILE}")
    print(f"  ({new_count} new records added)")
    if hours:
        print(f"  Range: {hours[0]['t']} to {hours[-1]['t']}")
    print(f"\n  Now push to GitHub:")
    print(f"    git add fartcoin_hourly.json")
    print(f"    git commit -m 'Backfill {DAYS_TOTAL} days FARTCOIN hourly data'")
    print(f"    git push")

if __name__ == "__main__":
    main()
