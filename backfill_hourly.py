#!/usr/bin/env python3
"""
backfill_hourly.py — Fetches hourly BTC, FARTCOIN, and SPX6900 data from
CoinGecko free API starting from Sep 1 2025.

Generates two JSON files:
  - fartcoin_hourly.json  (BTC + FARTCOIN aligned hourly data)
  - spx6900_hourly.json   (BTC + SPX6900 aligned hourly data)

Run locally or via GitHub Actions:
    pip install requests
    python backfill_hourly.py
"""

import json
import time
import requests
from datetime import datetime, timezone, timedelta

START_DATE = "2025-09-01"
CHUNK_DAYS = 7  # Fetch in 7-day chunks (CoinGecko returns hourly for ranges < 90 days)

FARTCOIN_FILE = "fartcoin_hourly.json"
SPX6900_FILE = "spx6900_hourly.json"


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


def fetch_hourly_all(coin_id, start_date=START_DATE):
    """Fetch hourly data in 7-day chunks from start_date to now."""
    now = datetime.now(tz=timezone.utc)
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    all_prices = {}

    # Build chunks from start to now
    chunks = []
    chunk_start = start
    while chunk_start < now:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), now)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    for i, (cs, ce) in enumerate(chunks):
        from_ts = int(cs.timestamp())
        to_ts = int(ce.timestamp())
        print(f"  Chunk {i+1}/{len(chunks)}: {cs.strftime('%Y-%m-%d')} to {ce.strftime('%Y-%m-%d')}...", end=" ")
        try:
            prices = fetch_hourly_chunk(coin_id, from_ts, to_ts)
            all_prices.update(prices)
            print(f"got {len(prices)} candles")
        except Exception as e:
            print(f"ERROR: {e}")

        if i < len(chunks) - 1:
            time.sleep(6)

    return all_prices


def load_existing(filename):
    """Load existing JSON data."""
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            hours = {h["t"]: h for h in data.get("hours", [])}
            print(f"  Loaded {len(hours)} existing records from {filename}")
            return hours
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"  No existing {filename} found, starting fresh")
        return {}


def save_json(filename, hours_dict):
    """Save hourly data to JSON file."""
    hours = sorted(hours_dict.values(), key=lambda x: x["t"])
    output = {
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hours": hours
    }
    with open(filename, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    return hours


def main():
    print(f"=== Hourly Data Backfill (from {START_DATE}) ===\n")

    # --- Fetch BTC (shared by both files) ---
    print("Fetching BTC hourly data...")
    btc_prices = fetch_hourly_all("bitcoin")
    print(f"  Total BTC candles: {len(btc_prices)}\n")

    print("Waiting 10s for rate limits...\n")
    time.sleep(10)

    # --- Fetch FARTCOIN ---
    print("Fetching FARTCOIN hourly data...")
    fart_prices = fetch_hourly_all("fartcoin")
    print(f"  Total FARTCOIN candles: {len(fart_prices)}\n")

    print("Waiting 10s for rate limits...\n")
    time.sleep(10)

    # --- Fetch SPX6900 ---
    print("Fetching SPX6900 hourly data...")
    spx_prices = fetch_hourly_all("spx6900")
    print(f"  Total SPX6900 candles: {len(spx_prices)}\n")

    # --- Build fartcoin_hourly.json ---
    print(f"Building {FARTCOIN_FILE}...")
    existing_fart = load_existing(FARTCOIN_FILE)
    fart_timestamps = sorted(set(btc_prices.keys()) & set(fart_prices.keys()))
    new_fart = 0
    for ts in fart_timestamps:
        if ts not in existing_fart:
            new_fart += 1
        existing_fart[ts] = {"t": ts, "btc": round(btc_prices[ts], 2), "fart": round(fart_prices[ts], 6)}
    hours = save_json(FARTCOIN_FILE, existing_fart)
    print(f"  Saved {len(hours)} records ({new_fart} new)")
    if hours:
        print(f"  Range: {hours[0]['t']} to {hours[-1]['t']}\n")

    # --- Build spx6900_hourly.json ---
    print(f"Building {SPX6900_FILE}...")
    existing_spx = load_existing(SPX6900_FILE)
    spx_timestamps = sorted(set(btc_prices.keys()) & set(spx_prices.keys()))
    new_spx = 0
    for ts in spx_timestamps:
        if ts not in existing_spx:
            new_spx += 1
        existing_spx[ts] = {"t": ts, "btc": round(btc_prices[ts], 2), "spx": round(spx_prices[ts], 6)}
    hours = save_json(SPX6900_FILE, existing_spx)
    print(f"  Saved {len(hours)} records ({new_spx} new)")
    if hours:
        print(f"  Range: {hours[0]['t']} to {hours[-1]['t']}\n")

    print("Done! Push both files to GitHub:")
    print(f"  git add {FARTCOIN_FILE} {SPX6900_FILE}")
    print(f"  git commit -m 'Backfill hourly data from {START_DATE}'")
    print(f"  git push")


if __name__ == "__main__":
    main()
