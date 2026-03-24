#!/usr/bin/env python3
"""
backfill_hourly.py — Fetches hourly BTC, FARTCOIN, and SPX6900 data from
CoinGecko free API starting from Sep 1 2025.

Generates two JSON files:
  - fartcoin_hourly.json  (BTC + FARTCOIN aligned hourly data)
  - spx6900_hourly.json   (BTC + SPX6900 aligned hourly data)

SMART MODE: On subsequent runs, only fetches data from the last known
timestamp to now (instead of re-fetching everything from Sep 1 2025).
This dramatically reduces API calls from ~90 to ~3-6 per run.

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


def fetch_hourly_range(coin_id, start_dt, end_dt):
    """Fetch hourly data in 7-day chunks from start_dt to end_dt."""
    all_prices = {}
    chunks = []
    chunk_start = start_dt
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_dt)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    if not chunks:
        print("  No chunks needed (data is up to date)")
        return all_prices

    print(f"  {len(chunks)} chunk(s) to fetch...")
    for i, (cs, ce) in enumerate(chunks):
        from_ts = int(cs.timestamp())
        to_ts = int(ce.timestamp())
        print(f"  Chunk {i+1}/{len(chunks)}: {cs.strftime('%Y-%m-%d %H:%M')} to {ce.strftime('%Y-%m-%d %H:%M')}...", end=" ")
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
    """Load existing JSON data and return (dict, latest_timestamp)."""
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            hours = {h["t"]: h for h in data.get("hours", [])}
            latest = max(hours.keys()) if hours else None
            print(f"  Loaded {len(hours)} existing records from {filename}")
            if latest:
                print(f"  Latest existing data: {latest}")
            return hours, latest
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"  No existing {filename} found, starting fresh")
        return {}, None


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


def get_smart_start(existing_fart, existing_spx):
    """Determine the smart start date based on existing data.
    Only re-fetch from 2 days before the latest data point (overlap for safety).
    Falls back to START_DATE if no existing data."""
    latest_times = []
    for existing in [existing_fart, existing_spx]:
        _, latest = existing if isinstance(existing, tuple) else (None, None)
        if latest:
            latest_times.append(latest)

    if not latest_times:
        return datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Use the OLDEST latest time (so we fill gaps in whichever file is behind)
    oldest_latest = min(latest_times)
    # Go back 2 days for safety overlap
    smart_start = datetime.strptime(oldest_latest, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc) - timedelta(days=2)
    return smart_start


def main():
    print(f"=== Hourly Data Backfill (from {START_DATE}) ===\n")

    # Load existing data first to determine smart start
    print(f"Checking existing data...")
    existing_fart, latest_fart = load_existing(FARTCOIN_FILE)
    existing_spx, latest_spx = load_existing(SPX6900_FILE)

    now = datetime.now(tz=timezone.utc)
    full_start = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Determine smart start — only fetch from last known point
    if latest_fart and latest_spx:
        oldest_latest = min(latest_fart, latest_spx)
        smart_start = datetime.strptime(oldest_latest, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc) - timedelta(days=2)
        print(f"\n  Smart mode: fetching from {smart_start.strftime('%Y-%m-%d %H:%M')} (2 days before oldest latest)")
    else:
        smart_start = full_start
        print(f"\n  Full backfill mode: fetching from {START_DATE}")

    # --- Fetch BTC (shared by both files) ---
    print(f"\nFetching BTC hourly data...")
    btc_prices = fetch_hourly_range("bitcoin", smart_start, now)
    print(f"  Total BTC candles fetched: {len(btc_prices)}\n")

    print("Waiting 8s for rate limits...\n")
    time.sleep(8)

    # --- Fetch FARTCOIN ---
    print("Fetching FARTCOIN hourly data...")
    fart_prices = fetch_hourly_range("fartcoin", smart_start, now)
    print(f"  Total FARTCOIN candles fetched: {len(fart_prices)}\n")

    print("Waiting 8s for rate limits...\n")
    time.sleep(8)

    # --- Fetch SPX6900 ---
    print("Fetching SPX6900 hourly data...")
    spx_prices = fetch_hourly_range("spx6900", smart_start, now)
    print(f"  Total SPX6900 candles fetched: {len(spx_prices)}\n")

    # --- Build fartcoin_hourly.json ---
    print(f"Building {FARTCOIN_FILE}...")
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

    print("Done!")


if __name__ == "__main__":
    main()
