#!/usr/bin/env python3
"""
update_data.py — Fetches latest daily OHLC for BTC, ETH, MSTR, BMNR
and appends any missing trading days to data.json.

Data sources (all free, no API key required):
  - BTC/ETH  : CoinGecko free public API
  - MSTR/BMNR: Yahoo Finance via yfinance library

Run manually:   python update_data.py
Run via GitHub Actions: automated nightly at 8am AEST (Mon–Fri)
"""

import json
import sys
import time
from datetime import datetime, timedelta, date
import requests
import yfinance as yf

DATA_FILE = "data.json"

def load_data():
    with open(DATA_FILE, "r") as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"Saved {len(data)} records to {DATA_FILE}")

def get_last_date(data):
    return data[-1]["date"] if data else "2021-01-01"

def date_range(start_str, end_str):
    """Yield YYYY-MM-DD strings from start+1 to end (inclusive)."""
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    current = start + timedelta(days=1)
    while current <= end:
        yield str(current)
        current += timedelta(days=1)

def fetch_coingecko_ohlc(coin_id, start_str, end_str):
    """
    Fetch daily OHLC from CoinGecko free API.
    Returns dict keyed by YYYY-MM-DD with {close, high, low}.
    CoinGecko free tier: 30 calls/min, no API key needed.
    """
    start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp())
    end_ts   = int((datetime.strptime(end_str, "%Y-%m-%d") + timedelta(days=1)).timestamp())

    result = {}

    # Use /market_chart/range for close prices
    url = (
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
        f"?vs_currency=usd&from={start_ts}&to={end_ts}"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        chart = resp.json()

        # prices = [[timestamp_ms, price], ...]
        for ts_ms, price in chart.get("prices", []):
            d = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
            if d not in result:
                result[d] = {"close": price, "high": price, "low": price}

        # Override with OHLC data for high/low
        ohlc_url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
            f"?vs_currency=usd&days=90"
        )
        time.sleep(1.5)  # Rate limit: 30 req/min on free tier
        resp2 = requests.get(ohlc_url, timeout=15)
        if resp2.ok:
            for ts_ms, o, h, l, c in resp2.json():
                d = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
                if d in result:
                    result[d]["high"] = h
                    result[d]["low"]  = l
                    result[d]["close"] = c

    except Exception as e:
        print(f"  CoinGecko error for {coin_id}: {e}")

    return result

def fetch_yahoo_ohlc(ticker, start_str, end_str):
    """
    Fetch daily OHLC from Yahoo Finance via yfinance.
    Returns dict keyed by YYYY-MM-DD with {close, high, low}.
    """
    result = {}
    try:
        # Add a day buffer to end for yfinance
        end_dt = (datetime.strptime(end_str, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start_str, end=end_dt, progress=False, auto_adjust=True)
        if df.empty:
            return result
        # Flatten multi-level columns if present (yfinance >= 1.x)
        if hasattr(df.columns, 'levels') and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)
        for idx, row in df.iterrows():
            d = idx.strftime("%Y-%m-%d")
            c = row["Close"]
            h = row["High"]
            l = row["Low"]
            # Handle Series/array values from multi-ticker returns
            if hasattr(c, 'item'):
                c, h, l = c.item(), h.item(), l.item()
            elif hasattr(c, '__iter__') and not isinstance(c, str):
                c, h, l = float(list(c)[0]), float(list(h)[0]), float(list(l)[0])
            result[d] = {
                "close": round(float(c), 4),
                "high":  round(float(h), 4),
                "low":   round(float(l), 4),
            }
    except Exception as e:
        print(f"  Yahoo Finance error for {ticker}: {e}")
    return result

def is_weekday(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return d.weekday() < 5  # Mon=0 ... Fri=4

def main():
    print(f"=== Dashboard data updater — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ===")

    data = load_data()
    last_date = get_last_date(data)
    today_str = str(date.today())

    # We fetch up to yesterday (completed candle)
    target_end = str(date.today() - timedelta(days=1))

    print(f"Last record in data.json: {last_date}")
    print(f"Fetching up to:           {target_end}")

    if last_date >= target_end:
        print("Data is already up to date. Nothing to do.")
        return

    # Build list of new dates we need (skip already-live entries)
    existing_dates = {d["date"] for d in data}
    # Remove any synthetic live data point for today if it exists
    data = [d for d in data if d["date"] != today_str]

    fetch_start = last_date  # fetch_coingecko_ohlc/yahoo will start from day after
    fetch_end   = target_end

    print(f"\nFetching CoinGecko BTC data...")
    time.sleep(1)
    btc_data = fetch_coingecko_ohlc("bitcoin", fetch_start, fetch_end)
    print(f"  Got {len(btc_data)} BTC days")

    print(f"Fetching CoinGecko ETH data...")
    time.sleep(2)  # Be polite to free API
    eth_data = fetch_coingecko_ohlc("ethereum", fetch_start, fetch_end)
    print(f"  Got {len(eth_data)} ETH days")

    print(f"Fetching Yahoo Finance MSTR data...")
    mstr_data = fetch_yahoo_ohlc("MSTR", fetch_start, fetch_end)
    print(f"  Got {len(mstr_data)} MSTR days")

    print(f"Fetching Yahoo Finance BMNR data...")
    bmnr_data = fetch_yahoo_ohlc("BMNR", fetch_start, fetch_end)
    print(f"  Got {len(bmnr_data)} BMNR days")

    # Merge new data into records
    new_records = []
    for d_str in date_range(last_date, fetch_end):
        if d_str in existing_dates:
            continue  # Already have this date

        has_btc  = d_str in btc_data
        has_eth  = d_str in eth_data
        has_mstr = d_str in mstr_data
        has_bmnr = d_str in bmnr_data

        # Skip days with no data at all (weekends for stocks, gaps)
        if not has_btc and not has_eth:
            # Crypto trades 24/7 — if no crypto data, it was a missing day
            continue

        record = {"date": d_str}

        if has_btc:
            record["btc"]           = round(btc_data[d_str]["close"], 2)
            record["btc_high_price"] = round(btc_data[d_str]["high"],  2)
            record["btc_low_price"]  = round(btc_data[d_str]["low"],   2)

        if has_eth:
            record["eth"]           = round(eth_data[d_str]["close"], 2)
            record["eth_high_price"] = round(eth_data[d_str]["high"],  2)
            record["eth_low_price"]  = round(eth_data[d_str]["low"],   2)

        if has_mstr:
            record["mstr"]           = round(mstr_data[d_str]["close"], 2)
            record["mstr_high_price"] = round(mstr_data[d_str]["high"],  2)
            record["mstr_low_price"]  = round(mstr_data[d_str]["low"],   2)

        if has_bmnr:
            record["bmnr"]           = round(bmnr_data[d_str]["close"], 2)
            record["bmnr_high_price"] = round(bmnr_data[d_str]["high"],  2)
            record["bmnr_low_price"]  = round(bmnr_data[d_str]["low"],   2)

        new_records.append(record)
        print(f"  + Added {d_str}: BTC={record.get('btc','—')} ETH={record.get('eth','—')} MSTR={record.get('mstr','—')} BMNR={record.get('bmnr','—')}")

    if new_records:
        data.extend(new_records)
        data.sort(key=lambda x: x["date"])
        save_data(data)
        print(f"\n✓ Added {len(new_records)} new records. Total: {len(data)}")
    else:
        print("\n No new records to add.")

    # Backfill missing MSTR/BMNR for existing weekday records
    patched = 0
    for rec in data:
        d_str = rec["date"]
        if not is_weekday(d_str):
            continue
        needs_mstr = "mstr" not in rec and d_str in mstr_data
        needs_bmnr = "bmnr" not in rec and d_str in bmnr_data
        if needs_mstr:
            rec["mstr"]           = round(mstr_data[d_str]["close"], 2)
            rec["mstr_high_price"] = round(mstr_data[d_str]["high"],  2)
            rec["mstr_low_price"]  = round(mstr_data[d_str]["low"],   2)
            patched += 1
        if needs_bmnr:
            rec["bmnr"]           = round(bmnr_data[d_str]["close"], 2)
            rec["bmnr_high_price"] = round(bmnr_data[d_str]["high"],  2)
            rec["bmnr_low_price"]  = round(bmnr_data[d_str]["low"],   2)
            patched += 1
    if patched:
        save_data(data)
        print(f"✓ Backfilled {patched} missing stock entries.")

if __name__ == "__main__":
    main()


# ─── Hourly FARTCOIN + BTC data ────────────────────────────────────────────
FARTCOIN_FILE = "fartcoin_hourly.json"

def load_fartcoin_data():
    try:
        with open(FARTCOIN_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"last_updated": "", "hours": []}

def save_fartcoin_data(data):
    with open(FARTCOIN_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))

def update_fartcoin_hourly():
    """Fetch last 7 days of hourly data and append any new hours."""
    print("\n=== Updating FARTCOIN hourly data ===")
    existing = load_fartcoin_data()
    existing_timestamps = {h["t"] for h in existing.get("hours", [])}

    # Fetch 7 days of hourly data (safe within CoinGecko free tier)
    from datetime import timezone
    new_hours = []

    for coin_id, key in [("bitcoin", "btc"), ("fartcoin", "fart")]:
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=7&interval=hourly"
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            prices = resp.json().get("prices", [])
            print(f"  {coin_id}: {len(prices)} hourly candles fetched")

            for ts_ms, price in prices:
                ts_str = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
                # Find or create entry for this timestamp
                entry = next((h for h in new_hours if h["t"] == ts_str), None)
                if entry is None:
                    entry = {"t": ts_str}
                    new_hours.append(entry)
                entry[key] = round(price, 6 if key == "fart" else 2)

            time.sleep(3)  # Rate limit
        except Exception as e:
            print(f"  Error fetching {coin_id}: {e}")

    # Only keep hours that have both BTC and FART
    valid_new = [h for h in new_hours if "btc" in h and "fart" in h and h["t"] not in existing_timestamps]
    valid_new.sort(key=lambda x: x["t"])

    if valid_new:
        existing["hours"] = sorted(existing.get("hours", []) + valid_new, key=lambda x: x["t"])
        existing["last_updated"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        save_fartcoin_data(existing)
        print(f"  ✓ Added {len(valid_new)} new hourly records. Total: {len(existing['hours'])}")
    else:
        print("  No new hourly records to add.")

# Call it at end of main
if __name__ == "__main__":
    main()
    update_fartcoin_hourly()
