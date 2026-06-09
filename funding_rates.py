#!/usr/bin/env python3
"""
funding_rates.py — Tracks Hyperliquid hourly funding rates for BTC, SPX, FARTCOIN.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Hyperliquid charges funding every 1 hour                                ║
║  - On each run, the script pulls the last ~14 days of hourly funding for   ║
║    each tracked coin and saves to funding_rates.json (powers the chart)    ║
║  - At ~21:00 UTC Mon-Fri (US market close, DST-safe), it checks the latest ║
║    funding rate for each enabled coin and sends a Slack alert if NEGATIVE  ║
║  - Alerts respect notification_settings.json — disabled coins are skipped  ║
║  - Idempotent: it won't double-alert within the same UTC hour              ║
║                                                                            ║
║  HOW TO ADD A COIN                                                         ║
║  ─────────────────                                                         ║
║  Add the Hyperliquid perp symbol to COINS below. The frontend reads        ║
║  the coin list from the JSON, so no other code change is required.         ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import requests

DATA_FILE     = Path("funding_rates.json")
SETTINGS_FILE = Path("notification_settings.json")

# Hyperliquid perp symbols. SPX on HL = SPX6900 memecoin (not S&P 500).
COINS = ["BTC", "SPX", "FARTCOIN"]

HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def fetch_funding_history(coin, hours_back=14 * 24):
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - hours_back * 3600 * 1000
    try:
        resp = requests.post(
            HL_INFO_URL,
            json={"type": "fundingHistory", "coin": coin, "startTime": start_ms},
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  {coin} fetch error: {e}")
        return []


def load_settings():
    if not SETTINGS_FILE.exists():
        return {}
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"  settings load error: {e}")
        return {}


def send_slack(webhook, text):
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=10)
        if not resp.ok:
            print(f"  Slack send failed: {resp.status_code} {resp.text[:200]}")
        return resp.ok
    except Exception as e:
        print(f"  Slack send error: {e}")
        return False


def main():
    now = datetime.now(timezone.utc)
    print(f"=== Funding rates updater — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    # Load existing JSON or initialize
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            data = json.load(f)
    else:
        data = {"last_updated": "", "coins": {}, "alert_state": {}}
    data.setdefault("coins", {})
    data.setdefault("alert_state", {})  # per-coin: last alert hour key

    # Pull funding for each coin
    for coin in COINS:
        print(f"\nFetching {coin} funding history...")
        history = fetch_funding_history(coin)
        if not history:
            print(f"  {coin}: no data returned")
            continue
        entries = [
            {"t": int(e["time"]), "rate": float(e["fundingRate"])}
            for e in history
        ]
        entries.sort(key=lambda x: x["t"])
        # Dedup by timestamp
        seen = {}
        for e in entries:
            seen[e["t"]] = e
        entries = sorted(seen.values(), key=lambda x: x["t"])

        latest = entries[-1] if entries else None
        data["coins"][coin] = {
            "label": coin,
            "history": entries,
            "current_rate": latest["rate"] if latest else None,
            "current_time": latest["t"] if latest else None,
        }
        if latest:
            r_pct = latest["rate"] * 100
            ann   = latest["rate"] * 24 * 365 * 100
            print(f"  {coin}: {len(entries)} hourly pts · latest {r_pct:+.4f}%/h ({ann:+.2f}% ann)")

    data["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"\nSaved funding data for {len(data['coins'])} coins.")

    # ── Alert window: US market close, daily Mon-Fri at 21:00 UTC ──
    # 21:00 UTC = 5pm EDT (DST) / 4pm EST — always at or after the 4pm ET close.
    if now.hour != 21 or now.weekday() >= 5:
        print(f"\nSkipping alert check (current {now.strftime('%a %H:%M UTC')} is not Mon-Fri 21:00 UTC).")
        return

    print("\n>>> US market close window — checking for negative funding...")
    settings = load_settings()
    webhook  = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("  No SLACK_WEBHOOK_URL set — skipping alert.")
        return

    today_key = now.strftime("%Y-%m-%d")
    for coin in COINS:
        key = f"funding_alert_{coin.lower()}"
        if not settings.get(key, True):  # default ON
            print(f"  {coin}: alert disabled in settings — skip.")
            continue
        cd = data["coins"].get(coin)
        if not cd or cd.get("current_rate") is None:
            print(f"  {coin}: no rate available — skip.")
            continue
        rate = cd["current_rate"]
        # Dedupe per coin per UTC date
        if data["alert_state"].get(coin) == today_key:
            print(f"  {coin}: already alerted today ({today_key}) — skip.")
            continue
        if rate < 0:
            r_pct = rate * 100
            ann   = rate * 24 * 365 * 100
            msg = (f":rotating_light: *{coin} funding is NEGATIVE on Hyperliquid*\n"
                   f"Current rate: `{r_pct:+.4f}% per hour` ({ann:+.2f}% annualised)\n"
                   f"Checked at US market close on {today_key}.")
            if send_slack(webhook, msg):
                data["alert_state"][coin] = today_key
                print(f"  Alert sent for {coin}.")
        else:
            print(f"  {coin}: funding positive ({rate*100:+.4f}%/h) — no alert.")

    # Persist updated alert_state
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))


if __name__ == "__main__":
    main()
