#!/usr/bin/env python3
"""
funding_rates.py — Captures ONE daily snapshot of Hyperliquid funding rates
for BTC, SPX, FARTCOIN at US market close (21:00 UTC Mon-Fri).

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs once daily on cron (Mon-Fri 21:00 UTC) via update-funding.yml      ║
║  - For each tracked coin, fetches the latest hourly funding rate from      ║
║    Hyperliquid and appends a single snapshot {date, rate} to history       ║
║  - If that rate is NEGATIVE and the alert is enabled in                    ║
║    notification_settings.json, sends a Slack notification                  ║
║  - Idempotent: re-running on the same UTC date is a no-op                  ║
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


def fetch_latest_funding(coin):
    """Fetch the most recent hourly funding payment for `coin`."""
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - 6 * 3600 * 1000  # last 6 hours is plenty
    try:
        resp = requests.post(
            HL_INFO_URL,
            json={"type": "fundingHistory", "coin": coin, "startTime": start_ms},
            timeout=20,
        )
        resp.raise_for_status()
        history = resp.json()
        if not history:
            return None
        latest = max(history, key=lambda e: e["time"])
        return {"rate": float(latest["fundingRate"]), "time_ms": int(latest["time"])}
    except Exception as e:
        print(f"  {coin} fetch error: {e}")
        return None


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


def migrate_legacy(coin_data):
    """One-time migration: drop legacy hourly 'history', initialise snapshots if absent."""
    if "snapshots" not in coin_data:
        coin_data["snapshots"] = []
    if "history" in coin_data:
        del coin_data["history"]
    return coin_data


def main():
    now = datetime.now(timezone.utc)
    today_key = now.strftime("%Y-%m-%d")
    print(f"=== Funding rates daily snapshot — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            data = json.load(f)
    else:
        data = {"last_updated": "", "coins": {}, "alert_state": {}}
    data.setdefault("coins", {})
    data.setdefault("alert_state", {})

    settings = load_settings()
    webhook  = os.environ.get("SLACK_WEBHOOK_URL")

    for coin in COINS:
        print(f"\n{coin}...")
        coin_data = data["coins"].setdefault(coin, {"label": coin})
        coin_data["label"] = coin
        migrate_legacy(coin_data)

        existing_dates = {s["date"] for s in coin_data["snapshots"]}
        if today_key in existing_dates:
            print(f"  Snapshot for {today_key} already exists — no-op.")
            continue

        latest = fetch_latest_funding(coin)
        if latest is None:
            print(f"  No data returned — skip.")
            continue

        snapshot = {
            "date": today_key,
            "rate": latest["rate"],
            "captured_at":     now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "funding_time_ms": latest["time_ms"],
        }
        coin_data["snapshots"].append(snapshot)
        coin_data["snapshots"].sort(key=lambda s: s["date"])
        # Keep up to ~120 weekday snapshots (~6 months) — old entries pruned
        if len(coin_data["snapshots"]) > 120:
            coin_data["snapshots"] = coin_data["snapshots"][-120:]
        coin_data["current_rate"] = latest["rate"]
        coin_data["current_date"] = today_key

        r_pct = latest["rate"] * 100
        ann   = latest["rate"] * 24 * 365 * 100
        print(f"  Captured {r_pct:+.4f}%/h  ({ann:+.2f}% annualised)")

        # Slack alert path
        key = f"funding_alert_{coin.lower()}"
        if not settings.get(key, True):
            print(f"  alert disabled in settings — skip Slack.")
            continue
        if latest["rate"] >= 0:
            print(f"  rate positive — no alert needed.")
            continue
        if data["alert_state"].get(coin) == today_key:
            print(f"  already alerted today — skip.")
            continue
        if not webhook:
            print(f"  no SLACK_WEBHOOK_URL set — would have alerted.")
            continue

        msg = (f":rotating_light: *{coin} funding is NEGATIVE on Hyperliquid*\n"
               f"Current rate: `{r_pct:+.4f}% per hour` ({ann:+.2f}% annualised)\n"
               f"Captured at US market close on {today_key}.")
        if send_slack(webhook, msg):
            data["alert_state"][coin] = today_key
            print(f"  Alert sent.")

    data["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"\nSaved.")


if __name__ == "__main__":
    main()
