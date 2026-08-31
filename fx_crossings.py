#!/usr/bin/env python3
"""
fx_crossings.py — Hourly USD/JPY threshold-crossing Slack alerts.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs hourly inside the main GitHub Actions workflow (update-data.yml)   ║
║  - Fetches the current USD/JPY rate from HYPERLIQUID (xyz:JPY perp) —      ║
║    24/7 pricing, including weekends. Previously used Yahoo which is        ║
║    closed weekends and pauses trading briefly daily.                       ║
║  - Compares to the last recorded rate in fx_crossings_state.json           ║
║  - For every threshold crossed (up OR down) between last and current,      ║
║    adds a line to a single Slack message                                   ║
║  - Sends one consolidated Slack post to SLACK_WEBHOOK_URL if any           ║
║    crossings happened; silent otherwise                                    ║
║  - Idempotent: first run just seeds state, no alerts                       ║
║  - Soft-exit on fetch failure (avoids GH email spam from transient         ║
║    upstream outages)                                                       ║
║                                                                            ║
║  Adjust thresholds by editing THRESHOLDS below.                            ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import requests

STATE_FILE = Path("fx_crossings_state.json")

# Hyperliquid builder-DEX perp for USD/JPY. Priced in JPY-per-USD, trades 24/7.
PAIR_SYMBOL = "xyz:JPY"
PAIR_LABEL  = "USD/JPY"

# JPY-per-USD levels. Alert fires on cross above or below.
THRESHOLDS = [159, 160, 161, 162, 163, 164, 165]

HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception as e:
        print(f"  state load error: {e}")
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def fetch_current_rate():
    """Fetch the latest USD/JPY rate from Hyperliquid xyz:JPY perp (24/7)."""
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - 6 * 3600 * 1000  # last 6 hours of 1h candles
    resp = requests.post(
        HL_INFO_URL,
        json={
            "type": "candleSnapshot",
            "req": {
                "coin": PAIR_SYMBOL,
                "interval": "1h",
                "startTime": start_ms,
                "endTime": end_ms,
            },
        },
        timeout=15,
    )
    resp.raise_for_status()
    candles = resp.json()
    if not candles:
        raise ValueError(f"HL returned no candle data for {PAIR_SYMBOL}")
    latest = max(candles, key=lambda c: c.get("t", 0))
    price = float(latest.get("c"))
    # Sanity check — USDJPY has been between ~75 and ~180 historically
    if price < 50 or price > 300:
        raise ValueError(f"suspicious USDJPY value: {price}")
    return price


def find_crossings(prev, curr, thresholds):
    crossings = []
    for t in thresholds:
        if prev < t <= curr:
            crossings.append(("up", t))
        elif curr < t <= prev:
            crossings.append(("down", t))
    return crossings


def send_slack(webhook, text):
    try:
        r = requests.post(webhook, json={"text": text}, timeout=10)
        if not r.ok:
            print(f"  Slack send failed: {r.status_code} {r.text[:200]}")
        return r.ok
    except Exception as e:
        print(f"  Slack send error: {e}")
        return False


def main():
    now = datetime.now(timezone.utc)
    print(f"=== FX crossings check ({PAIR_LABEL}) — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    try:
        curr = fetch_current_rate()
    except Exception as e:
        # Soft-exit — Yahoo occasionally rate-limits or blocks GHA IPs, we don't
        # want that to spam email-on-failure notifications. Next hourly run retries.
        print(f"WARNING: Yahoo fetch failed (soft-exit, will retry next run): {e}")
        return

    state = load_state()
    prev = state.get("last_rate")

    print(f"Current {PAIR_LABEL}: {curr:.4f}")
    if prev is not None:
        print(f"Previous check:      {prev:.4f}")
    else:
        print("Previous check:      (none — seeding state)")

    state["last_rate"] = curr
    state["last_check_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    save_state(state)

    if prev is None:
        print("First run — state seeded. No alerts on first run.")
        return

    crossings = find_crossings(prev, curr, THRESHOLDS)
    if not crossings:
        print("No thresholds crossed since last check.")
        return

    print(f"Crossings detected: {crossings}")

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("No SLACK_WEBHOOK_URL — would have alerted.")
        return

    lines = [
        f":yen: *{PAIR_LABEL} Threshold Crossed*",
        f"Current rate:   `{curr:.4f}`",
        f"Previous check: `{prev:.4f}`",
        "",
    ]
    for direction, t in crossings:
        if direction == "up":
            lines.append(f":arrow_up: Crossed ABOVE `{t}` (USD strengthened / JPY weakened)")
        else:
            lines.append(f":arrow_down: Crossed BELOW `{t}` (USD weakened / JPY strengthened)")
    msg = "\n".join(lines)

    if send_slack(webhook, msg):
        print("Alert sent.")


if __name__ == "__main__":
    main()
