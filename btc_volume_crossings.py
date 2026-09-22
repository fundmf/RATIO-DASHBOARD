#!/usr/bin/env python3
"""
btc_volume_crossings.py — Hourly BTC 24h-volume threshold-crossing Slack alerts.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs hourly inside update-data.yml                                      ║
║  - Fetches BTC's rolling 24h volume in USD from CoinGecko                  ║
║  - Compares to the last-seen value in btc_volume_crossings_state.json      ║
║  - For every 10-billion-USD threshold crossed between the two values       ║
║    (30B, 40B, 50B, …, in both directions), adds a line to a single        ║
║    Slack message and sends it to SLACK_WEBHOOK_URL                        ║
║  - Idempotent — the first run just seeds state, no alerts                 ║
║  - Soft-exit on fetch failure so CoinGecko rate-limits don't spam GH      ║
║    email-on-failure notifications                                         ║
║                                                                            ║
║  Edit THRESHOLDS_B to add / remove / change the levels.                    ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import requests

STATE_FILE = Path("btc_volume_crossings_state.json")

COIN_ID = "bitcoin"
COIN_LABEL = "BTC"

# Volume thresholds in BILLIONS USD. Every 10B from 30B up.
THRESHOLDS_B = [30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]

COINGECKO_URL = (
    f"https://api.coingecko.com/api/v3/simple/price"
    f"?ids={COIN_ID}&vs_currencies=usd&include_24hr_vol=true"
)


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


def fetch_current_volume_b():
    """Fetch BTC's rolling 24h volume in USD-billions."""
    r = requests.get(
        COINGECKO_URL,
        headers={"User-Agent": "Mozilla/5.0 BtcVolumeMonitor/1.0"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    vol_usd = data.get(COIN_ID, {}).get("usd_24h_vol")
    if vol_usd is None:
        raise ValueError(f"CoinGecko response missing usd_24h_vol: {data}")
    return float(vol_usd) / 1_000_000_000.0


def find_crossings(prev_b, curr_b, thresholds):
    """Return list of (direction, threshold_b). 'up' = prev<t<=curr, 'down' = curr<t<=prev."""
    crossings = []
    for t in thresholds:
        if prev_b < t <= curr_b:
            crossings.append(("up", t))
        elif curr_b < t <= prev_b:
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
    print(f"=== BTC volume crossings check — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    try:
        curr_b = fetch_current_volume_b()
    except Exception as e:
        print(f"WARNING: CoinGecko fetch failed (soft-exit, will retry next run): {e}")
        return

    state = load_state()
    prev_b = state.get("last_volume_b")

    print(f"Current 24h volume: ${curr_b:,.2f}B")
    if prev_b is not None:
        print(f"Previous check:     ${prev_b:,.2f}B")
    else:
        print("Previous check:     (none — seeding state)")

    state["last_volume_b"] = curr_b
    state["last_check_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    save_state(state)

    if prev_b is None:
        print("First run — state seeded. No alerts on first run.")
        return

    crossings = find_crossings(prev_b, curr_b, THRESHOLDS_B)
    if not crossings:
        print("No thresholds crossed since last check.")
        return

    print(f"Crossings detected: {crossings}")

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("No SLACK_WEBHOOK_URL — would have alerted.")
        return

    lines = [
        f":bar_chart: *{COIN_LABEL} Volume Threshold Crossed*",
        f"Current 24h volume: `${curr_b:,.2f}B`",
        f"Previous check:     `${prev_b:,.2f}B`",
        "",
    ]
    for direction, t in crossings:
        if direction == "up":
            lines.append(f":arrow_up: Crossed ABOVE `${t}B`")
        else:
            lines.append(f":arrow_down: Crossed BELOW `${t}B`")
    msg = "\n".join(lines)

    if send_slack(webhook, msg):
        print("Alert sent.")


if __name__ == "__main__":
    main()
