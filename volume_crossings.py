#!/usr/bin/env python3
"""
volume_crossings.py — Hourly FARTCOIN volume threshold crossing alerts.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs hourly inside the main GitHub Actions workflow (update-data.yml)   ║
║  - Fetches current 24h rolling volume for FARTCOIN from CoinGecko          ║
║  - Compares to the last recorded volume in volume_crossings_state.json     ║
║  - For every threshold crossed (up OR down) between last and current, adds ║
║    one line to a Slack message                                             ║
║  - Sends a single consolidated Slack post to SLACK_WEBHOOK_URL if any      ║
║    crossings happened; silent otherwise                                    ║
║  - Idempotent: first run just seeds state, no alerts                       ║
║                                                                            ║
║  Adjust thresholds by editing THRESHOLDS_M below.                          ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import requests

STATE_FILE = Path("volume_crossings_state.json")

COIN_ID = "fartcoin"
COIN_LABEL = "FARTCOIN"

# Volume thresholds in millions USD. Edit this list to add/remove/change.
THRESHOLDS_M = [50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700]

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


def fetch_current_volume_m():
    """Fetch current 24h rolling volume in USD-millions."""
    r = requests.get(
        COINGECKO_URL,
        headers={"User-Agent": "Mozilla/5.0 VolumeMonitor/1.0"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    vol_usd = data.get(COIN_ID, {}).get("usd_24h_vol")
    if vol_usd is None:
        raise ValueError(f"CoinGecko response missing usd_24h_vol: {data}")
    return float(vol_usd) / 1_000_000.0


def find_crossings(prev_m, curr_m, thresholds):
    """Return list of (direction, threshold_m) tuples for every crossing.
    direction is 'up' if we crossed from below to at-or-above, 'down' otherwise.
    """
    crossings = []
    for t in thresholds:
        if prev_m < t <= curr_m:
            crossings.append(("up", t))
        elif curr_m < t <= prev_m:
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
    print(f"=== Volume crossings check — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    try:
        curr_m = fetch_current_volume_m()
    except Exception as e:
        print(f"ERROR: CoinGecko fetch failed: {e}")
        sys.exit(1)

    state = load_state()
    prev_m = state.get("last_volume_m")

    print(f"Current 24h volume: ${curr_m:,.2f}M")
    print(f"Previous check:     ${prev_m:,.2f}M" if prev_m is not None else "Previous check:     (none — seeding state)")

    # Always update state (even on first run so future runs have a baseline)
    state["last_volume_m"] = curr_m
    state["last_check_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    save_state(state)

    if prev_m is None:
        print("First run — state seeded. No alerts on first run.")
        return

    crossings = find_crossings(prev_m, curr_m, THRESHOLDS_M)
    if not crossings:
        print("No thresholds crossed since last check.")
        return

    print(f"Crossings detected: {crossings}")

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("No SLACK_WEBHOOK_URL — would have alerted.")
        return

    lines = [
        f":chart_with_upwards_trend: *{COIN_LABEL} Volume Threshold Crossed*",
        f"Current 24h volume: `${curr_m:,.1f}M`",
        f"Previous check:     `${prev_m:,.1f}M`",
        "",
    ]
    for direction, t in crossings:
        if direction == "up":
            lines.append(f":arrow_up: Crossed ABOVE `${t}M`")
        else:
            lines.append(f":arrow_down: Crossed BELOW `${t}M`")
    msg = "\n".join(lines)

    if send_slack(webhook, msg):
        print("Alert sent.")


if __name__ == "__main__":
    main()
