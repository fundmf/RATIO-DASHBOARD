#!/usr/bin/env python3
"""
fx_crossings.py — Hourly USD/JPY threshold-crossing Slack alerts.

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs hourly inside the main GitHub Actions workflow (update-data.yml)   ║
║  - Fetches the current USD/JPY spot rate from Yahoo Finance                ║
║  - Compares to the last recorded rate in fx_crossings_state.json           ║
║  - For every threshold crossed (up OR down) between last and current,      ║
║    adds a line to a single Slack message                                   ║
║  - Sends one consolidated Slack post to SLACK_WEBHOOK_URL if any           ║
║    crossings happened; silent otherwise                                    ║
║  - Idempotent: first run just seeds state, no alerts                       ║
║  - Soft-exit on fetch failure (avoids GH email spam from transient Yahoo   ║
║    outages)                                                                ║
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

PAIR_SYMBOL = "USDJPY=X"   # Yahoo Finance FX ticker
PAIR_LABEL  = "USD/JPY"

# JPY-per-USD levels. Alert fires on cross above or below.
THRESHOLDS = [159, 160, 161, 162, 163, 164, 165]

YAHOO_URL = (
    f"https://query1.finance.yahoo.com/v8/finance/chart/{PAIR_SYMBOL}"
    f"?interval=1h&range=1d"
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


def fetch_current_rate():
    """Fetch the current USD/JPY spot rate via Yahoo Finance."""
    r = requests.get(
        YAHOO_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/128.0.0.0 Safari/537.36",
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    result = data.get("chart", {}).get("result")
    if not result:
        raise ValueError(f"Yahoo response missing chart.result: {data}")
    meta = result[0].get("meta", {})
    # Prefer regularMarketPrice (most recent tick), fall back to last valid hourly close
    price = meta.get("regularMarketPrice")
    if price is None:
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        price = next((c for c in reversed(closes) if c is not None), None)
    if price is None:
        raise ValueError("Yahoo response has no price data")
    price = float(price)
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
