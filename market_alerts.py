#!/usr/bin/env python3
"""
market_alerts.py — Sends Slack alerts when Oil or Nasdaq trigger significant moves.

Thresholds (same as dashboard Geopolitics tab):
  - WTI Crude Oil (CL=F): 3% move in 3 hours
  - Nasdaq 100 (^NDX):    2% move in 2 hours

Reads Yahoo Finance intraday data, checks for threshold breaches,
and sends alerts via SLACK_WEBHOOK_URL. Tracks state in
market_alert_state.json to avoid duplicate alerts.

Run via GitHub Actions every hour.
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone, timedelta

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
STATE_FILE = "market_alert_state.json"

MARKETS = [
    {
        "ticker": "CL=F",
        "name": "WTI Crude Oil",
        "threshold_pct": 3.0,
        "window_hours": 3,
        "emoji": "\U0001f6e2\ufe0f",  # oil drum
    },
    {
        "ticker": "^NDX",
        "name": "Nasdaq 100",
        "threshold_pct": 2.0,
        "window_hours": 2,
        "emoji": "\U0001f4c8",  # chart increasing
    },
]

YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"alerts": []}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def fetch_yahoo_intraday(ticker):
    """Fetch 1-day intraday data at 5min intervals from Yahoo Finance."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=5m&range=1d"
    try:
        resp = requests.get(url, headers=YAHOO_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", [])
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        closes = quote.get("close", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        return {
            "meta": meta,
            "timestamps": timestamps,
            "closes": closes,
            "highs": highs,
            "lows": lows,
        }
    except Exception as e:
        print(f"  Failed to fetch {ticker}: {e}")
        return None


def check_market(market):
    """Check if a market has triggered its threshold.

    Scans ALL rolling windows of size window_hours across the full day's
    intraday data (5-min bars). This catches spikes that happen and settle
    between hourly cron runs — no move gets missed.

    Returns alert dict or None.
    """
    ticker = market["ticker"]
    name = market["name"]
    threshold = market["threshold_pct"]
    window_hrs = market["window_hours"]

    print(f"\n  Checking {name} ({ticker})...")
    data = fetch_yahoo_intraday(ticker)
    if not data or not data["timestamps"]:
        print(f"    No data available")
        return None

    ts = data["timestamps"]
    closes = data["closes"]
    highs = data["highs"]
    lows = data["lows"]
    meta = data["meta"]

    # Build clean bars: (timestamp, close, high, low)
    bars = []
    for i in range(len(ts)):
        if closes[i] is not None and highs[i] is not None and lows[i] is not None:
            bars.append((ts[i], closes[i], highs[i], lows[i]))

    if len(bars) < 2:
        print(f"    Not enough data points ({len(bars)})")
        return None

    window_sec = window_hrs * 3600
    current_price = meta.get("regularMarketPrice") or bars[-1][1]
    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")

    # Scan every rolling window: for each bar, look back window_hrs and
    # compute max(high) - min(low) as a % of min(low)
    best_move = 0
    best_end_idx = -1
    best_high = 0
    best_low = 0

    for end in range(len(bars)):
        end_ts = bars[end][0]
        start_ts = end_ts - window_sec
        # Collect bars within this window
        win_highs = []
        win_lows = []
        for j in range(end, -1, -1):
            if bars[j][0] < start_ts:
                break
            win_highs.append(bars[j][2])
            win_lows.append(bars[j][3])

        if len(win_highs) < 2:
            continue

        rh = max(win_highs)
        rl = min(win_lows)
        if rl <= 0:
            continue
        move_pct = (rh - rl) / rl * 100

        if move_pct > best_move:
            best_move = move_pct
            best_end_idx = end
            best_high = rh
            best_low = rl

    # Determine direction at the peak window
    if best_end_idx >= 0 and best_end_idx > 0:
        # Find the start bar of the best window
        end_ts = bars[best_end_idx][0]
        start_ts = end_ts - window_sec
        win_bars = [b for b in bars if b[0] >= start_ts and b[0] <= end_ts]
        direction_up = win_bars[-1][1] > win_bars[0][1] if len(win_bars) >= 2 else True
    else:
        direction_up = True

    day_change = None
    if prev_close and prev_close > 0:
        day_change = (current_price - prev_close) / prev_close * 100

    print(f"    Price: ${current_price:.2f} | Best rolling window move: {best_move:.2f}% | Threshold: {threshold}%")

    if best_move >= threshold:
        print(f"    *** TRIGGERED *** (peak high ${best_high:.2f}, low ${best_low:.2f})")
        return {
            "name": name,
            "ticker": ticker,
            "emoji": market["emoji"],
            "price": current_price,
            "move_pct": round(best_move, 2),
            "threshold": threshold,
            "window_hours": window_hrs,
            "direction": "up" if direction_up else "down",
            "day_change": round(day_change, 2) if day_change is not None else None,
            "market_state": meta.get("marketState", "UNKNOWN"),
        }
    else:
        print(f"    No trigger")
        return None


def make_state_key(alert):
    """Create a dedup key: ticker + hour (one alert per market per hour max)."""
    now = datetime.now(timezone.utc)
    return f"{alert['ticker']}:{now.strftime('%Y-%m-%d-%H')}"


def send_slack_alert(alerts):
    """Send Slack message for triggered market alerts."""
    if not SLACK_WEBHOOK_URL:
        print("  No SLACK_WEBHOOK_URL set — skipping")
        return False

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "\U0001f6a8 Market Alert — Geopolitics"}
        },
        {"type": "divider"},
    ]

    for a in alerts:
        arrow = "\u25b2" if a["direction"] == "up" else "\u25bc"
        dir_label = "UP" if a["direction"] == "up" else "DOWN"
        day_str = f" | Day: {'+' if a['day_change'] >= 0 else ''}{a['day_change']}%" if a["day_change"] is not None else ""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{a['emoji']} *{a['name']}* — {a['move_pct']}% {arrow} {dir_label}\n"
                    f":moneybag: Price: ${a['price']:,.2f}{day_str}\n"
                    f":warning: Moved >{a['threshold']}% in {a['window_hours']}hrs"
                )
            }
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "_Geopolitics Alert  |  Ratio Dashboard_"}]
    })

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=15)
        if resp.status_code == 200:
            print(f"  Slack alert sent ({len(alerts)} market(s))")
            return True
        else:
            print(f"  Slack failed: {resp.status_code} — {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  Slack error: {e}")
        return False


def main():
    if "--test" in sys.argv:
        print("=== TEST MODE ===")
        test_alerts = [{
            "name": "WTI Crude Oil",
            "ticker": "CL=F",
            "emoji": "\U0001f6e2\ufe0f",
            "price": 71.50,
            "move_pct": 3.5,
            "threshold": 3.0,
            "window_hours": 3,
            "direction": "down",
            "day_change": -2.8,
            "market_state": "REGULAR",
        }]
        send_slack_alert(test_alerts)
        return

    print("=== Market Alerts Check ===")
    now = datetime.now(timezone.utc)
    print(f"  Time (UTC): {now.strftime('%Y-%m-%d %H:%M')}")

    state = load_state()
    previous_keys = set(state.get("alerts", []))

    triggered = []
    for market in MARKETS:
        alert = check_market(market)
        if alert:
            key = make_state_key(alert)
            if key in previous_keys:
                print(f"    Already alerted this hour — skipping")
            else:
                triggered.append(alert)
                previous_keys.add(key)

    if triggered:
        print(f"\n{len(triggered)} new alert(s) to send")
        success = send_slack_alert(triggered)
        if success:
            # Only save state after successful send
            state["alerts"] = list(previous_keys)
            # Prune old keys (>48hrs old based on key format)
            cutoff = (now - timedelta(hours=48)).strftime("%Y-%m-%d-%H")
            state["alerts"] = [k for k in state["alerts"] if k.split(":")[-1] >= cutoff]
            save_state(state)
    else:
        print("\nNo new alerts")
        # Still save to prune old keys
        cutoff = (now - timedelta(hours=48)).strftime("%Y-%m-%d-%H")
        state["alerts"] = [k for k in state.get("alerts", []) if k.split(":")[-1] >= cutoff]
        save_state(state)

    print("Done!")


if __name__ == "__main__":
    main()
