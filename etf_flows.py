#!/usr/bin/env python3
"""
etf_flows.py — Scrapes Farside.co.uk daily BTC ETF flow totals and detects
sign-flip events with magnitude >= threshold for Slack alerts.

╔════════════════════════════════════════════════════════════════════════════╗
║  ALERT LOGIC                                                               ║
║  ───────────                                                               ║
║  Alert fires when ALL conditions are met:                                  ║
║    1. yesterday and today have OPPOSITE signs (one positive, one negative) ║
║    2. |today - yesterday| >= threshold (default $300M)                     ║
║    3. etf_flow_alert_btc is enabled in notification_settings.json          ║
║    4. We have not already alerted for this 'today' date                    ║
║                                                                            ║
║  Examples (threshold = $300M):                                             ║
║    Y = -200M, T = +200M  → flip, diff $400M → ALERT                        ║
║    Y = +50M,  T = -300M  → flip, diff $350M → ALERT                        ║
║    Y = +50M,  T = +500M  → no flip          → no alert                     ║
║    Y = -50M,  T = +200M  → flip, diff $250M → no alert (under threshold)   ║
╚════════════════════════════════════════════════════════════════════════════╝

Run daily via GitHub Actions. Recommended cron: 04:00 UTC (after Farside
publishes the prior US trading day's flows).
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
import requests

DATA_FILE     = Path("etf_flows.json")
SETTINGS_FILE = Path("notification_settings.json")
FARSIDE_URL   = "https://farside.co.uk/btc/"
DEFAULT_THRESHOLD_M = 300.0  # millions USD


def load_settings():
    if not SETTINGS_FILE.exists():
        return {}
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"  settings load error: {e}")
        return {}


def fetch_farside():
    """Fetch and parse Farside's BTC ETF daily flow table."""
    resp = requests.get(
        FARSIDE_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; RatioDashboard/1.0)"},
        timeout=30,
    )
    resp.raise_for_status()
    return parse_farside_html(resp.text)


def parse_farside_html(html):
    """
    Each data row has cells [Date, IBIT, FBTC, ..., Total].
    Date format: '04 Jun 2024' style.
    Total is in millions USD. Negative shown as '(123.4)' or '-123.4'.
    """
    # Farside's <tr> tags carry style/class attrs (e.g. <tr style="...">) — must
    # allow attributes on the opening tag, otherwise zero rows are matched.
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    days = []
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 3:
            continue
        date_str  = re.sub(r'<[^>]+>', '', cells[0]).strip()
        total_str = re.sub(r'<[^>]+>', '', cells[-1]).replace('&nbsp;', '').strip()
        try:
            d = datetime.strptime(date_str, "%d %b %Y").date()
        except ValueError:
            continue
        try:
            t = total_str.replace(',', '').replace('$', '').strip()
            if t in ('', '-'):
                total_m = 0.0
            elif t.startswith('(') and t.endswith(')'):
                total_m = -float(t[1:-1])
            else:
                total_m = float(t)
        except ValueError:
            continue
        days.append({"date": d.strftime("%Y-%m-%d"), "total_m_usd": round(total_m, 2)})
    days.sort(key=lambda x: x["date"])
    return days


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
    print(f"=== ETF flows updater — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    settings    = load_settings()
    threshold_m = float(settings.get("etf_flow_threshold_m_usd", DEFAULT_THRESHOLD_M))

    # Scrape Farside
    try:
        new_days = fetch_farside()
    except Exception as e:
        print(f"Farside fetch error: {e}")
        return
    if not new_days:
        print("No data parsed from Farside.")
        return
    print(f"Parsed {len(new_days)} days from Farside. Last 3: {new_days[-3:]}")

    # Load or initialize JSON
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            data = json.load(f)
    else:
        data = {"last_updated": "", "days": [], "last_alert_date": ""}
    data.setdefault("days", [])
    data.setdefault("last_alert_date", "")

    # Merge
    by_date = {d["date"]: d for d in data["days"]}
    for d in new_days:
        by_date[d["date"]] = d
    data["days"] = sorted(by_date.values(), key=lambda x: x["date"])
    data["last_updated"]   = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    data["threshold_m_usd"] = threshold_m

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"Saved {len(data['days'])} days to {DATA_FILE}")

    # ── Alert ──
    if not settings.get("etf_flow_alert_btc", True):
        print("ETF flow alert disabled in settings — skip.")
        return
    if len(data["days"]) < 2:
        print("Not enough history for sign-flip detection.")
        return

    yesterday = data["days"][-2]
    today     = data["days"][-1]
    if data["last_alert_date"] == today["date"]:
        print(f"Already alerted for {today['date']} — skip.")
        return

    y = yesterday["total_m_usd"]
    t = today["total_m_usd"]
    diff = abs(t - y)
    sign_flip = (t > 0 > y) or (t < 0 < y)

    print(f"Yesterday ({yesterday['date']}): ${y:+,.2f}M")
    print(f"Today     ({today['date']}): ${t:+,.2f}M")
    print(f"Diff: ${diff:,.2f}M · Sign flip: {sign_flip} · Threshold: ${threshold_m:,.0f}M")

    if not (sign_flip and diff >= threshold_m):
        print("Conditions not met — no alert.")
        return

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("No SLACK_WEBHOOK_URL set — skipping Slack post.")
        return

    direction = ":chart_with_upwards_trend: turned POSITIVE" if t > 0 else ":chart_with_downwards_trend: turned NEGATIVE"
    msg = (f":bell: *BTC ETF Flow Sign Flip*\n"
           f"Yesterday ({yesterday['date']}): `${y:+,.1f}M`\n"
           f"Today ({today['date']}): `${t:+,.1f}M`\n"
           f"{direction}\n"
           f"Diff: `${diff:,.1f}M` (threshold ${threshold_m:,.0f}M)\n"
           f"Source: {FARSIDE_URL}")
    if send_slack(webhook, msg):
        data["last_alert_date"] = today["date"]
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        print(f"Alert sent. Dedupe state updated to {today['date']}.")


if __name__ == "__main__":
    main()
