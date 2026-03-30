#!/usr/bin/env python3
"""
custom_alerts.py — Sends Slack alerts for custom team reminders.

Reads custom_alerts.json, checks for due alerts, sends to Slack
via SLACK_WEBHOOK_URL_CUSTOM, and marks them as sent.

Run via GitHub Actions every hour.

Usage:
    SLACK_WEBHOOK_URL_CUSTOM=https://hooks.slack.com/... python custom_alerts.py
    python custom_alerts.py --test   # sends a test alert
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL_CUSTOM", "")
ALERTS_FILE = "custom_alerts.json"

TZ_LABELS = {
    "Australia/Sydney": "AEST",
    "America/New_York": "ET",
    "America/Chicago": "CT",
    "America/Los_Angeles": "PT",
    "Europe/London": "GMT",
    "UTC": "UTC",
    "Asia/Tokyo": "JST",
    "Asia/Singapore": "SGT",
}


def load_alerts():
    """Load alerts from JSON file."""
    try:
        with open(ALERTS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"alerts": []}


def save_alerts(data):
    """Save alerts to JSON file."""
    with open(ALERTS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def parse_alert_utc(alert):
    """Get UTC datetime for an alert."""
    utc_iso = alert.get("utc_iso", "")
    if utc_iso:
        return datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))

    dt_str = alert.get("datetime", "")
    tz_str = alert.get("timezone", "UTC")
    if not dt_str:
        return None
    try:
        naive = datetime.fromisoformat(dt_str)
        tz = ZoneInfo(tz_str)
        return naive.replace(tzinfo=tz).astimezone(timezone.utc)
    except Exception:
        return None


def format_in_tz(utc_dt, tz_name):
    """Format a UTC datetime in a given timezone."""
    tz = ZoneInfo(tz_name)
    local = utc_dt.astimezone(tz)
    label = TZ_LABELS.get(tz_name, tz_name)
    return local.strftime(f"%a %d %b %Y, %I:%M %p {label}")


def send_slack_alert(alert, utc_dt):
    """Send a single alert to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("  No SLACK_WEBHOOK_URL_CUSTOM set, skipping")
        return False

    tz_name = alert.get("timezone", "UTC")
    set_tz_str = format_in_tz(utc_dt, tz_name)
    aest_str = format_in_tz(utc_dt, "Australia/Sydney")

    desc = alert.get("description", "")
    desc_line = f"\n:memo: {desc}" if desc else ""

    # Show both timezones, skip AEST duplicate if already in AEST
    time_lines = f":calendar: *{set_tz_str}*"
    if tz_name != "Australia/Sydney":
        time_lines += f"\n:clock3: {aest_str}"
    time_lines += desc_line

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f514 {alert.get('title', 'Custom Alert')}"}
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": time_lines}
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Custom Alert  |  Ratio Dashboard_"}]
        }
    ]

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=15)
        resp.raise_for_status()
        print(f"  Slack alert sent: {alert.get('title', '?')}")
        return True
    except Exception as e:
        print(f"  Failed to send: {e}")
        return False


def send_test_alert():
    """Send a test alert with a fake event."""
    if not SLACK_WEBHOOK_URL:
        print("No SLACK_WEBHOOK_URL_CUSTOM set")
        return

    now = datetime.now(timezone.utc)
    test_alert = {
        "title": "Test Custom Alert",
        "description": "This is a test reminder from the Ratio Dashboard.",
        "timezone": "Australia/Sydney",
    }
    success = send_slack_alert(test_alert, now)
    print(f"Test alert {'sent' if success else 'failed'}!")


def main():
    if "--test" in sys.argv:
        send_test_alert()
        return

    print("=== Custom Alerts Check ===")
    now = datetime.now(timezone.utc)
    print(f"  Current time (UTC): {now.strftime('%Y-%m-%d %H:%M')}")

    data = load_alerts()
    alerts = data.get("alerts", [])
    print(f"  Total alerts: {len(alerts)}")

    pending = [a for a in alerts if not a.get("sent")]
    print(f"  Pending: {len(pending)}")

    sent_count = 0
    for alert in pending:
        utc_dt = parse_alert_utc(alert)
        if not utc_dt:
            print(f"  Skipping (no valid time): {alert.get('title', '?')}")
            continue

        hours_until = (utc_dt - now).total_seconds() / 3600
        print(f"  {alert.get('title', '?')}: {hours_until:.1f}hrs away")

        # Send if alert time has passed or is within 30 min
        if hours_until <= 0.5:
            success = send_slack_alert(alert, utc_dt)
            if success:
                alert["sent"] = True
                sent_count += 1

    if sent_count:
        print(f"\nSent {sent_count} alert(s)")

    # Clean up old sent alerts (>7 days past)
    cutoff = now - timedelta(days=7)
    original_count = len(data["alerts"])
    data["alerts"] = [
        a for a in data["alerts"]
        if not a.get("sent") or (
            parse_alert_utc(a) and parse_alert_utc(a) > cutoff
        )
    ]
    removed = original_count - len(data["alerts"])
    if removed:
        print(f"  Cleaned up {removed} old sent alert(s)")

    save_alerts(data)
    print("Done!")


if __name__ == "__main__":
    main()
