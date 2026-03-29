#!/usr/bin/env python3
"""
forex_calendar_alert.py — Sends Slack alerts for upcoming high-impact USD
economic events from Forex Factory calendar.

Checks this week + next week's calendar. Sends a reminder ~24 hours before
each event. Tracks already-alerted events to avoid duplicate notifications.

Data source: nfs.faireconomy.media (mirrors Forex Factory)
Run via GitHub Actions every hour.

Usage:
    SLACK_WEBHOOK_URL=https://hooks.slack.com/... python forex_calendar_alert.py
    python forex_calendar_alert.py --test   # sends a test alert
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone, timedelta

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
STATE_FILE = "forex_alert_state.json"

# Only alert for these
CURRENCY_FILTER = "USD"
IMPACT_FILTER = "High"

# Alert window: send alert when event is 22-26 hours away (wider window so hourly cron reliably catches it)
ALERT_WINDOW_MIN_HOURS = 22
ALERT_WINDOW_MAX_HOURS = 26


def fetch_calendar():
    """Fetch this week + next week calendar data."""
    events = []
    for period in ["thisweek", "nextweek"]:
        url = f"https://nfs.faireconomy.media/ff_calendar_{period}.json"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            events.extend(resp.json())
        except Exception as e:
            print(f"  Warning: Failed to fetch {period}: {e}")
    return events


def filter_events(events):
    """Filter for USD + High impact only."""
    filtered = []
    for ev in events:
        if ev.get("country") != CURRENCY_FILTER:
            continue
        if ev.get("impact") != IMPACT_FILTER:
            continue
        filtered.append(ev)
    return filtered


def parse_event_time(ev):
    """Parse event datetime string to UTC datetime."""
    date_str = ev.get("date", "")
    if not date_str:
        return None
    try:
        # Format: "2026-03-31T10:00:00-04:00" or similar
        dt = datetime.fromisoformat(date_str)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def load_state():
    """Load already-alerted event IDs."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"alerted": []}


def save_state(state):
    """Save state to file."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def event_id(ev):
    """Create a unique ID for an event."""
    return f"{ev.get('date', '')}|{ev.get('title', '')}"


def format_aest(dt_utc):
    """Format a UTC datetime as AEST string."""
    aest = dt_utc + timedelta(hours=10)
    return aest.strftime("%a %d %b %Y, %I:%M %p AEST")


def format_utc(dt_utc):
    """Format a UTC datetime as US Eastern string."""
    et = dt_utc - timedelta(hours=4)  # Approximate ET
    return et.strftime("%I:%M %p ET")


def send_slack_alert(events_to_alert):
    """Send a Slack message with upcoming events."""
    if not SLACK_WEBHOOK_URL:
        print("  No SLACK_WEBHOOK_URL set, skipping")
        return False

    # Group events by date
    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": ":rotating_light: USD High Impact Events — 24hr Reminder"}
    })

    for ev, dt_utc in events_to_alert:
        forecast = ev.get("forecast", "—") or "—"
        previous = ev.get("previous", "—") or "—"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{ev['title']}*\n"
                    f":calendar: {format_aest(dt_utc)}\n"
                    f":clock3: {format_utc(dt_utc)}\n"
                    f":chart_with_upwards_trend: Forecast: *{forecast}*  |  Previous: *{previous}*"
                )
            }
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "_Source: Forex Factory  |  Filter: USD, High Impact_"}]
    })

    payload = {"blocks": blocks}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        print(f"  Slack alert sent for {len(events_to_alert)} event(s)")
        return True
    except Exception as e:
        print(f"  Failed to send Slack alert: {e}")
        return False


def send_test_alert():
    """Send a test alert with a fake event."""
    if not SLACK_WEBHOOK_URL:
        print("No SLACK_WEBHOOK_URL set")
        return

    now = datetime.now(timezone.utc)
    test_events = [(
        {
            "title": "Non-Farm Employment Change",
            "country": "USD",
            "impact": "High",
            "forecast": "180K",
            "previous": "151K",
            "date": (now + timedelta(hours=24)).isoformat()
        },
        now + timedelta(hours=24)
    ), (
        {
            "title": "Unemployment Rate",
            "country": "USD",
            "impact": "High",
            "forecast": "4.1%",
            "previous": "4.0%",
            "date": (now + timedelta(hours=24)).isoformat()
        },
        now + timedelta(hours=24)
    )]

    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": ":test_tube: TEST — USD High Impact Events — 24hr Reminder"}
    })

    for ev, dt_utc in test_events:
        forecast = ev.get("forecast", "—") or "—"
        previous = ev.get("previous", "—") or "—"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{ev['title']}*\n"
                    f":calendar: {format_aest(dt_utc)}\n"
                    f":clock3: {format_utc(dt_utc)}\n"
                    f":chart_with_upwards_trend: Forecast: *{forecast}*  |  Previous: *{previous}*"
                )
            }
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "_:test_tube: This is a test alert  |  Source: Forex Factory  |  Filter: USD, High Impact_"}]
    })

    payload = {"blocks": blocks}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
    print(f"Test alert sent! Status: {resp.status_code}")


def main():
    if "--test" in sys.argv:
        send_test_alert()
        return

    print("=== Forex Calendar Alert Check ===")
    now = datetime.now(timezone.utc)
    print(f"  Current time (UTC): {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Current time (AEST): {format_aest(now)}")

    # Fetch and filter
    print("\nFetching calendar data...")
    all_events = fetch_calendar()
    print(f"  Total events: {len(all_events)}")

    usd_high = filter_events(all_events)
    print(f"  USD High Impact: {len(usd_high)}")

    for ev in usd_high:
        dt = parse_event_time(ev)
        if dt:
            hrs = (dt - now).total_seconds() / 3600
            print(f"    {ev['title']}: {format_aest(dt)} ({hrs:.1f}hrs away)")

    # Check which events are in the 24hr alert window
    state = load_state()
    events_to_alert = []

    for ev in usd_high:
        dt = parse_event_time(ev)
        if not dt:
            continue

        hours_until = (dt - now).total_seconds() / 3600
        eid = event_id(ev)

        if ALERT_WINDOW_MIN_HOURS <= hours_until <= ALERT_WINDOW_MAX_HOURS:
            if eid not in state["alerted"]:
                events_to_alert.append((ev, dt))
                print(f"  >> ALERTING: {ev['title']} in {hours_until:.1f}hrs")

    # Send alerts — only mark as alerted if webhook succeeds
    if events_to_alert:
        print(f"\nSending alert for {len(events_to_alert)} event(s)...")
        success = send_slack_alert(events_to_alert)
        if success:
            for ev, dt in events_to_alert:
                state["alerted"].append(event_id(ev))
            save_state(state)
        else:
            print("  Webhook failed — will retry next run")
    else:
        print("\nNo new events in alert window.")

    # Clean up old alerted events (older than 48hrs ago)
    cutoff = (now - timedelta(hours=48)).isoformat()
    state["alerted"] = [eid for eid in state["alerted"] if eid.split("|")[0] > cutoff]
    save_state(state)

    print("Done!")


if __name__ == "__main__":
    main()
