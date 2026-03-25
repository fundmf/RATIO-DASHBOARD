#!/usr/bin/env python3
"""
detect_events.py — Runs divergence detection on hourly data (same logic as the dashboard)
and sends Slack alerts for any NEW events detected.

Reads:
  - fartcoin_hourly.json  (BTC + FARTCOIN hourly data)
  - spx6900_hourly.json   (BTC + SPX6900 hourly data)
  - last_events.json      (previously detected events — to avoid duplicate alerts)

Writes:
  - last_events.json      (updated with newly detected events)

Env vars:
  - SLACK_WEBHOOK_URL     (Slack Incoming Webhook URL)
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone

FARTCOIN_FILE = "fartcoin_hourly.json"
SPX6900_FILE = "spx6900_hourly.json"
LAST_EVENTS_FILE = "last_events.json"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Default thresholds (same as dashboard defaults)
BTC_DROP_THR = 1.5 / 100      # 1.5% BTC drop over 24hrs
FART_CRASH_THR = 5.0 / 100    # 5% FARTCOIN crash threshold
SPX_CRASH_THR = 5.0 / 100     # 5% SPX6900 crash threshold
FART_DROP_THR = 1.5 / 100     # 1.5% FARTCOIN drop (inverse)
FART_PUMP_THR = 5.0 / 100     # 5% FARTCOIN pump threshold (inverse)
BFSPX_BTC_THR = 1.5 / 100     # BTC/FC->SPX BTC drop threshold
BFSPX_CRASH_THR = 5.0 / 100   # BTC/FC->SPX SPX crash threshold

LB = 24   # lookback hours
CW = 72   # crash window hours


def load_json(filename):
    """Load hourly JSON data file."""
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            return data.get("hours", [])
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_last_events():
    """Load previously detected event keys."""
    try:
        with open(LAST_EVENTS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"fartcoin": [], "inverse_fartcoin": [], "spx": [], "bfspx": []}


def save_last_events(events):
    """Save detected event keys."""
    with open(LAST_EVENTS_FILE, "w") as f:
        json.dump(events, f, indent=2)


def au_day(timestamp_str):
    """Get Australian date string from ISO timestamp (for one-event-per-day rule)."""
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    # AEST = UTC+10
    from datetime import timedelta
    aest = dt + timedelta(hours=10)
    return aest.strftime("%Y-%m-%d")


def cluster_hours(div_hours):
    """Cluster divergence hours with gap <= 3hrs."""
    if not div_hours:
        return []
    clusters = []
    cur = [div_hours[0]]
    for k in range(1, len(div_hours)):
        if div_hours[k] - div_hours[k - 1] <= 3:
            cur.append(div_hours[k])
        else:
            clusters.append(list(cur))
            cur = [div_hours[k]]
    clusters.append(cur)
    return clusters


def detect_fartcoin_events(data):
    """FARTCOIN Analysis: BTC drops, FARTCOIN holds positive -> track FARTCOIN crash."""
    div_hours = []
    for i in range(LB, len(data)):
        bn, bp = data[i].get("btc"), data[i - LB].get("btc")
        fn, fp = data[i].get("fart"), data[i - LB].get("fart")
        if not all([bn, bp, fn, fp]):
            continue
        if (bn - bp) / bp <= -BTC_DROP_THR and (fn - fp) / fp >= 0:
            div_hours.append(i)

    clusters = cluster_hours(div_hours)
    events = []
    used = set()
    used_days = set()

    for cl in clusters:
        ds, de = cl[0], cl[-1]
        if any(i in used for i in cl):
            continue
        day = au_day(data[ds]["t"])
        if day in used_days:
            continue

        peak_f = max(data[i]["fart"] for i in cl if data[i].get("fart"))
        min_f = peak_f
        for j in range(de + 1, min(de + CW + 1, len(data))):
            if data[j].get("fart") is not None:
                if data[j]["fart"] > peak_f:
                    peak_f = data[j]["fart"]
                if data[j]["fart"] < min_f:
                    min_f = data[j]["fart"]

        crash_pct = (peak_f - min_f) / peak_f if peak_f else 0
        confirmed = crash_pct >= FART_CRASH_THR

        events.append({
            "type": "fartcoin",
            "time": data[ds]["t"],
            "day": day,
            "btc_price": data[ds]["btc"],
            "alt_price": data[ds]["fart"],
            "crash_pct": round(crash_pct * 100, 1),
            "confirmed": confirmed,
        })
        used_days.add(day)
        if confirmed:
            used.update(cl)

    return events


def detect_inverse_fartcoin_events(data):
    """Inverse FARTCOIN: FARTCOIN drops, BTC holds positive -> track FARTCOIN pump."""
    div_hours = []
    for i in range(LB, len(data)):
        bn, bp = data[i].get("btc"), data[i - LB].get("btc")
        fn, fp = data[i].get("fart"), data[i - LB].get("fart")
        if not all([bn, bp, fn, fp]):
            continue
        if (fn - fp) / fp <= -FART_DROP_THR and (bn - bp) / bp >= 0:
            div_hours.append(i)

    clusters = cluster_hours(div_hours)
    events = []
    used = set()
    used_days = set()

    for cl in clusters:
        ds, de = cl[0], cl[-1]
        if any(i in used for i in cl):
            continue
        day = au_day(data[ds]["t"])
        if day in used_days:
            continue

        start_f = data[ds]["fart"]
        peak_f = start_f
        for j in range(de + 1, min(de + CW + 1, len(data))):
            if data[j].get("fart") is not None and data[j]["fart"] > peak_f:
                peak_f = data[j]["fart"]

        pump_pct = (peak_f - start_f) / start_f if start_f else 0
        confirmed = pump_pct >= FART_PUMP_THR

        events.append({
            "type": "inverse_fartcoin",
            "time": data[ds]["t"],
            "day": day,
            "btc_price": data[ds]["btc"],
            "alt_price": data[ds]["fart"],
            "pump_pct": round(pump_pct * 100, 1),
            "confirmed": confirmed,
        })
        used_days.add(day)
        if confirmed:
            used.update(cl)

    return events


def detect_spx_events(data):
    """SPX Analysis: BTC drops, SPX6900 holds positive -> track SPX6900 crash."""
    div_hours = []
    for i in range(LB, len(data)):
        bn, bp = data[i].get("btc"), data[i - LB].get("btc")
        sn, sp = data[i].get("spx"), data[i - LB].get("spx")
        if not all([bn, bp, sn, sp]):
            continue
        if (bn - bp) / bp <= -BTC_DROP_THR and (sn - sp) / sp >= 0:
            div_hours.append(i)

    clusters = cluster_hours(div_hours)
    events = []
    used = set()
    used_days = set()

    for cl in clusters:
        ds, de = cl[0], cl[-1]
        if any(i in used for i in cl):
            continue
        day = au_day(data[ds]["t"])
        if day in used_days:
            continue

        peak_s = max(data[i]["spx"] for i in cl if data[i].get("spx"))
        min_s = peak_s
        for j in range(de + 1, min(de + CW + 1, len(data))):
            if data[j].get("spx") is not None:
                if data[j]["spx"] > peak_s:
                    peak_s = data[j]["spx"]
                if data[j]["spx"] < min_s:
                    min_s = data[j]["spx"]

        crash_pct = (peak_s - min_s) / peak_s if peak_s else 0
        confirmed = crash_pct >= SPX_CRASH_THR

        events.append({
            "type": "spx",
            "time": data[ds]["t"],
            "day": day,
            "btc_price": data[ds]["btc"],
            "alt_price": data[ds]["spx"],
            "crash_pct": round(crash_pct * 100, 1),
            "confirmed": confirmed,
        })
        used_days.add(day)
        if confirmed:
            used.update(cl)

    return events


def detect_bfspx_events(fart_data, spx_data):
    """BTC/FC -> SPX: BTC drops + FARTCOIN holds (same as FARTCOIN analysis), then track SPX6900 crash."""
    # Build merged dataset: match fartcoin timestamps with spx data
    spx_lookup = {h["t"]: h["spx"] for h in spx_data if h.get("spx")}

    # Use fart_data as base, add spx prices
    merged = []
    for h in fart_data:
        spx_price = spx_lookup.get(h["t"])
        if spx_price is not None:
            merged.append({"t": h["t"], "btc": h["btc"], "fart": h.get("fart"), "spx": spx_price})

    div_hours = []
    for i in range(LB, len(merged)):
        bn, bp = merged[i].get("btc"), merged[i - LB].get("btc")
        fn, fp = merged[i].get("fart"), merged[i - LB].get("fart")
        if not all([bn, bp, fn, fp]):
            continue
        if (bn - bp) / bp <= -BFSPX_BTC_THR and (fn - fp) / fp >= 0:
            div_hours.append(i)

    clusters = cluster_hours(div_hours)
    events = []
    used = set()
    used_days = set()

    for cl in clusters:
        ds, de = cl[0], cl[-1]
        if any(i in used for i in cl):
            continue
        day = au_day(merged[ds]["t"])
        if day in used_days:
            continue

        peak_s = merged[ds].get("spx", 0)
        min_s = peak_s
        for j in range(de + 1, min(de + CW + 1, len(merged))):
            if merged[j].get("spx") is not None:
                if merged[j]["spx"] > peak_s:
                    peak_s = merged[j]["spx"]
                if merged[j]["spx"] < min_s:
                    min_s = merged[j]["spx"]

        crash_pct = (peak_s - min_s) / peak_s if peak_s else 0
        confirmed = crash_pct >= BFSPX_CRASH_THR

        events.append({
            "type": "bfspx",
            "time": merged[ds]["t"],
            "day": day,
            "btc_price": merged[ds]["btc"],
            "fart_price": merged[ds].get("fart"),
            "spx_price": merged[ds].get("spx"),
            "crash_pct": round(crash_pct * 100, 1),
            "confirmed": confirmed,
        })
        used_days.add(day)
        if confirmed:
            used.update(cl)

    return events


def format_slack_message(new_events):
    """Format a Slack message payload with blocks."""
    lines = []
    for e in new_events:
        etype = {
            "fartcoin": "FARTCOIN Analysis",
            "inverse_fartcoin": "Inverse FARTCOIN",
            "spx": "SPX6900 Analysis",
            "bfspx": "BTC/FC -> SPX",
        }.get(e["type"], e["type"])

        time_str = datetime.fromisoformat(e["time"].replace("Z", "+00:00")).strftime("%d %b %Y %H:%M UTC")

        if e["type"] == "inverse_fartcoin":
            result = f"Pump: {e.get('pump_pct', 0)}%" if e.get("confirmed") else f"Pending ({e.get('pump_pct', 0)}%)"
        else:
            result = f"Crash: {e.get('crash_pct', 0)}%" if e.get("confirmed") else f"Pending ({e.get('crash_pct', 0)}%)"

        status = ":rotating_light: CONFIRMED" if e.get("confirmed") else ":warning: DETECTED"
        lines.append(f"*[{status}] {etype}* — {time_str}\n>BTC: ${e.get('btc_price', 0):,.0f} | Result: {result}")

    body = "\n\n".join(lines)
    detected_at = datetime.now(tz=timezone.utc).strftime("%d %b %Y %H:%M UTC")

    payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": ":chart_with_upwards_trend: RATIO Dashboard Alert", "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": body}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Detected at {detected_at}"}]},
        ]
    }
    return payload


def send_slack_alert(new_events):
    """Send alert to Slack via Incoming Webhook."""
    if not SLACK_WEBHOOK_URL:
        print("  SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return False

    payload = format_slack_message(new_events)

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        if resp.status_code == 200:
            print(f"  Slack alert sent successfully ({len(new_events)} events)")
            return True
        else:
            print(f"  Slack alert failed: {resp.status_code} — {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  Slack alert error: {e}")
        return False


def main():
    print("=== Divergence Event Detection ===\n")

    # Load data
    fart_data = load_json(FARTCOIN_FILE)
    spx_data = load_json(SPX6900_FILE)
    print(f"Loaded {len(fart_data)} fartcoin hours, {len(spx_data)} spx hours\n")

    if not fart_data and not spx_data:
        print("No data available — exiting")
        sys.exit(0)

    # Load previous events
    last = load_last_events()

    # Detect events across all 4 analysis types
    all_events = []

    print("Running FARTCOIN Analysis...")
    fart_events = detect_fartcoin_events(fart_data)
    print(f"  Found {len(fart_events)} total events")
    all_events.extend(fart_events)

    print("Running Inverse FARTCOIN Analysis...")
    ifart_events = detect_inverse_fartcoin_events(fart_data)
    print(f"  Found {len(ifart_events)} total events")
    all_events.extend(ifart_events)

    print("Running SPX6900 Analysis...")
    spx_events = detect_spx_events(spx_data)
    print(f"  Found {len(spx_events)} total events")
    all_events.extend(spx_events)

    print("Running BTC/FC -> SPX Analysis...")
    bfspx_events = detect_bfspx_events(fart_data, spx_data)
    print(f"  Found {len(bfspx_events)} total events")
    all_events.extend(bfspx_events)

    # Build current event keys (type + day is unique per one-event-per-day rule)
    current_keys = {f"{e['type']}:{e['day']}" for e in all_events}
    previous_keys = set()
    for etype in last:
        for key in last[etype]:
            previous_keys.add(key)

    # Find new events
    new_keys = current_keys - previous_keys
    new_events = [e for e in all_events if f"{e['type']}:{e['day']}" in new_keys]

    print(f"\nTotal events across all analyses: {len(all_events)}")
    print(f"Previously known: {len(previous_keys)}")
    print(f"New events: {len(new_events)}")

    if new_events:
        print("\n--- New Events ---")
        for e in new_events:
            print(f"  [{e['type']}] {e['time']} — {'CONFIRMED' if e['confirmed'] else 'DETECTED'}")

        # Send Slack alert
        print("\nSending Slack alert...")
        send_slack_alert(new_events)
    else:
        print("\nNo new events — no alert needed")

    # Update last_events.json
    updated = {
        "fartcoin": [f"fartcoin:{e['day']}" for e in fart_events],
        "inverse_fartcoin": [f"inverse_fartcoin:{e['day']}" for e in ifart_events],
        "spx": [f"spx:{e['day']}" for e in spx_events],
        "bfspx": [f"bfspx:{e['day']}" for e in bfspx_events],
    }
    save_last_events(updated)
    print(f"\nSaved {sum(len(v) for v in updated.values())} event keys to {LAST_EVENTS_FILE}")
    print("Done!")


if __name__ == "__main__":
    main()
