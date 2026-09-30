#!/usr/bin/env python3
"""
gpu_prices.py — Weekly NVIDIA H100 / H200 cloud rental prices -> gpu_prices.json

Primary:     GetDeploying GPU price dataset (CC BY 4.0), ON_DEMAND rows,
             `provider_median_price` = median of each provider's cheapest
             on-demand $/GPU-hour, one row per week (Monday). This is the same
             number GetDeploying publishes as its GPU Rental Price Index.
Fallback:    the index series embedded in getdeploying.com/gpu-price-trends
             (identical values — verified 0 mismatches over 53 weeks, Sep 2026).
Cross-check: Silicon Data neo-cloud rental indices (SDH100RT, H200), daily
             readings for the trailing 7 days — independent methodology.

Safety: validates every value (price bounds, provider count, Monday dates,
freshness, week-on-week jump), never deletes stored weeks, never exits
non-zero (no GitHub email spam), and posts ONE Slack warning per day when a
source fails or goes stale so a broken feed is never silent.
"""

import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

DATA_FILE = Path("gpu_prices.json")
WEEKLY_FROM = "2026-08-03"  # first Monday of Aug 2026 — weekly series starts here
CHIPS = {"H100": "nvidia-h100", "H200": "nvidia-h200"}

GD_JSON = "https://getdeploying.com/dataset/gpu-prices/weekly.json"
GD_CSV = "https://getdeploying.com/dataset/gpu-prices/{slug}.csv"
GD_PAGE = "https://getdeploying.com/gpu-price-trends"
SD_PAGE = "https://www.silicondata.com/products/silicon-index/{chip}"

PRICE_MIN, PRICE_MAX = 0.5, 30.0   # $/GPU-hour sanity bounds
MIN_PROVIDERS = 10
MAX_WOW_JUMP = 0.35                # >35% week-on-week move is flagged
STALE_DAYS = 16                    # latest source week older than this = stale
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; RatioDashboard/1.0; weekly GPU price index)"}


def get(url):
    last = None
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001 — retried, then surfaced
            last = e
    raise last


def fetch_primary():
    """{chip: {week: (price, providers)}} from the official dataset (JSON, then per-chip CSV)."""
    out = {c: {} for c in CHIPS}
    try:
        rows = get(GD_JSON).json()["data"]
        for r in rows:
            for chip, slug in CHIPS.items():
                if r.get("gpu_slug") == slug and r.get("billing_type") == "ON_DEMAND":
                    out[chip][r["date"]] = (float(r["provider_median_price"]), int(r["provider_count"]))
        if all(out.values()):
            return out, "getdeploying weekly.json"
    except Exception as e:  # noqa: BLE001
        print(f"  weekly.json failed: {e}")
    import csv
    import io
    out = {c: {} for c in CHIPS}
    for chip, slug in CHIPS.items():
        try:
            for r in csv.DictReader(io.StringIO(get(GD_CSV.format(slug=slug)).text)):
                if r["billing_type"] == "ON_DEMAND":
                    out[chip][r["date"]] = (float(r["provider_median_price"]), int(r["provider_count"]))
        except Exception as e:  # noqa: BLE001
            print(f"  {slug}.csv failed: {e}")
    return out, "getdeploying per-chip CSV"


def fetch_fallback():
    """Same index, parsed from the JSON embedded in the public trends page."""
    html = get(GD_PAGE).text
    out = {}
    for chip, slug in CHIPS.items():
        m = re.search(r'\{"slug": "%s", "name"[^{}]*?"points": (\[[^\]]*\])' % slug, html)
        if not m:
            raise ValueError(f"{slug} series not found on trends page")
        out[chip] = {p["date"]: (float(p["value"]), int(p["providers"])) for p in json.loads(m.group(1))}
    return out, "getdeploying trends page"


def fetch_all_history():
    """Full page index (back to Apr 2025) — used for the yearly averages."""
    return fetch_fallback()[0]


def fetch_crosscheck():
    """{chip: [{date, price}]} Silicon Data neo-cloud daily readings (last 7 days)."""
    out = {}
    for chip in CHIPS:
        html = get(SD_PAGE.format(chip=chip.lower())).text.replace('\\"', '"')
        key = f'"key":"{chip.lower()}-neo"'
        i = html.find(key)
        if i < 0:
            raise ValueError(f"Silicon Data card {chip.lower()}-neo not found")
        m = re.search(r'"points":(\[[^\]]*\])', html[i:i + 4000])
        pts = json.loads(m.group(1))
        pts = [p for p in pts if PRICE_MIN <= float(p["price"]) <= PRICE_MAX]
        if not pts:
            raise ValueError(f"Silicon Data {chip}: no valid readings")
        out[chip] = [{"date": p["date"], "price": round(float(p["price"]), 4)} for p in pts]
    return out


def validate(series, warnings):
    """Drop invalid points; return cleaned {chip: {week: (price, providers)}}."""
    clean = {}
    for chip, pts in series.items():
        clean[chip] = {}
        for wk, (price, prov) in sorted(pts.items()):
            try:
                is_monday = date.fromisoformat(wk).weekday() == 0
            except ValueError:
                is_monday = False
            if not is_monday:
                warnings.append(f"{chip} {wk}: not a Monday week label — skipped")
            elif not PRICE_MIN <= price <= PRICE_MAX:
                warnings.append(f"{chip} {wk}: price ${price} outside sanity bounds — skipped")
            elif prov < MIN_PROVIDERS:
                warnings.append(f"{chip} {wk}: only {prov} providers — skipped")
            else:
                clean[chip][wk] = (price, prov)
    return clean


def yearly_points(history):
    """Yearly representative prices. 2025 & 2026 (Jan-Jul) are averages of the
    same weekly index; earlier years are sourced reference figures."""
    def avg(chip, lo, hi):
        v = [p for wk, (p, _) in history[chip].items() if lo <= wk < hi]
        return (round(sum(v) / len(v), 2), len(v)) if v else (None, 0)

    h25, n25 = avg("H100", "2025-01-01", "2026-01-01")
    h26, n26 = avg("H100", "2026-01-01", "2026-08-01")
    g25, m25 = avg("H200", "2025-01-01", "2026-01-01")
    g26, m26 = avg("H200", "2026-01-01", "2026-08-01")
    gd = {"source": "GetDeploying GPU Rental Price Index", "url": GD_PAGE, "comparable": True}
    sd_hist = "https://www.silicondata.com/blog/h100-rental-price-over-time"
    return {
        "H100": [
            {"year": "2022", "price": None, "note": "No rental market yet — H100 began shipping late 2022; cloud availability started 2023."},
            {"year": "2023", "price": 7.76, "comparable": False,
             "basis": "Hyperscaler median, Aug–Dec 2023 (only segment with data — scarcity pricing)",
             "source": "Silicon Data H100 rental price history", "url": sd_hist},
            {"year": "2024", "price": 2.99, "comparable": False,
             "basis": "Neo-cloud median, Jun–Dec 2024",
             "source": "Silicon Data H100 rental price history", "url": sd_hist},
            {"year": "2025", "price": h25, **gd, "basis": f"Average of {n25} weekly medians, Apr–Dec 2025"},
            {"year": "2026 Jan–Jul", "price": h26, **gd, "basis": f"Average of {n26} weekly medians, Jan–Jul 2026"},
        ],
        "H200": [
            {"year": "2022", "price": None, "note": "Not released (H200 announced Nov 2023)."},
            {"year": "2023", "price": None, "note": "Not released until 2024."},
            {"year": "2024", "price": None, "note": "Limited cloud availability from late 2024 — no reliable public price series."},
            {"year": "2025", "price": g25, **gd, "basis": f"Average of {m25} weekly medians, Apr–Dec 2025 (5–24 providers)"},
            {"year": "2026 Jan–Jul", "price": g26, **gd, "basis": f"Average of {m26} weekly medians, Jan–Jul 2026"},
        ],
    }


def send_slack(text):
    hook = os.environ.get("SLACK_WEBHOOK_URL")
    if not hook:
        print("  (no SLACK_WEBHOOK_URL) would warn:", text)
        return
    try:
        requests.post(hook, json={"text": text}, timeout=10)
    except Exception as e:  # noqa: BLE001
        print(f"  Slack warning failed: {e}")


def main():
    now = datetime.now(timezone.utc)
    print(f"=== GPU prices — {now:%Y-%m-%d %H:%M} UTC ===")
    data = json.loads(DATA_FILE.read_text()) if DATA_FILE.exists() else {}
    weekly = {w["week"]: w for w in data.get("weekly", [])}
    warnings, problems = [], []

    # ── primary weekly series ──
    source = None
    try:
        raw, source = fetch_primary()
        if not all(raw.values()):
            raise ValueError("dataset missing H100 or H200 on-demand rows")
    except Exception as e:  # noqa: BLE001
        print(f"  primary failed: {e} — trying fallback")
        try:
            raw, source = fetch_fallback()
        except Exception as e2:  # noqa: BLE001
            raw = None
            problems.append(f"All GetDeploying sources failed ({e2}); kept existing data.")

    history = None
    if raw:
        clean = validate(raw, warnings)
        latest = min(max(clean[c]) for c in CHIPS if clean[c])
        age = (now.date() - date.fromisoformat(latest)).days
        if age > STALE_DAYS:
            problems.append(f"Latest GetDeploying week is {latest} ({age} days old) — source may have stopped updating.")
        for chip in CHIPS:
            for wk, (price, prov) in clean[chip].items():
                if wk < WEEKLY_FROM:
                    continue
                row = weekly.setdefault(wk, {"week": wk})
                old = row.get(chip)
                if old and abs(old - price) / old > 0.01:
                    warnings.append(f"{chip} {wk}: source revised ${old} -> ${price}")
                row[chip] = round(price, 4)
                row[f"{chip}_providers"] = prov
        # week-on-week jump check on the merged series
        weeks = sorted(weekly)
        for chip in CHIPS:
            vals = [(w, weekly[w].get(chip)) for w in weeks if weekly[w].get(chip)]
            for (w0, a), (w1, b) in zip(vals, vals[1:]):
                if abs(b - a) / a > MAX_WOW_JUMP:
                    problems.append(f"{chip} moved {(b - a) / a:+.0%} from {w0} to {w1} — check source.")
        # Yearly averages: page series reaches back to Apr 2025 but skips a few
        # weeks; the dataset only holds the last 53 weeks but is complete — merge.
        try:
            history = fetch_all_history()
            for chip in CHIPS:
                history[chip].update(raw[chip])
        except Exception as e:  # noqa: BLE001
            warnings.append(f"Yearly averages not refreshed (trends page failed: {e}).")

    # ── independent cross-check ──
    cross = data.get("crosscheck", {"H100": [], "H200": []})
    cross_ok = True
    try:
        for chip, pts in fetch_crosscheck().items():
            merged = {p["date"]: p for p in cross.get(chip, [])}
            merged.update({p["date"]: p for p in pts})
            cross[chip] = sorted(merged.values(), key=lambda p: p["date"])
    except Exception as e:  # noqa: BLE001
        cross_ok = False
        warnings.append(f"Silicon Data cross-check failed: {e}")

    out = {
        "last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "unit": "USD per GPU-hour, on-demand cloud rental",
        "primary_source": {
            "name": "GetDeploying GPU Rental Price Index (CC BY 4.0)",
            "url": "https://getdeploying.com/dataset/gpu-prices",
            "method": "Weekly median of each provider's cheapest on-demand price per GPU-hour",
        },
        "crosscheck_source": {
            "name": "Silicon Data rental price index — neo-cloud segment",
            "url": "https://www.silicondata.com/products/silicon-index/h100",
        },
        "yearly": yearly_points(history) if history else data.get("yearly"),
        "weekly": [weekly[w] for w in sorted(weekly)],
        "latest_week": max(weekly) if weekly else None,
        "crosscheck": cross,
        "health": {
            "last_run": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "primary": source or "failed",
            "crosscheck": "ok" if cross_ok else "failed",
            "problems": problems,
            "warnings": warnings[-20:],
            "last_alert_date": data.get("health", {}).get("last_alert_date"),
        },
    }
    if problems or not cross_ok:
        today = now.strftime("%Y-%m-%d")
        if out["health"]["last_alert_date"] != today:
            msgs = problems + ([] if cross_ok else ["Silicon Data cross-check unavailable."])
            send_slack(":warning: *GPU price tracker needs attention*\n• " + "\n• ".join(msgs))
            out["health"]["last_alert_date"] = today

    DATA_FILE.write_text(json.dumps(out, indent=1) + "\n")
    print(f"  source: {source} · weeks stored: {len(weekly)} · latest: {out['latest_week']}")
    for w in out["weekly"][-3:]:
        print("  ", w)
    for p in problems + warnings:
        print("  !", p)


if __name__ == "__main__":
    main()
