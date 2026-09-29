#!/usr/bin/env python3
"""
backfill_ai_hardware.py — One-off: rebuild weekly history for ai_hardware_prices.json
from Internet Archive (Wayback Machine) snapshots of the same public price pages.

For each ISO week since START, takes the latest archived snapshot of:
  memory: dramexchange.com homepage, trendforce.com DRAM + NAND price pages
  GPUs:   runpod.io/pricing (embedded gpuTypes JSON), Lambda pricing pages
and parses it with the same parsers as the live collector. Weeks with no
snapshot are left empty (no interpolation). Weeks already captured live are
never overwritten. Downloads are cached in .wayback_cache/ so reruns are cheap.

    python backfill_ai_hardware.py
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import ai_hardware_prices as aihp

START = "20210901"
CACHE = Path(".wayback_cache")
CDX = "http://web.archive.org/cdx/search/cdx"

SOURCES = {
    "dx":       (["dramexchange.com/"], "memory"),
    "tf_dram":  (["trendforce.com/price/dram/dram_spot"], "memory"),
    "tf_flash": (["trendforce.com/price/flash/flash_spot"], "memory"),
    "runpod":   (["runpod.io/pricing", "runpod.io/gpu-instance/pricing"], "gpu"),
    "lambda":   (["lambdalabs.com/service/gpu-cloud", "lambdalabs.com/service/gpu-cloud/pricing",
                  "lambda.ai/pricing", "lambda.ai/service/gpu-cloud"], "gpu"),
}


def get(url, params=None, tries=5):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=90,
                             headers={"User-Agent": "ratio-dashboard-backfill/1.0"})
            if r.status_code == 200:
                return r
            if r.status_code in (429, 502, 503, 504):
                time.sleep(10 * (i + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(10 * (i + 1))
    return None


def week_of(ts):
    d = datetime.strptime(ts[:8], "%Y%m%d")
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


def weekly_snapshots(urls):
    """{week: (timestamp, original_url)} — latest 200-OK snapshot per ISO week."""
    picks = {}
    for u in urls:
        r = get(CDX, {"url": u, "from": START, "output": "json", "fl": "timestamp,original",
                      "filter": "statuscode:200", "collapse": "timestamp:8"})
        rows = r.json()[1:] if r is not None and r.text.strip() else []
        for ts, orig in rows:
            w = week_of(ts)
            if w not in picks or ts > picks[w][0]:
                picks[w] = (ts, orig)
        print(f"  CDX {u}: {len(rows)} daily snapshots")
    return picks


def fetch_snapshot(key, ts, orig):
    CACHE.mkdir(exist_ok=True)
    f = CACHE / f"{key}_{ts}.html"
    if f.exists():
        return f.read_text(encoding="utf8", errors="ignore")
    r = get(f"http://web.archive.org/web/{ts}id_/{orig}")
    if r is None:
        return None
    f.write_text(r.text, encoding="utf8")
    time.sleep(1)
    return r.text


def parse(key, html):
    if key == "dx":
        return aihp.parse_dramexchange_home(html)
    if key.startswith("tf_"):
        return aihp.parse_trendforce(html)
    if key == "runpod":
        return aihp.parse_runpod_page(html)
    if key == "lambda":
        return {m: {"lambda": p} for m, p in aihp.parse_lambda(html).items()}
    return {}


def main():
    data = json.loads(aihp.DATA_FILE.read_text())
    live_weeks = {w["week"] for w in data["weeks"] if not w.get("backfill")}
    by_week = {w["week"]: w for w in data["weeks"]}

    jobs = []
    for key, (urls, _) in SOURCES.items():
        print(f"Listing snapshots for {key}...")
        for week, (ts, orig) in weekly_snapshots(urls).items():
            if week not in live_weeks:
                jobs.append((key, week, ts, orig))
    print(f"{len(jobs)} snapshots to process")

    def work(job):
        key, week, ts, orig = job
        html = fetch_snapshot(key, ts, orig)
        return job, (parse(key, html) if html else {})

    stats = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for n, ((key, week, ts, orig), res) in enumerate(ex.map(work, jobs), 1):
            stats.setdefault(key, [0, 0])[0] += 1
            if not res:
                continue
            stats[key][1] += 1
            w = by_week.setdefault(week, {"week": week, "gpus": {}, "memory": {}, "backfill": True,
                                          "captured_at": f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}T{ts[8:10]}:{ts[10:12]}:00Z",
                                          "snapshots": {}})
            w.setdefault("snapshots", {})[key] = ts
            if SOURCES[key][1] == "memory":
                for title, tbl in res.items():
                    tbl["source_updated"] = tbl.get("source_updated") or f"archived {ts[:4]}-{ts[4:6]}-{ts[6:8]}"
                    # TrendForce pages (titled, incl. contract tables) take precedence over the homepage
                    if key.startswith("tf_") or title not in w["memory"]:
                        w["memory"][title] = tbl
            else:
                for model, vals in res.items():
                    w["gpus"].setdefault(model, {}).update(vals)
            if n % 50 == 0:
                print(f"  processed {n}/{len(jobs)}")

    data["weeks"] = sorted(by_week.values(), key=lambda w: w["week"])
    aihp.DATA_FILE.write_text(json.dumps(data, separators=(",", ":")))
    print("Parsed snapshots per source (fetched, usable):", stats)
    print(f"Saved {len(data['weeks'])} weeks, {data['weeks'][0]['week']} → {data['weeks'][-1]['week']}")


if __name__ == "__main__":
    main()
