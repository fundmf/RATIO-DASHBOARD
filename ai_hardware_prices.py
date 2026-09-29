#!/usr/bin/env python3
"""
ai_hardware_prices.py — Weekly AI-chip rental prices + DRAM/NAND spot prices.

Writes ai_hardware_prices.json:
  {last_updated, weeks: [{week (Monday, UTC), captured_at,
                          gpus:   {model: {runpod_secure, runpod_community, vast_median, vast_n}},
                          memory: {table_title: {"source_updated": str, "items": {item: avg_usd}}}}]}

Runs daily; the current week's entry is overwritten each run so it always holds
the latest price for that week. Each source fails independently (soft-exit).

Sources (all public, no key):
  - RunPod GraphQL gpuTypes  — list $/GPU-hr (secure = datacenter, community)
  - vast.ai bundles API      — marketplace on-demand offers → median $/GPU-hr
  - TrendForce price pages   — DRAM / GDDR / LPDDR / NAND spot + contract averages (USD)
"""

import json
import re
import statistics
import urllib.parse
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path

import requests

DATA_FILE = Path("ai_hardware_prices.json")
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")

# Canonical model -> (RunPod gpuType id, [vast.ai gpu_name values])
GPU_MODELS = {
    "A100 PCIe 80GB":  ("NVIDIA A100 80GB PCIe", ["A100 PCIE"]),
    "A100 SXM 80GB":   ("NVIDIA A100-SXM4-80GB", ["A100 SXM4"]),
    "H100 PCIe":       ("NVIDIA H100 PCIe", ["H100 PCIE"]),
    "H100 SXM":        ("NVIDIA H100 80GB HBM3", ["H100 SXM"]),
    "H100 NVL":        ("NVIDIA H100 NVL", ["H100 NVL"]),
    "H200 SXM":        ("NVIDIA H200", ["H200"]),
    "H200 NVL":        ("NVIDIA H200 NVL", ["H200 NVL"]),
    "B200":            ("NVIDIA B200", ["B200"]),
    "B300":            ("NVIDIA B300 SXM6 AC", ["B300"]),
    "MI300X":          ("AMD Instinct MI300X OAM", ["MI300X"]),
}

TRENDFORCE_PAGES = [
    "https://www.trendforce.com/price/dram/dram_spot",
    "https://www.trendforce.com/price/flash/flash_spot",
]


def http_get(url, **kw):
    headers = {"User-Agent": BROWSER_UA, "Accept": "text/html,application/json,*/*",
               "Accept-Language": "en-US,en;q=0.9"}
    try:
        from curl_cffi import requests as cffi
        return cffi.get(url, headers=headers, impersonate="chrome", timeout=30, **kw)
    except ImportError:
        return requests.get(url, headers=headers, timeout=30, **kw)


def fetch_runpod():
    q = {"query": "query { gpuTypes { id securePrice communityPrice } }"}
    r = requests.post("https://api.runpod.io/graphql", json=q, timeout=20)
    r.raise_for_status()
    by_id = {g["id"]: g for g in r.json()["data"]["gpuTypes"]}
    out = {}
    for model, (rp_id, _) in GPU_MODELS.items():
        g = by_id.get(rp_id)
        if not g:
            continue
        # RunPod reports 0 (and a 0.5 placeholder on some cards) when a tier has no stock.
        sec = g.get("securePrice") or None
        com = g.get("communityPrice") or None
        if com is not None and sec is not None and com < sec * 0.3:
            com = None
        out[model] = {"runpod_secure": sec, "runpod_community": com}
    return out


def fetch_vast():
    out = {}
    for model, (_, names) in GPU_MODELS.items():
        prices = []
        for name in names:
            q = {"gpu_name": {"eq": name}, "rentable": {"eq": True},
                 "type": "on-demand", "limit": 500}
            url = "https://console.vast.ai/api/v0/bundles/?q=" + urllib.parse.quote(json.dumps(q))
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for o in r.json().get("offers", []):
                n = max(o.get("num_gpus") or 1, 1)
                if o.get("dph_total"):
                    prices.append(o["dph_total"] / n)
        if prices:
            out[model] = {"vast_median": round(statistics.median(prices), 3), "vast_n": len(prices)}
    return out


def clean(s):
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def parse_trendforce(html):
    """Return {table_title: {"source_updated": str, "items": {item: avg}}}.
    Uses the 'Session Average' column (spot) or 'Average' column (contract)."""
    tables = {}
    starts = [m.start() for m in re.finditer(r"<table", html)]
    prev_end = 0
    for i, start in enumerate(starts):
        end = html.find("</table>", start)
        pre = html[prev_end:start]
        prev_end = end
        titles = re.findall(r'<[^>]+class="[^"]*(?:title|name|head)[^"]*"[^>]*>(.*?)</', pre, re.S)
        titles = [clean(t) for t in titles if clean(t)]
        if not titles:
            continue
        title = titles[-1]
        upd = re.findall(r"Last Update\s*([0-9]{4}-[0-9]{2}-[0-9]{2}[^<]{0,20})", pre)
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html[start:end], re.S)
        header, items = None, {}
        for row in rows:
            cells = [clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
            if not cells:
                continue
            if cells[0].lower() == "item":
                header = [c.lower() for c in cells]
                continue
            if not header:
                continue
            col = next((header.index(h) for h in ("session average", "average") if h in header), None)
            if col is None or col >= len(cells):
                continue
            try:
                items[cells[0]] = float(cells[col].replace(",", ""))
            except ValueError:
                continue
        if items:
            tables[title] = {"source_updated": clean(upd[-1]) if upd else "", "items": items}
    return tables


def fetch_trendforce():
    out = {}
    for url in TRENDFORCE_PAGES:
        r = http_get(url)
        r.raise_for_status()
        out.update(parse_trendforce(r.text))
    return out


def main():
    now = datetime.now(timezone.utc)
    week = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    print(f"=== AI hardware prices — {now:%Y-%m-%d %H:%M} UTC (week of {week}) ===")

    data = json.loads(DATA_FILE.read_text()) if DATA_FILE.exists() else {"weeks": []}
    prev = next((w for w in data["weeks"] if w["week"] == week), {})
    gpus = dict(prev.get("gpus", {}))
    memory = dict(prev.get("memory", {}))
    got_any = False

    for label, fn in (("RunPod", fetch_runpod), ("vast.ai", fetch_vast)):
        try:
            res = fn()
            for model, vals in res.items():
                gpus.setdefault(model, {}).update(vals)
            print(f"  {label}: {len(res)} models")
            got_any = got_any or bool(res)
        except Exception as e:
            print(f"  WARNING: {label} failed (keeping last value this week): {e}")

    try:
        res = fetch_trendforce()
        memory.update(res)
        print(f"  TrendForce: {len(res)} tables, {sum(len(t['items']) for t in res.values())} items")
        got_any = got_any or bool(res)
    except Exception as e:
        print(f"  WARNING: TrendForce failed (keeping last value this week): {e}")

    if not got_any:
        print("No source returned data — nothing written.")
        return

    entry = {"week": week, "captured_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
             "gpus": gpus, "memory": memory}
    data["weeks"] = sorted([w for w in data["weeks"] if w["week"] != week] + [entry],
                           key=lambda w: w["week"])
    data["last_updated"] = entry["captured_at"]
    DATA_FILE.write_text(json.dumps(data, separators=(",", ":")))
    print(f"Saved {len(data['weeks'])} week(s) to {DATA_FILE}")


if __name__ == "__main__":
    main()
