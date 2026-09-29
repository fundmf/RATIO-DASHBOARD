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
  - GetDeploying weekly CSV  — market-wide median $/GPU-hr (main chips), weeks[].market
  - Gatewell / Compute Exchange — dealer purchase prices, weeks[].purchase
  - Lambda pricing page      — list $/GPU-hr
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
    "A100 40GB":       ("NVIDIA A100-SXM4-40GB", []),
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
        if sec == 0.5:  # RunPod placeholder when a tier has no stock
            sec = None
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


def norm_title(t):
    # "DRAM Contract Price (2H Jul)" -> "DRAM Contract Price" so the series stays continuous
    return re.sub(r"\s*\([^)]*\)\s*$", "", t).strip()


def norm_item(name):
    # Older DRAMeXchange labels: "DDR3  4Gb 512Mx8 1600/1866Mbps", "DDR4 8G (1G*8) 2400 Mbps"
    n = re.sub(r"\s+", " ", name.replace("-->", "")).strip()
    n = re.sub(r"\s?Mbps$", "", n)
    n = re.sub(r"\)(?=\d)", ") ", n)
    n = n.replace("*", "x")
    n = re.sub(r"\b(\d+)G \(", r"\1Gb (", n)
    return n


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
        title = norm_title(titles[-1])
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
                items[norm_item(cells[0])] = float(cells[col].replace(",", ""))
            except ValueError:
                continue
        if items:
            tables[title] = {"source_updated": clean(upd[-1]) if upd else "", "items": items}
    return tables


def canon_gpu(name, mem_gb=None):
    """Map a provider's GPU label to our canonical model key (None = not tracked)."""
    n = name.upper()
    if "GH200" in n:
        return "GH200"
    for m in ("B300", "B200", "H200", "H100", "A100", "MI300X"):
        if m in n:
            break
    else:
        return None
    if m == "A100":
        if mem_gb == 40 or "40GB" in n.replace(" ", ""):
            return "A100 40GB"
        return "A100 PCIe 80GB" if "PCIE" in n else "A100 SXM 80GB"
    if m == "H100":
        return "H100 PCIe" if "PCIE" in n else "H100 NVL" if "NVL" in n else "H100 SXM"
    if m == "H200":
        return "H200 NVL" if "NVL" in n else "H200 SXM"
    return m


def page_text(html):
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", t)))


def parse_lambda(html):
    """Lambda on-demand list price per GPU-hour -> {model: lowest $/GPU-hr}.
    Handles old '1x NVIDIA A100 40 GB ... $1.10 / hr' (per instance),
    '8x NVIDIA H100 SXM 80 GB ... $2.99 / GPU / hr', and the 2025+ table with a
    PRICE/GPU/HR column. Reserved/1-Click cluster rows ('2 weeks – 1 year') are ignored."""
    t = page_text(html)
    found = {}

    def add(label, gb, price):
        m = canon_gpu(label, int(gb))
        if m and 0.2 < price < 50:
            found[m] = min(found.get(m, price), round(price, 4))

    for n, label, gb, price, per_gpu in re.findall(
            r"(\d+)x NVIDIA ([A-Za-z0-9 ]+?) (\d+) GB(?:(?!NVIDIA|CONTACT)[^$]){0,160}?\$\s?([\d.]+) / (GPU / )?hr", t):
        add(label, gb, float(price) if per_gpu else float(price) / int(n))
    if "PRICE/GPU/HR" in t.upper():
        for label, gb, price in re.findall(
                r"NVIDIA ((?:[A-Z]+\d+[A-Za-z0-9]*)(?: (?:SXM\d?|PCIe|NVL))?) (\d+) GB (?:(?!NVIDIA)[^$]){0,120}?\$\s?([\d.]+)", t):
            add(label, gb, float(price))
    return found


def parse_runpod_page(html):
    """Archived runpod.io/pricing pages embed gpuTypes JSON (same fields as the API)."""
    h = html.replace('\\"', '"')
    out = {}
    for m in re.finditer(r'"id":"((?:NVIDIA|AMD)[^"]+)"', h):
        seg = h[m.end():m.end() + 1500]
        nxt = seg.find('"id":"')
        seg = seg if nxt < 0 else seg[:nxt]
        sp = re.search(r'"securePrice":([\d.]+)', seg)
        cp = re.search(r'"communityPrice":([\d.]+)', seg)
        if not sp:
            continue
        model = next((k for k, (rp, _) in GPU_MODELS.items() if rp == m.group(1)), None)
        if not model:
            continue
        sec = float(sp.group(1)) or None
        com = float(cp.group(1)) if cp else None
        if sec == 0.5:
            sec = None
        if not com or (sec and com < sec * 0.3):
            com = None
        out[model] = {"runpod_secure": sec, "runpod_community": com}
    return out


def classify_dx_table(first_item):
    i = first_item.upper()
    if "DIMM" in i:
        return "Module Spot Price"
    if i.startswith("GDDR"):
        return "GDDR Spot Price"
    if i.startswith("LPDDR"):
        return "LPDDR Spot Price"
    if i.startswith("DDR"):
        return "DRAM Spot Price"
    if i.startswith(("SLC", "MLC")):
        return "NAND Flash Spot Price"
    if re.match(r"^\d+GB (TLC|QLC)", i):
        return "Wafer Spot Price"
    if i.startswith("MICROSD"):
        return "Memory Card Spot Price"
    return None


def parse_dramexchange_home(html):
    """dramexchange.com homepage: untitled 'Item' tables -> titled like TrendForce pages."""
    out = {}
    for tbl in re.findall(r"<table.*?</table>", html, re.S):
        header, items = None, {}
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S):
            cells = [clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
            if not cells:
                continue
            if cells[0].lower() == "item":
                header = [c.lower() for c in cells]
                continue
            if not header or "session average" not in header:
                continue
            col = header.index("session average")
            try:
                items[norm_item(cells[0])] = float(cells[col].replace(",", ""))
            except (ValueError, IndexError):
                continue
        if items:
            title = classify_dx_table(next(iter(items)))
            if title and title not in out:
                out[title] = {"source_updated": "", "items": items}
    return out


def fetch_lambda():
    r = http_get("https://lambda.ai/pricing")
    r.raise_for_status()
    return {m: {"lambda": p} for m, p in parse_lambda(r.text).items()}


# getdeploying.com weekly dataset (CC BY 4.0): market-wide median across ~87 providers
GD_CSV = "https://getdeploying.com/dataset/gpu-prices/weekly.csv"
# Main chips only — the others (GB200/GB300, GH200, MI3xx, Gaudi) have 1-4 providers, too thin for a median.
GD_FAMILIES = {
    "nvidia-a100": "A100", "nvidia-h100": "H100", "nvidia-h200": "H200",
    "nvidia-b200": "B200", "nvidia-b300": "B300",
}


def _parse_gd_rows(text, out):
    import csv, io
    for row in csv.DictReader(io.StringIO(text)):
        fam = GD_FAMILIES.get(row.get("gpu_slug"))
        if not fam or row.get("billing_type") != "ON_DEMAND" or not row.get("median_price"):
            continue
        d = datetime.strptime(row["date"], "%Y-%m-%d")
        week = (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")
        out.setdefault(week, {})[fam] = {
            "median": round(float(row["median_price"]), 4),
            "providers": int(row["provider_count"] or 0),
            "offerings": int(row["offering_count"] or 0),
            "min": float(row["min_price"]) if row["min_price"] else None,
            "max": float(row["max_price"]) if row["max_price"] else None,
        }


def fetch_market():
    """{week: {family: {median, providers, offerings, min, max}}} — on-demand, all weeks in the dataset.
    Falls back to the per-chip CSV files if the combined file fails."""
    out = {}
    try:
        r = requests.get(GD_CSV, headers={"User-Agent": BROWSER_UA}, timeout=60)
        r.raise_for_status()
        _parse_gd_rows(r.text, out)
    except Exception as e:
        print(f"  getdeploying weekly.csv failed ({e}) — trying per-chip files")
    if len({f for w in out.values() for f in w}) < len(GD_FAMILIES):
        for slug in GD_FAMILIES:
            try:
                r = requests.get(GD_CSV.replace("weekly.csv", f"{slug}.csv"),
                                 headers={"User-Agent": BROWSER_UA}, timeout=60)
                r.raise_for_status()
                _parse_gd_rows(r.text, out)
            except Exception as e:
                print(f"  getdeploying {slug}.csv failed: {e}")
    return out


GATEWELL_URL = "https://gatewellusa.com/ai-compute/datacenter-gpu-prices"
CE_BASE = "https://compute.exchange/hardware-market/"
CE_PAGES = {  # canonical model -> slug suffix
    "A100 40GB": "a100-40gb", "A100 SXM 80GB": "a100-80gb", "H100 PCIe": "h100-pcie",
    "H100 SXM": "h100-sxm5", "H200 SXM": "h200",
}


def purchase_model(label):
    n = label.upper()
    for k in ("MI355X", "MI325X"):
        if k in n:
            return k
    if "GAUDI 3" in n:
        return "Gaudi 3"
    if "RTX" in n:
        return None
    return canon_gpu(label)


def parse_gatewell(html):
    """New-unit dealer quotes per GPU: {model: {new_quote (20-49 units), new_quote_bulk (75-99)}}."""
    out = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        cells = [clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if len(cells) < 4 or not cells[1].startswith("$"):
            continue
        model = purchase_model(cells[0])
        if not model:
            continue
        prices = [float(c.replace("$", "").replace(",", "")) for c in cells[1:4] if re.match(r"^\$[\d,]+$", c)]
        if len(prices) == 3:
            out[model] = {"new_quote": prices[0], "new_quote_bulk": prices[2]}
    return out


def parse_ce(html, kind):
    """compute.exchange indicative range -> {kind_low, kind_high, ce_period}."""
    t = page_text(html)
    m = re.search(r"Indicative Range \(([^)]+)\) \$([\d,]+) – \$([\d,]+)", t)
    if not m:
        return {}
    return {f"{kind}_low": float(m.group(2).replace(",", "")),
            f"{kind}_high": float(m.group(3).replace(",", "")),
            "ce_period": m.group(1)}


def fetch_gatewell():
    r = http_get(GATEWELL_URL)
    r.raise_for_status()
    return parse_gatewell(r.text)


def fetch_compute_exchange():
    out = {}
    for model, slug in CE_PAGES.items():
        for kind, path in (("used", f"used-gpus/used-{slug}"), ("refurb", f"refurbished-gpus/refurbished-{slug}")):
            try:
                r = with_retries(lambda: http_get(CE_BASE + path), tries=2, wait=10)
                if r.status_code == 200:
                    v = parse_ce(r.text, kind)
                    if v:
                        out.setdefault(model, {}).update(v)
            except Exception as e:
                print(f"  WARNING: compute.exchange {path}: {e}")
    return out


def fetch_trendforce():
    out = {}
    for url in TRENDFORCE_PAGES:
        r = http_get(url)
        r.raise_for_status()
        out.update(parse_trendforce(r.text))
    return out


def fetch_dramexchange():
    """Backup for memory: dramexchange.com homepage shows the same TrendForce spot tables."""
    r = http_get("https://www.dramexchange.com/")
    r.raise_for_status()
    return parse_dramexchange_home(r.text)


# ─── Safeguards ────────────────────────────────────────────────────────────
import os
import time

STALE_DAYS = 8          # Slack warning if a source hasn't updated for this long
ALERT_EVERY_DAYS = 3    # don't repeat the same warning more often than this


def with_retries(fn, tries=3, wait=15):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(wait * (i + 1))
    raise last


def _count_gpu(res, key=None):
    return sum(1 for v in res.values() for k, p in v.items() if (key is None or k == key) and isinstance(p, (int, float)))


def validate_rental(res):
    """Drop any $/GPU-hr outside 0.10–60 (parser garbage)."""
    for v in res.values():
        for k in list(v):
            p = v[k]
            if k in ("vast_n",) or p is None:
                continue
            if not (0.10 <= p <= 60):
                v[k] = None
    return res


def validate_purchase(res):
    for v in res.values():
        for k in list(v):
            if isinstance(v[k], (int, float)) and not (1_000 <= v[k] <= 250_000):
                v.pop(k)
    return {m: v for m, v in res.items() if any(isinstance(x, (int, float)) for x in v.values())}


def validate_memory(res):
    for t in res.values():
        t["items"] = {k: v for k, v in t["items"].items() if 0.05 <= v <= 50_000}
    return {k: t for k, t in res.items() if t["items"]}


def validate_market(res):
    for fams in res.values():
        for f in list(fams):
            if not (0.10 <= fams[f]["median"] <= 60):
                fams.pop(f)
    return res


# name -> (fetch, validate, minimum values for a run to count as healthy, backup fetch or None)
SOURCES = {
    "getdeploying":     (fetch_market, validate_market, lambda r: len(r) >= 4, None),
    "trendforce":       (fetch_trendforce, validate_memory, lambda r: sum(len(t["items"]) for t in r.values()) >= 20, fetch_dramexchange),
    "gatewell":         (fetch_gatewell, validate_purchase, lambda r: len(r) >= 5, None),
    "compute_exchange": (fetch_compute_exchange, validate_purchase, lambda r: len(r) >= 3, None),
    "lambda":           (fetch_lambda, validate_rental, lambda r: _count_gpu(r, "lambda") >= 2, None),
    "runpod":           (fetch_runpod, validate_rental, lambda r: _count_gpu(r, "runpod_secure") >= 4, None),
    "vast":             (fetch_vast, validate_rental, lambda r: _count_gpu(r, "vast_median") >= 3, None),
}


def run_source(name):
    """Returns (result, error). Retries, validates, falls back to the backup source."""
    fetch, validate, healthy, backup = SOURCES[name]
    for label, fn in ((name, fetch), (f"{name} (backup)", backup)):
        if fn is None:
            continue
        try:
            res = validate(with_retries(fn))
            if healthy(res):
                return res, None
            err = f"{label}: too few values ({len(res)}) — page layout may have changed"
        except Exception as e:
            err = f"{label}: {type(e).__name__}: {e}"
        print(f"  WARNING {err}")
    return None, err


def merge_memory(old, new):
    """Item-level merge so a partially parsed table never deletes items we already have."""
    out = {t: {**v, "items": dict(v["items"])} for t, v in old.items()}
    for t, v in new.items():
        if t in out:
            out[t]["items"].update(v["items"])
            out[t]["source_updated"] = v.get("source_updated") or out[t].get("source_updated", "")
        else:
            out[t] = v
    return out


def send_stale_alerts(data, now):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    lines = []
    for name, st in data.get("sources", {}).items():
        last_ok = st.get("last_ok")
        if not last_ok:
            continue
        age = (now - datetime.fromisoformat(last_ok.replace("Z", "+00:00"))).days
        last_alert = st.get("last_alert")
        recently = last_alert and (now - datetime.fromisoformat(last_alert.replace("Z", "+00:00"))).days < ALERT_EVERY_DAYS
        if age >= STALE_DAYS and not recently:
            lines.append(f"• *{name}* — no successful update for {age} days. Last error: `{st.get('last_error') or 'n/a'}`")
            st["last_alert"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    if lines and webhook:
        msg = ":warning: *AI Chips & Memory tab — data source not updating*\n" + "\n".join(lines) + \
              "\nThe chart keeps its last good values. Check the run log in GitHub → Actions → Update AI Chip & Memory Prices."
        try:
            requests.post(webhook, json={"text": msg}, timeout=10)
        except Exception as e:
            print(f"  Slack alert failed: {e}")
    elif lines:
        print("Stale sources (no SLACK_WEBHOOK_URL):\n" + "\n".join(lines))


def main():
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    week = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    print(f"=== AI hardware prices — {now:%Y-%m-%d %H:%M} UTC (week of {week}) ===")

    data = json.loads(DATA_FILE.read_text()) if DATA_FILE.exists() else {"weeks": []}
    by_week = {w["week"]: w for w in data["weeks"]}
    prev = by_week.get(week, {})
    entry = {**prev, "week": week, "gpus": dict(prev.get("gpus", {})),
             "memory": dict(prev.get("memory", {})), "purchase": dict(prev.get("purchase", {}))}
    entry.pop("backfill", None)
    status = data.setdefault("sources", {})
    got_any = False

    for name in SOURCES:
        res, err = run_source(name)
        st = status.setdefault(name, {})
        st["last_attempt"] = stamp
        if res is None:
            st["last_error"] = err
            continue
        got_any = True
        st.update(last_ok=stamp, last_error=None)
        if name == "getdeploying":
            # The dataset re-publishes its trailing year each week; refresh every week it covers.
            for wk, fams in res.items():
                w = by_week.setdefault(wk, {"week": wk, "gpus": {}, "memory": {}, "backfill": True,
                                            "captured_at": wk + "T00:00:00Z"})
                w["market"] = {**w.get("market", {}), **fams}
            st["count"] = len(res)
        elif name == "trendforce":
            entry["memory"] = merge_memory(entry["memory"], res)
            st["count"] = sum(len(t["items"]) for t in res.values())
        elif name in ("gatewell", "compute_exchange"):
            for m, v in res.items():
                entry["purchase"].setdefault(m, {}).update(v)
            st["count"] = len(res)
        else:
            for m, v in res.items():
                entry["gpus"].setdefault(m, {}).update({k: x for k, x in v.items() if x is not None})
            st["count"] = len(res)
        print(f"  OK {name}: {st['count']}")

    send_stale_alerts(data, now)

    if got_any:
        entry["captured_at"] = stamp
        by_week[week] = {**by_week.get(week, {}), **entry, "market": by_week.get(week, {}).get("market", entry.get("market", {}))}
        data["weeks"] = sorted(by_week.values(), key=lambda w: w["week"])
        data["last_updated"] = stamp
    else:
        print("No source returned data — keeping existing data, recording status only.")
    DATA_FILE.write_text(json.dumps(data, separators=(",", ":")))
    print(f"Saved {len(data['weeks'])} week(s) to {DATA_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # never fail the workflow — status + Slack alerts surface problems
        import traceback
        traceback.print_exc()
        print(f"ai_hardware_prices.py crashed: {e}")
