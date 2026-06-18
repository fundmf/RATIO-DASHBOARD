#!/usr/bin/env python3
"""
funding_rates.py — Captures ONE daily snapshot of MARKET-AGGREGATE funding rates
for BTC, SPX (SPX6900), FARTCOIN at US market close (21:00 UTC Mon-Fri).

╔════════════════════════════════════════════════════════════════════════════╗
║  HOW IT WORKS                                                              ║
║  ────────────                                                              ║
║  - Runs once daily on cron (Mon-Fri 21:00 UTC) via update-funding.yml      ║
║  - For each coin, queries up to 8 major perp exchanges where the coin      ║
║    is listed (Binance, Bybit, OKX, Bitget, Gate, MEXC, KuCoin, HL)         ║
║  - Each exchange's funding rate is normalised to annualised % (most use    ║
║    8h funding intervals; HL uses 1h), then averaged across exchanges       ║
║  - The averaged annualised rate is converted back to a "per-hour           ║
║    equivalent" so the existing UI cards stay unchanged                     ║
║  - If aggregate rate is negative for an enabled coin → Slack alert         ║
║  - Idempotent: re-running on same date is a no-op                          ║
║  - Exchanges that fail (network, coin not listed) are silently skipped;    ║
║    the aggregate is computed from whatever fetchers did succeed            ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import requests

DATA_FILE     = Path("funding_rates.json")
SETTINGS_FILE = Path("notification_settings.json")

COINS = ["BTC", "SPX", "FARTCOIN"]


# ── Per-exchange fetchers ────────────────────────────────────────────────
# Each returns the latest funding rate as a float (decimal, e.g. 0.0001 = 0.01%).
# Raises on error. The wrapper in main() catches and skips.

def fetch_binance(symbol):
    r = requests.get(
        f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}",
        timeout=10,
    )
    r.raise_for_status()
    return float(r.json()["lastFundingRate"])


def fetch_bybit(symbol):
    r = requests.get(
        f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1",
        timeout=10,
    )
    r.raise_for_status()
    items = r.json().get("result", {}).get("list", [])
    if not items:
        raise ValueError("no funding entries returned")
    return float(items[0]["fundingRate"])


def fetch_okx(symbol):
    r = requests.get(
        f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}",
        timeout=10,
    )
    r.raise_for_status()
    items = r.json().get("data", [])
    if not items:
        raise ValueError("no funding entries returned")
    return float(items[0]["fundingRate"])


def fetch_bitget(symbol):
    r = requests.get(
        f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={symbol}&productType=USDT-FUTURES",
        timeout=10,
    )
    r.raise_for_status()
    items = r.json().get("data", [])
    if not items:
        raise ValueError("no funding entries returned")
    return float(items[0]["fundingRate"])


def fetch_gate(symbol):
    r = requests.get(
        f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}",
        timeout=10,
    )
    r.raise_for_status()
    return float(r.json()["funding_rate"])


def fetch_mexc(symbol):
    r = requests.get(
        f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}",
        timeout=10,
    )
    r.raise_for_status()
    return float(r.json()["data"]["fundingRate"])


def fetch_kucoin(symbol):
    r = requests.get(
        f"https://api-futures.kucoin.com/api/v1/funding-rate/{symbol}/current",
        timeout=10,
    )
    r.raise_for_status()
    return float(r.json()["data"]["value"])


def fetch_hyperliquid(symbol):
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - 6 * 3600 * 1000
    r = requests.post(
        "https://api.hyperliquid.xyz/info",
        json={"type": "fundingHistory", "coin": symbol, "startTime": start_ms},
        timeout=10,
    )
    r.raise_for_status()
    history = r.json()
    if not history:
        raise ValueError("no funding history returned")
    return float(max(history, key=lambda e: e["time"])["fundingRate"])


# ── Exchange registry ────────────────────────────────────────────────────
# period_h: how many hours between funding payments on this exchange.
# symbols : per-coin trading symbol (omit a coin to mark it as not listed).

EXCHANGES = {
    "binance":     {"name": "Binance",     "period_h": 8, "fetch": fetch_binance,
                    "symbols": {"BTC": "BTCUSDT", "FARTCOIN": "FARTCOINUSDT"}},
    "bybit":       {"name": "Bybit",       "period_h": 8, "fetch": fetch_bybit,
                    "symbols": {"BTC": "BTCUSDT", "SPX": "SPX6900USDT", "FARTCOIN": "FARTCOINUSDT"}},
    "okx":         {"name": "OKX",         "period_h": 8, "fetch": fetch_okx,
                    "symbols": {"BTC": "BTC-USDT-SWAP", "FARTCOIN": "FARTCOIN-USDT-SWAP"}},
    "bitget":      {"name": "Bitget",      "period_h": 8, "fetch": fetch_bitget,
                    "symbols": {"BTC": "BTCUSDT", "SPX": "SPX6900USDT", "FARTCOIN": "FARTCOINUSDT"}},
    "gate":        {"name": "Gate",        "period_h": 8, "fetch": fetch_gate,
                    "symbols": {"BTC": "BTC_USDT", "FARTCOIN": "FARTCOIN_USDT"}},
    "mexc":        {"name": "MEXC",        "period_h": 8, "fetch": fetch_mexc,
                    "symbols": {"BTC": "BTC_USDT", "SPX": "SPX6900_USDT", "FARTCOIN": "FARTCOIN_USDT"}},
    "kucoin":      {"name": "KuCoin",      "period_h": 8, "fetch": fetch_kucoin,
                    "symbols": {"BTC": "XBTUSDTM", "FARTCOIN": "FARTCOINUSDTM"}},
    "hyperliquid": {"name": "Hyperliquid", "period_h": 1, "fetch": fetch_hyperliquid,
                    "symbols": {"BTC": "BTC", "SPX": "SPX", "FARTCOIN": "FARTCOIN"}},
}


def load_settings():
    if not SETTINGS_FILE.exists():
        return {}
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"  settings load error: {e}")
        return {}


def send_slack(webhook, text):
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=10)
        if not resp.ok:
            print(f"  Slack send failed: {resp.status_code} {resp.text[:200]}")
        return resp.ok
    except Exception as e:
        print(f"  Slack send error: {e}")
        return False


def migrate_legacy(coin_data):
    """Initialise snapshots array if absent. Drops the old hourly 'history' key."""
    if "snapshots" not in coin_data:
        coin_data["snapshots"] = []
    if "history" in coin_data:
        del coin_data["history"]
    return coin_data


def aggregate_funding(coin):
    """
    Query every exchange where `coin` is listed. Return a dict with:
      - rate           (hourly-equivalent, decimal; powers UI's '%/h' display)
      - annualised_pct (mean of per-exchange annualised %)
      - exchange_count (number of exchanges that succeeded)
      - per_exchange   ({exchange_id: raw_rate_decimal} for traceability)
    Returns None if no exchange succeeded.
    """
    annualised = []
    per_ex = {}
    for ex_id, ex in EXCHANGES.items():
        if coin not in ex["symbols"]:
            continue
        symbol = ex["symbols"][coin]
        try:
            rate = ex["fetch"](symbol)
        except Exception as e:
            print(f"  {ex['name']:11} ({symbol:>18}): FAIL — {e}")
            continue
        if rate is None:
            print(f"  {ex['name']:11} ({symbol:>18}): no rate")
            continue
        # Normalise to annualised: rate per period * (24/period_h) * 365
        ann = rate * (24.0 / ex["period_h"]) * 365.0
        annualised.append(ann)
        per_ex[ex_id] = round(rate, 10)
        print(f"  {ex['name']:11} ({symbol:>18}): {rate*100:+.4f}% per {ex['period_h']}h -> {ann*100:+.2f}% ann")

    if not annualised:
        return None

    avg_annualised = sum(annualised) / len(annualised)
    hourly_eq = avg_annualised / (24.0 * 365.0)
    return {
        "rate":            round(hourly_eq, 10),
        "annualised_pct":  round(avg_annualised * 100, 4),
        "exchange_count":  len(annualised),
        "per_exchange":    per_ex,
    }


def main():
    now = datetime.now(timezone.utc)
    today_key = now.strftime("%Y-%m-%d")
    print(f"=== Funding rates aggregate snapshot — {now.strftime('%Y-%m-%d %H:%M UTC')} ===")

    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            data = json.load(f)
    else:
        data = {"last_updated": "", "coins": {}, "alert_state": {}}
    data.setdefault("coins", {})
    data.setdefault("alert_state", {})

    settings = load_settings()
    webhook  = os.environ.get("SLACK_WEBHOOK_URL")

    for coin in COINS:
        print(f"\n{coin}...")
        coin_data = data["coins"].setdefault(coin, {"label": coin})
        coin_data["label"] = coin
        migrate_legacy(coin_data)

        if today_key in {s["date"] for s in coin_data["snapshots"]}:
            print(f"  Snapshot for {today_key} already exists — no-op.")
            continue

        agg = aggregate_funding(coin)
        if agg is None:
            print(f"  No exchange returned data — skip.")
            continue

        snapshot = {
            "date":            today_key,
            "rate":            agg["rate"],
            "annualised_pct":  agg["annualised_pct"],
            "exchange_count":  agg["exchange_count"],
            "per_exchange":    agg["per_exchange"],
            "captured_at":     now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        coin_data["snapshots"].append(snapshot)
        coin_data["snapshots"].sort(key=lambda s: s["date"])
        if len(coin_data["snapshots"]) > 120:
            coin_data["snapshots"] = coin_data["snapshots"][-120:]
        coin_data["current_rate"]     = agg["rate"]
        coin_data["current_date"]     = today_key
        coin_data["exchange_count"]   = agg["exchange_count"]
        coin_data["annualised_pct"]   = agg["annualised_pct"]

        print(f"  AGGREGATE: {agg['annualised_pct']:+.2f}% annualised across {agg['exchange_count']} exchanges")

        # Slack alert path
        key = f"funding_alert_{coin.lower()}"
        if not settings.get(key, True):
            print(f"  alert disabled in settings — skip Slack.")
            continue
        if agg["rate"] >= 0:
            print(f"  aggregate positive — no alert needed.")
            continue
        if data["alert_state"].get(coin) == today_key:
            print(f"  already alerted today — skip.")
            continue
        if not webhook:
            print(f"  no SLACK_WEBHOOK_URL set — would have alerted.")
            continue

        contributors = ", ".join(EXCHANGES[k]["name"] for k in agg["per_exchange"].keys())
        msg = (f":rotating_light: *{coin} aggregate funding is NEGATIVE*\n"
               f"Market avg: `{agg['annualised_pct']:+.2f}% annualised` "
               f"across {agg['exchange_count']} exchange(s)\n"
               f"Sources: {contributors}\n"
               f"Captured at US market close on {today_key}.")
        if send_slack(webhook, msg):
            data["alert_state"][coin] = today_key
            print(f"  Alert sent.")

    data["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"\nSaved.")


if __name__ == "__main__":
    main()
