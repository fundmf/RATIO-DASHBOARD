# RATIO-DASHBOARD — Claude Context

Single-file crypto dashboard deployed on **Cloudflare Pages**, auto-updated via **GitHub Actions** (hourly). All UI lives in `index.html` (~2700 lines). Data lives in static JSON files committed to the repo.

---

## Repo layout

```
index.html                  ← entire frontend (HTML + CSS + JS, no bundler)
functions/_middleware.js    ← Cloudflare Pages Functions: password gate + API proxies (bypasses password for /api/crash-check)
functions/api/crash-check.js ← cron-triggered endpoint that detects fast price crashes and fires ntfy + Slack alerts
data.json                   ← daily OHLC for BTC/ETH/MSTR/BMNR (2021–present)
fartcoin_hourly.json        ← hourly BTC+FART prices  {last_updated, hours:[{t,btc,fart}]}
liquidity_daily.json        ← BTC price+volume daily  {last_updated, days:[{date,btc_price,volume}]}
fartcoin_liquidity_daily.json ← FART price+volume daily {last_updated, days:[{date,fart_price,volume}]}
spx6900_hourly.json         ← hourly SPX6900 data
ai_watchlist.json           ← weekly Friday closes for GTLB/CDW/ADBE/EXLS/ADP/^NDX
funding_rates.json          ← DAILY MARKET-AGGREGATE funding snapshots per coin (avg across Binance/Bybit/OKX/Bitget/Gate/MEXC/KuCoin/Hyperliquid); per_exchange breakdown in each snapshot + alert_state
etf_flows.json              ← daily BTC ETF net flows from Farside + last_alert_date
notification_settings.json  ← per-alert toggles + threshold; read by funding_rates.py, etf_flows.py, AND crash-check.js
crash_alert_state.json      ← cooldown timestamps per coin for crash monitor; updated only when an alert fires
custom_alerts.json          ← server-side alert state
update_data.py              ← daily OHLC updater (BTC/ETH via CoinGecko, MSTR/BMNR via yfinance)
backfill_hourly.py          ← updates fartcoin_hourly.json + spx6900_hourly.json
backfill_liquidity.py       ← updates liquidity_daily.json + fartcoin_liquidity_daily.json
update_ai_watchlist.py      ← weekly Friday-close fetcher for AI watchlist (yfinance)
funding_rates.py            ← DAILY market-aggregate funding snapshot — polls 8 exchanges, normalises to annualised, averages (Mon-Fri 21:00 UTC via update-funding.yml) + Slack alert if aggregate negative
etf_flows.py                ← Farside BTC ETF scrape + sign-flip Slack alert (dedicated workflow, 04/06/10 UTC redundant runs)
detect_events.py            ← Slack alerts for divergence events
market_alerts.py / forex_calendar_alert.py / custom_alerts.py  ← Slack alert bots
.github/workflows/update.yml ← runs all scripts hourly Mon–Fri, commits + pushes
```

---

## Navigation tabs (current order)

```html
<button class="tab-btn active" data-tab="ratio">Ratio Analysis</button>
<button class="tab-btn" data-tab="geo">Geopolitics</button>
<button class="tab-btn" data-tab="liq">Market Liquidity</button>
<button class="tab-btn" data-tab="btc">BTC</button>
<button class="tab-btn" data-tab="fart">FARTCOIN Analysis</button>
<button class="tab-btn" data-tab="fart2">FARTCOIN Analysis V2</button>
<button class="tab-btn" data-tab="fartmin">FARTCOIN Minute Analysis</button>
<button class="tab-btn" data-tab="ai">AI Affected Stock Watchlist</button>
<button class="tab-btn" data-tab="funding">Funding Rates</button>
<button class="tab-btn" data-tab="crash">Crash Alert</button>
<button class="tab-btn" data-tab="alerts">Custom Alerts</button>
<button class="tab-btn" data-tab="docs" style="margin-left:auto;...">Documentation</button>
```

Tab init guard (line ~1008):
```javascript
const tabInit={ratio:false,geo:false,liq:false,btc:false,fart:false,fart2:false,fartmin:false,ai:false,funding:false,crash:false,alerts:false};
```

Tab click wiring (lines ~1016–1021):
```javascript
if(id==='geo'&&!tabInit.geo){tabInit.geo=true;initGeopolitics();}
if(id==='liq'&&!tabInit.liq){tabInit.liq=true;initLiquidity();}
if(id==='fart'&&!tabInit.fart){tabInit.fart=true;initFartcoin();}
if(id==='fart2'&&!tabInit.fart2){tabInit.fart2=true;initFartcoinV2();}
if(id==='fartmin'&&!tabInit.fartmin){tabInit.fartmin=true;initFartMinAnalysis();}
if(id==='alerts'&&!tabInit.alerts){tabInit.alerts=true;initAlerts();}
```

---

## Key global variables

```javascript
let rawData = [];           // daily OHLC array from data.json
let fartData = [];          // hourly [{t:"2025-09-01T00:00:00Z", btc:..., fart:...}] — shared by fart + fart2 tabs
let liqData = [];           // daily BTC liquidity
let fartLiqData = [];       // daily FART liquidity
let fart2ChartInst = null;  // Chart.js instance for V2
```

---

## Tab: Ratio Analysis (`ratio`)

- Loads `data.json` on page load via `init()` (called at bottom of script)
- Fetches live BTC/ETH/MSTR/BMNR prices via Yahoo Finance proxy (`/api/yahoo`)
- Renders two Chart.js line charts with optional reference lines
- Key functions: `init()`, `loadHistoricalData()`, `fetchLivePrices()`, `buildLiveDataPoint()`, `renderChart()`, `prepareData()`, `updateCharts()`

---

## Tab: Market Liquidity (`liq`) — `initLiquidity()` line ~1488

**Load flow:**
1. Fetch `liquidity_daily.json` → `liqData` (388 days of BTC price+volume)
2. If loaded, `renderLiquidityChart()` immediately
3. Top-up last 30 days from CoinGecko → merge → re-render
4. Separately fetch `fartcoin_liquidity_daily.json` → `fartLiqData`
5. Top-up FART last 30 days from CoinGecko → merge → `renderFartLiqChart()` + `renderCombinedLiqChart()`
6. Auto-refresh every hour (last 2 days only)

**Known issue (UNSOLVED):** `liquidity_daily.json` fails to load (`r.ok = false`) on the deployed Cloudflare Pages site, even though it's committed in git. `fartcoin_liquidity_daily.json` (same format, similar size) works fine. Result: BTC chart only shows ~31 days instead of full history. Root cause unknown — possibly a CDN/middleware issue specific to that filename. **Do NOT add a CoinGecko fallback for full history** — rate limits and lag are a concern.

---

## Tab: FARTCOIN Analysis V1 (`fart`) — `initFartcoin()` line ~2118

- Loads `fartcoin_hourly.json` → `fartData` for fast initial render
- Tops up last 30 days from CoinGecko (`fetchFartcoinChunk(30)`) with 2.5s delay between BTC and FART calls
- Auto-refresh every hour
- Divergence criteria (V1): `|FART% - BTC%| >= threshold` over 24hr window (any direction)
- Shared `fartData` array — if V1 loads first, V2 reuses it

```javascript
// fartcoin_hourly.json format
{ "last_updated": "2026-04-15T10:13:41Z", "hours": [{"t":"2025-09-01T00:00:00Z","btc":57000,"fart":0.123}, ...] }
```

---

## Tab: FARTCOIN Analysis V2 (`fart2`) — `initFartcoinV2()` line ~2332

**Load flow:**
- If `fartData.length < 500` (V1 not visited yet), fetch `fartcoin_hourly.json` and merge into `fartData`
- Then `runFartAnalysisV2()` for immediate render
- Then `fetchFartcoinChunk(30)` to top up live data

**V2 divergence criteria** (different from V1):
```javascript
const LB=24, CW=72;
// Both BTC and FART must be positive over 24hr, AND BTC drops >= threshold in this 1-hour candle
const btc24Chg = (bn - bp) / bp;   // BTC 24hr change (must be > 0)
const fart24Chg = (fn - fp) / fp;  // FART 24hr change (must be > 0)
const btc1hDrop = (bPrev - bn) / bPrev;  // BTC 1-hour drop (must be >= gapThr, default 1.5%)
if (btc24Chg > 0 && fart24Chg > 0 && btc1hDrop >= gapThr) divHours.push(i);
```

Controls: `fart2GapThreshold` (default 1.5%), `fart2CrashThreshold`
Events clustered by 3-hour gaps, deduplicated per calendar day.

---

## Tab: FARTCOIN Minute Analysis (`fartmin`) — `initFartMinAnalysis()` line ~2557

**Fully static** — no API calls. Data hardcoded as JS arrays `FARTMIN_BTC` and `FARTMIN_FART`.

Time window: **8:29 PM Apr 14 – 2:29 AM Apr 15 2026 AEST Brisbane**
= UTC 2026-04-14T10:29:00Z → 2026-04-14T16:29:00Z
= Unix: START=1776162540, END=1776184140, 360 one-minute candles each

Data structure: `[{t: unix_sec, o: open, h: high, l: low, c: close}, ...]`

**Custom canvas renderer** `drawCandlesticks(canvasId, candles)`:
- Properties: `c.t`, `c.o`, `c.h`, `c.l`, `c.c` (shorthand — NOT `.time/.open/.high/.low/.close`)
- Green (#10b981) bullish, red (#ef4444) bearish
- Grid lines (6 ticks), time labels every 30 candles in AEST (Australia/Brisbane)
- Uses `requestAnimationFrame` to flush layout before drawing (canvas sizing fix)

---

## Tab: Custom Alerts (`alerts`) — `initAlerts()` line ~2674

- Stored in `localStorage` (client-side) + synced to `custom_alerts.json` via `/api/alerts` (GitHub API)
- `/api/alerts` endpoint implemented in `functions/_middleware.js`
- Uses `GITHUB_PAT` and `GITHUB_REPO` env vars set in Cloudflare Pages dashboard

---

## API proxy routes (`functions/_middleware.js`)

All requests go through middleware. Password check runs first (Basic auth, `CFP_PASSWORD` env var).

| Path | Purpose |
|------|---------|
| `/api/yahoo` | Yahoo Finance proxy (CORS bypass) |
| `/api/polymarket` | Polymarket gamma API proxy |
| `/api/deribit` | Deribit API proxy |
| `/api/alerts` | Read/write `custom_alerts.json` via GitHub Contents API |
| Everything else | `next()` → serve static files |

---

## Data update pipeline (`update_data.py`)

- BTC/ETH: CoinGecko free API (`/market_chart/range` + `/ohlc`)
- MSTR/BMNR: yfinance with **14-day lookback** (one `download()` call, no rate limit)
- Backfill loop **always overwrites** existing MSTR/BMNR with real yfinance data (corrects stale carry-forwards)
- `carry_forward_stock()`: fills holidays/fetch-failure weekdays with last known value
- US market holidays hardcoded in `US_MARKET_HOLIDAYS` set
- Fartcoin hourly data updated by `update_fartcoin_hourly()` at end of script

---

## CoinGecko API usage (free tier, 30 req/min)

- `fetch` called directly from browser (no proxy needed — CoinGecko allows CORS)
- `safeFetch()` wrapper handles timeouts
- Delays between calls: 1.5s (in update_data.py), 2.5s (in fetchFartcoinChunk)
- **Do NOT add extra full-history CoinGecko calls** — rate limits and page load lag

---

## Chart.js usage

- Loaded from CDN, no build step
- Charts created as `new Chart(ctx, config)`
- For V2: `fart2ChartInst` stored globally, destroyed before re-create
- Custom zone plugin used for divergence event highlighting

---

## Deployment

- **Cloudflare Pages** connected to GitHub repo, auto-deploys on push to `main`
- GitHub Actions workflow (`.github/workflows/update.yml`) runs hourly, commits updated JSON files
- Environment variables set in Cloudflare Pages dashboard: `CFP_PASSWORD`, `GITHUB_PAT`, `GITHUB_REPO`, `SLACK_WEBHOOK_URL`, `SLACK_WEBHOOK_URL_CUSTOM`
- Branch: `main`

---

## Pending issue

**Market Liquidity BTC chart only shows ~31 days** on the deployed site.
- `liquidity_daily.json` (26KB, 388 records from 2025-03-24, committed to git) returns `r.ok = false` when fetched on Cloudflare Pages
- `fartcoin_liquidity_daily.json` (25KB, same format) works fine
- Constraint: do NOT add extra CoinGecko API calls for full history fallback
- Likely fix approaches: debug Cloudflare CDN/middleware blocking this specific file, or embed the historical data inline in index.html similar to the minute chart candles
