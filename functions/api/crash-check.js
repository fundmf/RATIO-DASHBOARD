// functions/api/crash-check.js
// Cron-triggered endpoint that detects fast price crashes for a configured coin
// and fires ntfy URGENT + Slack alerts.
//
// Auth: requires ?key=<CRON_SECRET> query param (CRON_SECRET set as Cloudflare env var).
// The middleware skips its password gate for /api/crash-check so this endpoint
// is reachable from cron-job.org without dashboard credentials — the ?key check
// is what actually authorises the call.
//
// Settings live in notification_settings.json (editable via dashboard /api/settings):
//   - crash_alert_enabled       (bool)
//   - crash_alert_coin          (string, default "FARTCOIN")
//   - crash_alert_drop_pct      (number, default 8)
//   - crash_alert_window_min    (number, default 10)
//   - crash_alert_cooldown_min  (number, default 60)
//   - crash_alert_floor_price   (number|null, default null)
//   - ntfy_topic                (string, required)
//
// Cooldown state persisted in crash_alert_state.json (auto-committed via GitHub API).

export async function onRequest(context) {
    const { request, env } = context;
    const url = new URL(request.url);

    // Auth: secret key in query param
    const key = url.searchParams.get('key');
    if (!env.CRON_SECRET || key !== env.CRON_SECRET) {
        return json({ error: 'forbidden' }, 403);
    }

    try {
        const result = await runCheck(env);
        return json(result, 200);
    } catch (e) {
        return json({ error: e.message, stack: e.stack?.substring(0, 500) }, 500);
    }
}

function json(body, status = 200) {
    return new Response(JSON.stringify(body, null, 2), {
        status,
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
    });
}

async function runCheck(env) {
    const settings = await loadJson(env, 'notification_settings.json');
    if (!settings.crash_alert_enabled) {
        return { ok: true, reason: 'disabled' };
    }

    const coin        = settings.crash_alert_coin || 'FARTCOIN';
    const dropPct     = numberOr(settings.crash_alert_drop_pct, 8);
    const windowMin   = Math.max(2, numberOr(settings.crash_alert_window_min, 10));
    const cooldownMin = Math.max(1, numberOr(settings.crash_alert_cooldown_min, 60));
    const floorPrice  = settings.crash_alert_floor_price ?? null;
    const ntfyTopic   = settings.ntfy_topic;
    if (!ntfyTopic) return { ok: false, reason: 'ntfy_topic not set in settings' };

    // Cooldown check
    const state = await loadJson(env, 'crash_alert_state.json').catch(() => ({}));
    state.last_alerts = state.last_alerts || {};
    const now = Date.now();
    const lastAlert = state.last_alerts[coin];
    if (lastAlert && (now - lastAlert) < cooldownMin * 60 * 1000) {
        const remaining = Math.ceil((cooldownMin * 60 * 1000 - (now - lastAlert)) / 60000);
        return { ok: true, reason: 'in cooldown', remaining_min: remaining, coin };
    }

    // Fetch recent 1m candles
    const symbol = coinToBinanceSymbol(coin);
    const candles = await fetchBinanceCandles(symbol, windowMin + 2);
    if (!candles || candles.length < 2) {
        return { ok: false, reason: 'insufficient candle data', count: candles?.length || 0, symbol };
    }

    // Drop = current close vs highest high in the window
    const highs  = candles.map(c => parseFloat(c[2]));
    const closes = candles.map(c => parseFloat(c[4]));
    const currentPrice = closes[closes.length - 1];
    const windowHigh   = Math.max(...highs.slice(-windowMin));
    const dropPctActual = ((windowHigh - currentPrice) / windowHigh) * 100;

    // Floor check (don't alert on already-crashed assets bouncing)
    if (floorPrice && currentPrice < floorPrice) {
        return {
            ok: true,
            reason: 'price below configured floor',
            current_price: currentPrice,
            floor: floorPrice,
        };
    }

    // Threshold check
    if (dropPctActual < dropPct) {
        return {
            ok: true,
            reason: 'no crash detected',
            coin,
            drop_pct: round(dropPctActual, 2),
            threshold_pct: dropPct,
            current_price: currentPrice,
            window_high: windowHigh,
        };
    }

    // ── ALERT FAN-OUT ──
    const title = `${coin} CRASH ALERT`;
    const body  = `Down ${dropPctActual.toFixed(1)}% in last ${windowMin} min · Current $${formatPrice(currentPrice)} · Window high $${formatPrice(windowHigh)}`;

    const dispatched = await Promise.allSettled([
        sendNtfy(ntfyTopic, title, body),
        env.SLACK_WEBHOOK_URL
            ? sendSlack(env.SLACK_WEBHOOK_URL, `:rotating_light: *${title}*\n${body}`)
            : Promise.resolve('slack disabled'),
    ]);

    // Persist cooldown
    state.last_alerts[coin] = now;
    try {
        await saveJson(env, 'crash_alert_state.json', state, `Crash alert fired for ${coin}`);
    } catch (e) {
        // Don't block alert response on persistence failure
        console.error('state save failed', e);
    }

    return {
        ok: true,
        alerted: true,
        coin,
        drop_pct: round(dropPctActual, 2),
        threshold_pct: dropPct,
        current_price: currentPrice,
        window_high: windowHigh,
        ntfy_result: summariseResult(dispatched[0]),
        slack_result: summariseResult(dispatched[1]),
    };
}

function summariseResult(r) {
    if (r.status === 'fulfilled') return 'ok';
    return 'failed: ' + String(r.reason).substring(0, 200);
}

function coinToBinanceSymbol(coin) {
    const map = {
        BTC: 'BTCUSDT',
        ETH: 'ETHUSDT',
        SPX: 'SPXUSDT',
        FARTCOIN: 'FARTCOINUSDT',
    };
    return map[coin] || coin + 'USDT';
}

async function fetchBinanceCandles(symbol, minutes) {
    const limit = Math.min(Math.max(minutes, 2), 100);
    const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=${limit}`;
    const r = await fetch(url, { headers: { 'User-Agent': 'CrashMonitor/1.0' } });
    if (!r.ok) throw new Error('Binance klines API: ' + r.status);
    return await r.json();
}

async function loadJson(env, filename) {
    if (!env.GITHUB_PAT || !env.GITHUB_REPO) {
        throw new Error('GitHub credentials not configured (GITHUB_PAT/GITHUB_REPO env vars)');
    }
    const url = `https://api.github.com/repos/${env.GITHUB_REPO}/contents/${filename}`;
    const r = await fetch(url, {
        headers: {
            'Authorization': `Bearer ${env.GITHUB_PAT}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'CrashMonitor/1.0',
        },
    });
    if (r.status === 404) return {};
    if (!r.ok) throw new Error('GitHub GET ' + filename + ': ' + r.status);
    const file = await r.json();
    const content = atob(file.content.replace(/\n/g, ''));
    return JSON.parse(content);
}

async function saveJson(env, filename, data, message) {
    const url = `https://api.github.com/repos/${env.GITHUB_REPO}/contents/${filename}`;
    const getResp = await fetch(url, {
        headers: {
            'Authorization': `Bearer ${env.GITHUB_PAT}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'CrashMonitor/1.0',
        },
    });
    let sha = null;
    if (getResp.ok) {
        const file = await getResp.json();
        sha = file.sha;
    }
    const content = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2) + '\n')));
    const putBody = { message, content };
    if (sha) putBody.sha = sha;

    const putResp = await fetch(url, {
        method: 'PUT',
        headers: {
            'Authorization': `Bearer ${env.GITHUB_PAT}`,
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json',
            'User-Agent': 'CrashMonitor/1.0',
        },
        body: JSON.stringify(putBody),
    });
    if (!putResp.ok) {
        const text = await putResp.text();
        throw new Error('GitHub PUT ' + filename + ': ' + putResp.status + ' ' + text.substring(0, 200));
    }
    return true;
}

async function sendNtfy(topic, title, message) {
    const r = await fetch(`https://ntfy.sh/${encodeURIComponent(topic)}`, {
        method: 'POST',
        headers: {
            'Title': title,
            'Priority': 'urgent',
            'Tags': 'rotating_light',
        },
        body: message,
    });
    if (!r.ok) throw new Error('ntfy ' + r.status);
    return 'ok';
}

async function sendSlack(webhook, text) {
    const r = await fetch(webhook, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text }),
    });
    if (!r.ok) throw new Error('slack ' + r.status);
    return 'ok';
}

function numberOr(v, def) {
    const n = parseFloat(v);
    return isNaN(n) ? def : n;
}

function round(n, places) {
    const m = Math.pow(10, places);
    return Math.round(n * m) / m;
}

function formatPrice(p) {
    if (p < 0.001) return p.toFixed(8);
    if (p < 1)     return p.toFixed(4);
    if (p < 100)   return p.toFixed(2);
    return p.toFixed(0);
}
