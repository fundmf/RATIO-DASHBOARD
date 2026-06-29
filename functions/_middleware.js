// functions/_middleware.js
// Password protection + API proxy routes for Cloudflare Pages
//
// Setup: Add an environment variable called CFP_PASSWORD in your
// Cloudflare Pages dashboard with the password you want to use.

export async function onRequest(context) {
    const { request, env, next } = context;
    const url = new URL(request.url);

    // ── Crash-check endpoint bypasses password (has its own ?key=<CRON_SECRET> auth) ──
    // Lets cron-job.org or any external cron service hit it without dashboard credentials.
    if (url.pathname === '/api/crash-check') {
        return await next();
    }

    // ── Check password first (applies to everything) ──
    const PASSWORD = env.CFP_PASSWORD;
    if (PASSWORD) {
        const authorization = request.headers.get('Authorization');
        let authenticated = false;
        if (authorization) {
            const [scheme, encoded] = authorization.split(' ');
            if (scheme === 'Basic' && encoded) {
                try {
                    const decoded = atob(encoded);
                    const colonIndex = decoded.indexOf(':');
                    const submittedPassword = decoded.substring(colonIndex + 1);
                    if (submittedPassword === PASSWORD) {
                        authenticated = true;
                    }
                } catch (e) { /* Invalid base64 */ }
            }
        }
        if (!authenticated) {
            return new Response('Unauthorized - Please enter your password', {
                status: 401,
                headers: {
                    'WWW-Authenticate': 'Basic realm="Price Ratio Dashboard", charset="UTF-8"',
                    'Content-Type': 'text/plain',
                },
            });
        }
    }

    // ── Yahoo Finance proxy route ──
    // Bypasses CORS — Yahoo blocks third-party proxy services
    if (url.pathname === '/api/yahoo') {
        const targetUrl = url.searchParams.get('url');
        if (!targetUrl || !targetUrl.includes('finance.yahoo.com')) {
            return new Response('Bad request', { status: 400 });
        }
        try {
            const resp = await fetch(targetUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                },
            });
            const data = await resp.text();
            return new Response(data, {
                status: resp.status,
                headers: {
                    'Content-Type': resp.headers.get('Content-Type') || 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'public, max-age=300',
                },
            });
        } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), {
                status: 502,
                headers: { 'Content-Type': 'application/json' },
            });
        }
    }

    // ── Polymarket proxy route ──
    if (url.pathname === '/api/polymarket') {
        const targetUrl = url.searchParams.get('url');
        if (!targetUrl || !targetUrl.includes('gamma-api.polymarket.com')) {
            return new Response('Bad request', { status: 400 });
        }
        try {
            const resp = await fetch(targetUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0' },
            });
            const data = await resp.text();
            return new Response(data, {
                status: resp.status,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'public, max-age=600',
                },
            });
        } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), {
                status: 502,
                headers: { 'Content-Type': 'application/json' },
            });
        }
    }

    // ── Deribit proxy route ──
    if (url.pathname === '/api/deribit') {
        const targetUrl = url.searchParams.get('url');
        if (!targetUrl || !targetUrl.includes('deribit.com')) {
            return new Response('Bad request', { status: 400 });
        }
        try {
            const resp = await fetch(targetUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0' },
            });
            const data = await resp.text();
            return new Response(data, {
                status: resp.status,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'public, max-age=600',
                },
            });
        } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), {
                status: 502,
                headers: { 'Content-Type': 'application/json' },
            });
        }
    }

    // ── CMC Fear & Greed proxy route ──
    // Strategy:
    //   1. If CMC_API_KEY is configured (Cloudflare env var) → use the official
    //      Pro API and transform the response to the shape the frontend expects.
    //   2. Otherwise, try CMC's undocumented internal data API as a free fallback.
    //   3. If neither works, return a clean error and the UI shows "Could not load".
    if (url.pathname === '/api/cmc-fng') {
        // ── Path 1: official Pro API (preferred when API key is set) ──
        if (env.CMC_API_KEY) {
            try {
                const proResp = await fetch(
                    'https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical?start=1&limit=30',
                    {
                        headers: {
                            'X-CMC_PRO_API_KEY': env.CMC_API_KEY,
                            'Accept': 'application/json',
                        },
                    }
                );
                if (proResp.ok) {
                    const proJson = await proResp.json();
                    const entries = proJson?.data || [];

                    // Robust timestamp normaliser — returns unix SECONDS or NaN.
                    // Handles: number-seconds, number-ms, string-seconds, string-ms, ISO 8601.
                    const tsToSec = ts => {
                        if (ts == null) return NaN;
                        if (typeof ts === 'number') {
                            return ts > 1e12 ? Math.floor(ts / 1000) : ts;
                        }
                        const s = String(ts).trim();
                        if (/^\d+(\.\d+)?$/.test(s)) {
                            const n = parseFloat(s);
                            return n > 1e12 ? Math.floor(n / 1000) : Math.floor(n);
                        }
                        const ms = new Date(s).getTime();
                        return isNaN(ms) ? NaN : Math.floor(ms / 1000);
                    };
                    // Pull timestamp / value / classification using several known field names.
                    const tsOf = e => e.timestamp ?? e.time ?? e.ts ?? e.update_time ?? e.t;
                    const valOf = e => e.value ?? e.score ?? e.y;
                    const clsOf = e => e.value_classification ?? e.classification ?? e.score_text ?? e.yClassification ?? '';

                    const dataList = entries
                        .map(e => ({
                            x: tsToSec(tsOf(e)),
                            y: String(valOf(e) ?? ''),
                            yClassification: clsOf(e),
                        }))
                        .filter(e => !isNaN(e.x) && e.y !== '')
                        .sort((a, b) => a.x - b.x);

                    // Include a sample of the raw first entry so we can debug field-name mismatches
                    // without redeploying. Visible only if dataList ends up empty or short.
                    const body = { data: { dataList }, _source: 'cmc-pro' };
                    if (!dataList.length || dataList.length < entries.length) {
                        body._debug = {
                            entries_received: entries.length,
                            entries_kept: dataList.length,
                            raw_sample: entries[0] || null,
                            raw_keys: entries[0] ? Object.keys(entries[0]) : [],
                        };
                    }
                    return new Response(JSON.stringify(body), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*',
                            'Cache-Control': 'public, max-age=3600',
                            'X-Source': 'cmc-pro',
                        },
                    });
                }
                // Pro API rejected (key invalid, quota exceeded) — log and fall through to internal endpoint
                console.warn('CMC Pro API non-OK:', proResp.status);
            } catch (e) { /* fall through */ }
        }

        // ── Path 2: undocumented internal data API (free, no key) ──
        try {
            const cmcResp = await fetch(
                'https://api.coinmarketcap.com/data-api/v3/fear-greed/chart',
                {
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Origin': 'https://coinmarketcap.com',
                        'Referer': 'https://coinmarketcap.com/charts/fear-and-greed-index/',
                    },
                }
            );
            if (cmcResp.ok) {
                const text = await cmcResp.text();
                if (text && text.includes('dataList') && text.length > 100) {
                    return new Response(text, {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*',
                            'Cache-Control': 'public, max-age=3600',
                            'X-Source': 'cmc-internal',
                        },
                    });
                }
            }
        } catch (e) { /* fall through */ }

        return new Response(JSON.stringify({ error: 'CMC Fear & Greed unavailable (set CMC_API_KEY env var for reliable access)' }), {
            status: 502,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        });
    }

    // ── Notification Settings API (reads/writes notification_settings.json via GitHub) ──
    if (url.pathname === '/api/settings') {
        const PAT = env.GITHUB_PAT;
        const REPO = env.GITHUB_REPO;
        if (!PAT || !REPO) {
            return new Response(JSON.stringify({ error: 'Server not configured: set GITHUB_PAT and GITHUB_REPO env vars' }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
            });
        }
        const apiUrl = `https://api.github.com/repos/${REPO}/contents/notification_settings.json`;
        const ghHeaders = {
            'Authorization': `Bearer ${PAT}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'RatioDashboard/1.0',
        };

        if (request.method === 'GET') {
            try {
                const resp = await fetch(apiUrl, { headers: ghHeaders });
                if (!resp.ok) throw new Error('GitHub API: ' + resp.status);
                const file = await resp.json();
                const content = atob(file.content.replace(/\n/g, ''));
                return new Response(content, {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                });
            } catch (e) {
                return new Response(JSON.stringify({ error: e.message }), {
                    status: 502,
                    headers: { 'Content-Type': 'application/json' },
                });
            }
        }

        if (request.method === 'POST') {
            try {
                const body = await request.json();
                const getResp = await fetch(apiUrl, { headers: ghHeaders });
                let current = {};
                let sha = null;
                if (getResp.ok) {
                    const file = await getResp.json();
                    sha = file.sha;
                    try { current = JSON.parse(atob(file.content.replace(/\n/g, ''))); } catch {}
                }
                // Merge body into current settings (partial updates allowed)
                const merged = { ...current, ...body };
                const newContent = btoa(unescape(encodeURIComponent(JSON.stringify(merged, null, 2) + '\n')));
                const putBody = { message: 'Notification settings update', content: newContent };
                if (sha) putBody.sha = sha;
                const putResp = await fetch(apiUrl, {
                    method: 'PUT',
                    headers: { ...ghHeaders, 'Content-Type': 'application/json' },
                    body: JSON.stringify(putBody),
                });
                if (!putResp.ok) {
                    const errText = await putResp.text();
                    throw new Error(`GitHub PUT ${putResp.status}: ${errText.substring(0, 200)}`);
                }
                return new Response(JSON.stringify({ ok: true, settings: merged }), {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                });
            } catch (e) {
                return new Response(JSON.stringify({ error: e.message }), {
                    status: 502,
                    headers: { 'Content-Type': 'application/json' },
                });
            }
        }

        return new Response('Method not allowed', { status: 405 });
    }

    // ── Custom Alerts API (reads/writes custom_alerts.json via GitHub API) ──
    if (url.pathname === '/api/alerts') {
        const PAT = env.GITHUB_PAT;
        const REPO = env.GITHUB_REPO; // "owner/repo" format
        if (!PAT || !REPO) {
            return new Response(JSON.stringify({ error: 'Server not configured: set GITHUB_PAT and GITHUB_REPO env vars in Cloudflare Pages' }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
            });
        }
        const apiUrl = `https://api.github.com/repos/${REPO}/contents/custom_alerts.json`;
        const ghHeaders = {
            'Authorization': `Bearer ${PAT}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'RatioDashboard/1.0',
        };

        // GET — return current alerts
        if (request.method === 'GET') {
            try {
                const resp = await fetch(apiUrl, { headers: ghHeaders });
                if (!resp.ok) throw new Error('GitHub API: ' + resp.status);
                const file = await resp.json();
                const content = atob(file.content.replace(/\n/g, ''));
                return new Response(content, {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                });
            } catch (e) {
                return new Response(JSON.stringify({ error: e.message }), {
                    status: 502,
                    headers: { 'Content-Type': 'application/json' },
                });
            }
        }

        // POST — add or delete an alert
        if (request.method === 'POST') {
            try {
                const body = await request.json();

                // Fetch current file (need SHA for update)
                const getResp = await fetch(apiUrl, { headers: ghHeaders });
                let alerts = { alerts: [] };
                let sha = null;
                if (getResp.ok) {
                    const file = await getResp.json();
                    sha = file.sha;
                    alerts = JSON.parse(atob(file.content.replace(/\n/g, '')));
                }

                let commitMsg;
                if (body.action === 'delete') {
                    alerts.alerts = alerts.alerts.filter(a => a.id !== body.id);
                    commitMsg = `Custom alert: delete "${body.id}"`;
                } else {
                    alerts.alerts.push(body.alert);
                    commitMsg = `Custom alert: add "${body.alert?.title || 'untitled'}"`;
                }

                // Write back via GitHub Contents API
                const newContent = btoa(unescape(encodeURIComponent(JSON.stringify(alerts, null, 2))));
                const putBody = { message: commitMsg, content: newContent };
                if (sha) putBody.sha = sha;

                const putResp = await fetch(apiUrl, {
                    method: 'PUT',
                    headers: { ...ghHeaders, 'Content-Type': 'application/json' },
                    body: JSON.stringify(putBody),
                });
                if (!putResp.ok) {
                    const errText = await putResp.text();
                    throw new Error(`GitHub PUT ${putResp.status}: ${errText.substring(0, 200)}`);
                }
                return new Response(JSON.stringify({ ok: true }), {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                });
            } catch (e) {
                return new Response(JSON.stringify({ error: e.message }), {
                    status: 502,
                    headers: { 'Content-Type': 'application/json' },
                });
            }
        }

        return new Response('Method not allowed', { status: 405 });
    }

    // ── All other requests: serve normally ──
    return await next();
}

