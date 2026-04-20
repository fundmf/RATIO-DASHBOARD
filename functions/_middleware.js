// functions/_middleware.js
// Password protection + API proxy routes for Cloudflare Pages
//
// Setup: Add an environment variable called CFP_PASSWORD in your
// Cloudflare Pages dashboard with the password you want to use.

export async function onRequest(context) {
    const { request, env, next } = context;
    const url = new URL(request.url);

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
    // Tries CMC internal data API first; falls back to alternative.me if blocked/empty.
    if (url.pathname === '/api/cmc-fng') {
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
                // Verify it actually contains data before returning
                if (text && text.includes('dataList') && text.length > 100) {
                    return new Response(text, {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*',
                            'Cache-Control': 'public, max-age=3600',
                            'X-Source': 'cmc',
                        },
                    });
                }
            }
        } catch (e) { /* fall through */ }

        return new Response(JSON.stringify({ error: 'CMC Fear & Greed unavailable' }), {
            status: 502,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        });
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

