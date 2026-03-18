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

    // ── All other requests: serve normally ──
    return await next();
}

