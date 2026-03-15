// functions/_middleware.js
// Password protection for Cloudflare Pages - completely free
// 
// Setup: Add an environment variable called CFP_PASSWORD in your
// Cloudflare Pages dashboard with the password you want to use.
// The username shown in the browser prompt will be "dashboard".

export async function onRequest(context) {
    const { request, env, next } = context;

    // Get password from environment variable
    const PASSWORD = env.CFP_PASSWORD;

    // If no password is set, allow access (failsafe during setup)
    if (!PASSWORD) {
        return await next();
    }

    const authorization = request.headers.get('Authorization');

    if (authorization) {
        const [scheme, encoded] = authorization.split(' ');
        if (scheme === 'Basic' && encoded) {
            try {
                const decoded = atob(encoded);
                const colonIndex = decoded.indexOf(':');
                const submittedPassword = decoded.substring(colonIndex + 1);
                if (submittedPassword === PASSWORD) {
                    return await next();
                }
            } catch (e) {
                // Invalid base64, fall through to prompt
            }
        }
    }

    // No valid credentials — return a 401 to trigger browser password prompt
    return new Response('Unauthorized - Please enter your password', {
        status: 401,
        headers: {
            'WWW-Authenticate': 'Basic realm="Price Ratio Dashboard", charset="UTF-8"',
            'Content-Type': 'text/plain',
        },
    });
}
