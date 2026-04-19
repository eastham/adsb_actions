const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    const path = new URL(request.url).pathname;

    if (request.method === 'POST' && path === '/request') {
      return handleRequest(request, env);
    }

    if (request.method === 'POST' && path === '/subscribe') {
      return handleSubscribe(request, env);
    }

    return new Response('Not found', { status: 404 });
  }
};

async function handleRequest(request, env) {
  try {
    const text = await request.text();
    const params = new URLSearchParams(text);
    const airport = (params.get('airport') || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

    if (!airport || airport.length > 4) {
      return jsonResponse({ ok: false, error: 'Invalid airport code' }, 400);
    }

    const entry = JSON.stringify({
      airport,
      ts: new Date().toISOString(),
      ip: request.headers.get('CF-Connecting-IP') || 'unknown',
    }) + '\n';

    // Read existing log, append new entry, write back
    const key = 'requests.jsonl';
    const existing = await env.BUCKET.get(key);
    const prev = existing ? await existing.text() : '';
    await env.BUCKET.put(key, prev + entry, {
      httpMetadata: { contentType: 'application/jsonl' }
    });

    return jsonResponse({ ok: true });
  } catch (err) {
    return jsonResponse({ ok: false, error: 'Internal error' }, 500);
  }
}

async function handleSubscribe(request, env) {
  try {
    const text = await request.text();
    const params = new URLSearchParams(text);
    const email = (params.get('email') || '').trim().toLowerCase();

    if (!email || !email.includes('@') || email.length > 254) {
      return jsonResponse({ ok: false, error: 'Invalid email address' }, 400);
    }

    const entry = JSON.stringify({
      email,
      ts: new Date().toISOString(),
      ip: request.headers.get('CF-Connecting-IP') || 'unknown',
    }) + '\n';

    const key = 'subscribers.jsonl';
    const existing = await env.BUCKET.get(key);
    const prev = existing ? await existing.text() : '';
    await env.BUCKET.put(key, prev + entry, {
      httpMetadata: { contentType: 'application/jsonl' }
    });

    return jsonResponse({ ok: true });
  } catch (err) {
    return jsonResponse({ ok: false, error: 'Internal error' }, 500);
  }
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}
