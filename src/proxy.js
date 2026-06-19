const axios = require("axios");
const settings = require("./settings");
const state = require("./state");
const {
    decodeBase64Url,
    encodeBase64Url,
    getPublicHost,
    hashKey,
    isHlsUrl,
    isHttpUrl,
    routeBase,
    sleep,
    toAbsoluteUrl,
    upstreamAgentOptions
} = require("./utils");

const RELAY_HEADERS = {
    "User-Agent": settings.UPSTREAM_UA,
    "Accept": "*/*"
};

function setPlaylistHeaders(res) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
}

async function retryOperation(label, retries, delayMs, fn) {
    let lastErr = null;
    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            return await fn(attempt);
        } catch (err) {
            lastErr = err;
            if (attempt >= retries) break;
            if (delayMs > 0) await sleep(delayMs);
        }
    }
    throw lastErr || new Error(`${label} failed`);
}

async function fetchUpstreamManifest(upstream) {
    return retryOperation("manifest", settings.HLS_MANIFEST_RETRIES, settings.HLS_MANIFEST_RETRY_DELAY_MS, async attempt => {
        const response = await axios.get(upstream, {
            responseType: "text",
            timeout: settings.HLS_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers: RELAY_HEADERS,
            ...upstreamAgentOptions(),
            validateStatus: status => status >= 200 && status < 300
        });
        const data = String(response.data || "");
        if (!data.trimStart().startsWith("#EXTM3U")) throw new Error("Invalid HLS manifest");
        if (attempt > 0) console.warn(`[PROXY M3U8 RECOVERED] attempt=${attempt + 1}`);
        return { data, finalUrl: response.request?.res?.responseUrl || upstream };
    });
}

async function getRewrittenManifest(configKey, upstream, host, routeKey = configKey) {
    const key = hashKey(`${configKey}|${host}|${routeKey}|manifest|${upstream}`);
    if (state.manifestInflight.has(key)) return state.manifestInflight.get(key);

    const promise = (async () => {
        const fetched = await fetchUpstreamManifest(upstream);
        return {
            text: rewriteManifest(fetched.data, fetched.finalUrl, host, configKey, upstream, routeKey)
        };
    })();

    state.manifestInflight.set(key, promise);
    try {
        return await promise;
    } finally {
        state.manifestInflight.delete(key);
    }
}

function segmentMapKey(configKey, parentPlaylistUrl) {
    return hashKey(`${configKey}|segments|${parentPlaylistUrl}`);
}

function segmentIdentity(url) {
    try {
        const parsed = new URL(url);
        const stableParams = [];
        const volatileParam = /token|auth|sig|signature|expire|expires|key|hash|hmac|session|st|e/i;
        for (const [key, value] of parsed.searchParams.entries()) {
            if (!volatileParam.test(key)) stableParams.push(`${key}=${value}`);
        }
        return `${parsed.hostname}${parsed.pathname}${stableParams.length ? `?${stableParams.join("&")}` : ""}`;
    } catch {
        return hashKey(url);
    }
}

function rememberSegmentUrl(configKey, parentPlaylistUrl, segmentUrl) {
    if (!settings.SEGMENT_TOKEN_HEALING || !isHttpUrl(parentPlaylistUrl) || !isHttpUrl(segmentUrl)) return;
    const key = segmentMapKey(configKey, parentPlaylistUrl);
    const map = state.segmentMaps.get(key) || new Map();
    map.set(segmentIdentity(segmentUrl), segmentUrl);
    while (map.size > 500) map.delete(map.keys().next().value);
    state.segmentMaps.set(key, map);
}

function getRememberedSegmentUrl(configKey, parentPlaylistUrl, identity) {
    if (!settings.SEGMENT_TOKEN_HEALING || !identity) return "";
    return state.segmentMaps.get(segmentMapKey(configKey, parentPlaylistUrl))?.get(identity) || "";
}

function manifestProxyUrl(host, routeKey, url) {
    return `${routeBase(host, routeKey)}/proxy/live.m3u8?u=${encodeBase64Url(url)}`;
}

function segmentProxyUrl(host, routeKey, url, parentPlaylistUrl = "") {
    const params = new URLSearchParams({ u: encodeBase64Url(url) });
    if (settings.SEGMENT_TOKEN_HEALING && parentPlaylistUrl && isHttpUrl(parentPlaylistUrl)) {
        params.set("p", encodeBase64Url(parentPlaylistUrl));
        params.set("s", segmentIdentity(url));
    }
    return `${routeBase(host, routeKey)}/proxy/seg?${params.toString()}`;
}

function rewriteUriAttributes(line, baseUrl, makeUrl) {
    return line.replace(/URI=(["'])(.*?)\1/gi, (_match, quote, uri) => {
        return `URI=${quote}${makeUrl(toAbsoluteUrl(uri, baseUrl))}${quote}`;
    });
}

function rewriteManifest(text, baseUrl, host, configKey, parentPlaylistUrl = baseUrl, routeKey = configKey) {
    const isMaster = /#EXT-X-STREAM-INF/i.test(text);
    const rewriteTagUrl = url => isHlsUrl(url)
        ? manifestProxyUrl(host, routeKey, url)
        : segmentProxyUrl(host, routeKey, url, parentPlaylistUrl);

    const output = [];
    for (const line of String(text || "").split(/\r?\n/)) {
        const trimmed = line.trim();
        if (!trimmed) {
            output.push(line);
            continue;
        }

        if (trimmed.startsWith("#")) {
            output.push(/URI=/i.test(trimmed) ? rewriteUriAttributes(line, baseUrl, rewriteTagUrl) : line);
            continue;
        }

        const absolute = toAbsoluteUrl(trimmed, baseUrl);
        if (isMaster || isHlsUrl(absolute)) {
            output.push(manifestProxyUrl(host, routeKey, absolute));
            continue;
        }

        rememberSegmentUrl(configKey, parentPlaylistUrl, absolute);
        output.push(segmentProxyUrl(host, routeKey, absolute, parentPlaylistUrl));
    }

    return output.join("\n");
}

async function fetchSegmentStream(upstream, headers) {
    return retryOperation("segment", settings.SEGMENT_UPSTREAM_RETRIES, settings.SEGMENT_UPSTREAM_RETRY_DELAY_MS, async attempt => {
        const response = await axios.get(upstream, {
            responseType: "stream",
            timeout: settings.SEG_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers,
            ...upstreamAgentOptions(),
            decompress: false,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            validateStatus: status => status >= 200 && status < 300
        });
        if (attempt > 0) console.warn(`[PROXY SEG RECOVERED] attempt=${attempt + 1}`);
        return response;
    });
}

function isRecoverableSegmentError(err) {
    const status = Number(err?.response?.status || 0);
    if ([401, 403, 404, 407, 408, 410, 412, 425, 429].includes(status)) return true;
    if (status >= 500 && status <= 599) return true;
    return ["ECONNABORTED", "ECONNRESET", "EPIPE", "ETIMEDOUT"].includes(err?.code);
}

async function fetchSegmentWithHealing(configKey, routeKey, upstream, headers, req) {
    try {
        return await fetchSegmentStream(upstream, headers);
    } catch (err) {
        const parentPlaylistUrl = decodeBase64Url(req.query.p || "");
        const identity = String(req.query.s || segmentIdentity(upstream));
        if (!settings.SEGMENT_TOKEN_HEALING || !isHttpUrl(parentPlaylistUrl) || !isRecoverableSegmentError(err)) throw err;

        try {
            await getRewrittenManifest(configKey, parentPlaylistUrl, getPublicHost(req), routeKey);
            const healedUrl = getRememberedSegmentUrl(configKey, parentPlaylistUrl, identity);
            if (healedUrl && healedUrl !== upstream) {
                console.warn(`[PROXY SEG HEALED] status=${err?.response?.status || err?.code || "error"}`);
                return fetchSegmentStream(healedUrl, headers);
            }
        } catch (healErr) {
            console.warn(`[PROXY SEG HEAL FAILED] ${healErr.message}`);
        }

        throw err;
    }
}

function copyResponseHeaders(upstreamResponse, res) {
    [
        "content-type",
        "content-length",
        "content-range",
        "accept-ranges",
        "last-modified",
        "etag",
        "content-encoding"
    ].forEach(header => {
        if (upstreamResponse.headers[header]) res.setHeader(header, upstreamResponse.headers[header]);
    });
}

module.exports = {
    RELAY_HEADERS,
    copyResponseHeaders,
    decodeProxyUrl: decodeBase64Url,
    fetchSegmentWithHealing,
    getRewrittenManifest,
    manifestProxyUrl,
    rewriteManifest,
    segmentIdentity,
    segmentProxyUrl,
    setPlaylistHeaders
};
