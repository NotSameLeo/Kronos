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
    redactUrl,
    routeBase,
    sleep,
    toAbsoluteUrl,
    upstreamAgentOptions
} = require("./utils");

const RELAY_HEADERS = {
    "User-Agent": settings.UPSTREAM_UA,
    "Accept": "*/*",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
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
        const startedAt = Date.now();
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
        return {
            data,
            finalUrl: response.request?.res?.responseUrl || upstream,
            status: response.status,
            upstreamMs: Date.now() - startedAt
        };
    });
}

async function fetchCoalescedUpstreamManifest(cacheKey, upstream) {
    const coalesceMs = Number(settings.HLS_MANIFEST_COALESCE_MS || 0);
    if (coalesceMs <= 0) {
        const fetched = await fetchUpstreamManifest(upstream);
        return { ...fetched, manifestCache: "off", cacheAgeMs: 0 };
    }

    const recent = state.manifestRawRecent.get(cacheKey);
    const ageMs = recent ? Date.now() - recent.savedAt : Infinity;
    if (recent && ageMs >= 0 && ageMs <= coalesceMs) {
        return {
            data: recent.data,
            finalUrl: recent.finalUrl,
            status: recent.status,
            upstreamMs: 0,
            manifestCache: "hit",
            cacheAgeMs: ageMs
        };
    }

    const inFlight = state.manifestRawInflight.get(cacheKey);
    if (inFlight) {
        const fetched = await inFlight;
        return { ...fetched, manifestCache: "join", cacheAgeMs: 0 };
    }

    const promise = fetchUpstreamManifest(upstream).then(fetched => {
        rememberRecentRawManifest(cacheKey, fetched);
        return { ...fetched, manifestCache: "miss", cacheAgeMs: 0 };
    });
    state.manifestRawInflight.set(cacheKey, promise);
    try {
        return await promise;
    } finally {
        state.manifestRawInflight.delete(cacheKey);
    }
}

async function getRewrittenManifest(configKey, upstream, host, routeKey = configKey, req = null) {
    const key = hashKey(`${configKey}|${host}|${routeKey}|manifest|${upstream}`);
    if (state.manifestInflight.has(key)) return state.manifestInflight.get(key);
    const rawCacheKey = hashKey(`${configKey}|${routeKey}|raw-manifest|${upstream}`);

    const promise = (async () => {
        const startedAt = Date.now();
        const blockOfflinePlaceholders = req?.query?.pg !== "0";
        const liveEdgeDelaySeconds = liveEdgeDelayFromRequest(req);
        const startOffsetSeconds = startOffsetFromRequest(req);
        const holdBackSeconds = holdBackFromRequest(req);

        try {
            const fetched = await fetchCoalescedUpstreamManifest(rawCacheKey, upstream);
            const analysis = analyzeManifest(fetched.data, fetched.finalUrl);
            if (blockOfflinePlaceholders && isOfflinePlaceholderManifest(analysis)) {
                logManifestEvent({
                    req,
                    configKey,
                    routeKey,
                    upstream,
                    finalUrl: fetched.finalUrl,
                    status: fetched.status,
                    upstreamMs: fetched.upstreamMs,
                    rewriteMs: 0,
                    totalMs: Date.now() - startedAt,
                    bytes: fetched.data.length,
                    analysis,
                    blocked: true,
                    manifestCache: fetched.manifestCache,
                    cacheAgeMs: fetched.cacheAgeMs
                });
                const err = new Error("Upstream returned short offline placeholder");
                err.statusCode = 503;
                err.retryAfter = 2;
                throw err;
            }

            const rewriteStartedAt = Date.now();
            const manifestNonce = settings.HLS_CACHE_BUST_SEGMENTS
                ? hashKey(`${Date.now()}|${Math.random()}|${fetched.finalUrl}`, 12)
                : "";
            const text = rewriteManifest(
                fetched.data,
                fetched.finalUrl,
                host,
                configKey,
                upstream,
                routeKey,
                manifestNonce,
                blockOfflinePlaceholders,
                liveEdgeDelaySeconds,
                startOffsetSeconds,
                holdBackSeconds
            );
            logManifestEvent({
                req,
                configKey,
                routeKey,
                upstream,
                finalUrl: fetched.finalUrl,
                status: fetched.status,
                upstreamMs: fetched.upstreamMs,
                rewriteMs: Date.now() - rewriteStartedAt,
                totalMs: Date.now() - startedAt,
                bytes: fetched.data.length,
                analysis,
                manifestCache: fetched.manifestCache,
                cacheAgeMs: fetched.cacheAgeMs,
                holdBackSeconds
            });
            rememberLastGoodManifest(key, { text, analysis, upstream, routeKey });
            return { text, analysis, stale: false };
        } catch (err) {
            const stale = getLastGoodManifest(key);
            if (!stale) throw err;
            logStaleManifest({
                req,
                routeKey,
                configKey,
                upstream,
                err,
                ageMs: Date.now() - stale.savedAt,
                totalMs: Date.now() - startedAt,
                analysis: stale.analysis
            });
            return {
                text: stale.text,
                analysis: stale.analysis,
                stale: true,
                staleAgeMs: Date.now() - stale.savedAt
            };
        }
    })();

    state.manifestInflight.set(key, promise);
    try {
        return await promise;
    } finally {
        state.manifestInflight.delete(key);
    }
}

function rememberRecentRawManifest(key, fetched) {
    if (!settings.HLS_MANIFEST_COALESCE_MS) return;
    state.manifestRawRecent.set(key, {
        data: fetched.data,
        finalUrl: fetched.finalUrl,
        status: fetched.status,
        savedAt: Date.now()
    });
    while (state.manifestRawRecent.size > 100) {
        state.manifestRawRecent.delete(state.manifestRawRecent.keys().next().value);
    }
}

function rememberLastGoodManifest(key, entry) {
    if (!settings.HLS_STALE_MANIFEST_MAX_MS) return;
    state.manifestLastGood.set(key, {
        ...entry,
        savedAt: Date.now()
    });
    while (state.manifestLastGood.size > 200) {
        state.manifestLastGood.delete(state.manifestLastGood.keys().next().value);
    }
}

function getLastGoodManifest(key) {
    if (!settings.HLS_STALE_MANIFEST_MAX_MS) return null;
    const entry = state.manifestLastGood.get(key);
    if (!entry) return null;
    if (Date.now() - entry.savedAt > settings.HLS_STALE_MANIFEST_MAX_MS) {
        state.manifestLastGood.delete(key);
        return null;
    }
    return entry;
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

function segmentMetadataMapKey(configKey, parentPlaylistUrl) {
    return hashKey(`${configKey}|metadata|${parentPlaylistUrl}`);
}

function rememberSegmentMetadata(configKey, parentPlaylistUrl, metadata) {
    if (!settings.HLS_DIAGNOSTICS || !isHttpUrl(parentPlaylistUrl) || !metadata?.identity) return;
    const key = segmentMetadataMapKey(configKey, parentPlaylistUrl);
    const map = state.segmentMetadata.get(key) || new Map();
    map.set(metadata.identity, { ...metadata, seenAt: Date.now() });
    while (map.size > 800) map.delete(map.keys().next().value);
    state.segmentMetadata.set(key, map);
}

function getSegmentMetadata(configKey, parentPlaylistUrl, identity) {
    if (!settings.HLS_DIAGNOSTICS || !identity) return null;
    return state.segmentMetadata.get(segmentMetadataMapKey(configKey, parentPlaylistUrl))?.get(identity) || null;
}

function manifestProxyUrl(host, routeKey, url, blockOfflinePlaceholders = true, liveEdgeDelaySeconds = null, startOffsetSeconds = null, holdBackSeconds = null) {
    const params = new URLSearchParams({
        u: encodeBase64Url(url),
        pg: blockOfflinePlaceholders ? "1" : "0"
    });
    if (Number.isFinite(liveEdgeDelaySeconds) && liveEdgeDelaySeconds > 0) {
        params.set("d", String(Math.round(liveEdgeDelaySeconds)));
    }
    if (Number.isFinite(startOffsetSeconds) && startOffsetSeconds > 0) {
        params.set("st", String(Math.round(startOffsetSeconds)));
    }
    if (Number.isFinite(holdBackSeconds) && holdBackSeconds > 0) {
        params.set("hb", String(Math.round(holdBackSeconds)));
    }
    return `${routeBase(host, routeKey)}/proxy/live.m3u8?${params.toString()}`;
}

function segmentProxyUrl(host, routeKey, url, parentPlaylistUrl = "", manifestNonce = "") {
    const params = new URLSearchParams({ u: encodeBase64Url(url) });
    if ((settings.SEGMENT_TOKEN_HEALING || settings.HLS_DIAGNOSTICS) && parentPlaylistUrl && isHttpUrl(parentPlaylistUrl)) {
        params.set("p", encodeBase64Url(parentPlaylistUrl));
        params.set("s", segmentIdentity(url));
    }
    if (settings.HLS_CACHE_BUST_SEGMENTS && manifestNonce) params.set("m", manifestNonce);
    return `${routeBase(host, routeKey)}/proxy/seg?${params.toString()}`;
}

function rewriteUriAttributes(line, baseUrl, makeUrl) {
    return line.replace(/URI=(["'])(.*?)\1/gi, (_match, quote, uri) => {
        return `URI=${quote}${makeUrl(toAbsoluteUrl(uri, baseUrl))}${quote}`;
    });
}

function rewriteManifest(text, baseUrl, host, configKey, parentPlaylistUrl = baseUrl, routeKey = configKey, manifestNonce = "", blockOfflinePlaceholders = true, liveEdgeDelaySeconds = null, startOffsetSeconds = null, holdBackSeconds = null) {
    const isMaster = /#EXT-X-STREAM-INF/i.test(text);
    const endList = /#EXT-X-ENDLIST/i.test(text);
    const mediaSequence = readNumericTag(text, "#EXT-X-MEDIA-SEQUENCE");
    let mediaIndex = 0;
    const rewriteTagUrl = url => isHlsUrl(url)
        ? manifestProxyUrl(host, routeKey, url, blockOfflinePlaceholders, liveEdgeDelaySeconds, startOffsetSeconds, holdBackSeconds)
        : segmentProxyUrl(host, routeKey, url, parentPlaylistUrl, manifestNonce);

    const output = [];
    let pendingDuration = null;
    for (const line of String(text || "").split(/\r?\n/)) {
        const trimmed = line.trim();
        if (!trimmed) {
            output.push(line);
            continue;
        }

        if (trimmed.startsWith("#")) {
            if (/^#EXTINF:/i.test(trimmed)) pendingDuration = readExtinfDuration(trimmed);
            output.push(/URI=/i.test(trimmed) ? rewriteUriAttributes(line, baseUrl, rewriteTagUrl) : line);
            continue;
        }

        const absolute = toAbsoluteUrl(trimmed, baseUrl);
        if (isMaster || isHlsUrl(absolute)) {
            output.push(manifestProxyUrl(host, routeKey, absolute, blockOfflinePlaceholders, liveEdgeDelaySeconds, startOffsetSeconds, holdBackSeconds));
            continue;
        }

        rememberSegmentUrl(configKey, parentPlaylistUrl, absolute);
        const sequence = Number.isFinite(mediaSequence) ? mediaSequence + mediaIndex : null;
        rememberSegmentMetadata(configKey, parentPlaylistUrl, {
            identity: segmentIdentity(absolute),
            sequence,
            mediaIndex,
            duration: pendingDuration,
            urlHash: hashKey(absolute, 12)
        });
        mediaIndex++;
        pendingDuration = null;
        output.push(segmentProxyUrl(host, routeKey, absolute, parentPlaylistUrl, manifestNonce));
    }

    const delayed = applyLiveEdgeDelay(output.join("\n"), {
        enabled: !isMaster,
        endList,
        segmentCount: mediaIndex,
        delaySeconds: liveEdgeDelaySeconds
    });
    const holdBack = applyServerControlHoldBack(delayed, {
        enabled: !isMaster && !endList,
        holdBackSeconds
    });
    return applyStartOffset(holdBack, {
        enabled: !isMaster && !endList,
        offsetSeconds: startOffsetSeconds
    });
}

function applyLiveEdgeDelay(text, options = {}) {
    const delaySeconds = Number.isFinite(options.delaySeconds)
        ? options.delaySeconds
        : settings.HLS_LIVE_EDGE_DELAY_SECONDS;
    if (!options.enabled || options.endList || delaySeconds <= 0) return text;
    const minSegments = settings.HLS_LIVE_EDGE_MIN_SEGMENTS;
    if ((options.segmentCount || 0) <= minSegments) return text;

    const lines = String(text || "").split(/\r?\n/);
    const segments = [];
    let currentExtinf = -1;
    for (let index = 0; index < lines.length; index++) {
        const trimmed = lines[index].trim();
        if (/^#EXTINF:/i.test(trimmed)) {
            currentExtinf = index;
            continue;
        }
        if (!trimmed || trimmed.startsWith("#")) continue;
        if (!trimmed.includes("/proxy/seg?") || currentExtinf < 0) continue;
        segments.push({
            start: currentExtinf,
            end: index,
            duration: readExtinfDuration(lines[currentExtinf]) || 0
        });
        currentExtinf = -1;
    }

    if (segments.length <= minSegments) return text;
    let delayedSeconds = 0;
    let removeCount = 0;
    for (let index = segments.length - 1; index >= minSegments; index--) {
        delayedSeconds += segments[index].duration || 0;
        removeCount++;
        if (delayedSeconds >= delaySeconds) break;
    }
    if (!removeCount) return text;

    const remove = new Set();
    for (const segment of segments.slice(-removeCount)) {
        for (let index = segment.start; index <= segment.end; index++) remove.add(index);
    }
    return lines.filter((_line, index) => !remove.has(index)).join("\n");
}

function applyStartOffset(text, options = {}) {
    const offsetSeconds = Number(options.offsetSeconds || 0);
    if (!options.enabled || offsetSeconds <= 0 || /#EXT-X-START:/i.test(text)) return text;
    const lines = String(text || "").split(/\r?\n/);
    const insertAt = playlistHeaderInsertIndex(lines);
    lines.splice(insertAt, 0, `#EXT-X-START:TIME-OFFSET=-${Math.round(offsetSeconds)},PRECISE=NO`);
    return lines.join("\n");
}

function applyServerControlHoldBack(text, options = {}) {
    const holdBackSeconds = Number(options.holdBackSeconds || 0);
    if (!options.enabled || holdBackSeconds <= 0) return text;
    const rounded = Math.round(holdBackSeconds * 1000) / 1000;
    const lines = String(text || "").split(/\r?\n/);
    const existing = lines.findIndex(line => /^#EXT-X-SERVER-CONTROL:/i.test(line.trim()));
    if (existing >= 0) {
        if (/HOLD-BACK=/i.test(lines[existing])) return text;
        lines[existing] = `${lines[existing]},HOLD-BACK=${rounded}`;
        return lines.join("\n");
    }
    const versionIndex = lines.findIndex(line => /^#EXT-X-VERSION:/i.test(line.trim()));
    const insertAt = versionIndex >= 0 ? versionIndex + 1 : playlistHeaderInsertIndex(lines);
    lines.splice(insertAt, 0, `#EXT-X-SERVER-CONTROL:HOLD-BACK=${rounded}`);
    return lines.join("\n");
}

function playlistHeaderInsertIndex(lines) {
    let index = lines[0]?.trim() === "#EXTM3U" ? 1 : 0;
    while (index < lines.length && /^#EXT-X-(VERSION|SERVER-CONTROL|INDEPENDENT-SEGMENTS):/i.test(lines[index].trim())) {
        index++;
    }
    return index;
}

function liveEdgeDelayFromRequest(req) {
    const value = Number(req?.query?.d);
    if (!Number.isFinite(value)) return null;
    return Math.max(0, Math.min(180, value));
}

function startOffsetFromRequest(req) {
    const value = Number(req?.query?.st);
    if (!Number.isFinite(value)) return null;
    return Math.max(0, Math.min(60, value));
}

function holdBackFromRequest(req) {
    const value = Number(req?.query?.hb);
    if (!Number.isFinite(value)) return null;
    return Math.max(0, Math.min(60, value));
}

function readNumericTag(text, tag) {
    const pattern = new RegExp(`^${tag.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:(\\d+)`, "im");
    const value = Number((String(text || "").match(pattern) || [])[1]);
    return Number.isFinite(value) ? value : null;
}

function readExtinfDuration(line) {
    const value = Number((String(line || "").match(/^#EXTINF:([0-9.]+)/i) || [])[1]);
    return Number.isFinite(value) ? value : null;
}

function analyzeManifest(text, baseUrl) {
    const raw = String(text || "");
    const lines = raw.split(/\r?\n/);
    const isMaster = /#EXT-X-STREAM-INF/i.test(raw);
    const mediaSequence = readNumericTag(raw, "#EXT-X-MEDIA-SEQUENCE");
    const targetDuration = readNumericTag(raw, "#EXT-X-TARGETDURATION");
    const discontinuitySequence = readNumericTag(raw, "#EXT-X-DISCONTINUITY-SEQUENCE");
    let mediaIndex = 0;
    let segmentCount = 0;
    let variantCount = 0;
    let keyCount = 0;
    let mapCount = 0;
    let totalDuration = 0;
    let pendingDuration = null;
    let firstSegment = null;
    let lastSegment = null;
    let firstVariant = null;
    let lastVariant = null;

    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        if (/^#EXT-X-KEY/i.test(trimmed)) keyCount++;
        if (/^#EXT-X-MAP/i.test(trimmed)) mapCount++;
        if (/^#EXTINF:/i.test(trimmed)) {
            pendingDuration = readExtinfDuration(trimmed);
            continue;
        }
        if (trimmed.startsWith("#")) continue;

        const absolute = toAbsoluteUrl(trimmed, baseUrl);
        if (isMaster || isHlsUrl(absolute)) {
            variantCount++;
            const item = { urlHash: hashKey(absolute, 12), path: safePath(absolute) };
            firstVariant ||= item;
            lastVariant = item;
            continue;
        }

        const sequence = Number.isFinite(mediaSequence) ? mediaSequence + mediaIndex : null;
        const item = {
            identity: segmentIdentity(absolute),
            sequence,
            mediaIndex,
            duration: pendingDuration,
            urlHash: hashKey(absolute, 12),
            path: safePath(absolute)
        };
        segmentCount++;
        mediaIndex++;
        if (Number.isFinite(pendingDuration)) totalDuration += pendingDuration;
        firstSegment ||= item;
        lastSegment = item;
        pendingDuration = null;
    }

    return {
        kind: isMaster ? "master" : "media",
        mediaSequence,
        discontinuitySequence,
        targetDuration,
        segmentCount,
        variantCount,
        keyCount,
        mapCount,
        totalDuration: Math.round(totalDuration * 1000) / 1000,
        firstSegment,
        lastSegment,
        firstVariant,
        lastVariant,
        endList: /#EXT-X-ENDLIST/i.test(raw)
    };
}

function isOfflinePlaceholderManifest(analysis) {
    if (!settings.HLS_BLOCK_OFFLINE_PLACEHOLDERS) return false;
    if (!analysis || analysis.kind !== "media") return false;
    if (!analysis.endList) return false;
    if ((analysis.segmentCount || 0) < 1 || (analysis.segmentCount || 0) > 2) return false;
    return Number(analysis.totalDuration || 0) <= settings.HLS_OFFLINE_PLACEHOLDER_MAX_SECONDS;
}

function logManifestEvent(event) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const analysis = event.analysis || {};
    const first = analysis.firstSegment || analysis.firstVariant || null;
    const last = analysis.lastSegment || analysis.lastVariant || null;
    const fields = {
        route: shortValue(event.routeKey),
        cfg: hashKey(event.configKey, 10),
        kind: analysis.kind,
        status: event.status,
        upstreamMs: event.upstreamMs,
        rewriteMs: event.rewriteMs,
        totalMs: event.totalMs,
        bytes: event.bytes,
        mediaSeq: valueOrDash(analysis.mediaSequence),
        discSeq: valueOrDash(analysis.discontinuitySequence),
        target: valueOrDash(analysis.targetDuration),
        segs: analysis.segmentCount || 0,
        variants: analysis.variantCount || 0,
        keys: analysis.keyCount || 0,
        maps: analysis.mapCount || 0,
        duration: valueOrDash(analysis.totalDuration),
        firstSeq: valueOrDash(first?.sequence),
        lastSeq: valueOrDash(last?.sequence),
        first: first?.urlHash || "-",
        last: last?.urlHash || "-",
        live: analysis.endList ? 0 : 1,
        blocked: event.blocked ? 1 : 0,
        cache: event.manifestCache || "-",
        cacheAgeMs: valueOrDash(event.cacheAgeMs),
        holdBack: valueOrDash(event.holdBackSeconds),
        playerPositionKnown: 0,
        playlist: hashKey(event.upstream, 12),
        final: hashKey(event.finalUrl, 12),
        ip: clientAddress(event.req),
        ua: compactUserAgent(event.req)
    };
    if (settings.HLS_DIAGNOSTIC_URLS) {
        fields.upstream = redactUrl(event.upstream);
        fields.finalUrl = redactUrl(event.finalUrl);
    }
    console.log(`[HLS MANIFEST] ${formatFields(fields)}`);
}

function logStaleManifest(event) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const analysis = event.analysis || {};
    const fields = {
        route: shortValue(event.routeKey),
        cfg: hashKey(event.configKey, 10),
        reason: manifestErrorReason(event.err),
        status: event.err?.response?.status || "-",
        code: event.err?.code || "-",
        staleAgeMs: event.ageMs,
        totalMs: event.totalMs,
        mediaSeq: valueOrDash(analysis.mediaSequence),
        lastSeq: valueOrDash(analysis.lastSegment?.sequence),
        playlist: hashKey(event.upstream, 12),
        ip: clientAddress(event.req),
        ua: compactUserAgent(event.req)
    };
    console.warn(`[HLS MANIFEST STALE] ${formatFields(fields)}`);
}

function logSegmentRequest(context) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        cfg: hashKey(context.configKey, 10),
        fmt: context.streamFormat,
        urlExt: context.urlExtension,
        seg: context.metadata?.urlHash || hashKey(context.upstream, 12),
        seq: valueOrDash(context.metadata?.sequence),
        idx: valueOrDash(context.metadata?.mediaIndex),
        dur: valueOrDash(context.metadata?.duration),
        movement: context.movement,
        delta: valueOrDash(context.sequenceDelta),
        sincePrevMs: valueOrDash(context.sincePreviousMs),
        requests: context.session.requestCount,
        repeats: context.session.repeats,
        backtracks: context.session.backtracks,
        range: context.range || "-",
        parent: context.parentPlaylistUrl ? hashKey(context.parentPlaylistUrl, 12) : "-",
        playerPositionKnown: 0,
        ip: clientAddress(context.req),
        ua: compactUserAgent(context.req)
    };
    if (settings.HLS_DIAGNOSTIC_URLS) fields.url = redactUrl(context.upstream);
    if (settings.HLS_DIAGNOSTIC_HEADERS) fields.headers = headerSummary(context.req);
    console.log(`[HLS SEG REQ] ${formatFields(fields)}`);
}

function logSegmentResult(context, response, options = {}) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        fmt: context.streamFormat,
        urlExt: context.urlExtension,
        seg: context.metadata?.urlHash || hashKey(context.upstream, 12),
        seq: valueOrDash(context.metadata?.sequence),
        movement: context.movement,
        status: response.status,
        upstreamMs: response.kronosUpstreamMs,
        attempt: response.kronosAttempt,
        healed: options.healed ? 1 : 0,
        type: response.headers["content-type"] || "-",
        len: response.headers["content-length"] || "-",
        contentRange: response.headers["content-range"] || "-",
        acceptRanges: response.headers["accept-ranges"] || "-",
        cache: response.headers["cache-control"] || "-",
        healedUrl: context.healedUrlHash || "-"
    };
    console.log(`[HLS SEG OK] ${formatFields(fields)}`);
}

function logSegmentTransfer(context, info) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const expected = Number(info.expectedBytes || 0);
    const sent = Number(info.bytesSent || 0);
    const completeByBytes = expected > 0 ? sent >= expected : info.upstreamEnded && info.responseFinished;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        fmt: context.streamFormat,
        urlExt: context.urlExtension,
        seg: context.metadata?.urlHash || hashKey(context.upstream, 12),
        seq: valueOrDash(context.metadata?.sequence),
        movement: context.movement,
        bytesSent: sent,
        expectedBytes: expected || "-",
        complete: completeByBytes ? 1 : 0,
        responseFinished: info.responseFinished ? 1 : 0,
        clientClosedBeforeEnd: info.clientClosedBeforeEnd ? 1 : 0,
        upstreamEnded: info.upstreamEnded ? 1 : 0,
        upstreamError: info.upstreamError ? 1 : 0,
        durationMs: info.durationMs,
        kbps: info.durationMs > 0 ? Math.round((sent * 8) / info.durationMs) : "-"
    };
    console.log(`[HLS SEG SENT] ${formatFields(fields)}`);
    if (info.clientClosedBeforeEnd && !completeByBytes) {
        logPlayerDisconnect(context, {
            reason: info.reason || "client-close",
            bytesSent: sent,
            expectedBytes: expected || "-",
            durationMs: info.durationMs,
            upstreamEnded: info.upstreamEnded ? 1 : 0,
            upstreamError: info.upstreamError ? 1 : 0
        });
    }
}

function logPlayerDisconnect(context, info = {}) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        reason: info.reason || "client-close",
        fmt: context.streamFormat,
        urlExt: context.urlExtension,
        parent: context.parentPlaylistUrl ? hashKey(context.parentPlaylistUrl, 12) : "-",
        seg: context.metadata?.urlHash || hashKey(context.upstream, 12),
        seq: valueOrDash(context.metadata?.sequence),
        movement: context.movement,
        bytesSent: valueOrDash(info.bytesSent),
        expectedBytes: valueOrDash(info.expectedBytes),
        durationMs: valueOrDash(info.durationMs),
        upstreamEnded: valueOrDash(info.upstreamEnded),
        upstreamError: valueOrDash(info.upstreamError),
        ip: clientAddress(context.req),
        ua: compactUserAgent(context.req)
    };
    console.warn(`[PLAYER DISCONNECT] ${formatFields(fields)}`);
}

function logSegmentError(context, err, options = {}) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        fmt: context.streamFormat,
        urlExt: context.urlExtension,
        seg: context.metadata?.urlHash || hashKey(context.upstream, 12),
        seq: valueOrDash(context.metadata?.sequence),
        movement: context.movement,
        status: err?.response?.status || "-",
        code: err?.code || "-",
        healedAttempted: options.healedAttempted ? 1 : 0,
        msg: compactMessage(err?.message || "segment error")
    };
    console.warn(`[HLS SEG ERR] ${formatFields(fields)}`);
}

function monitorSegmentTransfer(upstreamResponse, req, res) {
    const context = upstreamResponse?.kronosContext;
    if (!settings.HLS_DIAGNOSTICS || !context || !upstreamResponse?.data) return;

    const startedAt = Date.now();
    let bytesSent = 0;
    let upstreamEnded = false;
    let upstreamError = false;
    let logged = false;
    const expectedBytes = Number(upstreamResponse.headers["content-length"] || 0);

    upstreamResponse.data.on("data", chunk => {
        bytesSent += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk);
    });
    upstreamResponse.data.on("end", () => {
        upstreamEnded = true;
    });
    upstreamResponse.data.on("error", () => {
        upstreamError = true;
    });

    const finish = reason => {
        if (logged) return;
        logged = true;
        const responseFinished = res.writableFinished || res.writableEnded;
        logSegmentTransfer(context, {
            reason,
            bytesSent,
            expectedBytes,
            responseFinished,
            clientClosedBeforeEnd: reason === "close" && !responseFinished,
            upstreamEnded,
            upstreamError,
            durationMs: Date.now() - startedAt
        });
    };

    res.once("finish", () => finish("finish"));
    res.once("close", () => finish("close"));
}

function formatFields(fields) {
    return Object.entries(fields)
        .map(([key, value]) => `${key}=${quoteField(value)}`)
        .join(" ");
}

function quoteField(value) {
    const text = String(value === undefined || value === null || value === "" ? "-" : value);
    return /[\s"]/u.test(text) ? JSON.stringify(text) : text;
}

function valueOrDash(value) {
    return value === undefined || value === null || Number.isNaN(value) ? "-" : value;
}

function compactMessage(value) {
    return String(value || "").replace(/\s+/g, " ").slice(0, 180);
}

function manifestErrorReason(err) {
    if (err?.statusCode === 503) return "blocked-placeholder";
    if (err?.response?.status) return `http-${err.response.status}`;
    if (err?.code) return err.code;
    return compactMessage(err?.message || "manifest-error");
}

function shortValue(value) {
    const text = String(value || "");
    if (!text) return "default";
    return text.length > 24 ? `${text.slice(0, 12)}...${text.slice(-8)}` : text;
}

function userAgent(req) {
    return String(req?.get?.("user-agent") || "");
}

function compactUserAgent(req) {
    return userAgent(req).replace(/\s+/g, " ").slice(0, 90) || "-";
}

function clientAddress(req) {
    const forwarded = String(req?.get?.("x-forwarded-for") || "").split(",")[0].trim();
    return forwarded || req?.ip || req?.socket?.remoteAddress || "-";
}

function headerSummary(req) {
    if (!req) return "-";
    return JSON.stringify({
        range: req.headers.range || "",
        accept: req.headers.accept || "",
        referer: req.headers.referer || "",
        origin: req.headers.origin || ""
    });
}

function safePath(value) {
    try {
        const parsed = new URL(value);
        return parsed.pathname.split("/").filter(Boolean).slice(-2).join("/") || parsed.pathname;
    } catch {
        return "";
    }
}

async function fetchSegmentStream(upstream, headers) {
    return retryOperation("segment", settings.SEGMENT_UPSTREAM_RETRIES, settings.SEGMENT_UPSTREAM_RETRY_DELAY_MS, async attempt => {
        const startedAt = Date.now();
        const segmentHeaders = {
            ...headers,
            Connection: settings.HLS_SEGMENT_UPSTREAM_KEEPALIVE ? "keep-alive" : "close"
        };
        const response = await axios.get(upstream, {
            responseType: "stream",
            timeout: settings.SEG_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers: segmentHeaders,
            ...(settings.HLS_SEGMENT_UPSTREAM_KEEPALIVE ? upstreamAgentOptions() : { httpAgent: false, httpsAgent: false }),
            decompress: false,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            validateStatus: status => status >= 200 && status < 300
        });
        if (attempt > 0) console.warn(`[PROXY SEG RECOVERED] attempt=${attempt + 1}`);
        response.kronosUpstreamMs = Date.now() - startedAt;
        response.kronosAttempt = attempt + 1;
        return response;
    });
}

async function relayLiveTs(configKey, routeKey, upstream, req, res) {
    const startedAt = Date.now();
    const sessionKey = hashKey(`${routeKey}|${clientAddress(req)}|${userAgent(req)}|${upstream}|live-ts`, 16);
    const upstreamHash = hashKey(upstream, 12);
    let clientClosed = false;
    let headersSent = false;
    let reconnects = 0;
    let totalBytes = 0;
    let activeResponse = null;
    let continuity = createTsContinuityState();

    const closeActive = () => closeUpstreamResponse(activeResponse);
    req.once("aborted", () => {
        clientClosed = true;
        closeActive();
    });
    res.once("close", () => {
        clientClosed = true;
        closeActive();
    });

    logLiveTs("START", {
        sid: sessionKey,
        route: shortValue(routeKey),
        cfg: hashKey(configKey, 10),
        src: upstreamHash,
        rangeIgnored: req.headers.range ? 1 : 0,
        ip: clientAddress(req),
        ua: compactUserAgent(req)
    });

    while (!clientClosed && !res.destroyed) {
        const partStartedAt = Date.now();
        let partBytes = 0;
        try {
            activeResponse = await fetchSegmentStream(upstream, RELAY_HEADERS);
            if (!headersSent) {
                res.status(200);
                setLiveTsHeaders(res, activeResponse);
                res.flushHeaders?.();
                headersSent = true;
            }

            if (reconnects > 0 && continuity.lastPcr90k !== null) continuity.waitingForForwardPcr = true;
            const partInfo = await pipeLiveTsPart(activeResponse, res, chunk => {
                const filtered = filterLiveTsChunk(continuity, chunk);
                const size = filtered.length;
                partBytes += size;
                totalBytes += size;
                return filtered;
            });

            logLiveTs("PART", {
                sid: sessionKey,
                route: shortValue(routeKey),
                src: upstreamHash,
                status: activeResponse.status,
                partBytes,
                totalBytes,
                droppedBytes: partInfo.droppedBytes,
                lastPcr: continuity.lastPcr90k ?? "-",
                resumedAtPcr: partInfo.resumedAtPcr ?? "-",
                partMs: Date.now() - partStartedAt,
                reconnects
            });
            closeUpstreamResponse(activeResponse);
            activeResponse = null;
            if (!clientClosed && !res.destroyed) {
                reconnects += 1;
                await sleep(settings.TS_LIVE_RECONNECT_DELAY_MS);
            }
        } catch (err) {
            closeUpstreamResponse(activeResponse);
            activeResponse = null;
            if (clientClosed || res.destroyed) break;
            logLiveTs("ERR", {
                sid: sessionKey,
                route: shortValue(routeKey),
                src: upstreamHash,
                status: err?.response?.status || "-",
                code: err?.code || "-",
                reconnects,
                msg: compactMessage(err?.message || "ts live relay error")
            }, true);
            if (!headersSent) throw err;
            reconnects += 1;
            await sleep(settings.TS_LIVE_ERROR_RETRY_MS);
        }
    }

    closeUpstreamResponse(activeResponse);
    if (!res.destroyed && !res.writableEnded) res.end();
    logLiveTs("END", {
        sid: sessionKey,
        route: shortValue(routeKey),
        src: upstreamHash,
        totalBytes,
        reconnects,
        durationMs: Date.now() - startedAt
    });
}

function pipeLiveTsPart(upstreamResponse, res, onChunk) {
    return new Promise((resolve, reject) => {
        let settled = false;
        const stream = upstreamResponse?.data;
        const info = { droppedBytes: 0, resumedAtPcr: null };
        const settle = (fn, value) => {
            if (settled) return;
            settled = true;
            res.off("drain", onDrain);
            res.off("close", onClose);
            stream?.off?.("data", onData);
            stream?.off?.("end", onEnd);
            stream?.off?.("error", onError);
            fn(value);
        };
        const onData = chunk => {
            const filtered = onChunk(chunk, info);
            info.droppedBytes += chunk.length - filtered.length;
            if (!filtered.length) return;
            info.resumedAtPcr ??= readLastPcr90k(filtered);
            if (!res.write(filtered)) stream.pause();
        };
        const onDrain = () => stream?.resume?.();
        const onEnd = () => settle(resolve, info);
        const onClose = () => settle(resolve, info);
        const onError = err => settle(reject, err);

        res.on("drain", onDrain);
        res.once("close", onClose);
        stream.on("data", onData);
        stream.once("end", onEnd);
        stream.once("error", onError);
    });
}

function createTsContinuityState() {
    return {
        pending: Buffer.alloc(0),
        lastPcr90k: null,
        waitingForForwardPcr: false
    };
}

function filterLiveTsChunk(state, chunk) {
    const input = Buffer.concat([state.pending, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
    const packetCount = Math.floor(input.length / 188);
    state.pending = input.subarray(packetCount * 188);
    if (!packetCount) return Buffer.alloc(0);

    const output = [];
    for (let index = 0; index < packetCount; index++) {
        const packet = input.subarray(index * 188, (index + 1) * 188);
        if (packet[0] !== 0x47) {
            state.pending = Buffer.alloc(0);
            continue;
        }

        const pcr90k = readPacketPcr90k(packet);
        if (state.waitingForForwardPcr) {
            if (pcr90k === null || !isForwardPcr(pcr90k, state.lastPcr90k)) continue;
            state.waitingForForwardPcr = false;
        }

        if (pcr90k !== null) state.lastPcr90k = pcr90k;
        output.push(packet);
    }

    return output.length ? Buffer.concat(output) : Buffer.alloc(0);
}

function isForwardPcr(next, previous) {
    if (previous === null) return true;
    const max = 2 ** 33;
    const diff = next >= previous ? next - previous : (max - previous) + next;
    return diff > 0 && diff < max / 2;
}

function readLastPcr90k(buffer) {
    let last = null;
    const packetCount = Math.floor(buffer.length / 188);
    for (let index = 0; index < packetCount; index++) {
        const pcr = readPacketPcr90k(buffer.subarray(index * 188, (index + 1) * 188));
        if (pcr !== null) last = pcr;
    }
    return last;
}

function readPacketPcr90k(packet) {
    if (packet.length < 188 || packet[0] !== 0x47) return null;
    const adaptationControl = (packet[3] >> 4) & 0x03;
    if (adaptationControl !== 2 && adaptationControl !== 3) return null;
    const adaptationLength = packet[4];
    if (adaptationLength < 7 || packet.length < 12) return null;
    const flags = packet[5];
    if ((flags & 0x10) === 0) return null;

    const base = (packet[6] * 2 ** 25)
        + (packet[7] << 17)
        + (packet[8] << 9)
        + (packet[9] << 1)
        + ((packet[10] & 0x80) >> 7);
    return base;
}

function setLiveTsHeaders(res, upstreamResponse) {
    const contentType = upstreamResponse?.headers?.["content-type"] || "video/mp2t";
    res.setHeader("Content-Type", String(contentType).includes("video") ? contentType : "video/mp2t");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.setHeader("Surrogate-Control", "no-store");
    res.setHeader("X-Accel-Buffering", "no");
    res.setHeader("X-Kronos-Relay", "1");
    res.setHeader("X-Kronos-Live-Ts", "1");
    res.setHeader("Accept-Ranges", "none");
    res.removeHeader("Content-Length");
    res.removeHeader("Content-Range");
    res.removeHeader("ETag");
    res.removeHeader("Last-Modified");
}

function logLiveTs(label, fields, warn = false) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const line = `[TS LIVE ${label}] ${formatFields(fields)}`;
    if (warn) console.warn(line);
    else console.log(line);
}

function closeUpstreamResponse(upstreamResponse) {
    try { upstreamResponse?.data?.destroy?.(); } catch {}
    try { upstreamResponse?.request?.destroy?.(); } catch {}
    try { upstreamResponse?.request?.socket?.destroy?.(); } catch {}
    try { upstreamResponse?.request?.res?.destroy?.(); } catch {}
}

function closePreviousClientUpstream(context) {
    if (!context?.clientStreamKey || !context.parentPlaylistUrl) return;
    const previous = state.activeSegmentUpstreams.get(context.clientStreamKey);
    if (!previous || previous.parentPlaylistUrl === context.parentPlaylistUrl) return;
    closeUpstreamResponse(previous.upstreamResponse);
    state.activeSegmentUpstreams.delete(context.clientStreamKey);
    if (settings.HLS_DIAGNOSTICS) {
        const ageMs = Date.now() - (previous.startedAt || Date.now());
        const prevParent = hashKey(previous.parentPlaylistUrl, 12);
        const nextParent = hashKey(context.parentPlaylistUrl, 12);
        console.warn(`[HLS ZAP CLOSE] sid=${context.sessionKey} prevParent=${prevParent} nextParent=${nextParent} ageMs=${ageMs}`);
        console.warn(`[PLAYER ZAP] ${formatFields({
            sid: context.sessionKey,
            route: shortValue(context.routeKey),
            prevParent,
            nextParent,
            nextSeg: context.metadata?.urlHash || hashKey(context.upstream, 12),
            nextSeq: valueOrDash(context.metadata?.sequence),
            ageMs,
            ip: clientAddress(context.req),
            ua: compactUserAgent(context.req)
        })}`);
    }
}

function registerActiveUpstream(context, upstreamResponse) {
    if (!context?.clientStreamKey || !context.parentPlaylistUrl || !upstreamResponse) return;
    state.activeSegmentUpstreams.set(context.clientStreamKey, {
        parentPlaylistUrl: context.parentPlaylistUrl,
        upstreamResponse,
        startedAt: Date.now()
    });
    trimActiveUpstreams();
}

function releaseActiveUpstream(upstreamResponse) {
    if (!upstreamResponse) return;
    for (const [key, active] of state.activeSegmentUpstreams.entries()) {
        if (active.upstreamResponse === upstreamResponse) state.activeSegmentUpstreams.delete(key);
    }
}

function trimActiveUpstreams() {
    if (state.activeSegmentUpstreams.size <= 100) return;
    const entries = [...state.activeSegmentUpstreams.entries()].sort((a, b) => (a[1].startedAt || 0) - (b[1].startedAt || 0));
    for (const [key, active] of entries.slice(0, state.activeSegmentUpstreams.size - 100)) {
        closeUpstreamResponse(active.upstreamResponse);
        state.activeSegmentUpstreams.delete(key);
    }
}

function isRecoverableSegmentError(err) {
    const status = Number(err?.response?.status || 0);
    if ([401, 403, 404, 407, 408, 410, 412, 425, 429].includes(status)) return true;
    if (status >= 500 && status <= 599) return true;
    return ["ECONNABORTED", "ECONNRESET", "EPIPE", "ETIMEDOUT"].includes(err?.code);
}

async function fetchSegmentWithHealing(configKey, routeKey, upstream, headers, req) {
    const context = buildSegmentContext(configKey, routeKey, upstream, headers, req);
    logSegmentRequest(context);
    closePreviousClientUpstream(context);
    try {
        const response = await fetchSegmentStream(upstream, headers);
        response.kronosContext = context;
        registerActiveUpstream(context, response);
        logSegmentResult(context, response, { healed: false });
        return response;
    } catch (err) {
        const parentPlaylistUrl = decodeBase64Url(req.query.p || "");
        const identity = String(req.query.s || segmentIdentity(upstream));
        if (!settings.SEGMENT_TOKEN_HEALING || !isHttpUrl(parentPlaylistUrl) || !isRecoverableSegmentError(err)) {
            logSegmentError(context, err, { healedAttempted: false });
            throw err;
        }

        try {
            await getRewrittenManifest(configKey, parentPlaylistUrl, getPublicHost(req), routeKey, req);
            const healedUrl = getRememberedSegmentUrl(configKey, parentPlaylistUrl, identity);
            if (healedUrl && healedUrl !== upstream) {
                console.warn(`[PROXY SEG HEALED] status=${err?.response?.status || err?.code || "error"}`);
                const healedResponse = await fetchSegmentStream(healedUrl, headers);
                const healedContext = { ...context, healedUrlHash: hashKey(healedUrl, 12) };
                healedResponse.kronosContext = healedContext;
                logSegmentResult(healedContext, healedResponse, { healed: true });
                return healedResponse;
            }
        } catch (healErr) {
            console.warn(`[PROXY SEG HEAL FAILED] ${healErr.message}`);
        }

        logSegmentError(context, err, { healedAttempted: true });
        throw err;
    }
}

function buildSegmentContext(configKey, routeKey, upstream, headers, req) {
    const parentPlaylistUrl = decodeBase64Url(req.query.p || "");
    const identity = String(req.query.s || segmentIdentity(upstream));
    const metadata = getSegmentMetadata(configKey, parentPlaylistUrl, identity);
    const urlExtension = inferUrlExtension(upstream);
    const streamFormat = inferSegmentStreamFormat(upstream, parentPlaylistUrl, metadata);
    const clientStreamKey = hashKey(`${routeKey}|${clientAddress(req)}|${userAgent(req)}`, 16);
    const sessionKey = hashKey(`${routeKey}|${clientAddress(req)}|${userAgent(req)}|${parentPlaylistUrl}`, 16);
    const previous = state.playbackSessions.get(sessionKey) || null;
    const now = Date.now();
    const sequence = Number.isFinite(metadata?.sequence) ? metadata.sequence : null;
    let movement = "unknown";
    let sequenceDelta = null;
    if (Number.isFinite(sequence) && Number.isFinite(previous?.lastSequence)) {
        sequenceDelta = sequence - previous.lastSequence;
        if (sequenceDelta > 0) movement = "forward";
        else if (sequenceDelta === 0) movement = "repeat";
        else movement = "backtrack";
    } else if (previous?.lastIdentity && previous.lastIdentity === identity) {
        movement = "repeat";
    }

    const nextSession = {
        lastIdentity: identity,
        lastSequence: sequence,
        lastAt: now,
        requestCount: (previous?.requestCount || 0) + 1,
        repeats: (previous?.repeats || 0) + (movement === "repeat" ? 1 : 0),
        backtracks: (previous?.backtracks || 0) + (movement === "backtrack" ? 1 : 0)
    };
    state.playbackSessions.set(sessionKey, nextSession);
    trimPlaybackSessions();

    return {
        configKey,
        routeKey,
        upstream,
        parentPlaylistUrl,
        streamFormat,
        urlExtension,
        clientStreamKey,
        identity,
        metadata,
        sessionKey,
        previous,
        session: nextSession,
        movement,
        sequenceDelta,
        range: headers.Range || "",
        sincePreviousMs: previous?.lastAt ? now - previous.lastAt : null,
        req
    };
}

function inferSegmentStreamFormat(upstream, parentPlaylistUrl, metadata) {
    if (metadata || parentPlaylistUrl) return "hls-segment";
    const ext = inferUrlExtension(upstream);
    if (ext === "ts") return "ts-direct";
    if (ext === "m3u8") return "hls-manifest";
    return ext ? `${ext}-direct` : "direct";
}

function inferUrlExtension(value) {
    try {
        const pathname = new URL(value).pathname;
        const match = pathname.match(/\.([a-z0-9]{2,6})$/i);
        return match ? match[1].toLowerCase() : "-";
    } catch {
        return "-";
    }
}

function trimPlaybackSessions() {
    if (state.playbackSessions.size <= 200) return;
    const entries = [...state.playbackSessions.entries()].sort((a, b) => (a[1].lastAt || 0) - (b[1].lastAt || 0));
    for (const [key] of entries.slice(0, state.playbackSessions.size - 200)) state.playbackSessions.delete(key);
}

function copyResponseHeaders(upstreamResponse, res) {
    const directTs = upstreamResponse?.kronosContext?.streamFormat === "ts-direct";
    const headers = directTs
        ? ["content-type", "content-encoding"]
        : [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
            "last-modified",
            "etag",
            "content-encoding"
        ];
    headers.forEach(header => {
        if (upstreamResponse.headers[header]) res.setHeader(header, upstreamResponse.headers[header]);
    });
    if (directTs) {
        res.removeHeader("Content-Length");
        res.removeHeader("Content-Range");
        res.removeHeader("Accept-Ranges");
        res.removeHeader("Last-Modified");
        res.removeHeader("ETag");
    }
    if (settings.SEGMENT_STRICT_NO_CACHE) {
        res.removeHeader("ETag");
        res.removeHeader("Last-Modified");
    }
}

module.exports = {
    RELAY_HEADERS,
    analyzeManifest,
    copyResponseHeaders,
    decodeProxyUrl: decodeBase64Url,
    fetchSegmentWithHealing,
    getRewrittenManifest,
    closeUpstreamResponse,
    releaseActiveUpstream,
    isOfflinePlaceholderManifest,
    manifestProxyUrl,
    monitorSegmentTransfer,
    relayLiveTs,
    rewriteManifest,
    segmentIdentity,
    segmentProxyUrl,
    setPlaylistHeaders
};
