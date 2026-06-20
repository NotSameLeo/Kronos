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

async function fetchStableManifest(context) {
    let holdStartedAt = 0;
    let holdAttempts = 0;
    let lastErr = null;

    while (true) {
        try {
            const fetched = await fetchUpstreamManifest(context.upstream);
            const analysis = analyzeManifest(fetched.data, fetched.finalUrl);
            const placeholder = context.blockOfflinePlaceholders && isOfflinePlaceholderManifest(analysis);
            const instability = placeholder
                ? null
                : detectLiveManifestInstability(getRememberedManifestWindow(context.configKey, context.upstream), analysis);

            if (!placeholder && !instability) {
                rememberLiveManifestWindow(context.configKey, context.upstream, analysis);
                if (holdAttempts > 0) {
                    logManifestHoldRecovered(context, {
                        attempts: holdAttempts,
                        holdMs: holdStartedAt ? Date.now() - holdStartedAt : 0,
                        mediaSeq: analysis.mediaSequence,
                        lastSeq: analysis.lastSegment?.sequence
                    });
                }
                return {
                    fetched,
                    analysis,
                    holdAttempts,
                    holdMs: holdStartedAt ? Date.now() - holdStartedAt : 0
                };
            }

            logManifestEvent({
                req: context.req,
                configKey: context.configKey,
                routeKey: context.routeKey,
                upstream: context.upstream,
                finalUrl: fetched.finalUrl,
                status: fetched.status,
                upstreamMs: fetched.upstreamMs,
                rewriteMs: 0,
                totalMs: Date.now() - context.startedAt,
                bytes: fetched.data.length,
                analysis,
                blocked: true,
                blockReason: placeholder ? "offline-placeholder" : instability.reason,
                holdAttempts
            });
            lastErr = manifestStabilityError(
                placeholder ? "Upstream returned short offline placeholder" : `Unstable live manifest: ${instability.reason}`,
                placeholder ? "offline-placeholder" : instability.reason
            );
        } catch (err) {
            if (!isRecoverableManifestError(err)) throw err;
            lastErr = err;
        }

        if (!holdStartedAt) holdStartedAt = Date.now();
        const waitMs = nextManifestHoldDelay(holdStartedAt);
        if (waitMs <= 0) throw lastErr;
        logManifestHold(context, {
            reason: manifestErrorReason(lastErr),
            attempts: holdAttempts,
            waitMs,
            elapsedMs: Date.now() - holdStartedAt,
            status: manifestErrorStatus(lastErr)
        });
        await sleep(waitMs);
        holdAttempts++;
    }
}

async function getRewrittenManifest(configKey, upstream, host, routeKey = configKey, req = null) {
    const key = hashKey(`${configKey}|${host}|${routeKey}|manifest|${upstream}`);
    if (state.manifestInflight.has(key)) return state.manifestInflight.get(key);

    const promise = (async () => {
        const startedAt = Date.now();
        const blockOfflinePlaceholders = req?.query?.pg !== "0";
        const { fetched, analysis, holdAttempts, holdMs } = await fetchStableManifest({
            req,
            configKey,
            routeKey,
            upstream,
            blockOfflinePlaceholders,
            startedAt
        });

        const rewriteStartedAt = Date.now();
        const manifestNonce = settings.HLS_CACHE_BUST_SEGMENTS
            ? hashKey(`${Date.now()}|${Math.random()}|${fetched.finalUrl}`, 12)
            : "";
        const text = rewriteManifest(fetched.data, fetched.finalUrl, host, configKey, upstream, routeKey, manifestNonce, blockOfflinePlaceholders);
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
            holdAttempts,
            holdMs
        });
        return {
            text,
            analysis
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

function manifestProxyUrl(host, routeKey, url, blockOfflinePlaceholders = true) {
    const params = new URLSearchParams({
        u: encodeBase64Url(url),
        pg: blockOfflinePlaceholders ? "1" : "0"
    });
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

function rewriteManifest(text, baseUrl, host, configKey, parentPlaylistUrl = baseUrl, routeKey = configKey, manifestNonce = "", blockOfflinePlaceholders = true) {
    const isMaster = /#EXT-X-STREAM-INF/i.test(text);
    const mediaSequence = readNumericTag(text, "#EXT-X-MEDIA-SEQUENCE");
    let mediaIndex = 0;
    const rewriteTagUrl = url => isHlsUrl(url)
        ? manifestProxyUrl(host, routeKey, url, blockOfflinePlaceholders)
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
            output.push(manifestProxyUrl(host, routeKey, absolute, blockOfflinePlaceholders));
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

    return applyLiveEdgeDelay(output.join("\n"), {
        enabled: !isMaster,
        endList: /#EXT-X-ENDLIST/i.test(text),
        segmentCount: mediaIndex
    });
}

function applyLiveEdgeDelay(text, options = {}) {
    if (!options.enabled || options.endList || settings.HLS_LIVE_EDGE_DELAY_SECONDS <= 0) return text;
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
        if (delayedSeconds >= settings.HLS_LIVE_EDGE_DELAY_SECONDS) break;
    }
    if (!removeCount) return text;

    const remove = new Set();
    for (const segment of segments.slice(-removeCount)) {
        for (let index = segment.start; index <= segment.end; index++) remove.add(index);
    }
    return lines.filter((_line, index) => !remove.has(index)).join("\n");
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

function manifestStabilityKey(configKey, upstream) {
    return hashKey(`${configKey}|manifest-window|${upstream}`, 20);
}

function manifestWindowFromAnalysis(analysis, seenAt = Date.now()) {
    if (!analysis || analysis.kind !== "media" || analysis.endList) return null;
    const first = analysis.firstSegment || null;
    const last = analysis.lastSegment || null;
    if (!Number.isFinite(analysis.mediaSequence) || !Number.isFinite(last?.sequence)) return null;
    return {
        mediaSequence: analysis.mediaSequence,
        discontinuitySequence: analysis.discontinuitySequence,
        firstSeq: first?.sequence,
        lastSeq: last.sequence,
        firstIdentity: first?.identity || first?.urlHash || "-",
        lastIdentity: last.identity || last.urlHash || "-",
        segmentCount: analysis.segmentCount || 0,
        seenAt
    };
}

function getRememberedManifestWindow(configKey, upstream) {
    return state.manifestWindows.get(manifestStabilityKey(configKey, upstream)) || null;
}

function rememberLiveManifestWindow(configKey, upstream, analysis) {
    const window = manifestWindowFromAnalysis(analysis);
    if (!window) return;
    state.manifestWindows.set(manifestStabilityKey(configKey, upstream), window);
    trimManifestWindows();
}

function trimManifestWindows() {
    if (state.manifestWindows.size <= 500) return;
    const entries = [...state.manifestWindows.entries()].sort((a, b) => (a[1].seenAt || 0) - (b[1].seenAt || 0));
    for (const [key] of entries.slice(0, state.manifestWindows.size - 500)) state.manifestWindows.delete(key);
}

function detectLiveManifestInstability(previous, analysis, now = Date.now()) {
    const current = manifestWindowFromAnalysis(analysis, now);
    if (!previous || !current) return null;
    if (now - (previous.seenAt || 0) > settings.HLS_MANIFEST_STABILITY_HISTORY_MS) return null;

    const sameDiscontinuity = current.discontinuitySequence === previous.discontinuitySequence;
    if (sameDiscontinuity && Number.isFinite(previous.lastSeq) && current.lastSeq < previous.lastSeq) {
        return { reason: "sequence-regression", previous, current };
    }

    const sameSequenceWindow = current.mediaSequence === previous.mediaSequence && current.lastSeq === previous.lastSeq;
    const changedStableIdentity = current.firstIdentity !== previous.firstIdentity || current.lastIdentity !== previous.lastIdentity;
    if (sameSequenceWindow && sameDiscontinuity && changedStableIdentity) {
        return { reason: "sequence-fork", previous, current };
    }

    return null;
}

function manifestStabilityError(message, reason) {
    const err = new Error(message);
    err.statusCode = 503;
    err.retryAfter = 2;
    err.kronosReason = reason;
    err.kronosRecoverable = true;
    return err;
}

function isRecoverableManifestError(err) {
    if (err?.kronosRecoverable) return true;
    const status = manifestErrorStatus(err);
    if ([407, 408, 409, 425, 429].includes(status)) return true;
    if (status >= 500 && status <= 599) return true;
    return ["ECONNABORTED", "ECONNRESET", "EPIPE", "ETIMEDOUT", "EAI_AGAIN"].includes(err?.code);
}

function manifestErrorStatus(err) {
    return Number(err?.response?.status || err?.statusCode || 0);
}

function manifestErrorReason(err) {
    if (err?.kronosReason) return err.kronosReason;
    const status = manifestErrorStatus(err);
    if (status) return `http-${status}`;
    return err?.code || compactMessage(err?.message || "manifest-error");
}

function nextManifestHoldDelay(holdStartedAt) {
    if (settings.HLS_MANIFEST_STABILITY_HOLD_MS <= 0) return 0;
    const remaining = settings.HLS_MANIFEST_STABILITY_HOLD_MS - (Date.now() - holdStartedAt);
    if (remaining <= 0) return 0;
    return Math.min(settings.HLS_MANIFEST_STABILITY_RETRY_DELAY_MS, remaining);
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
        blockReason: event.blockReason || "-",
        holdAttempts: valueOrDash(event.holdAttempts),
        holdMs: valueOrDash(event.holdMs),
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

function logManifestHold(context, info) {
    if (!settings.HLS_DIAGNOSTICS) return;
    console.warn(`[HLS MANIFEST HOLD] ${formatFields({
        route: shortValue(context.routeKey),
        cfg: hashKey(context.configKey, 10),
        reason: info.reason,
        status: info.status || "-",
        attempts: info.attempts,
        waitMs: info.waitMs,
        elapsedMs: info.elapsedMs,
        playlist: hashKey(context.upstream, 12),
        ip: clientAddress(context.req),
        ua: compactUserAgent(context.req)
    })}`);
}

function logManifestHoldRecovered(context, info) {
    if (!settings.HLS_DIAGNOSTICS) return;
    console.warn(`[HLS MANIFEST RECOVERED] ${formatFields({
        route: shortValue(context.routeKey),
        cfg: hashKey(context.configKey, 10),
        attempts: info.attempts,
        holdMs: info.holdMs,
        mediaSeq: valueOrDash(info.mediaSeq),
        lastSeq: valueOrDash(info.lastSeq),
        playlist: hashKey(context.upstream, 12),
        ip: clientAddress(context.req),
        ua: compactUserAgent(context.req)
    })}`);
}

function logSegmentRequest(context) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const fields = {
        sid: context.sessionKey,
        route: shortValue(context.routeKey),
        cfg: hashKey(context.configKey, 10),
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

function trimPlaybackSessions() {
    if (state.playbackSessions.size <= 200) return;
    const entries = [...state.playbackSessions.entries()].sort((a, b) => (a[1].lastAt || 0) - (b[1].lastAt || 0));
    for (const [key] of entries.slice(0, state.playbackSessions.size - 200)) state.playbackSessions.delete(key);
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
    detectLiveManifestInstability,
    fetchSegmentWithHealing,
    getRewrittenManifest,
    closeUpstreamResponse,
    releaseActiveUpstream,
    isOfflinePlaceholderManifest,
    manifestWindowFromAnalysis,
    manifestProxyUrl,
    monitorSegmentTransfer,
    rewriteManifest,
    segmentIdentity,
    segmentProxyUrl,
    setPlaylistHeaders
};
