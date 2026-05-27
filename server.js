const express = require("express");
const axios = require("axios");
const xml2js = require("xml2js");
const path = require("path");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 7000;

app.set("trust proxy", true);

app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Accept, Range");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
});

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

const memoryCache = {
    channelItems: {},
    channelIndex: {},
    epgData: {},
    hlsData: {},
    hlsInflight: {},
    streamStates: {},
    liveBuffers: {},
    segmentData: {},
    segmentInflight: {},
    segmentPrefetchQueue: [],
    segmentPrefetchActive: 0,
    segmentCacheBytes: 0,
    logoData: {},
    lastUpdate: {},
    isUpdating: {},
    stats: {
        startedAt: new Date().toISOString(),
        hls: {
            totalFetches: 0,
            okFetches: 0,
            failedFetches: 0,
            cacheHits: 0,
            staleHits: 0,
            coalescedFetches: 0,
            backgroundRefreshes: 0
        },
        liveBuffer: {
            playlistsBuilt: 0,
            segmentsStored: 0,
            segmentsUpdated: 0,
            segmentsDeduped: 0,
            prefetchQueued: 0
        },
        startup: {
            requested: 0,
            acquiring: 0,
            ready: 0,
            failed: 0,
            cancelled: 0,
            reusedReady: 0,
            waitedExisting: 0
        },
        segmentCache: {
            hits: 0,
            misses: 0,
            stored: 0,
            evicted: 0,
            prefetches: 0,
            prefetchOk: 0,
            prefetchError: 0,
            bytes: 0
        },
        seg: {
            total: 0,
            ok: 0,
            cache: 0,
            error: 0,
            aborted: 0,
            active: 0,
            bytes: 0
        }
    }
};

const CACHE_TTL = 30 * 60 * 1000;

// HLS self-relay tuning
// Dynamic-buffer mode: quick start + progressive server-side buffer.
const HLS_REFRESH_TTL = Number(process.env.HLS_REFRESH_TTL || 6000);
const HLS_MASTER_REFRESH_TTL = Number(process.env.HLS_MASTER_REFRESH_TTL || 30000);
const HLS_STALE_TTL = Number(process.env.HLS_STALE_TTL || 120000);
const HLS_LIVE_STALE_REFRESH_TTL = Number(process.env.HLS_LIVE_STALE_REFRESH_TTL || 25000);
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 12000);

// Keep HLS retries simple: the stream-acquire loop is now the real retry controller.
const HLS_RETRY_COUNT = Number(process.env.HLS_RETRY_COUNT || 1);
const HLS_RETRY_BASE_DELAY = Number(process.env.HLS_RETRY_BASE_DELAY || 300);
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);

// Segment logs: 1 = log every successful segment, 5 = one every 5, 0 = only errors.
const SEG_LOG_EVERY = Number(process.env.SEG_LOG_EVERY || 1);

// Dynamic live buffer.
// Phase 1: acquire a valid playlist for max 25s.
// Phase 2: start as soon as Kronos has enough real seconds cached.
// Phase 3: while playing, keep filling the buffer toward the target.
const LIVE_BUFFER_SEGMENTS = Number(process.env.LIVE_BUFFER_SEGMENTS || 32);
const LIVE_PLAYLIST_SEGMENTS = Number(process.env.LIVE_PLAYLIST_SEGMENTS || 10);
const LIVE_DELAY_SEGMENTS = Number(process.env.LIVE_DELAY_SEGMENTS || 2);
const MIN_LIVE_SEGMENTS_READY = Number(process.env.MIN_LIVE_SEGMENTS_READY || 2);
const MIN_LIVE_WARMUP_ATTEMPTS = Number(process.env.MIN_LIVE_WARMUP_ATTEMPTS || 2);
const MIN_LIVE_WARMUP_DELAY_MS = Number(process.env.MIN_LIVE_WARMUP_DELAY_MS || 2500);

// Startup and dynamic-buffer parameters.
const STREAM_STARTUP_PREBUFFER_ENABLED = String(process.env.STREAM_STARTUP_PREBUFFER_ENABLED || "1") !== "0";
const STREAM_ACQUIRE_TIMEOUT = Number(process.env.STREAM_ACQUIRE_TIMEOUT || 25000);
const STREAM_ACQUIRE_POLL_MS = Number(process.env.STREAM_ACQUIRE_POLL_MS || 2500);

const STREAM_PREBUFFER_START_SECONDS = Number(process.env.STREAM_PREBUFFER_START_SECONDS || 24);
const STREAM_PREBUFFER_START_MIN_SEGMENTS = Number(process.env.STREAM_PREBUFFER_START_MIN_SEGMENTS || 2);
const STREAM_PREBUFFER_TIMEOUT = Number(process.env.STREAM_PREBUFFER_TIMEOUT || 35000);

const DYNAMIC_BUFFER_CRITICAL_SECONDS = Number(process.env.DYNAMIC_BUFFER_CRITICAL_SECONDS || 10);
const DYNAMIC_BUFFER_LOW_SECONDS = Number(process.env.DYNAMIC_BUFFER_LOW_SECONDS || 20);
const DYNAMIC_BUFFER_TARGET_SECONDS = Number(process.env.DYNAMIC_BUFFER_TARGET_SECONDS || 45);
const DYNAMIC_BUFFER_MAX_SECONDS = Number(process.env.DYNAMIC_BUFFER_MAX_SECONDS || 60);

const STREAM_READY_TTL = Number(process.env.STREAM_READY_TTL || 5 * 60 * 1000);
const MIN_PLAYLIST_OUTPUT_SEGMENTS = Number(process.env.MIN_PLAYLIST_OUTPUT_SEGMENTS || STREAM_PREBUFFER_START_MIN_SEGMENTS);

// Compatibility aliases used by existing debug/stat code paths.
const PREBUFFER_MIN_SEGMENTS = STREAM_PREBUFFER_START_MIN_SEGMENTS;
const PREBUFFER_TARGET_SEGMENTS = Number(process.env.PREBUFFER_TARGET_SEGMENTS || 4);
const PREBUFFER_MIN_SECONDS = STREAM_PREBUFFER_START_SECONDS;
const PREBUFFER_WAIT_FOR_SEGMENTS_MS = STREAM_PREBUFFER_TIMEOUT;

const SEGMENT_CACHE_TTL = Number(process.env.SEGMENT_CACHE_TTL || 10 * 60 * 1000);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_BYTES || 512 * 1024 * 1024);
const SEGMENT_CACHE_MAX_SEGMENTS = Number(process.env.SEGMENT_CACHE_MAX_SEGMENTS || 100);
const SEGMENT_CACHE_MAX_ITEM_BYTES = Number(process.env.SEGMENT_CACHE_MAX_ITEM_BYTES || 24 * 1024 * 1024);
const SEGMENT_PREFETCH_CONCURRENCY = Number(process.env.SEGMENT_PREFETCH_CONCURRENCY || 2);
const SEGMENT_PREFETCH_AHEAD = Number(process.env.SEGMENT_PREFETCH_AHEAD || 8);
const SEGMENT_WAIT_FOR_PREFETCH_MS = Number(process.env.SEGMENT_WAIT_FOR_PREFETCH_MS || 2500);

const ADDON_TYPE = "tv";
const RELEASE_VERSION = "1.6.6";

const UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function decodeConfig(configKey) {
    try {
        const normalized = String(configKey || "").replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
        const decoded = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
        return decoded;
    } catch (err) {
        console.error("[DECODE CONFIG ERROR]", err.message);
        throw new Error("Invalid configuration token");
    }
}

function safeConfigForLog(config) {
    return { ...config, pp: config?.pp ? "***" : config?.pp };
}

function sanitizeUrlForLog(value) {
    try {
        const url = new URL(value);
        ["api_password", "password", "pass", "pwd", "u"].forEach(k => {
            if (url.searchParams.has(k)) url.searchParams.set(k, "***");
        });

        // Mask common Xtream-style credentials in paths: /live/user/pass/id.m3u8
        url.pathname = url.pathname.replace(/\/(live|movie|series)\/([^/]+)\/([^/]+)/i, "/$1/***/***");
        return url.toString();
    } catch (err) {
        return String(value || "")
            .replace(/((?:api_password|password|pass|pwd)=)[^&]+/gi, "$1***")
            .replace(/\/(live|movie|series)\/([^/]+)\/([^/]+)/i, "/$1/***/***");
    }
}

function formatBytes(bytes) {
    const value = Number(bytes || 0);
    if (value < 1024) return `${value}B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)}KB`;
    if (value < 1024 * 1024 * 1024) return `${(value / 1024 / 1024).toFixed(2)}MB`;
    return `${(value / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

function analyzeHLSPlaylist(data) {
    const text = String(data || "");
    const lines = text.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const segmentCount = lines.filter(line => !line.startsWith("#")).length;
    const mediaSequenceLine = lines.find(line => line.startsWith("#EXT-X-MEDIA-SEQUENCE:"));
    const targetDurationLine = lines.find(line => line.startsWith("#EXT-X-TARGETDURATION:"));

    return {
        isMaster: lines.some(line => line.startsWith("#EXT-X-STREAM-INF")),
        isLive: !lines.some(line => line.startsWith("#EXT-X-ENDLIST")),
        segmentCount,
        mediaSequence: mediaSequenceLine ? Number(mediaSequenceLine.split(":")[1]) : null,
        targetDuration: targetDurationLine ? Number(targetDurationLine.split(":")[1]) : null,
        bytes: Buffer.byteLength(text)
    };
}

function getHLSRefreshTTL(info) {
    if (info?.isMaster) return HLS_MASTER_REFRESH_TTL;
    if (info?.isLive && info?.targetDuration) {
        return Math.max(1000, Math.min(HLS_REFRESH_TTL, Math.floor(info.targetDuration * 500)));
    }
    return HLS_REFRESH_TTL;
}

function getSegmentCacheKey(sourceUrl) {
    return `seg:${hashKey(sourceUrl)}`;
}

function getHLSCacheKey(sourceUrl) {
    return `hls:${hashKey(sourceUrl)}`;
}

function isLiveMediaPlaylistInfo(info) {
    return Boolean(info && info.isLive && !info.isMaster);
}

function makeStartupCancelToken(sourceUrl, label = "stream") {
    return {
        sourceUrl,
        label,
        cacheKey: getHLSCacheKey(sourceUrl),
        createdAt: Date.now(),
        cancelled: false,
        reason: null
    };
}

function cancelStartupToken(token, reason = "cancelled") {
    if (!token || token.cancelled) return;
    token.cancelled = true;
    token.reason = reason;
}

function throwIfStartupCancelled(token) {
    if (token?.cancelled) {
        const err = new Error(`Startup cancelled: ${token.reason || "cancelled"}`);
        err.cancelled = true;
        throw err;
    }
}

function cancelOtherStartupStreams(currentCacheKey, label = "stream") {
    Object.entries(memoryCache.streamStates || {}).forEach(([cacheKey, state]) => {
        if (cacheKey === currentCacheKey) return;
        if (!state || !state.promise) return;
        if (!["acquiring", "prebuffering", "idle"].includes(state.status)) return;

        if (state.cancelToken) {
            cancelStartupToken(state.cancelToken, `new stream requested: ${label}`);
        }

        state.status = "cancelled";
        state.updatedAt = Date.now();
        state.lastError = `Cancelled because another stream was requested: ${label}`;

        console.log(`[STREAM CANCEL REQUEST] previous=${cacheKey} new="${label}"`);
    });
}

function isStartupCancelledError(err) {
    return Boolean(err?.cancelled || String(err?.message || "").startsWith("Startup cancelled:"));
}

function normalizeHeaderLineToAbsolute(line, baseUrl) {
    const trimmed = String(line || "").trim();

    if (!trimmed) return line;

    if (trimmed.startsWith("#")) {
        return String(line).replace(/URI=("([^\"]+)"|'([^']+)')/g, (match, quoted, doubleUri, singleUri) => {
            const uri = doubleUri || singleUri;
            const quote = quoted.startsWith("'") ? "'" : '"';
            const absUrl = toAbsoluteUrl(uri, baseUrl);
            return `URI=${quote}${absUrl}${quote}`;
        });
    }

    return toAbsoluteUrl(trimmed, baseUrl);
}

function extractSegmentIdentityFromUrl(value) {
    try {
        const url = new URL(value);
        const pathname = decodeURIComponent(url.pathname || "");
        const filename = pathname.split("/").filter(Boolean).pop() || "";
        const cleanFilename = filename.split("?")[0].split("#")[0];

        // Common IPTV/HLS form: channel_123.ts
        const numbered = cleanFilename.match(/(?:^|[_-])(\d+)\.(?:ts|m4s|mp4|aac|mp3)$/i);
        if (numbered) return `file:${numbered[1]}`;

        if (cleanFilename) return `file:${cleanFilename.toLowerCase()}`;
    } catch (err) {
        const raw = String(value || "");
        const filename = raw.split("?")[0].split("#")[0].split("/").filter(Boolean).pop() || "";
        const numbered = filename.match(/(?:^|[_-])(\d+)\.(?:ts|m4s|mp4|aac|mp3)$/i);
        if (numbered) return `file:${numbered[1]}`;
        if (filename) return `file:${filename.toLowerCase()}`;
    }

    return null;
}

function getSegmentLogicalKey(segment) {
    if (!segment) return null;
    if (Number.isFinite(segment.sequence)) return `seq:${segment.sequence}`;

    const identity = extractSegmentIdentityFromUrl(segment.url);
    if (identity) return identity;

    return segment.url ? `url:${hashKey(segment.url)}` : null;
}

function cloneSegmentWithLogicalKey(segment) {
    const logicalKey = getSegmentLogicalKey(segment);
    return {
        ...segment,
        logicalKey
    };
}

function shouldResetLiveBufferForParsedSegments(buffer, parsed) {
    if (!buffer || !Array.isArray(buffer.segments) || buffer.segments.length === 0) return false;
    if (!parsed || !Array.isArray(parsed.segments) || parsed.segments.length === 0) return false;

    const existingSequences = buffer.segments
        .map(segment => segment.sequence)
        .filter(Number.isFinite);

    const parsedSequences = parsed.segments
        .map(segment => segment.sequence)
        .filter(Number.isFinite);

    if (existingSequences.length === 0 || parsedSequences.length === 0) return false;

    const existingMax = Math.max(...existingSequences);
    const parsedMax = Math.max(...parsedSequences);

    // Some providers reset #EXT-X-MEDIA-SEQUENCE to 0 when a stream/session is rebuilt.
    // If that happens, clear the old logical sequence map; otherwise seq:0 from a new session
    // would collide with seq:0 from the old session.
    return parsedMax + 3 < existingMax;
}

function getCachedSegmentForLogicalSegment(segment) {
    if (!segment) return null;

    const urls = [
        segment.url,
        segment.cacheUrl,
        ...(Array.isArray(segment.altUrls) ? segment.altUrls : [])
    ].filter(Boolean);

    for (const url of urls) {
        const cached = getSegmentFromCache(url);
        if (cached) return cached;
    }

    return null;
}

function getBestUrlForLogicalSegment(existingSegment, incomingSegment) {
    if (incomingSegment?.url && getSegmentFromCache(incomingSegment.url)) {
        return incomingSegment.url;
    }

    if (existingSegment?.url && getSegmentFromCache(existingSegment.url)) {
        return existingSegment.url;
    }

    if (existingSegment?.cacheUrl && getSegmentFromCache(existingSegment.cacheUrl)) {
        return existingSegment.cacheUrl;
    }

    if (Array.isArray(existingSegment?.altUrls)) {
        const cachedAlt = existingSegment.altUrls.find(url => getSegmentFromCache(url));
        if (cachedAlt) return cachedAlt;
    }

    return incomingSegment?.url || existingSegment?.url || existingSegment?.cacheUrl || null;
}

function mergeLogicalSegment(existingSegment, incomingSegment) {
    const now = Date.now();
    const altUrls = new Set();

    [existingSegment?.url, existingSegment?.cacheUrl, incomingSegment?.url]
        .filter(Boolean)
        .forEach(url => altUrls.add(url));

    if (Array.isArray(existingSegment?.altUrls)) {
        existingSegment.altUrls.filter(Boolean).forEach(url => altUrls.add(url));
    }

    const bestUrl = getBestUrlForLogicalSegment(existingSegment, incomingSegment);
    const useIncomingLines = bestUrl === incomingSegment?.url || !existingSegment?.lines;

    return {
        ...(existingSegment || {}),
        ...incomingSegment,
        url: bestUrl,
        cacheUrl: getCachedSegmentForLogicalSegment(existingSegment)?.url || getCachedSegmentForLogicalSegment(incomingSegment)?.url || existingSegment?.cacheUrl || incomingSegment?.url || bestUrl,
        lines: useIncomingLines ? incomingSegment.lines : existingSegment.lines,
        duration: incomingSegment.duration || existingSegment?.duration || 0,
        firstSeenAt: existingSegment?.firstSeenAt || incomingSegment.firstSeenAt || now,
        lastSeenAt: now,
        altUrls: [...altUrls].slice(-6),
        logicalKey: incomingSegment.logicalKey || existingSegment?.logicalKey || getSegmentLogicalKey(incomingSegment)
    };
}

function parseHLSMediaPlaylist(playlist, baseUrl) {
    const text = String(playlist || "");
    const info = analyzeHLSPlaylist(text);
    const lines = text.split(/\r?\n/);
    const headerLines = [];
    const segments = [];
    let pendingSegmentLines = [];
    let pendingDuration = null;
    const originalMediaSequence = Number.isFinite(info.mediaSequence) ? info.mediaSequence : 0;

    for (const rawLine of lines) {
        const line = String(rawLine || "");
        const trimmed = line.trim();

        if (!trimmed) continue;
        if (trimmed.startsWith("#EXT-X-ENDLIST")) continue;

        if (trimmed.startsWith("#EXTINF")) {
            pendingSegmentLines = [normalizeHeaderLineToAbsolute(line, baseUrl)];
            const durationMatch = trimmed.match(/^#EXTINF:([0-9.]+)/);
            pendingDuration = durationMatch ? Number(durationMatch[1]) : null;
            continue;
        }

        if (pendingSegmentLines.length > 0) {
            const normalizedLine = normalizeHeaderLineToAbsolute(line, baseUrl);
            pendingSegmentLines.push(normalizedLine);

            if (!trimmed.startsWith("#")) {
                const sequence = originalMediaSequence + segments.length;
                const absoluteUrl = toAbsoluteUrl(trimmed, baseUrl);
                segments.push({
                    sequence,
                    url: absoluteUrl,
                    lines: pendingSegmentLines,
                    duration: Number.isFinite(pendingDuration) ? pendingDuration : (info.targetDuration || 10),
                    firstSeenAt: Date.now(),
                    lastSeenAt: Date.now()
                });
                pendingSegmentLines = [];
                pendingDuration = null;
            }

            continue;
        }

        headerLines.push(normalizeHeaderLineToAbsolute(line, baseUrl));
    }

    return { info, headerLines, segments, originalMediaSequence };
}

function getSegmentFromCache(sourceUrl) {
    const key = getSegmentCacheKey(sourceUrl);
    const cached = memoryCache.segmentData[key];

    if (!cached) return null;

    if (Date.now() - cached.fetchedAt > SEGMENT_CACHE_TTL) {
        delete memoryCache.segmentData[key];
        memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - cached.size);
        memoryCache.stats.segmentCache.evicted += 1;
        return null;
    }

    cached.lastAccessedAt = Date.now();
    return cached;
}

function evictSegmentCacheIfNeeded() {
    const entries = Object.entries(memoryCache.segmentData)
        .map(([key, value]) => ({ key, ...value }))
        .sort((a, b) => (a.lastAccessedAt || a.fetchedAt || 0) - (b.lastAccessedAt || b.fetchedAt || 0));

    while (
        memoryCache.segmentCacheBytes > SEGMENT_CACHE_MAX_BYTES ||
        Object.keys(memoryCache.segmentData).length > SEGMENT_CACHE_MAX_SEGMENTS
    ) {
        const victim = entries.shift();
        if (!victim) break;

        if (memoryCache.segmentData[victim.key]) {
            memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - memoryCache.segmentData[victim.key].size);
            delete memoryCache.segmentData[victim.key];
            memoryCache.stats.segmentCache.evicted += 1;
        }
    }
}

function storeSegmentInCache(sourceUrl, buffer, responseHeaders = {}, status = 200, source = "stream") {
    if (!Buffer.isBuffer(buffer)) return false;
    if (buffer.length <= 0 || buffer.length > SEGMENT_CACHE_MAX_ITEM_BYTES) return false;

    const key = getSegmentCacheKey(sourceUrl);
    const existing = memoryCache.segmentData[key];

    if (existing) {
        memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - existing.size);
    }

    const headers = {};
    [
        "content-type",
        "cache-control",
        "expires",
        "last-modified",
        "etag"
    ].forEach(h => {
        if (responseHeaders[h]) headers[h] = responseHeaders[h];
    });

    memoryCache.segmentData[key] = {
        url: sourceUrl,
        buffer,
        size: buffer.length,
        headers,
        status,
        fetchedAt: Date.now(),
        lastAccessedAt: Date.now(),
        source
    };

    memoryCache.segmentCacheBytes += buffer.length;
    memoryCache.stats.segmentCache.stored += 1;
    memoryCache.stats.segmentCache.bytes = memoryCache.segmentCacheBytes;
    evictSegmentCacheIfNeeded();

    return true;
}

function sendCachedSegment(req, res, cached, segmentNo = null) {
    const buffer = cached.buffer;
    const total = buffer.length;
    const rangeHeader = req.headers.range;
    let start = 0;
    let end = total - 1;
    let status = 200;

    Object.entries(cached.headers || {}).forEach(([key, value]) => {
        if (value) res.setHeader(key, value);
    });

    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("X-Kronos-Relay", "1");
    res.setHeader("X-Kronos-Cache", "HIT");

    if (rangeHeader) {
        const match = String(rangeHeader).match(/bytes=(\d*)-(\d*)/);
        if (match) {
            if (match[1] !== "") start = Number(match[1]);
            if (match[2] !== "") end = Number(match[2]);
            if (!Number.isFinite(start)) start = 0;
            if (!Number.isFinite(end) || end >= total) end = total - 1;
            if (start > end || start >= total) {
                res.status(416);
                res.setHeader("Content-Range", `bytes */${total}`);
                return res.end();
            }
            status = 206;
            res.setHeader("Content-Range", `bytes ${start}-${end}/${total}`);
        }
    }

    const chunk = buffer.subarray(start, end + 1);
    res.status(status);
    res.setHeader("Content-Length", chunk.length);

    if (!res.getHeader("content-type")) {
        res.setHeader("Content-Type", cached.headers?.["content-type"] || "video/mp2t");
    }

    // Detailed segment timing is logged once in finalizeSegment().
    return res.end(chunk);
}

async function fetchSegmentToCache(sourceUrl, reason = "prefetch") {
    const key = getSegmentCacheKey(sourceUrl);

    const cached = getSegmentFromCache(sourceUrl);
    if (cached) return cached;

    const inflight = memoryCache.segmentInflight[key];
    if (inflight) return inflight;

    const promise = (async () => {
        const started = Date.now();

        try {
            const response = await axios.get(sourceUrl, {
                responseType: "arraybuffer",
                timeout: SEG_REQUEST_TIMEOUT,
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "*/*",
                    "Accept-Encoding": "identity",
                    "Connection": "keep-alive"
                },
                decompress: false,
                maxBodyLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
                maxContentLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
                validateStatus(status) { return status >= 200 && status < 300; }
            });

            const buffer = Buffer.from(response.data);
            storeSegmentInCache(sourceUrl, buffer, response.headers, response.status, reason);
            memoryCache.stats.segmentCache.prefetchOk += reason === "prefetch" ? 1 : 0;

            console.log(`[SEG PREFETCH OK] bytes=${formatBytes(buffer.length)} time=${Date.now() - started}ms url=${sanitizeUrlForLog(sourceUrl)}`);
            return getSegmentFromCache(sourceUrl);
        } catch (err) {
            if (reason === "prefetch") memoryCache.stats.segmentCache.prefetchError += 1;
            console.error(`[SEG PREFETCH ERROR] reason=${reason} url=${sanitizeUrlForLog(sourceUrl)} err=${err.message}`);
            throw err;
        }
    })();

    memoryCache.segmentInflight[key] = promise;

    try {
        return await promise;
    } finally {
        delete memoryCache.segmentInflight[key];
    }
}

function processSegmentPrefetchQueue() {
    while (
        memoryCache.segmentPrefetchActive < SEGMENT_PREFETCH_CONCURRENCY &&
        memoryCache.segmentPrefetchQueue.length > 0
    ) {
        const sourceUrl = memoryCache.segmentPrefetchQueue.shift();
        const key = getSegmentCacheKey(sourceUrl);

        if (getSegmentFromCache(sourceUrl) || memoryCache.segmentInflight[key]) {
            continue;
        }

        memoryCache.segmentPrefetchActive += 1;
        memoryCache.stats.segmentCache.prefetches += 1;

        fetchSegmentToCache(sourceUrl, "prefetch")
            .catch(() => {})
            .finally(() => {
                memoryCache.segmentPrefetchActive = Math.max(0, memoryCache.segmentPrefetchActive - 1);
                processSegmentPrefetchQueue();
            });
    }
}

function queueSegmentPrefetch(sourceUrl) {
    if (!sourceUrl || !isHttpUrl(sourceUrl)) return;

    const key = getSegmentCacheKey(sourceUrl);
    if (getSegmentFromCache(sourceUrl) || memoryCache.segmentInflight[key]) return;
    if (memoryCache.segmentPrefetchQueue.includes(sourceUrl)) return;

    memoryCache.segmentPrefetchQueue.push(sourceUrl);
    memoryCache.stats.liveBuffer.prefetchQueued += 1;
    processSegmentPrefetchQueue();
}

async function waitForSegmentPrefetch(sourceUrl, maxWaitMs = SEGMENT_WAIT_FOR_PREFETCH_MS) {
    const key = getSegmentCacheKey(sourceUrl);
    const inflight = memoryCache.segmentInflight[key];

    if (!inflight || maxWaitMs <= 0) return null;

    try {
        return await Promise.race([
            inflight,
            sleep(maxWaitMs).then(() => null)
        ]);
    } catch (err) {
        return null;
    }
}

function getLiveBuffer(sourceUrl) {
    return memoryCache.liveBuffers[getHLSCacheKey(sourceUrl)] || null;
}

function selectBufferedSegments(buffer, options = {}) {
    if (!buffer || !Array.isArray(buffer.segments) || buffer.segments.length === 0) {
        return {
            ready: false,
            segments: [],
            effectiveDelay: 0,
            totalBufferSegments: 0,
            totalDuration: 0
        };
    }

    const minOutputSegments = Number(options.minOutputSegments || MIN_PLAYLIST_OUTPUT_SEGMENTS);
    const maxPlaylistSegments = Number(options.maxPlaylistSegments || LIVE_PLAYLIST_SEGMENTS);

    let effectiveDelay = Math.max(0, Number(options.delaySegments ?? LIVE_DELAY_SEGMENTS));

    // Do not let delay destroy the visible playlist during startup.
    // Example: buffer=3 delay=2 out=1 was too fragile.
    while (effectiveDelay > 0 && buffer.segments.length - effectiveDelay < minOutputSegments) {
        effectiveDelay -= 1;
    }

    const delayedSegments = effectiveDelay > 0
        ? buffer.segments.slice(0, Math.max(0, buffer.segments.length - effectiveDelay))
        : buffer.segments.slice();

    const selected = delayedSegments.slice(-Math.max(1, maxPlaylistSegments));
    const totalDuration = selected.reduce((sum, segment) => sum + (Number(segment.duration) || 0), 0);

    return {
        ready: selected.length >= minOutputSegments,
        segments: selected,
        effectiveDelay,
        totalBufferSegments: buffer.segments.length,
        totalDuration
    };
}

function getCachedCountForSegments(segments) {
    return (segments || []).filter(segment => getCachedSegmentForLogicalSegment(segment)).length;
}

function getCachedDurationForSegments(segments) {
    return (segments || []).reduce((sum, segment) => {
        return sum + (getCachedSegmentForLogicalSegment(segment) ? (Number(segment.duration) || 0) : 0);
    }, 0);
}

function getSegmentDurationSum(segments) {
    return (segments || []).reduce((sum, segment) => sum + (Number(segment.duration) || 0), 0);
}

function getDynamicBufferMode(cachedSeconds) {
    if (cachedSeconds < DYNAMIC_BUFFER_CRITICAL_SECONDS) return "critical";
    if (cachedSeconds < DYNAMIC_BUFFER_LOW_SECONDS) return "low";
    if (cachedSeconds < DYNAMIC_BUFFER_TARGET_SECONDS) return "filling";
    if (cachedSeconds >= DYNAMIC_BUFFER_MAX_SECONDS) return "full";
    return "target";
}

function selectSegmentsUntilSeconds(segments, targetSeconds, maxSegments = SEGMENT_PREFETCH_AHEAD) {
    const selected = [];
    let total = 0;

    for (const segment of segments || []) {
        if (!segment?.url) continue;
        selected.push(segment);
        total += Number(segment.duration) || 0;

        if (total >= targetSeconds || selected.length >= maxSegments) {
            break;
        }
    }

    return selected;
}

async function waitForCachedBufferSeconds(segments, minSeconds, minSegments, maxWaitMs, label = "startup", cancelToken = null) {
    const uniqueSegments = [];
    const seen = new Set();

    (segments || []).forEach(segment => {
        const key = segment?.logicalKey || getSegmentLogicalKey(segment) || segment?.url;
        if (!segment?.url || seen.has(key)) return;
        seen.add(key);
        uniqueSegments.push(segment);
    });

    uniqueSegments.forEach(segment => queueSegmentPrefetch(segment.url));

    const started = Date.now();

    while (Date.now() - started <= maxWaitMs) {
        throwIfStartupCancelled(cancelToken);

        const cachedCount = getCachedCountForSegments(uniqueSegments);
        const cachedSeconds = getCachedDurationForSegments(uniqueSegments);

        if (cachedCount >= Math.min(minSegments, uniqueSegments.length) && cachedSeconds >= minSeconds) {
            return {
                cachedCount,
                total: uniqueSegments.length,
                cachedSeconds,
                waitedMs: Date.now() - started
            };
        }

        await sleep(300);
    }

    return {
        cachedCount: getCachedCountForSegments(uniqueSegments),
        total: uniqueSegments.length,
        cachedSeconds: getCachedDurationForSegments(uniqueSegments),
        waitedMs: Date.now() - started
    };
}

function queueDynamicPrefetchForBuffer(buffer, label = "stream") {
    if (!buffer || !Array.isArray(buffer.segments) || buffer.segments.length === 0) {
        return { mode: "empty", cachedSeconds: 0, queued: 0 };
    }

    const cachedSeconds = getCachedDurationForSegments(buffer.segments);
    const mode = getDynamicBufferMode(cachedSeconds);

    if (cachedSeconds >= DYNAMIC_BUFFER_MAX_SECONDS) {
        return { mode, cachedSeconds, queued: 0 };
    }

    const uncachedSegments = buffer.segments
        .filter(segment => !getCachedSegmentForLogicalSegment(segment))
        .slice(-Math.max(1, SEGMENT_PREFETCH_AHEAD));

    const neededSeconds = Math.max(0, DYNAMIC_BUFFER_TARGET_SECONDS - cachedSeconds);
    const candidates = selectSegmentsUntilSeconds(
        uncachedSegments,
        Math.max(neededSeconds, DYNAMIC_BUFFER_LOW_SECONDS),
        SEGMENT_PREFETCH_AHEAD
    );

    candidates.forEach(segment => queueSegmentPrefetch(segment.url));

    if (candidates.length > 0) {
        console.log(
            `[DYNAMIC BUFFER] channel="${label}" mode=${mode} cached≈${Math.round(cachedSeconds)}s` +
            ` target=${DYNAMIC_BUFFER_TARGET_SECONDS}s queued=${candidates.length}`
        );
    }

    return { mode, cachedSeconds, queued: candidates.length };
}

function buildPlaylistFromBufferedSegments(buffer, selectedSegments, effectiveDelay = LIVE_DELAY_SEGMENTS) {
    if (!buffer || !Array.isArray(selectedSegments) || selectedSegments.length === 0) {
        return null;
    }

    const firstSeq = selectedSegments[0].sequence;
    let hasMediaSequence = false;

    const headerLines = (buffer.headerLines || [])
        .filter(line => !String(line).trim().startsWith("#EXT-X-ENDLIST"))
        .map(line => {
            if (String(line).trim().startsWith("#EXT-X-MEDIA-SEQUENCE:")) {
                hasMediaSequence = true;
                return `#EXT-X-MEDIA-SEQUENCE:${firstSeq}`;
            }
            return line;
        });

    if (!hasMediaSequence) {
        headerLines.push(`#EXT-X-MEDIA-SEQUENCE:${firstSeq}`);
    }

    return [
        ...headerLines,
        ...selectedSegments.flatMap(segment => segment.lines)
    ].join("\n") + "\n";
}

async function acquireInitialHLS(sourceUrl, label = "stream", cancelToken = null) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const started = Date.now();
    let fetches = 0;
    let lastErr = null;

    console.log(`[STREAM ACQUIRE] channel="${label}" timeout=${STREAM_ACQUIRE_TIMEOUT}ms`);

    while (Date.now() - started < STREAM_ACQUIRE_TIMEOUT) {
        throwIfStartupCancelled(cancelToken);
        fetches += 1;

        try {
            const hls = await fetchAndStoreHLSRaw(cacheKey, sourceUrl, "acquire");
            throwIfStartupCancelled(cancelToken);

            const info = hls.info || analyzeHLSPlaylist(hls.playlist);

            if (isLiveMediaPlaylistInfo(info) && info.segmentCount > 0) {
                updateLiveBufferFromPlaylist(sourceUrl, hls.playlist, hls.baseUrl, {
                    minOutputSegments: 1,
                    maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
                    label
                });

                console.log(
                    `[STREAM ACQUIRE OK] channel="${label}" fetches=${fetches}` +
                    ` segs=${info.segmentCount}` +
                    `${info.targetDuration ? ` target=${info.targetDuration}s` : ""}` +
                    ` time=${Date.now() - started}ms`
                );

                return hls;
            }

            if (info.isMaster) {
                console.log(`[STREAM ACQUIRE OK] channel="${label}" master-playlist time=${Date.now() - started}ms`);
                return hls;
            }

            lastErr = new Error("Playlist valid but without playable segments");
        } catch (err) {
            if (isStartupCancelledError(err)) throw err;
            lastErr = err;
            console.error(`[STREAM ACQUIRE ERROR] channel="${label}" fetch=${fetches} err=${err.message}`);
        }

        const remaining = STREAM_ACQUIRE_TIMEOUT - (Date.now() - started);
        if (remaining > 0) {
            await sleep(Math.min(STREAM_ACQUIRE_POLL_MS, remaining));
        }
    }

    throw new Error(lastErr ? `Acquire failed: ${lastErr.message}` : "Acquire failed: timeout");
}

async function prebufferInitialStream(sourceUrl, label = "stream", cancelToken = null) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const started = Date.now();

    console.log(
        `[STREAM PREBUFFER START] channel="${label}" start=${STREAM_PREBUFFER_START_SECONDS}s` +
        ` minSegments=${STREAM_PREBUFFER_START_MIN_SEGMENTS} timeout=${STREAM_PREBUFFER_TIMEOUT}ms`
    );

    while (Date.now() - started < STREAM_PREBUFFER_TIMEOUT) {
        throwIfStartupCancelled(cancelToken);

        const hls = await fetchAndStoreHLSRaw(cacheKey, sourceUrl, "prebuffer");
        throwIfStartupCancelled(cancelToken);

        updateLiveBufferFromPlaylist(sourceUrl, hls.playlist, hls.baseUrl, {
            minOutputSegments: 1,
            maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
            delaySegments: 0,
            label
        });

        const buffer = getLiveBuffer(sourceUrl);
        const selection = selectBufferedSegments(buffer, {
            minOutputSegments: 1,
            maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
            delaySegments: 0
        });

        const neededSegments = selectSegmentsUntilSeconds(
            selection.segments,
            STREAM_PREBUFFER_START_SECONDS,
            LIVE_PLAYLIST_SEGMENTS
        );

        const prebuffer = await waitForCachedBufferSeconds(
            neededSegments,
            STREAM_PREBUFFER_START_SECONDS,
            STREAM_PREBUFFER_START_MIN_SEGMENTS,
            Math.min(3500, Math.max(500, STREAM_PREBUFFER_TIMEOUT - (Date.now() - started))),
            label,
            cancelToken
        );

        throwIfStartupCancelled(cancelToken);

        const totalDuration = getSegmentDurationSum(selection.segments);
        const cachedSeconds = getCachedDurationForSegments(selection.segments);
        const ready =
            prebuffer.cachedSeconds >= STREAM_PREBUFFER_START_SECONDS &&
            prebuffer.cachedCount >= STREAM_PREBUFFER_START_MIN_SEGMENTS;

        console.log(
            `[STREAM PREBUFFER] channel="${label}" cached≈${Math.round(cachedSeconds)}s/${STREAM_PREBUFFER_START_SECONDS}s` +
            ` cachedSegs=${prebuffer.cachedCount}/${selection.segments.length}` +
            ` playlist≈${Math.round(totalDuration)}s` +
            ` waited=${prebuffer.waitedMs}ms`
        );

        if (ready) {
            queueDynamicPrefetchForBuffer(buffer, label);

            console.log(
                `[STREAM READY] channel="${label}" startBuffer≈${Math.round(prebuffer.cachedSeconds)}s` +
                ` segs=${prebuffer.cachedCount} total=${Date.now() - started}ms` +
                ` dynamicTarget=${DYNAMIC_BUFFER_TARGET_SECONDS}s`
            );

            return {
                status: "ready",
                cachedSegments: prebuffer.cachedCount,
                cachedSeconds: prebuffer.cachedSeconds,
                totalMs: Date.now() - started
            };
        }

        await sleep(700);
    }

    throw new Error(`Prebuffer failed: cached less than ${STREAM_PREBUFFER_START_SECONDS}s after ${Date.now() - started}ms`);
}

async function ensureStreamPrebuffered(sourceUrl, label = "stream", cancelToken = null, options = {}) {
    if (!STREAM_STARTUP_PREBUFFER_ENABLED) {
        return { skipped: true };
    }

    const cacheKey = getHLSCacheKey(sourceUrl);
    const shouldCancelOthers = options.cancelOthers !== false;

    if (shouldCancelOthers) {
        cancelOtherStartupStreams(cacheKey, label);
    }

    let state = memoryCache.streamStates[cacheKey];

    if (!state) {
        state = {
            status: "idle",
            createdAt: Date.now(),
            updatedAt: Date.now(),
            readyAt: 0,
            promise: null,
            lastError: null,
            cancelToken: null
        };
        memoryCache.streamStates[cacheKey] = state;
    }

    const existingBuffer = getLiveBuffer(sourceUrl);
    const existingSelection = selectBufferedSegments(existingBuffer, {
        minOutputSegments: STREAM_PREBUFFER_START_MIN_SEGMENTS,
        maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
        delaySegments: 0
    });

    const existingCachedSeconds = getCachedDurationForSegments(existingSelection.segments);
    const existingCachedSegments = getCachedCountForSegments(existingSelection.segments);

    if (
        state.status === "ready" &&
        Date.now() - state.readyAt <= STREAM_READY_TTL &&
        existingCachedSeconds >= STREAM_PREBUFFER_START_SECONDS &&
        existingCachedSegments >= STREAM_PREBUFFER_START_MIN_SEGMENTS
    ) {
        memoryCache.stats.startup.reusedReady += 1;
        queueDynamicPrefetchForBuffer(existingBuffer, label);

        return {
            skipped: false,
            status: "ready",
            reused: true,
            cachedSegments: existingCachedSegments,
            cachedSeconds: existingCachedSeconds
        };
    }

    if (state.promise) {
        memoryCache.stats.startup.waitedExisting += 1;
        console.log(`[STREAM WAIT] channel="${label}" status=${state.status}`);
        return state.promise;
    }

    const effectiveToken = cancelToken || makeStartupCancelToken(sourceUrl, label);

    memoryCache.stats.startup.requested += 1;

    state.status = "acquiring";
    state.updatedAt = Date.now();
    state.lastError = null;
    state.cancelToken = effectiveToken;

    state.promise = (async () => {
        const started = Date.now();

        try {
            memoryCache.stats.startup.acquiring += 1;

            throwIfStartupCancelled(effectiveToken);
            await acquireInitialHLS(sourceUrl, label, effectiveToken);

            state.status = "prebuffering";
            state.updatedAt = Date.now();

            throwIfStartupCancelled(effectiveToken);
            const ready = await prebufferInitialStream(sourceUrl, label, effectiveToken);

            throwIfStartupCancelled(effectiveToken);

            state.status = "ready";
            state.readyAt = Date.now();
            state.updatedAt = Date.now();
            state.lastError = null;
            state.cancelToken = null;
            memoryCache.stats.startup.ready += 1;

            return {
                ...ready,
                totalStartupMs: Date.now() - started
            };
        } catch (err) {
            if (isStartupCancelledError(err)) {
                state.status = "cancelled";
                state.updatedAt = Date.now();
                state.lastError = err.message;
                state.cancelToken = null;
                memoryCache.stats.startup.cancelled += 1;

                console.log(
                    `[STREAM CANCELLED] channel="${label}" total=${Date.now() - started}ms reason=${err.message}`
                );

                throw err;
            }

            state.status = "failed";
            state.updatedAt = Date.now();
            state.lastError = err.message;
            state.cancelToken = null;
            memoryCache.stats.startup.failed += 1;

            console.error(
                `[STREAM FAILED] channel="${label}" total=${Date.now() - started}ms reason=${err.message}`
            );

            throw err;
        }
    })();

    try {
        return await state.promise;
    } finally {
        state.promise = null;
        if (state.cancelToken === effectiveToken) {
            state.cancelToken = null;
        }
    }
}

function updateLiveBufferFromPlaylist(sourceUrl, playlist, baseUrl, options = {}) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const parsed = parseHLSMediaPlaylist(playlist, baseUrl);

    if (parsed.info.isMaster || !parsed.info.isLive || parsed.segments.length === 0) {
        return { playlist, info: parsed.info, cacheKey, buffered: false };
    }

    let buffer = memoryCache.liveBuffers[cacheKey];

    if (!buffer || shouldResetLiveBufferForParsedSegments(buffer, parsed)) {
        if (buffer) {
            console.log(
                `[LIVE BUFFER RESET] channel="${options.label || "stream"}"` +
                ` old=${buffer.segments?.length || 0} newSeq=${parsed.info.mediaSequence}`
            );
        }

        buffer = {
            headerLines: [],
            segments: [],
            segmentMap: {},
            lastUpdated: 0,
            lastMediaSequence: null
        };
        memoryCache.liveBuffers[cacheKey] = buffer;
    }

    buffer.headerLines = parsed.headerLines;
    buffer.lastUpdated = Date.now();
    buffer.lastMediaSequence = parsed.info.mediaSequence;

    parsed.segments.forEach(rawSegment => {
        const segment = cloneSegmentWithLogicalKey(rawSegment);
        const key = segment.logicalKey;

        if (!key) return;

        const existing = buffer.segmentMap[key];

        if (!existing) {
            buffer.segmentMap[key] = {
                ...segment,
                firstSeenAt: Date.now(),
                lastSeenAt: Date.now(),
                altUrls: [segment.url].filter(Boolean)
            };
            buffer.segments.push(buffer.segmentMap[key]);
            memoryCache.stats.liveBuffer.segmentsStored += 1;
        } else {
            const beforeUrl = existing.url;
            const merged = mergeLogicalSegment(existing, segment);

            Object.keys(existing).forEach(k => delete existing[k]);
            Object.assign(existing, merged);

            memoryCache.stats.liveBuffer.segmentsUpdated += 1;

            if (beforeUrl && segment.url && beforeUrl !== segment.url) {
                memoryCache.stats.liveBuffer.segmentsDeduped += 1;
                console.log(
                    `[LIVE DEDUPE] channel="${options.label || "stream"}"` +
                    ` key=${key} keeping=${sanitizeUrlForLog(existing.url)} incoming=${sanitizeUrlForLog(segment.url)}`
                );
            }
        }
    });

    buffer.segments = buffer.segments
        .sort((a, b) => a.sequence - b.sequence)
        .slice(-LIVE_BUFFER_SEGMENTS);

    buffer.segmentMap = {};
    buffer.segments.forEach(segment => {
        const key = segment.logicalKey || getSegmentLogicalKey(segment);
        if (!key) return;
        segment.logicalKey = key;
        buffer.segmentMap[key] = segment;
    });

    const selection = selectBufferedSegments(buffer, {
        minOutputSegments: options.minOutputSegments || MIN_PLAYLIST_OUTPUT_SEGMENTS,
        maxPlaylistSegments: options.maxPlaylistSegments || LIVE_PLAYLIST_SEGMENTS,
        delaySegments: options.delaySegments ?? LIVE_DELAY_SEGMENTS
    });

    const dynamic = queueDynamicPrefetchForBuffer(buffer, options.label || "stream");

    // Always make sure the visible playlist is being cached, but do not over-prefetch
    // beyond the dynamic target/max buffer.
    selection.segments.forEach(segment => {
        if (!getCachedSegmentForLogicalSegment(segment)) queueSegmentPrefetch(segment.url);
    });

    if (!selection.segments.length) {
        return { playlist, info: parsed.info, cacheKey, buffered: false };
    }

    const rebuilt = buildPlaylistFromBufferedSegments(buffer, selection.segments, selection.effectiveDelay);

    if (!rebuilt) {
        return { playlist, info: parsed.info, cacheKey, buffered: false };
    }

    memoryCache.stats.liveBuffer.playlistsBuilt += 1;

    const cachedSeconds = getCachedDurationForSegments(selection.segments);
    const cachedSegments = getCachedCountForSegments(selection.segments);

    return {
        playlist: rebuilt,
        info: analyzeHLSPlaylist(rebuilt),
        cacheKey,
        buffered: true,
        bufferSize: buffer.segments.length,
        delayedSegments: selection.effectiveDelay,
        playlistSegments: selection.segments.length,
        cachedSegments,
        cachedSeconds,
        duration: selection.totalDuration,
        dynamicMode: dynamic.mode,
        dynamicCachedSeconds: dynamic.cachedSeconds,
        dynamicQueued: dynamic.queued,
        segments: selection.segments
    };
}

function shouldLogSegment(segmentNumber, ok = true) {
    if (!ok) return true;
    return SEG_LOG_EVERY > 0 && segmentNumber % SEG_LOG_EVERY === 0;
}

function isValidHLSManifest(data) {
    const text = String(data || "").trim();
    return text.startsWith("#EXTM3U") && (
        text.includes("#EXTINF") ||
        text.includes("#EXT-X-STREAM-INF") ||
        text.includes("#EXT-X-TARGETDURATION") ||
        text.includes("#EXT-X-MEDIA-SEQUENCE") ||
        text.includes("#EXT-X-ENDLIST")
    );
}

function getErrorStatus(err) {
    return err?.response?.status || err?.status || null;
}

function shouldRetryHLS(err) {
    if (err?.retryable) return true;
    const status = getErrorStatus(err);
    if (!status) return true;
    return [200, 408, 425, 429, 500, 502, 503, 504].includes(status);
}

function encodeProxyUrl(url) {
    return Buffer.from(String(url), "utf8").toString("base64url");
}

function decodeProxyUrl(encoded) {
    return Buffer.from(String(encoded), "base64url").toString("utf8");
}

function hashKey(value) {
    return crypto.createHash("sha1").update(String(value)).digest("hex").substring(0, 20);
}

async function fetchHLSManifest(sourceUrl) {
    let lastErr = null;

    for (let attempt = 1; attempt <= HLS_RETRY_COUNT; attempt++) {
        const started = Date.now();
        try {
            memoryCache.stats.hls.totalFetches += 1;

            const response = await axios.get(sourceUrl, {
                timeout: HLS_REQUEST_TIMEOUT,
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "application/x-mpegURL, application/vnd.apple.mpegurl, audio/mpegurl, text/plain, */*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus(status) { return status >= 200 && status < 300; }
            });

            if (!isValidHLSManifest(response.data)) {
                const invalidErr = new Error("Invalid HLS manifest from upstream");
                invalidErr.response = response;
                invalidErr.retryable = true;
                throw invalidErr;
            }

            const info = analyzeHLSPlaylist(response.data);
            memoryCache.stats.hls.okFetches += 1;
            console.log(
                `[HLS OK] ${info.isMaster ? "master" : "media"} live=${info.isLive} segs=${info.segmentCount}` +
                `${info.mediaSequence !== null ? ` seq=${info.mediaSequence}` : ""}` +
                `${info.targetDuration !== null ? ` target=${info.targetDuration}s` : ""}` +
                ` size=${formatBytes(info.bytes)} time=${Date.now() - started}ms`
            );

            return response;
        } catch (err) {
            lastErr = err;
            memoryCache.stats.hls.failedFetches += 1;
            const status = getErrorStatus(err);
            const attemptInfo = HLS_RETRY_COUNT > 1 ? ` try=${attempt}/${HLS_RETRY_COUNT}` : "";
            console.error(`[HLS ERROR]${attemptInfo}`, status ? `status=${status}` : "", err.message);
            if (attempt >= HLS_RETRY_COUNT || !shouldRetryHLS(err)) break;
            await sleep(HLS_RETRY_BASE_DELAY * attempt);
        }
    }

    throw lastErr;
}

async function fetchHLSManifestWithWarmup(sourceUrl, reason = "request") {
    let response = await fetchHLSManifest(sourceUrl);
    let info = analyzeHLSPlaylist(response.data);

    if (!isLiveMediaPlaylistInfo(info) || info.segmentCount >= MIN_LIVE_SEGMENTS_READY) {
        return response;
    }

    for (let attempt = 1; attempt <= MIN_LIVE_WARMUP_ATTEMPTS; attempt++) {
        console.log(`[HLS WARMUP] reason=${reason} segs=${info.segmentCount}/${MIN_LIVE_SEGMENTS_READY} wait=${MIN_LIVE_WARMUP_DELAY_MS}ms`);
        await sleep(MIN_LIVE_WARMUP_DELAY_MS);

        const nextResponse = await fetchHLSManifest(sourceUrl);
        const nextInfo = analyzeHLSPlaylist(nextResponse.data);

        response = nextResponse;
        info = nextInfo;

        if (info.segmentCount >= MIN_LIVE_SEGMENTS_READY) {
            break;
        }
    }

    return response;
}

async function fetchAndStoreHLSRaw(cacheKey, sourceUrl, reason = "request") {
    const inflight = memoryCache.hlsInflight[cacheKey];
    if (inflight) {
        memoryCache.stats.hls.coalescedFetches += 1;
        console.log(`[HLS WAIT] Reusing in-flight fetch reason=${reason} key=${cacheKey}`);
        const result = await inflight;
        return { ...result, cacheStatus: "coalesced" };
    }

    const promise = (async () => {
        const shouldSkipWarmup = ["acquire", "prebuffer"].includes(String(reason)) || String(reason).startsWith("startup-");
        const response = shouldSkipWarmup
            ? await fetchHLSManifest(sourceUrl)
            : await fetchHLSManifestWithWarmup(sourceUrl, reason);
        const finalUrl = response.request?.res?.responseUrl || sourceUrl;
        const info = analyzeHLSPlaylist(response.data);
        const result = {
            playlist: response.data,
            baseUrl: finalUrl,
            updatedAt: Date.now(),
            info,
            refreshing: false
        };
        memoryCache.hlsData[cacheKey] = result;
        return { ...result, cacheStatus: "fetched" };
    })();

    memoryCache.hlsInflight[cacheKey] = promise;
    try {
        return await promise;
    } finally {
        delete memoryCache.hlsInflight[cacheKey];
    }
}

async function refreshHLSInBackground(cacheKey, sourceUrl) {
    const cached = memoryCache.hlsData[cacheKey];
    if (cached?.refreshing || memoryCache.hlsInflight[cacheKey]) return;

    memoryCache.stats.hls.backgroundRefreshes += 1;
    memoryCache.hlsData[cacheKey] = { ...(cached || {}), refreshing: true };

    try {
        await fetchAndStoreHLSRaw(cacheKey, sourceUrl, "background");
    } catch (err) {
        console.error("[HLS BACKGROUND REFRESH ERROR]", err.message);
        if (memoryCache.hlsData[cacheKey]) {
            memoryCache.hlsData[cacheKey].refreshing = false;
        }
    }
}

async function getCachedHLSRaw(sourceUrl) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const cached = memoryCache.hlsData[cacheKey];
    const now = Date.now();

    if (cached?.playlist) {
        const age = now - cached.updatedAt;
        const info = cached.info || analyzeHLSPlaylist(cached.playlist);
        const refreshTtl = getHLSRefreshTTL(info);

        if (age <= refreshTtl) {
            memoryCache.stats.hls.cacheHits += 1;
            return {
                playlist: cached.playlist,
                baseUrl: cached.baseUrl,
                info,
                cacheStatus: "hit",
                age
            };
        }

        // Conservative live mode:
        // for live media playlists, serve the existing buffered playlist immediately
        // and refresh in background. This avoids tiny 1-2s playback stalls caused by
        // waiting for the upstream m3u8 exactly when Stremio wants the next segment.
        if (isLiveMediaPlaylistInfo(info) && age <= HLS_LIVE_STALE_REFRESH_TTL) {
            memoryCache.stats.hls.staleHits += 1;
            refreshHLSInBackground(cacheKey, sourceUrl);
            return {
                playlist: cached.playlist,
                baseUrl: cached.baseUrl,
                info,
                cacheStatus: "stale-refresh-live",
                age
            };
        }

        if (age <= HLS_STALE_TTL) {
            memoryCache.stats.hls.staleHits += 1;
            refreshHLSInBackground(cacheKey, sourceUrl);
            return {
                playlist: cached.playlist,
                baseUrl: cached.baseUrl,
                info,
                cacheStatus: isLiveMediaPlaylistInfo(info) ? "stale-live" : "stale-refresh",
                age
            };
        }

        try {
            return await fetchAndStoreHLSRaw(cacheKey, sourceUrl, "expired-cache");
        } catch (err) {
            memoryCache.stats.hls.staleHits += 1;
            console.error("[HLS] Upstream failed; returning last cached playlist", err.message);
            return {
                playlist: cached.playlist,
                baseUrl: cached.baseUrl,
                info,
                cacheStatus: "stale-fallback",
                age
            };
        }
    }

    return fetchAndStoreHLSRaw(cacheKey, sourceUrl, "miss");
}

function toAbsoluteUrl(value, baseUrl) {
    try {
        return new URL(value, baseUrl).toString();
    } catch (err) {
        return value;
    }
}

function isHttpUrl(value) {
    return /^https?:\/\//i.test(String(value || ""));
}

function isHlsUrl(url) {
    return /\.m3u8(?:[?#].*)?$/i.test(String(url || ""));
}

function trimLiveMediaPlaylistToLiveEdge(playlist) {
    // Compatibility wrapper retained for older references.
    // Conservative mode no longer trims to the live edge directly.
    return String(playlist || "");
}

function rewriteHLSPlaylistThroughKronos(playlist, baseUrl, hostBase, configKey) {
    const proxiedSegment = (absUrl) => `${hostBase}/${configKey}/seg?u=${encodeProxyUrl(absUrl)}`;
    const proxiedPlaylist = (absUrl) => `${hostBase}/${configKey}/pl?u=${encodeProxyUrl(absUrl)}`;
    const proxyForUrl = (absUrl) => isHlsUrl(absUrl) ? proxiedPlaylist(absUrl) : proxiedSegment(absUrl);

    return String(playlist || "").split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed) return line;

        if (trimmed.startsWith("#")) {
            // Handles URI="..." and URI='...' in tags such as EXT-X-KEY, EXT-X-MAP, EXT-X-MEDIA.
            return line.replace(/URI=("([^\"]+)"|'([^']+)')/g, (match, quoted, doubleUri, singleUri) => {
                const uri = doubleUri || singleUri;
                const quote = quoted.startsWith("'") ? "'" : '"';
                const absUrl = toAbsoluteUrl(uri, baseUrl);
                return `URI=${quote}${proxyForUrl(absUrl)}${quote}`;
            });
        }

        const absUrl = toAbsoluteUrl(trimmed, baseUrl);
        return proxyForUrl(absUrl);
    }).join("\n");
}

app.get("/logo.svg", (req, res) => {
    res.setHeader("Content-Type", "image/svg+xml");
    res.send(`
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
            <rect width="100" height="100" rx="20" fill="#0b0c10"/>
            <circle cx="50" cy="50" r="35" fill="none" stroke="url(#kronosGrad)" stroke-width="4" stroke-dasharray="5 3"/>
            <path d="M50 25 V50 L65 50" fill="none" stroke="#ff5e00" stroke-width="4" stroke-linecap="round"/>
            <path d="M35 50 Q50 30 65 50" fill="none" stroke="#ff007f" stroke-width="2" opacity="0.7"/>
            <path d="M25 50 Q50 15 75 50" fill="none" stroke="#38dff4" stroke-width="1.5" opacity="0.4"/>
            <defs>
                <linearGradient id="kronosGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stop-color="#ff5e00"/>
                    <stop offset="100%" stop-color="#ff007f"/>
                </linearGradient>
            </defs>
        </svg>
    `);
});

app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        version: RELEASE_VERSION,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString()
    });
});

async function updateEPGCache(epgUrl) {
    if (!epgUrl) return {};
    try {
        const response = await axios.get(epgUrl, { timeout: 15000 });
        const parser = new xml2js.Parser({ explicitArray: false, ignoreAttrs: false });
        const result = await parser.parseStringPromise(response.data);
        const programmesByChannel = {};

        if (result.tv && result.tv.programme) {
            const programmes = Array.isArray(result.tv.programme) ? result.tv.programme : [result.tv.programme];
            programmes.forEach(prog => {
                if (!prog.$ || !prog.$.channel || !prog.$.start || !prog.$.stop) return;
                const start = parseXMLTVDate(prog.$.start);
                const stop = parseXMLTVDate(prog.$.stop);
                if (Number.isNaN(start.getTime()) || Number.isNaN(stop.getTime())) return;
                const channelKey = normalizeEpgId(prog.$.channel);
                if (!programmesByChannel[channelKey]) programmesByChannel[channelKey] = [];
                programmesByChannel[channelKey].push({
                    start, stop,
                    title: getXmlText(prog.title) || "Programma senza titolo",
                    desc: getXmlText(prog.desc)
                });
            });
        }

        const tempEpgMap = {};
        Object.entries(programmesByChannel).forEach(([channelKey, programmes]) => {
            const selectedProgramme = selectBestProgramme(programmes);
            if (!selectedProgramme) return;
            const label = selectedProgramme.isLive ? "In onda" : "EPG disponibile";
            const desc = selectedProgramme.desc ? ` - ${selectedProgramme.desc}` : "";
            tempEpgMap[channelKey] = `${label}: ${selectedProgramme.title} (${formatTime(selectedProgramme.start)} - ${formatTime(selectedProgramme.stop)})${desc}`;
        });

        memoryCache.epgData[epgUrl] = tempEpgMap;
        return tempEpgMap;
    } catch (err) {
        console.error("[EPG ERROR]", err.message);
        return memoryCache.epgData[epgUrl] || {};
    }
}

function parseXMLTVDate(str) {
    const match = String(str || "").match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-]\d{4}))?/);
    if (!match) return new Date(str);
    const [, year, month, day, hour, minute, second, offset] = match;
    const timezone = offset ? `${offset.slice(0, 3)}:${offset.slice(3)}` : "Z";
    return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}${timezone}`);
}

function getXmlText(value) {
    if (!value) return "";
    if (typeof value === "string") return value;
    if (typeof value === "object" && value._) return value._;
    return "";
}

function formatTime(date) {
    return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function selectBestProgramme(programmes) {
    const now = new Date();
    const sorted = programmes.slice().sort((a, b) => a.start - b.start);
    const live = sorted.find(programme => now >= programme.start && now <= programme.stop);
    if (live) return { ...live, isLive: true };
    const future = sorted.find(programme => programme.start > now);
    if (future) return { ...future, isLive: false };
    const latest = sorted[sorted.length - 1];
    return latest ? { ...latest, isLive: false } : null;
}

function normalizeEpgId(id) {
    let key = String(id || "").toLowerCase().trim();
    key = key.replace(/\.it$/i, "").replace(/[^a-z0-9]/g, "").replace(/hd$/i, "");
    if (key === "20mediaset") return "20";
    if (key === "mediasetextra") return "mediasetextra";
    return key;
}

function getConfiguredLists(config) {
    if (Array.isArray(config.l) && config.l.length) {
        return config.l
            .map((list, index) => ({
                name: String(list.n || `Lista ${index + 1}`).trim() || `Lista ${index + 1}`,
                url: String(list.u || "").trim()
            }))
            .filter(list => list.url);
    }
    return [{
        name: String(config.ln || "Kronos").trim() || "Kronos",
        url: String(config.u || "").trim()
    }].filter(list => list.url);
}

async function fetchPlaylist(sourceUrl) {
    console.log("[FETCH PLAYLIST] Attempting to fetch:", sanitizeUrlForLog(sourceUrl));
    try {
        const response = await axios.get(sourceUrl, {
            timeout: 60000,
            maxRedirects: 5,
            headers: {
                "User-Agent": UPSTREAM_UA,
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive"
            },
            validateStatus(status) { return status >= 200 && status < 300; }
        });
        console.log("[FETCH PLAYLIST] Success, size:", String(response.data || "").length);
        return response.data;
    } catch (err) {
        console.error("[FETCH PLAYLIST ERROR]", sanitizeUrlForLog(sourceUrl), err.message);
        throw err;
    }
}

function escapeXml(value) {
    return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

async function getLogoDataUri(logoUrl) {
    if (!isHttpUrl(logoUrl)) return "";
    if (memoryCache.logoData[logoUrl]) return memoryCache.logoData[logoUrl];
    try {
        const response = await axios.get(logoUrl, {
            responseType: "arraybuffer",
            timeout: 10000,
            maxContentLength: 2 * 1024 * 1024,
            headers: { "User-Agent": "Kronos/" + RELEASE_VERSION, "Accept": "image/*,*/*;q=0.8" }
        });
        const contentType = String(response.headers["content-type"] || "image/png").split(";")[0];
        const dataUri = `data:${contentType};base64,${Buffer.from(response.data).toString("base64")}`;
        memoryCache.logoData[logoUrl] = dataUri;
        return dataUri;
    } catch (err) {
        return "";
    }
}

function parseM3UChannels(data, source = {}) {
    const lines = String(data || "").split("\n");
    const channels = [];
    let currentChannel = null;

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.startsWith("#EXTINF:")) {
            const name = (line.match(/,(.+)$/) || [, "Canale Sconosciuto"])[1].trim();
            const group = (line.match(/group-title="([^"]+)"/) || [, "Altri Canali"])[1].trim();
            const logoMatch = line.match(/tvg-logo="([^"]+)"/);
            const tvgId = (line.match(/tvg-id="([^"]+)"/) || [, null])[1];
            const logo = logoMatch ? logoMatch[1] : `https://placehold.co/512x512/111827/ffffff?text=${encodeURIComponent(name.substring(0, 5))}`;
            currentChannel = { name, group, logo, tvgId, sourceName: source.name || "Kronos", sourceUrl: source.url || "" };
        } else if (line.startsWith("http") && currentChannel) {
            currentChannel.url = line;
            currentChannel.id = "channel_" + crypto.createHash("sha1").update(`${source.url || ""}|${line}`).digest("hex").substring(0, 20);
            channels.push(currentChannel);
            currentChannel = null;
        }
    }

    console.log(`[PARSE M3U] Parsed ${channels.length} channels from ${source.name}`);
    return channels;
}

function decorateChannelName(channel, totalLists, mode) {
    if (totalLists <= 1 || mode !== "filter") return channel.name;
    const baseName = channel.name.replace(/\s*\([^)]*\)\s*$/g, "").trim();
    return `${baseName} (${getListAbbreviation(channel.sourceName)})`;
}

function getListAbbreviation(name) {
    const clean = String(name || "LST").replace(/[^a-z0-9]/gi, "");
    return (clean || "LST").slice(0, 3);
}

function normalizeGroupName(group) {
    return String(group || "").trim().toLowerCase();
}

function normalizeSearchText(value) {
    return String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/&/g, " e ")
        .replace(/[^a-z0-9]+/gi, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function getChannelSearchText(channel) {
    const fields = [
        channel.name,
        channel.group,
        channel.sourceName,
        channel.tvgId,
        channel.description
    ].filter(Boolean);

    const normalized = normalizeSearchText(fields.join(" "));
    const compact = normalized.replace(/\s+/g, "");

    return { normalized, compact };
}

function matchesChannelSearch(channel, rawQuery) {
    const query = normalizeSearchText(rawQuery);
    if (!query) return true;

    const { normalized, compact } = getChannelSearchText(channel);
    const compactQuery = query.replace(/\s+/g, "");

    if (normalized.includes(query)) return true;
    if (compact.includes(compactQuery)) return true;

    const tokens = query.split(" ").filter(Boolean);
    if (tokens.length === 0) return true;

    // Robust Stremio search: exact full-string matching can be flaky when the UI
    // sends spaces/numbers/punctuation differently. Token matching keeps "Canali 2"
    // discoverable even if the search payload is slightly normalized.
    return tokens.every(token => normalized.includes(token));
}

function getExtraParams(extra) {
    const params = {};
    if (!extra) return params;
    const cleanExtra = decodeURIComponent(String(extra)).replace(/\.json$/i, "");
    cleanExtra.split("&").forEach(pair => {
        const eqIndex = pair.indexOf("=");
        if (eqIndex === -1) return;
        const name = pair.slice(0, eqIndex);
        const value = pair.slice(eqIndex + 1);
        if (name) params[name] = decodeURIComponent(value || "");
    });
    return params;
}

function getCatalogSourceName(catalogId) {
    if (!String(catalogId || "").startsWith("kronos_list_")) return null;
    return Buffer.from(catalogId.replace("kronos_list_", ""), "hex").toString("utf8");
}

function toCatalogId(name) {
    return `kronos_list_${Buffer.from(name).toString("hex")}`;
}

function sortChannelsByName(channels) {
    return channels.slice().sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
}

function buildChannelIndex(channels) {
    return channels.reduce((index, channel) => { index[channel.id] = channel; return index; }, {});
}

async function getChannelById(configKey, config, id) {
    let channel = memoryCache.channelIndex[configKey]?.[id];
    if (channel) return channel;
    const channels = await getChannelsFromCache(configKey, config);
    channel = channels.find(ch => ch.id === id);
    if (channel) return channel;
    await fetchAndProcessChannels(configKey, config, { force: true });
    return memoryCache.channelIndex[configKey]?.[id] || null;
}

function isPlayableHttpUrl(url) {
    return /^https?:\/\//i.test(String(url || ""));
}

// All HLS streams always go through Kronos self-relay.
function buildStream(channel, host, configKey) {
    if (isHlsUrl(channel.url) || isPlayableHttpUrl(channel.url)) {
        // For non-HLS direct streams we still go through /hls/ which falls back to seg-style relay
        // but most realistic IPTV lists are HLS, so we treat both the same.
        if (isHlsUrl(channel.url)) {
            return {
                title: channel.name,
                name: "Kronos",
                url: `${host}/${configKey}/hls/${channel.id}/index.m3u8`,
                behaviorHints: { notWebReady: true, bingeGroup: `kronos-${channel.id}` }
            };
        }
        // direct non-HLS stream → proxy through /seg
        return {
            title: channel.name,
            name: "Kronos",
            url: `${host}/${configKey}/seg?u=${encodeProxyUrl(channel.url)}`,
            behaviorHints: { notWebReady: true, bingeGroup: `kronos-${channel.id}` }
        };
    }
    return { title: `${channel.name} - sorgente web`, name: "Kronos", externalUrl: channel.url };
}

function toMeta(channel, host, configKey = "", config = {}) {
    const fallbackLogo = `${host}/logo.svg`;
    const poster = configKey ? `${host}/${configKey}/poster/${channel.id}.svg` : (channel.logo || fallbackLogo);
    const logo = channel.logo || fallbackLogo;
    const stream = configKey ? buildStream(channel, host, configKey) : null;
    return {
        id: channel.id,
        type: ADDON_TYPE,
        name: channel.name,
        poster,
        logo,
        description: channel.description,
        posterShape: "square",
        background: poster,
        genres: channel.group ? [channel.group] : undefined,
        behaviorHints: {
            defaultVideoId: channel.id,
            hasScheduledVideos: false
        },
        videos: [{
            id: channel.id,
            title: channel.name,
            released: new Date(0).toISOString(),
            thumbnail: poster,
            overview: channel.description,
            available: true,
            streams: stream ? [stream] : undefined
        }]
    };
}

async function fetchAndProcessChannels(configKey, config, options = {}) {
    if (memoryCache.isUpdating[configKey] && !options.force) return;
    memoryCache.isUpdating[configKey] = true;
    try {
        const epgMap = config.e ? await updateEPGCache(config.e) : {};
        const configuredLists = getConfiguredLists(config);
        const selectedGroups = Array.isArray(config.g) ? config.g : [];
        const selectedGroupSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";

        const parsedChannelGroups = await Promise.all(configuredLists.map(async list => {
            const playlistData = await fetchPlaylist(list.url);
            return parseM3UChannels(playlistData, list);
        }));

        const channels = parsedChannelGroups.flat()
            .filter(channel => {
                if (config.gm === "list") return true;
                if (config.gm === "bucket") return true;
                if (selectedGroupSet.size === 0) return true;
                return selectedGroupSet.has(normalizeGroupName(channel.group));
            })
            .map(channel => ({
                ...channel,
                name: decorateChannelName(channel, configuredLists.length, config.gm),
                group: config.gm === "bucket" ? bucketGroup : channel.group,
                description: channel.tvgId && epgMap[normalizeEpgId(channel.tvgId)]
                    ? epgMap[normalizeEpgId(channel.tvgId)]
                    : "K.R.O.N.O.S. - Nessun dato guida oraria"
            }));

        console.log("[DEBUG FETCH] Final channel count:", channels.length);
        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.lastUpdate[configKey] = Date.now();
    } catch (err) {
        console.error("[KRONOS ERROR]", err.message);
    } finally {
        memoryCache.isUpdating[configKey] = false;
    }
}

async function getChannelsFromCache(configKey, config) {
    const cachedData = memoryCache.channelItems[configKey];
    if (!cachedData) {
        await fetchAndProcessChannels(configKey, config);
        return memoryCache.channelItems[configKey] || [];
    }
    if (!memoryCache.channelIndex[configKey]) {
        memoryCache.channelIndex[configKey] = buildChannelIndex(cachedData);
    }
    if (Date.now() - (memoryCache.lastUpdate[configKey] || 0) > CACHE_TTL) {
        fetchAndProcessChannels(configKey, config);
    }
    return cachedData;
}

function getPublicHost(req) {
    const forwardedProto = req.get("x-forwarded-proto") || req.protocol;
    const forwardedHost = req.get("x-forwarded-host") || req.get("host");
    return `${forwardedProto}://${forwardedHost}`;
}

app.get("/:base64Config/manifest.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        const host = getPublicHost(req);

        const catalogs = getConfiguredLists(config).map(list => {
            const catalogChannels = channels.filter(channel => channel.sourceName === list.name);
            const catalogGroups = [...new Set(catalogChannels.map(c => c.group))]
                .filter(g => g && g.trim())
                .sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" }));

            const extras = [{ name: "search", isRequired: false }];
            if (catalogGroups.length > 0) {
                extras.push({ name: "genre", options: catalogGroups, isRequired: false });
            }

            return {
                id: toCatalogId(list.name),
                type: ADDON_TYPE,
                name: list.name,
                extra: extras
            };
        });

        res.json({
            id: "org.stremio.kronos.channel",
            version: RELEASE_VERSION,
            name: "Kronos",
            description: "TV",
            logo: `${host}/logo.svg`,
            resources: ["catalog", "meta", "stream"],
            types: [ADDON_TYPE],
            idPrefixes: ["channel_"],
            behaviorHints: {
                configurable: true,
                configurationRequired: false
            },
            catalogs
        });
    } catch (err) {
        console.error("[ERROR] Manifest generation failed:", err);
        res.status(500).json({ error: "Errore Token" });
    }
});

app.post("/api/analyze-link", async (req, res) => {
    try {
        const playlistData = await fetchPlaylist(req.body.url);
        const channels = parseM3UChannels(playlistData, {
            name: req.body.name || "Lista",
            url: req.body.url
        });
        const groupMap = new Map();

        channels.forEach(channel => {
            groupMap.set(channel.group, (groupMap.get(channel.group) || 0) + 1);
        });

        const groups = [...groupMap.entries()]
            .map(([name, count]) => ({ name, count }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));

        res.json({ totalChannels: channels.length, groups });
    } catch (err) {
        res.status(400).json({ error: "Impossibile analizzare la lista M3U" });
    }
});

app.post("/api/analyze-lists", async (req, res) => {
    try {
        const lists = getConfiguredLists({ l: req.body.lists || [] });
        const parsedChannelGroups = await Promise.all(lists.map(async list => {
            const playlistData = await fetchPlaylist(list.url);
            return parseM3UChannels(playlistData, list);
        }));

        const channels = parsedChannelGroups.flat();
        const groupMap = new Map();

        channels.forEach(channel => {
            const current = groupMap.get(channel.group) || {
                name: channel.group,
                count: 0,
                sources: new Set()
            };
            current.count += 1;
            current.sources.add(channel.sourceName);
            groupMap.set(channel.group, current);
        });

        const groups = [...groupMap.values()]
            .map(group => ({
                name: group.name,
                count: group.count,
                sources: [...group.sources]
            }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));

        res.json({
            totalChannels: channels.length,
            totalLists: lists.length,
            groups
        });
    } catch (err) {
        res.status(400).json({ error: "Impossibile analizzare le liste M3U" });
    }
});

async function catalogResponse(req, res) {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        const extraParams = getExtraParams(req.params.extra);
        const targetGroup = extraParams.genre || null;
        const targetSource = getCatalogSourceName(req.params.id);
        const searchQuery = extraParams.search ? String(extraParams.search).trim() : null;
        const host = getPublicHost(req);

        const filteredChannels = sortChannelsByName(channels.filter(channel => {
            const matchesSource = targetSource ? channel.sourceName === targetSource : true;
            const matchesGroup = targetGroup ? normalizeGroupName(channel.group) === normalizeGroupName(targetGroup) : true;
            const matchesSearch = matchesChannelSearch(channel, searchQuery);
            return matchesSource && matchesGroup && matchesSearch;
        }));

        console.log(
            `[CATALOG] id=${req.params.id}` +
            `${searchQuery ? ` search="${searchQuery}"` : ""}` +
            `${targetGroup ? ` genre="${targetGroup}"` : ""}` +
            ` results=${filteredChannels.length}/${channels.length}`
        );

        const metas = filteredChannels.map(c => toMeta(c, host, configKey, config));
        res.json({ metas });
    } catch (err) {
        console.error("[ERROR CATALOG]", err);
        res.status(500).json({ metas: [] });
    }
}

app.get("/:base64Config/catalog/:type/:id.json", catalogResponse);
app.get("/:base64Config/catalog/:type/:id/:extra.json", catalogResponse);

app.get("/:base64Config/meta/:type/:id.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        const host = getPublicHost(req);
        if (!c) return res.status(404).json({ meta: null });
        res.json({ meta: toMeta(c, host, configKey, config) });
    } catch (err) {
        res.status(500).json({ meta: null });
    }
});

app.get("/:base64Config/poster/:id.svg", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        const host = getPublicHost(req);
        const logoUrl = c?.logo || `${host}/logo.svg`;
        const logoDataUri = await getLogoDataUri(logoUrl);
        const name = c?.name || "Kronos";
        const initials = name
            .replace(/\([^)]*\)/g, "")
            .split(/\s+/)
            .filter(Boolean)
            .slice(0, 2)
            .map(part => part[0])
            .join("")
            .toUpperCase() || "TV";

        const logoMarkup = logoDataUri
            ? `<image href="${escapeXml(logoDataUri)}" x="58" y="74" width="396" height="286" preserveAspectRatio="xMidYMid meet"/>`
            : `<text x="256" y="274" text-anchor="middle" fill="#111827" font-family="Arial, sans-serif" font-size="86" font-weight="800">${escapeXml(initials)}</text>`;

        res.setHeader("Content-Type", "image/svg+xml");
        res.setHeader("Cache-Control", "public, max-age=604800");
        res.send(`
            <svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
                <defs>
                    <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
                        <stop offset="0" stop-color="#111827"/>
                        <stop offset="1" stop-color="#050814"/>
                    </linearGradient>
                </defs>
                <rect width="512" height="512" rx="56" fill="url(#bg)"/>
                <rect x="28" y="28" width="456" height="456" rx="44" fill="rgba(255,255,255,0.05)" stroke="rgba(255,255,255,0.16)"/>
                <rect x="46" y="58" width="420" height="318" rx="32" fill="#f8fafc"/>
                ${logoMarkup}
                <text x="256" y="424" text-anchor="middle" fill="#f5f7fb" font-family="Arial, sans-serif" font-size="30" font-weight="700">${escapeXml(name.slice(0, 34))}</text>
            </svg>
        `);
    } catch (err) {
        res.status(404).send("");
    }
});

// --- SELF-RELAY ENDPOINTS ---

// Main HLS entry point: fetch source manifest, rewrite all URLs through Kronos
app.get("/:base64Config/hls/:id/index.m3u8", async (req, res) => {
    let startupToken = null;

    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        if (!c) return res.status(404).send("#EXTM3U\n");

        const host = getPublicHost(req);
        startupToken = makeStartupCancelToken(c.url, c.name);

        req.on("close", () => {
            if (!res.writableEnded && startupToken) {
                cancelStartupToken(startupToken, "client closed HLS startup request");
            }
        });

        await ensureStreamPrebuffered(c.url, c.name, startupToken, { cancelOthers: true });

        const { playlist, baseUrl, cacheStatus, age } = await getCachedHLSRaw(c.url);
        const buffered = updateLiveBufferFromPlaylist(c.url, playlist, baseUrl, {
            minOutputSegments: MIN_PLAYLIST_OUTPUT_SEGMENTS,
            maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
            label: c.name
        });
        const outputPlaylist = buffered.playlist;
        const rewritten = rewriteHLSPlaylistThroughKronos(outputPlaylist, baseUrl, host, configKey);

        const playlistInfo = buffered.info || analyzeHLSPlaylist(outputPlaylist);
        console.log(
            `[HLS ROUTE] channel="${c.name}" cache=${cacheStatus}` +
            `${typeof age === "number" ? ` age=${age}ms` : ""}` +
            ` type=${playlistInfo.isMaster ? "master" : "media"} segs=${playlistInfo.segmentCount}` +
            `${playlistInfo.mediaSequence !== null ? ` seq=${playlistInfo.mediaSequence}` : ""}` +
            `${buffered.buffered ? ` buffer=${buffered.bufferSize} delay=${buffered.delayedSegments} out=${buffered.playlistSegments}` : ""}` +
            `${buffered.cachedSegments !== undefined ? ` cached=${buffered.cachedSegments}` : ""}` +
            `${buffered.cachedSeconds !== undefined ? ` cached≈${Math.round(buffered.cachedSeconds)}s` : ""}` +
            `${buffered.dynamicMode ? ` mode=${buffered.dynamicMode}` : ""}`
        );

        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, no-transform");
        res.setHeader("Pragma", "no-cache");
        res.send(rewritten);
    } catch (err) {
        if (isStartupCancelledError(err)) {
            console.log("[HLS ROUTE CANCELLED]", err.message);
            if (!res.headersSent && !res.destroyed) {
                return res.status(499).send("#EXTM3U\n#EXT-X-ENDLIST\n");
            }
            return;
        }

        console.error("[ERROR HLS ROUTE]", err.message);
        if (!res.headersSent && !res.destroyed) {
            res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
        }
    }
});

// Nested playlist relay: variant playlists in master playlists, etc.
app.get("/:base64Config/pl", async (req, res) => {
    let startupToken = null;

    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");

        const sourceUrl = decodeProxyUrl(req.query.u);
        const host = getPublicHost(req);
        startupToken = makeStartupCancelToken(sourceUrl, "nested-playlist");

        req.on("close", () => {
            if (!res.writableEnded && startupToken) {
                cancelStartupToken(startupToken, "client closed nested playlist request");
            }
        });

        await ensureStreamPrebuffered(sourceUrl, "nested-playlist", startupToken, { cancelOthers: false });

        const { playlist, baseUrl, cacheStatus, age } = await getCachedHLSRaw(sourceUrl);
        const buffered = updateLiveBufferFromPlaylist(sourceUrl, playlist, baseUrl, {
            minOutputSegments: MIN_PLAYLIST_OUTPUT_SEGMENTS,
            maxPlaylistSegments: LIVE_PLAYLIST_SEGMENTS,
            label: "nested-playlist"
        });
        const outputPlaylist = buffered.playlist;
        const rewritten = rewriteHLSPlaylistThroughKronos(outputPlaylist, baseUrl, host, configKey);

        const playlistInfo = buffered.info || analyzeHLSPlaylist(outputPlaylist);
        console.log(
            `[PL ROUTE] cache=${cacheStatus}` +
            `${typeof age === "number" ? ` age=${age}ms` : ""}` +
            ` type=${playlistInfo.isMaster ? "master" : "media"} segs=${playlistInfo.segmentCount}` +
            `${playlistInfo.mediaSequence !== null ? ` seq=${playlistInfo.mediaSequence}` : ""}` +
            `${buffered.buffered ? ` buffer=${buffered.bufferSize} delay=${buffered.delayedSegments} out=${buffered.playlistSegments}` : ""}` +
            `${buffered.cachedSegments !== undefined ? ` cached=${buffered.cachedSegments}` : ""}` +
            `${buffered.cachedSeconds !== undefined ? ` cached≈${Math.round(buffered.cachedSeconds)}s` : ""}` +
            `${buffered.dynamicMode ? ` mode=${buffered.dynamicMode}` : ""}`
        );

        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
        res.send(rewritten);
    } catch (err) {
        if (isStartupCancelledError(err)) {
            console.log("[PL ROUTE CANCELLED]", err.message);
            if (!res.headersSent && !res.destroyed) {
                return res.status(499).send("#EXTM3U\n#EXT-X-ENDLIST\n");
            }
            return;
        }

        console.error("[ERROR PL ROUTE]", err.message);
        if (!res.headersSent && !res.destroyed) {
            res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
        }
    }
});

// Segment relay: streams .ts or any non-playlist resource from upstream to client
app.get("/:base64Config/seg", async (req, res) => {
    let upstream = null;
    let segmentNo = null;
    let sourceUrl = null;
    let started = Date.now();
    let bytes = 0;
    let finalized = false;
    let cacheChunks = [];
    let shouldStoreStream = true;
    let responseHeadersForCache = {};
    let responseStatusForCache = 200;

    function finalizeSegment(result, err = null) {
        if (finalized || segmentNo === null) return;
        finalized = true;

        memoryCache.stats.seg.active = Math.max(0, memoryCache.stats.seg.active - 1);

        if (result === "ok") memoryCache.stats.seg.ok += 1;
        else if (result === "cache") memoryCache.stats.seg.cache += 1;
        else if (result === "aborted") memoryCache.stats.seg.aborted += 1;
        else memoryCache.stats.seg.error += 1;

        memoryCache.stats.seg.bytes += bytes;

        const ok = result === "ok" || result === "cache";
        if (shouldLogSegment(segmentNo, ok)) {
            const duration = Date.now() - started;
            const range = req.headers.range ? ` range=${req.headers.range}` : "";
            const status = res.statusCode || "?";
            const label = result === "cache" ? "SEG CACHE" : ok ? "SEG OK" : result === "aborted" ? "SEG ABORT" : "SEG ERROR";

            console.log(
                `[${label} #${segmentNo}] status=${status} bytes=${formatBytes(bytes)} time=${duration}ms active=${memoryCache.stats.seg.active}${range}` +
                `${sourceUrl ? ` url=${sanitizeUrlForLog(sourceUrl)}` : ""}` +
                `${err ? ` err=${err.message}` : ""}`
            );
        }
    }

    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).end();

        sourceUrl = decodeProxyUrl(req.query.u);
        segmentNo = ++memoryCache.stats.seg.total;
        memoryCache.stats.seg.active += 1;

        const cached = getSegmentFromCache(sourceUrl);
        if (cached) {
            memoryCache.stats.segmentCache.hits += 1;
            bytes = cached.buffer.length;
            sendCachedSegment(req, res, cached, segmentNo);
            finalizeSegment("cache");
            return;
        }

        memoryCache.stats.segmentCache.misses += 1;

        const waitedCached = await waitForSegmentPrefetch(sourceUrl, SEGMENT_WAIT_FOR_PREFETCH_MS);
        if (waitedCached) {
            memoryCache.stats.segmentCache.hits += 1;
            bytes = waitedCached.buffer.length;
            sendCachedSegment(req, res, waitedCached, segmentNo);
            finalizeSegment("cache");
            return;
        }

        const headers = {
            "User-Agent": UPSTREAM_UA,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive"
        };

        if (req.headers.range) headers.Range = req.headers.range;

        const response = await axios.get(sourceUrl, {
            responseType: "stream",
            timeout: SEG_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers,
            decompress: false,
            maxBodyLength: Infinity,
            maxContentLength: Infinity,
            validateStatus(status) { return status >= 200 && status < 400; }
        });

        upstream = response.data;
        responseHeadersForCache = response.headers || {};
        responseStatusForCache = response.status;

        const headersToForward = [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
            "last-modified",
            "etag",
            "cache-control",
            "expires"
        ];

        headersToForward.forEach(h => {
            if (response.headers[h]) res.setHeader(h, response.headers[h]);
        });

        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Cache", "MISS");
        res.status(response.status);

        upstream.on("data", chunk => {
            bytes += chunk.length;

            if (shouldStoreStream && bytes <= SEGMENT_CACHE_MAX_ITEM_BYTES) {
                cacheChunks.push(chunk);
            } else {
                shouldStoreStream = false;
                cacheChunks = [];
            }
        });

        res.on("finish", () => {
            if (shouldStoreStream && cacheChunks.length > 0 && (responseStatusForCache === 200 || responseStatusForCache === 206)) {
                const buffer = Buffer.concat(cacheChunks);
                storeSegmentInCache(sourceUrl, buffer, responseHeadersForCache, responseStatusForCache, "stream");
            }
            finalizeSegment("ok");
        });

        res.on("close", () => {
            if (!res.writableEnded) finalizeSegment("aborted");
        });

        upstream.on("error", err => {
            console.error("[SEG STREAM ERROR]", err.message);
            finalizeSegment("error", err);
            if (!res.headersSent) res.status(502);
            res.end();
        });

        req.on("close", () => {
            if (upstream && !upstream.destroyed) upstream.destroy();
        });

        upstream.pipe(res);
    } catch (err) {
        const status = err.response?.status || 502;
        if (!res.headersSent) res.status(status);
        res.end();

        if (upstream && !upstream.destroyed) upstream.destroy();

        finalizeSegment("error", err);
    }
});

app.get("/:base64Config/stream/:type/:id.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        if (!c) return res.json({ streams: [] });

        const host = getPublicHost(req);
        res.json({ streams: [buildStream(c, host, configKey)] });
    } catch (err) {
        res.json({ streams: [] });
    }
});

app.get("/configure", (req, res) => res.redirect("/"));

app.get("/:base64Config/configure", (req, res) => {
    res.redirect(`/?config=${encodeURIComponent(req.params.base64Config)}`);
});

app.get("/stats", (req, res) => {
    res.json({
        version: RELEASE_VERSION,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        stats: memoryCache.stats,
        hlsCacheKeys: Object.keys(memoryCache.hlsData).length,
        hlsInflightKeys: Object.keys(memoryCache.hlsInflight).length,
        liveBufferKeys: Object.keys(memoryCache.liveBuffers).length,
        segmentCacheKeys: Object.keys(memoryCache.segmentData).length,
        segmentCacheBytes: memoryCache.segmentCacheBytes,
        segmentPrefetchActive: memoryCache.segmentPrefetchActive,
        segmentPrefetchQueue: memoryCache.segmentPrefetchQueue.length,
        streamStates: Object.entries(memoryCache.streamStates).map(([key, state]) => ({
            key,
            status: state.status,
            readyAt: state.readyAt,
            updatedAt: state.updatedAt,
            lastError: state.lastError
        }))
    });
});

app.get("/:base64Config/debug", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);

        res.json({
            config: safeConfigForLog(config),
            mode: "self-relay",
            version: RELEASE_VERSION,
            totalChannels: channels.length,
            sampleChannels: channels.slice(0, 3),
            uniqueGroups: [...new Set(channels.map(c => c.group))],
            uniqueSourceNames: [...new Set(channels.map(c => c.sourceName))],
            configuredLists: getConfiguredLists(config),
            hlsSettings: {
                HLS_REFRESH_TTL,
                HLS_MASTER_REFRESH_TTL,
                HLS_STALE_TTL,
                HLS_LIVE_STALE_REFRESH_TTL,
                HLS_REQUEST_TIMEOUT,
                HLS_RETRY_COUNT,
                SEG_REQUEST_TIMEOUT,
                SEG_LOG_EVERY,
                LIVE_BUFFER_SEGMENTS,
                LIVE_PLAYLIST_SEGMENTS,
                LIVE_DELAY_SEGMENTS,
                MIN_LIVE_SEGMENTS_READY,
                MIN_LIVE_WARMUP_ATTEMPTS,
                MIN_LIVE_WARMUP_DELAY_MS,
                STREAM_STARTUP_PREBUFFER_ENABLED,
                STREAM_ACQUIRE_TIMEOUT,
                STREAM_ACQUIRE_POLL_MS,
                STREAM_PREBUFFER_START_SECONDS,
                STREAM_PREBUFFER_START_MIN_SEGMENTS,
                STREAM_PREBUFFER_TIMEOUT,
                DYNAMIC_BUFFER_CRITICAL_SECONDS,
                DYNAMIC_BUFFER_LOW_SECONDS,
                DYNAMIC_BUFFER_TARGET_SECONDS,
                DYNAMIC_BUFFER_MAX_SECONDS,
                STREAM_READY_TTL,
                MIN_PLAYLIST_OUTPUT_SEGMENTS,
                SEGMENT_CACHE_TTL,
                SEGMENT_CACHE_MAX_BYTES,
                SEGMENT_CACHE_MAX_SEGMENTS,
                SEGMENT_PREFETCH_CONCURRENCY,
                SEGMENT_PREFETCH_AHEAD
            },
            cacheInfo: {
                lastUpdate: memoryCache.lastUpdate[configKey],
                isUpdating: memoryCache.isUpdating[configKey],
                hlsCacheKeys: Object.keys(memoryCache.hlsData).length,
                hlsInflightKeys: Object.keys(memoryCache.hlsInflight).length,
                liveBufferKeys: Object.keys(memoryCache.liveBuffers).length,
                segmentCacheKeys: Object.keys(memoryCache.segmentData).length,
                segmentCacheBytes: memoryCache.segmentCacheBytes,
                segmentPrefetchActive: memoryCache.segmentPrefetchActive,
                segmentPrefetchQueue: memoryCache.segmentPrefetchQueue.length,
                streamStateKeys: Object.keys(memoryCache.streamStates).length
            },
            stats: memoryCache.stats
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, "0.0.0.0", () => {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`K.R.O.N.O.S. ${RELEASE_VERSION} - Self-Relay Mode`);
    console.log(`${"=".repeat(60)}`);
    console.log(`Server: http://0.0.0.0:${PORT}`);
    console.log(`HLS live refresh: ${HLS_REFRESH_TTL}ms | live stale refresh: ${HLS_LIVE_STALE_REFRESH_TTL}ms | stale fallback: ${HLS_STALE_TTL}ms`);
    console.log(`Live buffer: keep=${LIVE_BUFFER_SEGMENTS} | playlist=${LIVE_PLAYLIST_SEGMENTS} | delay=${LIVE_DELAY_SEGMENTS} | minReady=${MIN_LIVE_SEGMENTS_READY}`);
    console.log(`Startup dynamic buffer: enabled=${STREAM_STARTUP_PREBUFFER_ENABLED} | acquire=${STREAM_ACQUIRE_TIMEOUT}ms | start=${STREAM_PREBUFFER_START_SECONDS}s/${STREAM_PREBUFFER_START_MIN_SEGMENTS}seg | prebufferTimeout=${STREAM_PREBUFFER_TIMEOUT}ms`);
    console.log(`Dynamic buffer: critical=${DYNAMIC_BUFFER_CRITICAL_SECONDS}s | low=${DYNAMIC_BUFFER_LOW_SECONDS}s | target=${DYNAMIC_BUFFER_TARGET_SECONDS}s | max=${DYNAMIC_BUFFER_MAX_SECONDS}s`);
    console.log(`Segment cache: max=${formatBytes(SEGMENT_CACHE_MAX_BYTES)} | maxSegments=${SEGMENT_CACHE_MAX_SEGMENTS} | prefetchConcurrency=${SEGMENT_PREFETCH_CONCURRENCY}`);
    console.log(`Segment timeout: ${SEG_REQUEST_TIMEOUT}ms | segment log every: ${SEG_LOG_EVERY}`);
    console.log(`Node: ${process.version}`);
    console.log(`${"=".repeat(60)}\n`);
});
