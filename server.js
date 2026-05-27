const express = require("express");
const axios = require("axios");
const xml2js = require("xml2js");
const path = require("path");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 7000;

app.set('trust proxy', true);

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
    channelInflight: {},
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
    playbackStates: {},
    activeSessions: {},
    logoData: {},
    lastUpdate: {},
    isUpdating: {}
};
const CACHE_TTL = 30 * 60 * 1000;
const UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const HLS_REFRESH_TTL = Number(process.env.HLS_REFRESH_TTL || 2000);
const HLS_MASTER_REFRESH_TTL = Number(process.env.HLS_MASTER_REFRESH_TTL || 30000);
const HLS_STALE_TTL = Number(process.env.HLS_STALE_TTL || 120000);
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 12000);
const STREAM_ACQUIRE_TIMEOUT = Number(process.env.STREAM_ACQUIRE_TIMEOUT || 25000);
const STREAM_ACQUIRE_POLL_MS = Number(process.env.STREAM_ACQUIRE_POLL_MS || 1000);
const STREAM_PREBUFFER_SECONDS = Number(process.env.STREAM_PREBUFFER_SECONDS || 30);
const STREAM_PREBUFFER_TIMEOUT = Number(process.env.STREAM_PREBUFFER_TIMEOUT || 70000);
const DYNAMIC_BUFFER_TARGET_SECONDS = Number(process.env.DYNAMIC_BUFFER_TARGET_SECONDS || 70);
const DYNAMIC_BUFFER_MAX_SECONDS = Number(process.env.DYNAMIC_BUFFER_MAX_SECONDS || 90);
const LIVE_BUFFER_SEGMENTS = Number(process.env.LIVE_BUFFER_SEGMENTS || 36);
const LIVE_PLAYLIST_SEGMENTS = Number(process.env.LIVE_PLAYLIST_SEGMENTS || 10);
const LIVE_DELAY_SEGMENTS = Number(process.env.LIVE_DELAY_SEGMENTS || 1);
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);
const SEGMENT_CACHE_TTL = Number(process.env.SEGMENT_CACHE_TTL || 10 * 60 * 1000);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_BYTES || 768 * 1024 * 1024);
const SEGMENT_CACHE_MAX_SEGMENTS = Number(process.env.SEGMENT_CACHE_MAX_SEGMENTS || 180);
const SEGMENT_CACHE_MAX_ITEM_BYTES = Number(process.env.SEGMENT_CACHE_MAX_ITEM_BYTES || 24 * 1024 * 1024);
const SEGMENT_PREFETCH_CONCURRENCY = Number(process.env.SEGMENT_PREFETCH_CONCURRENCY || 4);
const SEGMENT_PREFETCH_AHEAD = Number(process.env.SEGMENT_PREFETCH_AHEAD || 14);
const SEGMENT_WAIT_FOR_PREFETCH_MS = Number(process.env.SEGMENT_WAIT_FOR_PREFETCH_MS || 3000);
const PLAYER_SEGMENT_SLOW_MS = Number(process.env.PLAYER_SEGMENT_SLOW_MS || 2500);
const PLAYER_SEGMENT_GAP_WARN_MS = Number(process.env.PLAYER_SEGMENT_GAP_WARN_MS || 30000);
const ADDON_TYPE = "kronos";
const RELEASE_VERSION = "1.6.3";

function decodeConfig(configKey) {
    try {
        const normalized = String(configKey || "")
            .replace(/-/g, "+")
            .replace(/_/g, "/");
        const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
        const decoded = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
        console.log('[DECODE CONFIG] Successfully decoded config');
        return decoded;
    } catch (err) {
        console.error('[DECODE CONFIG ERROR]', err.message);
        throw new Error('Invalid configuration token');
    }
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
                    start,
                    stop,
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
    key = key.replace(/\.it$/i, "");
    key = key.replace(/[^a-z0-9]/g, "");
    key = key.replace(/hd$/i, "");

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
    console.log('[FETCH PLAYLIST] Attempting to fetch:', sourceUrl);
    
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
            validateStatus: function (status) {
                return status >= 200 && status < 300;
            }
        });
        
        console.log('[FETCH PLAYLIST] Success:', sourceUrl, 'Size:', response.data.length);
        return response.data;
    } catch (err) {
        console.error('[FETCH PLAYLIST ERROR]', sourceUrl, err.message);
        if (err.response) {
            console.error('[FETCH PLAYLIST ERROR] Status:', err.response.status);
            console.error('[FETCH PLAYLIST ERROR] Headers:', err.response.headers);
        }
        throw err;
    }
}

function toAbsoluteUrl(value, baseUrl) {
    try {
        return new URL(value, baseUrl).toString();
    } catch (err) {
        return value;
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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

function formatBytes(bytes) {
    const value = Number(bytes || 0);
    if (value < 1024) return `${value}B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)}KB`;
    return `${(value / 1024 / 1024).toFixed(2)}MB`;
}

function formatSeconds(value) {
    return `${Math.round(Number(value || 0))}s`;
}

function getBufferStatus(cachedSeconds) {
    if (cachedSeconds < STREAM_PREBUFFER_SECONDS) return "warming";
    if (cachedSeconds < DYNAMIC_BUFFER_TARGET_SECONDS) return "filling";
    if (cachedSeconds >= DYNAMIC_BUFFER_MAX_SECONDS) return "full";
    return "healthy";
}

function getPlaybackKey(req) {
    return `${req.params.base64Config}:${req.ip || req.get("x-forwarded-for") || "client"}`;
}

function getSegmentLabel(sourceUrl) {
    try {
        const url = new URL(sourceUrl);
        return decodeURIComponent(url.pathname.split("/").filter(Boolean).pop() || "segment");
    } catch (err) {
        return String(sourceUrl || "").split("?")[0].split("/").filter(Boolean).pop() || "segment";
    }
}

function notePlayerSegment(req, sourceUrl, event = {}) {
    const key = getPlaybackKey(req);
    const now = Date.now();
    const state = memoryCache.playbackStates[key] || {
        startedAt: now,
        lastSegmentAt: 0,
        ok: 0,
        errors: 0,
        slow: 0,
        possibleBuffering: 0
    };

    const gapMs = state.lastSegmentAt ? now - state.lastSegmentAt : 0;
    state.lastSegmentAt = now;

    if (event.ok) state.ok += 1;
    if (event.error) state.errors += 1;
    if (event.durationMs >= PLAYER_SEGMENT_SLOW_MS) state.slow += 1;

    const gapRisk = gapMs >= PLAYER_SEGMENT_GAP_WARN_MS;
    const slowRisk = event.durationMs >= PLAYER_SEGMENT_SLOW_MS;
    const errorRisk = Boolean(event.error);

    if (gapRisk || slowRisk || errorRisk) {
        state.possibleBuffering += 1;
        console.warn(
            `[PLAYER BUFFER RISK] segment=${getSegmentLabel(sourceUrl)}` +
            ` reason=${errorRisk ? "segment-error" : slowRisk ? "slow-segment" : "long-request-gap"}` +
            `${event.status ? ` status=${event.status}` : ""}` +
            `${event.durationMs !== undefined ? ` time=${event.durationMs}ms` : ""}` +
            `${gapMs ? ` gap=${gapMs}ms` : ""}` +
            `${event.source ? ` source=${event.source}` : ""}` +
            ` totals=ok:${state.ok},slow:${state.slow},errors:${state.errors}`
        );
    }

    memoryCache.playbackStates[key] = state;
    return { gapMs, state };
}

function createCancelledError(message) {
    const err = new Error(message || "Cancelled because another channel became active");
    err.code = "KRONOS_SESSION_CANCELLED";
    return err;
}

function activatePlaybackSession(req, channel) {
    const key = getPlaybackKey(req);
    const previous = memoryCache.activeSessions[key];
    const channelId = channel?.id || hashKey(channel?.url || "unknown");
    const channelName = channel?.name || "stream";

    if (previous?.channelId === channelId) {
        previous.lastSeenAt = Date.now();
        return previous;
    }

    const session = {
        key,
        id: `${Date.now()}:${Math.random().toString(36).slice(2, 8)}`,
        channelId,
        channelName,
        sourceUrl: channel?.url || "",
        startedAt: Date.now(),
        lastSeenAt: Date.now()
    };

    memoryCache.activeSessions[key] = session;

    if (previous) {
        cancelSessionWork(previous, `channel-switch:${channelName}`);
        console.log(`[CHANNEL SWITCH] from="${previous.channelName}" to="${channelName}"`);
    } else {
        console.log(`[CHANNEL SESSION] channel="${channelName}" active`);
    }

    return session;
}

function isSessionActive(session) {
    if (!session?.key || !session?.id) return true;
    return memoryCache.activeSessions[session.key]?.id === session.id;
}

function assertSessionActive(session, label = "stream") {
    if (isSessionActive(session)) return;
    console.log(`[STARTUP CANCELLED] channel="${label}" reason=channel-switch`);
    throw createCancelledError(`Cancelled ${label}: another channel became active`);
}

function cancelSessionWork(session, reason = "inactive-session") {
    if (!session) return;

    const before = memoryCache.segmentPrefetchQueue.length;
    memoryCache.segmentPrefetchQueue = memoryCache.segmentPrefetchQueue.filter(item => {
        if (typeof item === "string") return true;
        return item.sessionId !== session.id;
    });
    const removed = before - memoryCache.segmentPrefetchQueue.length;
    if (removed > 0) {
        console.log(`[PREFETCH CANCELLED] channel="${session.channelName}" reason=${reason} removed=${removed}`);
    }
}

function getHLSCacheKey(sourceUrl) {
    return `hls:${hashKey(sourceUrl)}`;
}

function getSegmentCacheKey(sourceUrl) {
    return `seg:${hashKey(sourceUrl)}`;
}

function analyzeHLSPlaylist(data) {
    const text = String(data || "");
    const lines = text.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const mediaSequenceLine = lines.find(line => line.startsWith("#EXT-X-MEDIA-SEQUENCE:"));
    const targetDurationLine = lines.find(line => line.startsWith("#EXT-X-TARGETDURATION:"));

    return {
        isMaster: lines.some(line => line.startsWith("#EXT-X-STREAM-INF")),
        isLive: !lines.some(line => line.startsWith("#EXT-X-ENDLIST")),
        segmentCount: lines.filter(line => line && !line.startsWith("#")).length,
        mediaSequence: mediaSequenceLine ? Number(mediaSequenceLine.split(":")[1]) : null,
        targetDuration: targetDurationLine ? Number(targetDurationLine.split(":")[1]) : null,
        bytes: Buffer.byteLength(text)
    };
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

function getHLSRefreshTTL(info) {
    if (info?.isMaster) return HLS_MASTER_REFRESH_TTL;
    if (info?.isLive && info?.targetDuration) {
        return Math.max(1000, Math.min(HLS_REFRESH_TTL, Math.floor(info.targetDuration * 500)));
    }
    return HLS_REFRESH_TTL;
}

function extractSegmentIdentityFromUrl(value) {
    try {
        const url = new URL(value);
        const filename = decodeURIComponent(url.pathname || "").split("/").filter(Boolean).pop() || "";
        const cleanFilename = filename.split("?")[0].split("#")[0];
        if (cleanFilename) return `file:${cleanFilename.toLowerCase()}`;
    } catch (err) {
        const filename = String(value || "").split("?")[0].split("#")[0].split("/").filter(Boolean).pop() || "";
        if (filename) return `file:${filename.toLowerCase()}`;
    }
    return null;
}

function getSegmentLogicalKey(segment) {
    if (!segment) return null;
    return extractSegmentIdentityFromUrl(segment.url) || (segment.url ? `url:${hashKey(segment.url)}` : null);
}

function normalizeHeaderLineToAbsolute(line, baseUrl) {
    const trimmed = String(line || "").trim();
    if (!trimmed) return line;
    if (!trimmed.startsWith("#")) return toAbsoluteUrl(trimmed, baseUrl);

    return String(line).replace(/URI=("([^"]+)"|'([^']+)')/g, (match, quoted, doubleUri, singleUri) => {
        const uri = doubleUri || singleUri;
        const quote = quoted.startsWith("'") ? "'" : '"';
        return `URI=${quote}${toAbsoluteUrl(uri, baseUrl)}${quote}`;
    });
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
                const absoluteUrl = toAbsoluteUrl(trimmed, baseUrl);
                const duration = Number.isFinite(pendingDuration) ? pendingDuration : (info.targetDuration || 10);
                segments.push({
                    sequence: originalMediaSequence + segments.length,
                    url: absoluteUrl,
                    lines: pendingSegmentLines,
                    duration,
                    logicalKey: extractSegmentIdentityFromUrl(absoluteUrl) || `url:${hashKey(absoluteUrl)}`,
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

    return { info, headerLines, segments };
}

function rewriteHLSPlaylistThroughKronos(playlist, baseUrl, hostBase, configKey) {
    const proxiedSegment = absUrl => `${hostBase}/${configKey}/proxy/seg?u=${encodeProxyUrl(absUrl)}`;
    const proxiedPlaylist = absUrl => `${hostBase}/${configKey}/proxy/pl?u=${encodeProxyUrl(absUrl)}`;
    const proxyForUrl = absUrl => isHlsUrl(absUrl) ? proxiedPlaylist(absUrl) : proxiedSegment(absUrl);

    return String(playlist || "").split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed) return line;

        if (trimmed.startsWith("#")) {
            return line.replace(/URI=("([^"]+)"|'([^']+)')/g, (match, quoted, doubleUri, singleUri) => {
                const uri = doubleUri || singleUri;
                const quote = quoted.startsWith("'") ? "'" : '"';
                return `URI=${quote}${proxyForUrl(toAbsoluteUrl(uri, baseUrl))}${quote}`;
            });
        }

        return proxyForUrl(toAbsoluteUrl(trimmed, baseUrl));
    }).join("\n");
}

function escapeXml(value) {
    return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function isHttpUrl(value) {
    return /^https?:\/\//i.test(String(value || ""));
}

async function getLogoDataUri(logoUrl) {
    if (!isHttpUrl(logoUrl)) return "";
    if (memoryCache.logoData[logoUrl]) return memoryCache.logoData[logoUrl];

    try {
        const response = await axios.get(logoUrl, {
            responseType: "arraybuffer",
            timeout: 10000,
            maxContentLength: 2 * 1024 * 1024,
            headers: {
                "User-Agent": `Kronos/${RELEASE_VERSION}`,
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
            }
        });

        const contentType = String(response.headers["content-type"] || "image/png").split(";")[0];
        const dataUri = `data:${contentType};base64,${Buffer.from(response.data).toString("base64")}`;
        memoryCache.logoData[logoUrl] = dataUri;
        return dataUri;
    } catch (err) {
        return "";
    }
}

async function fetchHLSManifest(sourceUrl) {
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
        throw new Error("Invalid HLS manifest from upstream");
    }

    const info = analyzeHLSPlaylist(response.data);
    console.log(
        `[HLS OK] ${info.isMaster ? "master" : "media"} live=${info.isLive}` +
        ` segs=${info.segmentCount}` +
        `${info.mediaSequence !== null ? ` seq=${info.mediaSequence}` : ""}` +
        `${info.targetDuration !== null ? ` target=${info.targetDuration}s` : ""}`
    );

    return response;
}

async function fetchAndStoreHLSRaw(cacheKey, sourceUrl, reason = "request") {
    if (memoryCache.hlsInflight[cacheKey]) {
        return memoryCache.hlsInflight[cacheKey];
    }

    const promise = (async () => {
        const response = await fetchHLSManifest(sourceUrl);
        const finalUrl = response.request?.res?.responseUrl || sourceUrl;
        const result = {
            playlist: response.data,
            baseUrl: finalUrl,
            info: analyzeHLSPlaylist(response.data),
            updatedAt: Date.now(),
            reason
        };
        memoryCache.hlsData[cacheKey] = result;
        return result;
    })();

    memoryCache.hlsInflight[cacheKey] = promise;
    try {
        return await promise;
    } finally {
        delete memoryCache.hlsInflight[cacheKey];
    }
}

async function getCachedHLSRaw(sourceUrl) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const cached = memoryCache.hlsData[cacheKey];
    const now = Date.now();

    if (cached?.playlist) {
        const info = cached.info || analyzeHLSPlaylist(cached.playlist);
        const age = now - cached.updatedAt;
        if (age <= getHLSRefreshTTL(info)) {
            return { ...cached, cacheStatus: "hit", age };
        }
    }

    try {
        const fresh = await fetchAndStoreHLSRaw(cacheKey, sourceUrl, cached ? "refresh" : "miss");
        return { ...fresh, cacheStatus: cached ? "refresh" : "miss", age: 0 };
    } catch (err) {
        if (cached?.playlist && now - cached.updatedAt <= HLS_STALE_TTL) {
            console.error("[HLS STALE FALLBACK]", err.message);
            return { ...cached, cacheStatus: "stale-fallback", age: now - cached.updatedAt };
        }
        throw err;
    }
}

function getSegmentFromCache(sourceUrl) {
    const key = getSegmentCacheKey(sourceUrl);
    const cached = memoryCache.segmentData[key];
    if (!cached) return null;

    if (Date.now() - cached.fetchedAt > SEGMENT_CACHE_TTL) {
        memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - cached.size);
        delete memoryCache.segmentData[key];
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
        if (!memoryCache.segmentData[victim.key]) continue;
        memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - memoryCache.segmentData[victim.key].size);
        delete memoryCache.segmentData[victim.key];
    }
}

function storeSegmentInCache(sourceUrl, buffer, responseHeaders = {}, status = 200) {
    if (!Buffer.isBuffer(buffer) || buffer.length <= 0 || buffer.length > SEGMENT_CACHE_MAX_ITEM_BYTES) return false;

    const key = getSegmentCacheKey(sourceUrl);
    const existing = memoryCache.segmentData[key];
    if (existing) {
        memoryCache.segmentCacheBytes = Math.max(0, memoryCache.segmentCacheBytes - existing.size);
    }

    const headers = {};
    ["content-type", "cache-control", "expires", "last-modified", "etag"].forEach(h => {
        if (responseHeaders[h]) headers[h] = responseHeaders[h];
    });

    memoryCache.segmentData[key] = {
        url: sourceUrl,
        buffer,
        size: buffer.length,
        headers,
        status,
        fetchedAt: Date.now(),
        lastAccessedAt: Date.now()
    };
    memoryCache.segmentCacheBytes += buffer.length;
    evictSegmentCacheIfNeeded();
    return true;
}

async function fetchSegmentToCache(sourceUrl) {
    const cached = getSegmentFromCache(sourceUrl);
    if (cached) return cached;

    const key = getSegmentCacheKey(sourceUrl);
    if (memoryCache.segmentInflight[key]) return memoryCache.segmentInflight[key];

    const promise = (async () => {
        const started = Date.now();
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
        storeSegmentInCache(sourceUrl, buffer, response.headers, response.status);
        console.log(`[SEG PREFETCH OK] bytes=${formatBytes(buffer.length)} time=${Date.now() - started}ms`);
        return getSegmentFromCache(sourceUrl);
    })();

    memoryCache.segmentInflight[key] = promise;
    try {
        return await promise;
    } finally {
        delete memoryCache.segmentInflight[key];
    }
}

function queueSegmentPrefetch(sourceUrl, context = {}) {
    if (!isHttpUrl(sourceUrl)) return;
    if (context.session && !isSessionActive(context.session)) {
        console.log(`[PREFETCH SKIP] channel="${context.label || context.session.channelName || "stream"}" reason=inactive-session`);
        return;
    }

    const key = getSegmentCacheKey(sourceUrl);
    if (getSegmentFromCache(sourceUrl) || memoryCache.segmentInflight[key]) return;
    if (memoryCache.segmentPrefetchQueue.some(item => (typeof item === "string" ? item : item.sourceUrl) === sourceUrl)) return;

    memoryCache.segmentPrefetchQueue.push({
        sourceUrl,
        sessionKey: context.session?.key || null,
        sessionId: context.session?.id || null,
        label: context.label || context.session?.channelName || "stream"
    });
    processSegmentPrefetchQueue();
}

function processSegmentPrefetchQueue() {
    while (
        memoryCache.segmentPrefetchActive < SEGMENT_PREFETCH_CONCURRENCY &&
        memoryCache.segmentPrefetchQueue.length > 0
    ) {
        const item = memoryCache.segmentPrefetchQueue.shift();
        const sourceUrl = typeof item === "string" ? item : item.sourceUrl;
        const session = typeof item === "string" || !item.sessionKey ? null : { key: item.sessionKey, id: item.sessionId };
        const label = typeof item === "string" ? "stream" : item.label;

        if (session && !isSessionActive(session)) {
            console.log(`[PREFETCH SKIP] channel="${label}" reason=inactive-session`);
            continue;
        }

        const key = getSegmentCacheKey(sourceUrl);

        if (getSegmentFromCache(sourceUrl) || memoryCache.segmentInflight[key]) continue;

        memoryCache.segmentPrefetchActive += 1;
        fetchSegmentToCache(sourceUrl)
            .catch(err => console.error("[SEG PREFETCH ERROR]", err.message))
            .finally(() => {
                memoryCache.segmentPrefetchActive = Math.max(0, memoryCache.segmentPrefetchActive - 1);
                processSegmentPrefetchQueue();
            });
    }
}

async function waitForSegmentPrefetch(sourceUrl, maxWaitMs = SEGMENT_WAIT_FOR_PREFETCH_MS) {
    const inflight = memoryCache.segmentInflight[getSegmentCacheKey(sourceUrl)];
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

function getCachedSegmentForLogicalSegment(segment) {
    const urls = [
        segment?.cacheUrl,
        segment?.url,
        ...(Array.isArray(segment?.altUrls) ? segment.altUrls : [])
    ].filter(Boolean);

    for (const url of urls) {
        const cached = getSegmentFromCache(url);
        if (cached) return cached;
    }

    return null;
}

function mergeLiveSegment(existing, incoming) {
    return {
        ...(existing || {}),
        ...incoming,
        url: incoming.url || existing?.url,
        sourceUrl: incoming.url,
        cacheUrl: incoming.url || existing?.cacheUrl,
        lines: incoming.lines,
        duration: incoming.duration || existing?.duration || 0,
        firstSeenAt: existing?.firstSeenAt || incoming.firstSeenAt || Date.now(),
        lastSeenAt: Date.now(),
        altUrls: [incoming.url].filter(Boolean),
        logicalKey: incoming.logicalKey || existing?.logicalKey || getSegmentLogicalKey(incoming)
    };
}

function getLiveBuffer(sourceUrl) {
    return memoryCache.liveBuffers[getHLSCacheKey(sourceUrl)] || null;
}

function updateLiveBufferFromPlaylist(sourceUrl, playlist, baseUrl, context = {}) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    const parsed = parseHLSMediaPlaylist(playlist, baseUrl);

    if (parsed.info.isMaster || !parsed.info.isLive || parsed.segments.length === 0) {
        return { playlist, info: parsed.info, buffered: false, segments: [] };
    }

    let buffer = memoryCache.liveBuffers[cacheKey];
    if (!buffer) {
        buffer = { headerLines: [], segments: [], segmentMap: {}, lastUpdated: 0 };
        memoryCache.liveBuffers[cacheKey] = buffer;
    }

    buffer.headerLines = parsed.headerLines;
    buffer.lastUpdated = Date.now();

    parsed.segments.forEach(incoming => {
        const key = incoming.logicalKey || getSegmentLogicalKey(incoming);
        if (!key) return;

        const existing = buffer.segmentMap[key];
        if (!existing) {
            buffer.segmentMap[key] = {
                ...incoming,
                sourceUrl: incoming.url,
                cacheUrl: incoming.url,
                firstSeenAt: Date.now(),
                lastSeenAt: Date.now(),
                altUrls: [incoming.url]
            };
            buffer.segments.push(buffer.segmentMap[key]);
        } else {
            Object.assign(existing, mergeLiveSegment(existing, incoming));
        }
    });

    buffer.segments = buffer.segments
        .sort((a, b) => (a.sequence || 0) - (b.sequence || 0))
        .slice(-LIVE_BUFFER_SEGMENTS);

    buffer.segmentMap = {};
    buffer.segments.forEach(segment => {
        const key = segment.logicalKey || getSegmentLogicalKey(segment);
        if (!key) return;
        segment.logicalKey = key;
        buffer.segmentMap[key] = segment;
    });

    queueDynamicPrefetchForBuffer(buffer, context);

    const selected = selectBufferedSegments(buffer);
    const rebuilt = buildPlaylistFromBufferedSegments(buffer, selected.segments);
    return {
        playlist: rebuilt || playlist,
        info: rebuilt ? analyzeHLSPlaylist(rebuilt) : parsed.info,
        buffered: Boolean(rebuilt),
        bufferSize: buffer.segments.length,
        playlistSegments: selected.segments.length,
        cachedSeconds: getCachedDurationForSegments(selected.segments),
        segments: selected.segments
    };
}

function selectBufferedSegments(buffer) {
    if (!buffer?.segments?.length) return { segments: [] };
    const delayed = LIVE_DELAY_SEGMENTS > 0 && buffer.segments.length > 2
        ? buffer.segments.slice(0, Math.max(1, buffer.segments.length - LIVE_DELAY_SEGMENTS))
        : buffer.segments.slice();
    return { segments: delayed.slice(-LIVE_PLAYLIST_SEGMENTS) };
}

function buildPlaylistFromBufferedSegments(buffer, selectedSegments) {
    if (!buffer || !selectedSegments?.length) return null;

    const firstSeq = selectedSegments[0].sequence;
    let hasMediaSequence = false;
    const headerLines = (buffer.headerLines || []).map(line => {
        if (String(line).trim().startsWith("#EXT-X-MEDIA-SEQUENCE:")) {
            hasMediaSequence = true;
            return `#EXT-X-MEDIA-SEQUENCE:${firstSeq}`;
        }
        return line;
    });

    if (!hasMediaSequence) headerLines.push(`#EXT-X-MEDIA-SEQUENCE:${firstSeq}`);

    return [
        ...headerLines,
        ...selectedSegments.flatMap(segment => {
            const outputUrl = segment.cacheUrl || segment.url;
            return (segment.lines || []).map(line => {
                const trimmed = String(line).trim();
                if (!trimmed || trimmed.startsWith("#")) return line;
                return outputUrl;
            });
        })
    ].join("\n") + "\n";
}

function getCachedDurationForSegments(segments) {
    return (segments || []).reduce((sum, segment) => {
        return sum + (getCachedSegmentForLogicalSegment(segment) ? (Number(segment.duration) || 0) : 0);
    }, 0);
}

function queueDynamicPrefetchForBuffer(buffer, context = {}) {
    if (!buffer?.segments?.length) return;
    if (context.session && !isSessionActive(context.session)) {
        console.log(`[DYNAMIC BUFFER] status=cancelled channel="${context.label || context.session.channelName || "stream"}" reason=inactive-session`);
        return;
    }

    const cachedSeconds = getCachedDurationForSegments(buffer.segments);
    if (cachedSeconds >= DYNAMIC_BUFFER_MAX_SECONDS) return;

    const candidates = buffer.segments
        .filter(segment => !getCachedSegmentForLogicalSegment(segment))
        .slice(-SEGMENT_PREFETCH_AHEAD);

    candidates.forEach(segment => queueSegmentPrefetch(segment.sourceUrl || segment.url, context));

    if (candidates.length > 0) {
        console.log(
            `[DYNAMIC BUFFER] status=${getBufferStatus(cachedSeconds)}` +
            ` cached=${formatSeconds(cachedSeconds)}` +
            ` target=${formatSeconds(DYNAMIC_BUFFER_TARGET_SECONDS)}` +
            ` max=${formatSeconds(DYNAMIC_BUFFER_MAX_SECONDS)}` +
            ` queued=${candidates.length}` +
            ` active=${memoryCache.segmentPrefetchActive}` +
            ` queue=${memoryCache.segmentPrefetchQueue.length}`
        );
    }
}

async function acquireInitialHLS(sourceUrl, label = "stream", context = {}) {
    const started = Date.now();
    let lastErr = null;
    let fetches = 0;

    console.log(
        `[STARTUP PHASE 1] channel="${label}" acquiring-stream` +
        ` maxWait=${formatSeconds(STREAM_ACQUIRE_TIMEOUT / 1000)}` +
        ` poll=${STREAM_ACQUIRE_POLL_MS}ms`
    );

    while (Date.now() - started < STREAM_ACQUIRE_TIMEOUT) {
        assertSessionActive(context.session, label);
        fetches += 1;

        try {
            const hls = await fetchAndStoreHLSRaw(getHLSCacheKey(sourceUrl), sourceUrl, "acquire");
            assertSessionActive(context.session, label);
            const info = hls.info || analyzeHLSPlaylist(hls.playlist);

            if (info.isMaster || info.segmentCount > 0) {
                console.log(
                    `[STARTUP PHASE 1 OK] channel="${label}" acquired` +
                    ` fetches=${fetches}` +
                    ` elapsed=${Date.now() - started}ms` +
                    ` type=${info.isMaster ? "master" : "media"}` +
                    ` segs=${info.segmentCount}` +
                    `${info.targetDuration ? ` targetDuration=${formatSeconds(info.targetDuration)}` : ""}`
                );
                return hls;
            }

            lastErr = new Error("Playlist valid but without playable segments");
        } catch (err) {
            lastErr = err;
            console.error(`[STARTUP PHASE 1 RETRY] channel="${label}" fetch=${fetches} err=${err.message}`);
        }

        await sleep(Math.min(STREAM_ACQUIRE_POLL_MS, Math.max(0, STREAM_ACQUIRE_TIMEOUT - (Date.now() - started))));
        assertSessionActive(context.session, label);
    }

    throw new Error(lastErr ? `Acquire failed: ${lastErr.message}` : "Acquire failed: timeout");
}

async function prebufferInitialStream(sourceUrl, label = "stream", context = {}) {
    const started = Date.now();

    console.log(
        `[STARTUP PHASE 2] channel="${label}" prebuffering` +
        ` target=${formatSeconds(STREAM_PREBUFFER_SECONDS)}` +
        ` timeout=${formatSeconds(STREAM_PREBUFFER_TIMEOUT / 1000)}` +
        ` dynamicTarget=${formatSeconds(DYNAMIC_BUFFER_TARGET_SECONDS)}`
    );

    let lastErr = null;
    let attempt = 0;

    while (Date.now() - started < STREAM_PREBUFFER_TIMEOUT) {
        assertSessionActive(context.session, label);
        attempt += 1;

        try {
            const hls = await fetchAndStoreHLSRaw(getHLSCacheKey(sourceUrl), sourceUrl, "prebuffer");
            assertSessionActive(context.session, label);
            const buffered = updateLiveBufferFromPlaylist(sourceUrl, hls.playlist, hls.baseUrl, context);
            const candidates = (buffered.segments || []).slice(-SEGMENT_PREFETCH_AHEAD);

            candidates.forEach(segment => queueSegmentPrefetch(segment.sourceUrl || segment.url, context));

            const cachedSeconds = getCachedDurationForSegments(candidates);
            console.log(
                `[STARTUP PHASE 2 PROGRESS] channel="${label}"` +
                ` cached=${formatSeconds(cachedSeconds)}/${formatSeconds(STREAM_PREBUFFER_SECONDS)}` +
                ` status=${getBufferStatus(cachedSeconds)}` +
                ` candidateSegs=${candidates.length}` +
                ` prefetchActive=${memoryCache.segmentPrefetchActive}` +
                ` prefetchQueue=${memoryCache.segmentPrefetchQueue.length}`
            );

            if (cachedSeconds >= STREAM_PREBUFFER_SECONDS) {
                console.log(
                    `[STARTUP COMPLETE] channel="${label}" stream-ready` +
                    ` startBuffer=${formatSeconds(cachedSeconds)}` +
                    ` target=${formatSeconds(STREAM_PREBUFFER_SECONDS)}` +
                    ` elapsed=${Date.now() - started}ms`
                );
                return { status: "ready", cachedSeconds };
            }

            lastErr = null;
        } catch (err) {
            if (err.code === "KRONOS_SESSION_CANCELLED") throw err;
            lastErr = err;
            console.warn(
                `[STARTUP PHASE 2 RETRY] channel="${label}" attempt=${attempt}` +
                ` err=${err.message} elapsed=${Date.now() - started}ms - continuing until timeout`
            );
        }

        await sleep(700);
        assertSessionActive(context.session, label);
    }

    throw new Error(
        `Prebuffer failed after ${Date.now() - started}ms` +
        `${lastErr ? ` lastError=${lastErr.message}` : ` (less than ${STREAM_PREBUFFER_SECONDS}s buffered)`}`
    );
}

async function ensureStreamReady(sourceUrl, label = "stream", context = {}) {
    const cacheKey = getHLSCacheKey(sourceUrl);
    let state = memoryCache.streamStates[cacheKey];

    if (!state) {
        state = { status: "idle", promise: null, readyAt: 0, lastError: null, generation: 0 };
        memoryCache.streamStates[cacheKey] = state;
    }

    const buffer = getLiveBuffer(sourceUrl);
    const cachedSeconds = getCachedDurationForSegments(buffer?.segments || []);

    // Fast path: once the playlist is acquired we serve immediately. Both "playable"
    // (Phase 1 done, Phase 2 still running in background) and "ready" (Phase 2 done)
    // qualify — the player starts on real content either way.
    if (state.status === "playable" || state.status === "ready") {
        assertSessionActive(context.session, label);
        if (buffer) queueDynamicPrefetchForBuffer(buffer, context);
        console.log(
            `[STARTUP REUSE] channel="${label}" status=${state.status}` +
            ` cached=${formatSeconds(cachedSeconds)}`
        );
        return { status: state.status, reused: true };
    }

    if (state.promise && state.status !== "cancelled" && state.status !== "failed") {
        console.log(`[STARTUP WAIT] channel="${label}" existingPhase=${state.status}`);
        return state.promise;
    }

    state.generation = (state.generation || 0) + 1;
    const myGen = state.generation;
    state.status = "acquiring";
    state.lastError = null;
    state.promise = (async () => {
        try {
            const initial = await acquireInitialHLS(sourceUrl, label, context);
            assertSessionActive(context.session, label);
            if (initial.info?.isMaster) {
                state.status = "ready";
                state.readyAt = Date.now();
                console.log(`[STARTUP COMPLETE] channel="${label}" master-playlist-ready`);
                return { status: "ready", master: true };
            }

            // Phase 1 done — seed the segment prefetch queue right away so the very
            // first segments the player asks for are either cached or already in flight.
            try {
                const hls = await getCachedHLSRaw(sourceUrl);
                const buffered = updateLiveBufferFromPlaylist(sourceUrl, hls.playlist, hls.baseUrl, context);
                const candidates = (buffered.segments || []).slice(-SEGMENT_PREFETCH_AHEAD);
                candidates.forEach(segment => queueSegmentPrefetch(segment.sourceUrl || segment.url, context));
            } catch (err) {
                if (err.code === "KRONOS_SESSION_CANCELLED") throw err;
                // Non-fatal here — the background prebuffer below will keep retrying.
            }

            state.status = "playable";
            state.readyAt = Date.now();
            state.lastError = null;
            console.log(
                `[STARTUP PLAYABLE] channel="${label}" playlist-ready` +
                ` (continuing prebuffer to ${formatSeconds(STREAM_PREBUFFER_SECONDS)} in background)`
            );

            // Continue prebuffer in background — enriches the cache so later segments
            // are already warm when the player gets to them, without blocking startup.
            setImmediate(() => {
                prebufferInitialStream(sourceUrl, label, context)
                    .then(() => {
                        if (state.generation === myGen) {
                            state.status = "ready";
                            console.log(`[STARTUP BACKGROUND PREBUFFER DONE] channel="${label}"`);
                        }
                    })
                    .catch(err => {
                        if (err.code === "KRONOS_SESSION_CANCELLED") return;
                        console.warn(`[STARTUP BACKGROUND PREBUFFER WARN] channel="${label}" ${err.message}`);
                    });
            });

            return { status: "playable", early: true };
        } catch (err) {
            // Only update state if no newer generation has superseded us. Otherwise
            // we'd corrupt the state of a fresh startup that took over after a
            // player-close or channel-switch cancellation.
            if (state.generation === myGen) {
                if (err.code === "KRONOS_SESSION_CANCELLED") {
                    state.status = "cancelled";
                } else {
                    state.status = "failed";
                }
                state.lastError = err.message;
            }
            throw err;
        } finally {
            if (state.generation === myGen) {
                state.promise = null;
            }
        }
    })();

    return state.promise;
}

function parseM3UChannels(data, source = {}) {
    const lines = String(data || "").split("\n");
    const channels = [];
    let currentChannel = null;

    console.log(`[PARSE M3U] Parsing playlist from ${source.name}, total lines: ${lines.length}`);

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.startsWith("#EXTINF:")) {
            const name = (line.match(/,(.+)$/) || [, "Canale Sconosciuto"])[1].trim();
            const group = (line.match(/group-title="([^"]+)"/) || [, "Altri Canali"])[1].trim();
            const logoMatch = line.match(/tvg-logo="([^"]+)"/);
            const tvgId = (line.match(/tvg-id="([^"]+)"/) || [, null])[1];
            const logo = logoMatch ? logoMatch[1] : "";

            currentChannel = {
                name,
                group,
                logo,
                tvgId,
                sourceName: source.name || "Kronos",
                sourceUrl: source.url || ""
            };
        } else if (line.startsWith("http") && currentChannel) {
            currentChannel.url = line;
            currentChannel.id = "channel_" + crypto.createHash("sha1").update(`${source.url || ""}|${line}`).digest("hex").substring(0, 20);
            channels.push(currentChannel);
            currentChannel = null;
        }
    }

    console.log(`[PARSE M3U] Parsed ${channels.length} channels from ${source.name}`);
    if (channels.length > 0) {
        const groups = [...new Set(channels.map(c => c.group))];
        console.log(`[PARSE M3U] Groups found in ${source.name}:`, groups);
    }

    return channels;
}

function decorateChannelName(channel, totalLists, mode) {
    const displayName = stripInitialCountryPrefix(channel.name);
    if (totalLists <= 1 || mode !== "filter") return displayName;
    const baseName = displayName.replace(/\s*\([^)]*\)\s*$/g, "").trim();
    return `${baseName} (${getListAbbreviation(channel.sourceName)})`;
}

function stripInitialCountryPrefix(name) {
    return String(name || "").replace(/^\s*IT:\s*/i, "").trim();
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

function matchesChannelSearch(channel, rawQuery) {
    const query = normalizeSearchText(rawQuery);
    if (!query) return true;

    const haystack = normalizeSearchText([
        channel.name,
        channel.group,
        channel.sourceName,
        channel.tvgId,
        channel.description
    ].filter(Boolean).join(" "));

    const compactHaystack = haystack.replace(/\s+/g, "");
    const compactQuery = query.replace(/\s+/g, "");

    if (haystack.includes(query)) return true;
    if (compactHaystack.includes(compactQuery)) return true;

    return query.split(" ").filter(Boolean).every(token => haystack.includes(token));
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
    return channels.reduce((index, channel) => {
        index[channel.id] = channel;
        return index;
    }, {});
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

function isHlsUrl(url) {
    return /\.m3u8(?:[?#].*)?$/i.test(String(url || ""));
}

function buildStream(channel, host, configKey, config) {
    if (isHlsUrl(channel.url)) {
        return {
            title: channel.name,
            name: "Kronos",
            url: `${host}/${configKey}/hls/${channel.id}/index.m3u8`,
            behaviorHints: {
                notWebReady: true,
                bingeGroup: `kronos-${channel.id}`
            }
        };
    }

    if (isPlayableHttpUrl(channel.url)) {
        return {
            title: channel.name,
            name: "Kronos",
            url: `${host}/${configKey}/proxy/seg?u=${encodeProxyUrl(channel.url)}`,
            behaviorHints: {
                notWebReady: true,
                bingeGroup: `kronos-${channel.id}`
            }
        };
    }

    return {
        title: `${channel.name} - sorgente web`,
        name: "Kronos",
        externalUrl: channel.url
    };
}

function toMeta(channel, host, configKey = "", config = {}) {
    const fallbackLogo = `${host}/logo.svg`;
    const poster = configKey ? `${host}/${configKey}/poster/${channel.id}.svg?v=${encodeURIComponent(RELEASE_VERSION)}` : (channel.logo || fallbackLogo);
    const logo = channel.logo || fallbackLogo;
    const stream = configKey ? buildStream(channel, host, configKey, config) : null;

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
    if (memoryCache.channelInflight[configKey] && !options.force) {
        console.log(`[CATALOG CACHE] Waiting existing playlist load config=${configKey.substring(0, 12)}...`);
        return memoryCache.channelInflight[configKey];
    }

    const promise = (async () => {
        memoryCache.isUpdating[configKey] = true;
        console.log(`[CATALOG LOAD] Start config=${configKey.substring(0, 12)}... force=${Boolean(options.force)}`);
        const epgMap = config.e ? await updateEPGCache(config.e) : {};
        const configuredLists = getConfiguredLists(config);
        console.log(`[CATALOG LOAD] Lists=${configuredLists.length} mode=${config.gm || "default"}`);
        
        const selectedGroups = Array.isArray(config.g) ? config.g : [];
        console.log(`[CATALOG LOAD] SelectedGroups=${selectedGroups.length ? selectedGroups.join(", ") : "all"}`);
        
        const selectedGroupSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";
        
        const parsedChannelGroups = await Promise.all(configuredLists.map(async list => {
            console.log(`[CATALOG LOAD] Fetch playlist name="${list.name}"`);
            const playlistData = await fetchPlaylist(list.url);
            const parsed = parseM3UChannels(playlistData, list);
            console.log(`[CATALOG LOAD] Parsed list="${list.name}" channels=${parsed.length}`);
            return parsed;
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

        console.log(`[CATALOG LOAD OK] FinalChannels=${channels.length}`);
        if (channels.length > 0) {
            const uniqueGroups = [...new Set(channels.map(c => c.group))];
            console.log(`[CATALOG LOAD OK] Groups=${uniqueGroups.length}`);
        }

        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.lastUpdate[configKey] = Date.now();
        return channels;
    })();

    memoryCache.channelInflight[configKey] = promise;

    try {
        return await promise;
    } catch (err) {
        console.error("[CATALOG LOAD ERROR]", err.message);
        throw err;
    } finally {
        delete memoryCache.channelInflight[configKey];
        memoryCache.isUpdating[configKey] = false;
    }
}

async function getChannelsFromCache(configKey, config) {
    const cachedData = memoryCache.channelItems[configKey];
    if (!cachedData) {
        console.log(`[CATALOG CACHE MISS] config=${configKey.substring(0, 12)}...`);
        return await fetchAndProcessChannels(configKey, config);
    }
    if (!memoryCache.channelIndex[configKey]) {
        console.log('[CATALOG CACHE] Rebuilding channel index');
        memoryCache.channelIndex[configKey] = buildChannelIndex(cachedData);
    }
    if (Date.now() - (memoryCache.lastUpdate[configKey] || 0) > CACHE_TTL) {
        console.log('[CATALOG CACHE STALE] Serving cached catalog and refreshing in background');
        fetchAndProcessChannels(configKey, config).catch(err => {
            console.error("[CATALOG BACKGROUND REFRESH ERROR]", err.message);
        });
    } else {
        console.log(`[CATALOG CACHE HIT] channels=${cachedData.length}`);
    }
    return cachedData;
}

function getPublicHost(req) {
    const forwardedProto = req.get('x-forwarded-proto') || req.protocol;
    const forwardedHost = req.get('x-forwarded-host') || req.get('host');
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

            const extra = [{ name: "search", isRequired: false }];
            if (catalogGroups.length > 0) {
                extra.push({
                    name: "genre",
                    options: catalogGroups,
                    isRequired: false
                });
            }

            return {
                id: toCatalogId(list.name),
                type: ADDON_TYPE,
                name: list.name,
                extra
            };
        });

        const manifest = {
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
        };
        
        console.log(`[MANIFEST] catalogs=${catalogs.length} channels=${channels.length} search=enabled allCatalog=removed`);
        res.json(manifest);
    } catch (err) {
        console.error('[ERROR] Manifest generation failed:', err);
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
            const current = groupMap.get(channel.group) || { name: channel.group, count: 0, sources: new Set() };
            current.count += 1;
            current.sources.add(channel.sourceName);
            groupMap.set(channel.group, current);
        });

        const groups = [...groupMap.values()]
            .map(group => ({ name: group.name, count: group.count, sources: [...group.sources] }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));

        res.json({ totalChannels: channels.length, totalLists: lists.length, groups });
    } catch (err) {
        res.status(400).json({ error: "Impossibile analizzare le liste M3U" });
    }
});

async function catalogResponse(req, res) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Content-Type", "application/json; charset=utf-8");

    const requestStarted = Date.now();
    const rawExtra = req.params.extra || "";
    const rawQuery = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";

    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);

        const extraParams = {
            ...getExtraParams(req.params.extra),
            ...Object.fromEntries(
                Object.entries(req.query || {}).map(([k, v]) => [k, Array.isArray(v) ? v[0] : v])
            )
        };
        const targetGroup = extraParams.genre || null;
        const targetSource = getCatalogSourceName(req.params.id);
        const searchQuery = extraParams.search ? String(extraParams.search).trim() : null;
        const host = getPublicHost(req);

        console.log(
            `[CATALOG REQ] id=${req.params.id}` +
            ` rawExtra="${rawExtra}"` +
            `${rawQuery ? ` rawQuery="${rawQuery}"` : ""}` +
            `${searchQuery ? ` search="${searchQuery}"` : ""}` +
            `${targetGroup ? ` genre="${targetGroup}"` : ""}` +
            ` cached=${Boolean(memoryCache.channelItems[configKey])}`
        );

        const channels = await getChannelsFromCache(configKey, config);

        const filteredChannels = sortChannelsByName(channels.filter(channel => {
            const matchesSource = targetSource ? channel.sourceName === targetSource : true;
            const matchesGroup = targetGroup ? normalizeGroupName(channel.group) === normalizeGroupName(targetGroup) : true;
            const matchesSearch = matchesChannelSearch(channel, searchQuery);
            return matchesSource && matchesGroup && matchesSearch;
        }));

        console.log(
            `[CATALOG ROUTE] id=${req.params.id}` +
            `${targetGroup ? ` genre="${targetGroup}"` : ""}` +
            `${searchQuery ? ` search="${searchQuery}"` : ""}` +
            ` results=${filteredChannels.length}/${channels.length}` +
            ` time=${Date.now() - requestStarted}ms`
        );

        const metas = filteredChannels.map(c => toMeta(c, host, configKey, config));
        res.json({ metas });
    } catch (err) {
        console.error('[ERROR CATALOG]', err);
        res.status(500).json({ metas: [] });
    }
}

app.get("/:base64Config/catalog/:type/:id.json", catalogResponse);
app.get("/:base64Config/catalog/:type/:id/:extra.json", catalogResponse);
app.get("/:base64Config/catalog/:type/:id/:extra", catalogResponse);

app.get("/:base64Config/meta/:type/:id.json", async (req, res) => {
    const configKey = req.params.base64Config;
    const config = decodeConfig(configKey);
    const c = await getChannelById(configKey, config, req.params.id);
    const host = getPublicHost(req);
    if (!c) return res.status(404).json({ meta: null });
    res.json({ meta: toMeta(c, host, configKey, config) });
});

app.get("/:base64Config/poster/:id.svg", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        const host = getPublicHost(req);
        const logoUrl = c?.logo || "";
        const logoDataUri = await getLogoDataUri(logoUrl);
        const name = stripInitialCountryPrefix(c?.name || "Kronos");
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
        res.setHeader("Cache-Control", "public, max-age=3600");
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

function sendCachedSegment(req, res, cached, source = "cache", started = Date.now()) {
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

    const durationMs = Date.now() - started;
    notePlayerSegment(req, cached.url, { ok: true, source, status, durationMs, bytes: chunk.length });
    console.log(
        `[PLAYER SEGMENT] segment=${getSegmentLabel(cached.url)}` +
        ` source=${source}` +
        ` status=${status}` +
        ` bytes=${formatBytes(chunk.length)}` +
        ` time=${durationMs}ms` +
        `${req.headers.range ? ` range=${req.headers.range}` : ""}`
    );

    return res.end(chunk);
}

app.get("/:base64Config/hls/:id/index.m3u8", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const c = await getChannelById(configKey, config, req.params.id);
        if (!c) return res.status(404).send("#EXTM3U\n");

        const host = getPublicHost(req);
        const session = activatePlaybackSession(req, c);

        let startupDone = false;
        req.on("close", () => {
            if (startupDone) return;
            if (memoryCache.activeSessions[session.key]?.id !== session.id) return;

            delete memoryCache.activeSessions[session.key];
            cancelSessionWork(session, "player-closed");

            // Force-reset the stream state so a subsequent re-open of the same channel
            // starts a fresh startup instead of attaching to the about-to-fail promise.
            // Bumping the generation also prevents the old promise from corrupting any
            // newer state when it finally throws.
            const cacheKey = getHLSCacheKey(c.url);
            const state = memoryCache.streamStates[cacheKey];
            if (state && state.status !== "ready") {
                state.generation = (state.generation || 0) + 1;
                state.status = "cancelled";
                state.promise = null;
                state.lastError = "player-closed-during-startup";
            }

            console.log(`[SESSION CLOSED] channel="${c.name}" reason=player-disconnected`);
        });

        await ensureStreamReady(c.url, c.name, { session, label: c.name });
        startupDone = true;

        const hls = await getCachedHLSRaw(c.url);
        assertSessionActive(session, c.name);
        const buffered = updateLiveBufferFromPlaylist(c.url, hls.playlist, hls.baseUrl, { session, label: c.name });
        const outputPlaylist = buffered.playlist || hls.playlist;
        const rewritten = rewriteHLSPlaylistThroughKronos(outputPlaylist, hls.baseUrl, host, configKey);

        console.log(
            `[HLS ROUTE] channel="${c.name}" cache=${hls.cacheStatus}` +
            `${typeof hls.age === "number" ? ` age=${hls.age}ms` : ""}` +
            ` segs=${buffered.info?.segmentCount ?? hls.info?.segmentCount ?? 0}` +
            `${buffered.cachedSeconds !== undefined ? ` cached≈${Math.round(buffered.cachedSeconds)}s` : ""}`
        );

        if (buffered.cachedSeconds !== undefined && buffered.cachedSeconds < STREAM_PREBUFFER_SECONDS) {
            console.warn(
                `[PLAYER BUFFER RISK] channel="${c.name}" reason=low-visible-playlist-cache` +
                ` visibleCached=${formatSeconds(buffered.cachedSeconds)}` +
                ` startupTarget=${formatSeconds(STREAM_PREBUFFER_SECONDS)}` +
                ` playlistSegs=${buffered.playlistSegments || 0}`
            );
        }

        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
        res.setHeader("Pragma", "no-cache");
        res.send(rewritten);
    } catch (err) {
        if (err.code === "KRONOS_SESSION_CANCELLED") {
            console.log(`[HLS ROUTE CANCELLED] ${err.message}`);
            return res.status(204).end();
        }
        console.error("[ERROR HLS ROUTE]", err.message);
        res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/pl", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");

        const sourceUrl = decodeProxyUrl(req.query.u);
        const host = getPublicHost(req);
        await ensureStreamReady(sourceUrl, "nested-playlist");

        const hls = await getCachedHLSRaw(sourceUrl);
        const buffered = updateLiveBufferFromPlaylist(sourceUrl, hls.playlist, hls.baseUrl);
        const outputPlaylist = buffered.playlist || hls.playlist;
        const rewritten = rewriteHLSPlaylistThroughKronos(outputPlaylist, hls.baseUrl, host, configKey);

        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
        res.setHeader("Pragma", "no-cache");
        res.send(rewritten);
    } catch (err) {
        console.error("[ERROR PL ROUTE]", err.message);
        res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/seg", async (req, res) => {
    let upstream = null;
    let bytes = 0;
    const started = Date.now();

    try {
        decodeConfig(req.params.base64Config);
        if (!req.query.u) return res.status(400).end();

        const sourceUrl = decodeProxyUrl(req.query.u);
        const cached = getSegmentFromCache(sourceUrl);
        if (cached) return sendCachedSegment(req, res, cached, "cache-hit", started);

        const prefetchWaitStarted = Date.now();
        const waitedCached = await waitForSegmentPrefetch(sourceUrl, SEGMENT_WAIT_FOR_PREFETCH_MS);
        if (waitedCached) {
            console.log(
                `[PLAYER WAITED PREFETCH] segment=${getSegmentLabel(sourceUrl)}` +
                ` waited=${Date.now() - prefetchWaitStarted}ms`
            );
            return sendCachedSegment(req, res, waitedCached, "prefetch-wait", started);
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
        const cacheChunks = [];
        const shouldStore = !req.headers.range && response.status === 200;

        ["content-type", "content-length", "content-range", "accept-ranges", "last-modified", "etag", "cache-control", "expires"].forEach(h => {
            if (response.headers[h]) res.setHeader(h, response.headers[h]);
        });

        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Cache", "MISS");
        res.status(response.status);

        upstream.on("data", chunk => {
            bytes += chunk.length;
            if (shouldStore && bytes <= SEGMENT_CACHE_MAX_ITEM_BYTES) {
                cacheChunks.push(chunk);
            }
        });

        res.on("finish", () => {
            if (shouldStore && cacheChunks.length > 0 && bytes <= SEGMENT_CACHE_MAX_ITEM_BYTES) {
                storeSegmentInCache(sourceUrl, Buffer.concat(cacheChunks), response.headers, response.status);
            }
            const durationMs = Date.now() - started;
            notePlayerSegment(req, sourceUrl, {
                ok: true,
                source: "upstream",
                status: response.status,
                durationMs,
                bytes
            });
            console.log(
                `[PLAYER SEGMENT] segment=${getSegmentLabel(sourceUrl)}` +
                ` source=upstream` +
                ` status=${response.status}` +
                ` bytes=${formatBytes(bytes)}` +
                ` time=${durationMs}ms` +
                `${req.headers.range ? ` range=${req.headers.range}` : ""}`
            );
        });

        upstream.on("error", err => {
            console.error("[SEG STREAM ERROR]", err.message);
            notePlayerSegment(req, sourceUrl, {
                error: true,
                source: "upstream-stream",
                status: res.statusCode || 502,
                durationMs: Date.now() - started
            });
            if (!res.headersSent) res.status(502);
            res.end();
        });

        req.on("close", () => {
            if (upstream && !upstream.destroyed) upstream.destroy();
        });

        upstream.pipe(res);
    } catch (err) {
        const sourceUrl = req.query.u ? decodeProxyUrl(req.query.u) : "";
        notePlayerSegment(req, sourceUrl, {
            error: true,
            source: "upstream-request",
            status: err.response?.status || 502,
            durationMs: Date.now() - started
        });
        if (!res.headersSent) res.status(err.response?.status || 502);
        res.end();
        if (upstream && !upstream.destroyed) upstream.destroy();
        console.error("[ERROR SEG ROUTE]", err.message);
    }
});

app.get("/:base64Config/stream/:type/:id.json", async (req, res) => {
    const configKey = req.params.base64Config;
    const config = decodeConfig(configKey);
    const c = await getChannelById(configKey, config, req.params.id);
    if (!c) return res.json({ streams: [] });

    const host = getPublicHost(req);
    const stream = buildStream(c, host, configKey, config);

    res.json({ streams: [stream] });
});

app.get("/configure", (req, res) => {
    res.redirect("/");
});

app.get("/:base64Config/configure", (req, res) => {
    res.redirect(`/?config=${encodeURIComponent(req.params.base64Config)}`);
});

app.get("/:base64Config/debug", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        
        const debugInfo = {
            config: config,
            totalChannels: channels.length,
            sampleChannels: channels.slice(0, 3),
            uniqueGroups: [...new Set(channels.map(c => c.group))],
            uniqueSourceNames: [...new Set(channels.map(c => c.sourceName))],
            configuredLists: getConfiguredLists(config),
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
                activeSessions: Object.values(memoryCache.activeSessions).map(session => ({
                    channelName: session.channelName,
                    channelId: session.channelId,
                    startedAt: session.startedAt,
                    lastSeenAt: session.lastSeenAt
                })),
                playbackStates: Object.entries(memoryCache.playbackStates).map(([key, state]) => ({
                    key,
                    ok: state.ok,
                    slow: state.slow,
                    errors: state.errors,
                    possibleBuffering: state.possibleBuffering,
                    lastSegmentAt: state.lastSegmentAt
                })),
                streamStates: Object.entries(memoryCache.streamStates).map(([key, state]) => ({
                    key,
                    status: state.status,
                    readyAt: state.readyAt,
                    lastError: state.lastError
                }))
            },
            hlsSettings: {
                HLS_REFRESH_TTL,
                HLS_MASTER_REFRESH_TTL,
                HLS_STALE_TTL,
                HLS_REQUEST_TIMEOUT,
                STREAM_ACQUIRE_TIMEOUT,
                STREAM_PREBUFFER_SECONDS,
                STREAM_PREBUFFER_TIMEOUT,
                DYNAMIC_BUFFER_TARGET_SECONDS,
                DYNAMIC_BUFFER_MAX_SECONDS,
                LIVE_BUFFER_SEGMENTS,
                LIVE_PLAYLIST_SEGMENTS,
                LIVE_DELAY_SEGMENTS,
                SEG_REQUEST_TIMEOUT,
                SEGMENT_CACHE_MAX_SEGMENTS,
                SEGMENT_PREFETCH_CONCURRENCY,
                SEGMENT_PREFETCH_AHEAD
            }
        };
        
        res.json(debugInfo);
    } catch (err) {
        res.status(500).json({ error: err.message, stack: err.stack });
    }
});

app.listen(PORT, "0.0.0.0", () => {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`K.R.O.N.O.S. ${RELEASE_VERSION} - Server Started`);
    console.log(`${'='.repeat(60)}`);
    console.log(`🌐 Server URL: http://0.0.0.0:${PORT}`);
    console.log(`📦 Node version: ${process.version}`);
    console.log(`🔧 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`💾 Memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
    console.log(`${'='.repeat(60)}\n`);
});
