const express = require("express");
const axios = require("axios");
const sax = require("sax");
const zlib = require("zlib");
const { Readable } = require("stream");
const path = require("path");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 7000;

const LOG_TIME_ZONE = process.env.LOG_TIME_ZONE || process.env.EPG_TIME_ZONE || "Europe/Rome";
const logTimeFormatter = new Intl.DateTimeFormat("it-IT", {
    timeZone: LOG_TIME_ZONE,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
});

function installTimestampedConsole() {
    const prefixArgs = args => [`[${logTimeFormatter.format(new Date())}]`, ...args];
    for (const method of ["log", "warn", "error"]) {
        const original = console[method].bind(console);
        console[method] = (...args) => original(...prefixArgs(args));
    }
}

installTimestampedConsole();

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

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────
const UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const RELEASE_VERSION = "3.2.6";
const ADDON_TYPE = "tv";
const CATALOG_TTL = 30 * 60 * 1000;
const EPG_CACHE_TTL = Number(process.env.EPG_CACHE_TTL || 6 * 60 * 60 * 1000);
const EPG_REQUEST_TIMEOUT = Number(process.env.EPG_REQUEST_TIMEOUT || 20000);
const EPG_MAX_BYTES = Number(process.env.EPG_MAX_BYTES || 160 * 1024 * 1024);
const EPG_RETRY_DELAY_MS = Number(process.env.EPG_RETRY_DELAY_MS || 15000);
const EPG_FIRST_CATALOG_WAIT_MS = Number(process.env.EPG_FIRST_CATALOG_WAIT_MS || 0);
const EPG_REFRESH_INTERVAL_MS = Number(process.env.EPG_REFRESH_INTERVAL_MS || EPG_CACHE_TTL);
const DEFAULT_EPG_PRELOAD_URL = "http://172.30.0.10:8080/guide.gzip";
const EPG_PRELOAD_URL = String(process.env.EPG_PRELOAD_URL || DEFAULT_EPG_PRELOAD_URL).trim();
const EPG_STARTUP_WATCH_MS = Number(process.env.EPG_STARTUP_WATCH_MS || 30000);
const CATALOG_PRELOAD_CONFIGS = parseCatalogPreloadConfigs(process.env.CATALOG_PRELOAD_CONFIGS || process.env.KRONOS_PRELOAD_CONFIGS || "");
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 20000);
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);
const PLAYLIST_REQUEST_TIMEOUT = Number(process.env.PLAYLIST_REQUEST_TIMEOUT || 20000);
const PLAYLIST_RETRY_WINDOW_MS = Number(process.env.PLAYLIST_RETRY_WINDOW_MS || 30000);
const PLAYLIST_RETRY_DELAY_MS = Number(process.env.PLAYLIST_RETRY_DELAY_MS || 2000);
const EPG_TIME_ZONE = process.env.EPG_TIME_ZONE || "Europe/Rome";

// Manifest retry — fixes Xtream "invalid manifest on first hit" (stream spins up
// server-side on first request, returns garbage, is valid on retry).
const MANIFEST_RETRY_WINDOW_MS = Number(process.env.MANIFEST_RETRY_WINDOW_MS || 15000);
const MANIFEST_RETRY_DELAY_MS = Number(process.env.MANIFEST_RETRY_DELAY_MS || 2000);
const MANIFEST_FORBIDDEN_BACKOFF_MS = Number(process.env.MANIFEST_FORBIDDEN_BACKOFF_MS || 10000);
const POLL_ERROR_BACKOFF_MS = Number(process.env.POLL_ERROR_BACKOFF_MS || 5000);

// Light read-ahead segment cache — absorbs upstream jitter, does NOT create
// bandwidth. Kept small and simple on purpose (no Range caching = no corruption).
// Provider exposes max_connections=1, so concurrency 1 by default: in steady state
// the prefetcher is the ONLY upstream connection and the player reads from cache.
// (Raising this only helps if your provider tolerates parallel segment fetches.)
const SEGMENT_PREFETCH_CONCURRENCY = Number(process.env.SEGMENT_PREFETCH_CONCURRENCY || 1);
const SEGMENT_CACHE_TTL = Number(process.env.SEGMENT_CACHE_TTL || 10 * 60 * 1000);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_BYTES || 256 * 1024 * 1024);
const SEGMENT_CACHE_MAX_ITEMS = Number(process.env.SEGMENT_CACHE_MAX_ITEMS || 48);
const SEGMENT_CACHE_MAX_ITEM_BYTES = Number(process.env.SEGMENT_CACHE_MAX_ITEM_BYTES || 24 * 1024 * 1024);
const SEGMENT_PLAYER_RETRIES = Number(process.env.SEGMENT_PLAYER_RETRIES || 2);
const SEGMENT_PLAYER_RETRY_DELAY_MS = Number(process.env.SEGMENT_PLAYER_RETRY_DELAY_MS || 350);
const PREFETCH_RETRY_DELAY_MS = Number(process.env.PREFETCH_RETRY_DELAY_MS || 1000);
const SLOW_SEGMENT_MS = Number(process.env.SLOW_SEGMENT_MS || 4000);
const SEGMENT_AUTH_FAILURE_LIMIT = Math.max(2, Number(process.env.SEGMENT_AUTH_FAILURE_LIMIT || 6));
const SEGMENT_AUTH_FAILURE_MIN_MS = Number(process.env.SEGMENT_AUTH_FAILURE_MIN_MS || 45000);
const SEGMENT_AUTH_RECOVERY_COOLDOWN_MS = Number(process.env.SEGMENT_AUTH_RECOVERY_COOLDOWN_MS || 60000);

// ── Background poller + retained buffer ─────────────────────────────────────────
// These channels are ON-DEMAND: the upstream starts a fresh encoder (seq=0, 1 seg)
// when first requested and tears it down when nobody fetches → on resume it restarts
// from scratch. To make playback seamless we run a background poller per active
// channel that (a) keeps the upstream stream ALIVE, (b) accumulates a retained
// buffer, and (c) prefetches segments. The player sees a smaller live window so
// it cannot wander back into old segments after a temporary stall.
const RETAIN_SEGMENTS = Number(process.env.RETAIN_SEGMENTS || 18);
// Warm segments only help the player if they are advertised in the playlist.
// Keep the option for providers that require an artificial live delay, but do
// not hide ready media by default.
const LIVE_DELAY_SEGMENTS = Math.max(0, Number(process.env.LIVE_DELAY_SEGMENTS ?? 0));
// Anti-jump limit: advance the served live edge by at most this many segments per
// request while retained history still exists. If upstream history disappears,
// catching up is unavoidable.
const MAX_EDGE_ADVANCE = Number(process.env.MAX_EDGE_ADVANCE || 2);
const MIN_VISIBLE_SEGMENTS = Math.max(1, Number(process.env.MIN_VISIBLE_SEGMENTS || 2));
const MIN_START_SEGMENTS = Math.max(MIN_VISIBLE_SEGMENTS + LIVE_DELAY_SEGMENTS, Number(process.env.MIN_START_SEGMENTS || 4));
const SERVE_SEGMENTS = Math.max(MIN_VISIBLE_SEGMENTS, Number(process.env.SERVE_SEGMENTS || 6));
const PREFETCH_WINDOW_SEGMENTS = Math.max(MIN_START_SEGMENTS, Number(process.env.PREFETCH_WINDOW_SEGMENTS || 8));
const PRIME_WAIT_MS = Number(process.env.PRIME_WAIT_MS || 1500);
const MANIFEST_TYPE_WAIT_MS = Number(process.env.MANIFEST_TYPE_WAIT_MS || 1500);
const STARTUP_REAL_SEGMENTS = Math.max(MIN_VISIBLE_SEGMENTS, Number(process.env.STARTUP_REAL_SEGMENTS || 3));
const STARTUP_REAL_WAIT_MS = Number(process.env.STARTUP_REAL_WAIT_MS || 70000);
const POLLER_IDLE_STOP_MS = Number(process.env.POLLER_IDLE_STOP_MS || 5 * 60 * 1000); // stop if player gone
const POLL_MIN_MS = Number(process.env.POLL_MIN_MS || 2500);
const POLL_MAX_MS = Number(process.env.POLL_MAX_MS || 12000);

// ─────────────────────────────────────────────────────────────────────────────
// In-memory catalog, EPG and logo caches. Playback state is managed below.
// ─────────────────────────────────────────────────────────────────────────────
const memoryCache = {
    channelItems: {},     // configKey -> channels[]
    channelIndex: {},     // configKey -> { id -> channel }
    channelInflight: {},  // configKey -> Promise
    epgMatchStats: {},    // configKey -> { matched, total, feedChannels }
    lastUpdate: {},       // configKey -> timestamp
    epgData: {},          // epgUrl -> { byKey, channelCount, programmeCount }
    epgLastUpdate: {},    // epgUrl -> timestamp
    epgInflight: {},      // epgUrl -> Promise
    epgRetryTimers: {},   // epgUrl -> timeout
    epgRefreshTimers: {}, // epgUrl -> timeout
    epgStatus: {},        // epgUrl -> diagnostics
    epgSubscribers: {},   // epgUrl -> Set<configKey>
    configByKey: {},      // configKey -> decoded config
    logoData: {},         // logoUrl -> data: URI
    isUpdating: {}
};

// ─────────────────────────────────────────────────────────────────────────────
// Config / utilities
// ─────────────────────────────────────────────────────────────────────────────
function decodeConfig(configKey) {
    try {
        const normalized = String(configKey || "").replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
        return JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    } catch (err) {
        throw new Error("Invalid configuration token");
    }
}

function parseCatalogPreloadConfigs(raw) {
    return String(raw || "")
        .split(/[,\s]+/)
        .map(extractConfigKey)
        .filter(Boolean);
}

function extractConfigKey(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const manifestMatch = raw.match(/(?:^|\/)([A-Za-z0-9_-]+)\/manifest\.json(?:$|[?#])/i)
        || raw.match(/(?:^|\/)([A-Za-z0-9_-]+)\/manifest\.json$/i);
    if (manifestMatch) return manifestMatch[1];
    try {
        const parsed = new URL(raw);
        const config = parsed.searchParams.get("config");
        if (config) return config;
        const parts = parsed.pathname.split("/").filter(Boolean);
        if (parts[0]) return parts[0];
    } catch {}
    return raw.replace(/^\/+|\/+$/g, "");
}

function encodeProxyUrl(url) { return Buffer.from(String(url), "utf8").toString("base64url"); }
function decodeProxyUrl(enc) { return Buffer.from(String(enc), "base64url").toString("utf8"); }
function hashKey(value) { return crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, 20); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function analyzeHLS(text) {
    const lines = String(text || "").split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    const td = lines.find(l => l.startsWith("#EXT-X-TARGETDURATION:"));
    const seq = lines.find(l => l.startsWith("#EXT-X-MEDIA-SEQUENCE:"));
    return {
        isMaster: lines.some(l => l.startsWith("#EXT-X-STREAM-INF")),
        isLive: !lines.some(l => l.startsWith("#EXT-X-ENDLIST")),
        segmentCount: lines.filter(l => l && !l.startsWith("#")).length,
        targetDuration: td ? Number(td.split(":")[1]) : null,
        mediaSequence: seq ? Number(seq.split(":")[1]) : null
    };
}

function toAbsoluteUrl(value, baseUrl) {
    try { return new URL(value, baseUrl).toString(); } catch { return value; }
}
function isHttpUrl(v) { return /^https?:\/\//i.test(String(v || "")); }
function isHlsUrl(v) { return /\.m3u8(?:[?#].*)?$/i.test(String(v || "")); }
function isPlayableHttpUrl(v) { return /^https?:\/\//i.test(String(v || "")); }

function escapeXml(value) {
    return String(value || "")
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function formatBytes(bytes) {
    const v = Number(bytes || 0);
    if (v < 1024) return `${v}B`;
    if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)}KB`;
    return `${(v / 1024 / 1024).toFixed(2)}MB`;
}

function getPublicHost(req) {
    const proto = req.get("x-forwarded-proto") || req.protocol;
    const host = req.get("x-forwarded-host") || req.get("host");
    return `${proto}://${host}`;
}

function getSegmentLabel(sourceUrl) {
    try {
        const u = new URL(sourceUrl);
        return decodeURIComponent(u.pathname.split("/").filter(Boolean).pop() || "segment");
    } catch {
        return String(sourceUrl || "").split("?")[0].split("/").filter(Boolean).pop() || "segment";
    }
}

function getHttpStatus(err) {
    const status = Number(err?.kronosStatus || err?.response?.status || 0);
    return Number.isFinite(status) && status > 0 ? status : null;
}

// ─────────────────────────────────────────────────────────────────────────────
// EPG
// ─────────────────────────────────────────────────────────────────────────────
function getCachedEPG(epgUrl) {
    return epgUrl ? (memoryCache.epgData[epgUrl] || null) : null;
}

function subscribeConfigToEPG(configKey, config) {
    const previousEpgUrl = memoryCache.configByKey[configKey]?.e;
    if (previousEpgUrl && previousEpgUrl !== config.e) memoryCache.epgSubscribers[previousEpgUrl]?.delete(configKey);
    memoryCache.configByKey[configKey] = config;
    if (!config.e) return;
    const subscribers = memoryCache.epgSubscribers[config.e] || new Set();
    subscribers.add(configKey);
    memoryCache.epgSubscribers[config.e] = subscribers;
    startEPGBackgroundRefresh(config.e, "config");
}

function clearEPGRetry(epgUrl) {
    const timer = memoryCache.epgRetryTimers[epgUrl];
    if (timer) clearTimeout(timer);
    delete memoryCache.epgRetryTimers[epgUrl];
}

function scheduleEPGRetry(epgUrl) {
    if (!epgUrl || memoryCache.epgRetryTimers[epgUrl]) return;
    const retryAt = Date.now() + EPG_RETRY_DELAY_MS;
    memoryCache.epgStatus[epgUrl] = { ...(memoryCache.epgStatus[epgUrl] || {}), retryAt };
    const timer = setTimeout(() => {
        delete memoryCache.epgRetryTimers[epgUrl];
        ensureEPGRefresh(epgUrl, { force: true }).catch(() => {});
    }, EPG_RETRY_DELAY_MS);
    if (timer.unref) timer.unref();
    memoryCache.epgRetryTimers[epgUrl] = timer;
    console.log(`[EPG RETRY SCHEDULED] in=${EPG_RETRY_DELAY_MS / 1000}s`);
}

function startEPGBackgroundRefresh(epgUrl, reason = "background") {
    if (!epgUrl) return;
    const retryAt = memoryCache.epgStatus[epgUrl]?.retryAt || 0;
    if (!getCachedEPG(epgUrl) && !memoryCache.epgInflight[epgUrl] && Date.now() >= retryAt) {
        console.log(`[EPG BACKGROUND] reason=${reason}`);
        ensureEPGRefresh(epgUrl).catch(err => console.error("[EPG BACKGROUND]", err.message));
    }
    scheduleEPGPeriodicRefresh(epgUrl);
}

function scheduleEPGPeriodicRefresh(epgUrl) {
    if (!epgUrl || EPG_REFRESH_INTERVAL_MS <= 0 || memoryCache.epgRefreshTimers[epgUrl]) return;
    const timer = setTimeout(() => {
        delete memoryCache.epgRefreshTimers[epgUrl];
        console.log(`[EPG REFRESH SCHEDULED] interval=${EPG_REFRESH_INTERVAL_MS / 1000}s`);
        ensureEPGRefresh(epgUrl, { force: true }).catch(err => console.error("[EPG REFRESH]", err.message));
    }, EPG_REFRESH_INTERVAL_MS);
    if (timer.unref) timer.unref();
    memoryCache.epgRefreshTimers[epgUrl] = timer;
}

function startEPGStartupPreload() {
    if (!EPG_PRELOAD_URL) return;
    console.log(`[EPG PRELOAD] url=${EPG_PRELOAD_URL}`);
    startEPGBackgroundRefresh(EPG_PRELOAD_URL, "startup");

    if (EPG_STARTUP_WATCH_MS <= 0) return;
    const started = Date.now();
    const timer = setInterval(() => {
        if (getCachedEPG(EPG_PRELOAD_URL) || Date.now() - started >= EPG_STARTUP_WATCH_MS) {
            clearInterval(timer);
            return;
        }
        startEPGBackgroundRefresh(EPG_PRELOAD_URL, "startup-watch");
    }, 3000);
    if (timer.unref) timer.unref();
}

function startCatalogStartupPreload() {
    if (!CATALOG_PRELOAD_CONFIGS.length) return;
    console.log(`[CATALOG PRELOAD] configs=${CATALOG_PRELOAD_CONFIGS.length}`);
    CATALOG_PRELOAD_CONFIGS.forEach(configKey => {
        try {
            const config = decodeConfig(configKey);
            fetchAndProcessChannels(configKey, config)
                .then(channels => console.log(`[CATALOG PRELOAD OK] channels=${channels.length}`))
                .catch(err => console.error(`[CATALOG PRELOAD ERROR] ${err.message}`));
        } catch (err) {
            console.error(`[CATALOG PRELOAD ERROR] invalid config: ${err.message}`);
        }
    });
}

function attachEPGToChannels(channels, epgData) {
    let matched = 0;
    const updated = channels.map(channel => {
        const epg = findEpgMatch(epgData, channel);
        if (epg) matched++;
        return { ...channel, description: epg?.description || "", epgId: epg?.id || null };
    });
    return { channels: updated, matched };
}

function refreshSubscribedChannels(epgUrl, epgData) {
    const subscribers = memoryCache.epgSubscribers[epgUrl] || new Set();
    subscribers.forEach(configKey => {
        if (memoryCache.configByKey[configKey]?.e !== epgUrl) return;
        const current = memoryCache.channelItems[configKey];
        if (!current) return;
        const { channels, matched } = attachEPGToChannels(current, epgData);
        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.epgMatchStats[configKey] = { matched, total: channels.length, feedChannels: epgData.channelCount };
        console.log(`[EPG APPLY] matched=${matched}/${channels.length} feedChannels=${epgData.channelCount}`);
    });
}

async function ensureEPGRefresh(epgUrl, options = {}) {
    if (!epgUrl) return null;
    const cached = memoryCache.epgData[epgUrl];
    if (cached && !options.force && Date.now() - (memoryCache.epgLastUpdate[epgUrl] || 0) < EPG_CACHE_TTL) return cached;
    if (memoryCache.epgInflight[epgUrl]) return memoryCache.epgInflight[epgUrl];
    const retryAt = memoryCache.epgStatus[epgUrl]?.retryAt || 0;
    if (!options.force && Date.now() < retryAt) return cached || null;

    const promise = (async () => {
    try {
        const startedAt = Date.now();
        console.log("[EPG FETCH]", epgUrl);
        memoryCache.epgStatus[epgUrl] = { state: "fetching", startedAt: Date.now(), retryAt: 0 };
        const response = await axios.get(epgUrl, {
            timeout: EPG_REQUEST_TIMEOUT,
            signal: AbortSignal.timeout(EPG_REQUEST_TIMEOUT),
            maxContentLength: EPG_MAX_BYTES,
            responseType: "arraybuffer",
            headers: { "User-Agent": `Kronos/${RELEASE_VERSION}`, "Accept": "application/xml, text/xml, application/gzip, */*" }
        });
        const downloadMs = Date.now() - startedAt;
        const { channelDefs, programmesById, programmeCount } = await parseXMLTVBuffer(Buffer.from(response.data));
        const parseMs = Date.now() - startedAt - downloadMs;

        const indexStartedAt = Date.now();
        const byKey = new Map();
        const xmlIds = new Set([...channelDefs.keys(), ...programmesById.keys()]);
        xmlIds.forEach(id => {
            const programmes = programmesById.get(id) || [];
            if (!programmes.length) return;
            const entry = { id, names: channelDefs.get(id) || [], programmes };
            getEpgMatchKeys([id, ...entry.names]).forEach(key => {
                const list = byKey.get(key) || [];
                list.push(entry);
                byKey.set(key, list);
            });
        });

        const data = { byKey, channelCount: xmlIds.size, programmeCount };
        memoryCache.epgData[epgUrl] = data;
        memoryCache.epgLastUpdate[epgUrl] = Date.now();
        memoryCache.epgStatus[epgUrl] = { state: "ready", updatedAt: Date.now(), retryAt: 0 };
        clearEPGRetry(epgUrl);
        console.log(`[EPG OK] channels=${data.channelCount} programmes=${data.programmeCount} keys=${byKey.size} download=${downloadMs}ms parse=${parseMs}ms index=${Date.now() - indexStartedAt}ms`);
        scheduleEPGPeriodicRefresh(epgUrl);
        refreshSubscribedChannels(epgUrl, data);
        return data;
    } catch (err) {
        console.warn("[EPG ERROR]", err.message);
        memoryCache.epgStatus[epgUrl] = { state: "error", error: err.message, failedAt: Date.now(), retryAt: 0 };
        scheduleEPGRetry(epgUrl);
        return cached || null;
    }
    })();
    memoryCache.epgInflight[epgUrl] = promise;
    try { return await promise; }
    finally { delete memoryCache.epgInflight[epgUrl]; }
}

function parseXMLTVBuffer(buffer) {
    const isGzip = buffer[0] === 0x1f && buffer[1] === 0x8b;
    const input = isGzip ? Readable.from([buffer]).pipe(zlib.createGunzip()) : Readable.from([buffer]);
    const channelDefs = new Map();
    const programmesById = new Map();
    const keepAfter = Date.now() - 5 * 60 * 1000;

    return new Promise((resolve, reject) => {
        const parser = sax.createStream(true, { trim: false, normalize: false });
        let channel = null;
        let programme = null;
        let capture = null;
        let text = "";
        let programmeCount = 0;

        const readAttr = (node, name) => {
            const value = node.attributes?.[name];
            return typeof value === "string" ? value : (value?.value || "");
        };
        const appendText = value => { if (capture) text += value; };

        parser.on("opentag", node => {
            if (node.name === "channel") {
                channel = { id: readAttr(node, "id"), names: [] };
            } else if (node.name === "display-name" && channel) {
                capture = "display-name"; text = "";
            } else if (node.name === "programme") {
                const stop = parseXMLTVDate(readAttr(node, "stop"));
                const keep = !Number.isNaN(stop.getTime()) && stop.getTime() >= keepAfter;
                programme = {
                    channel: readAttr(node, "channel"),
                    start: keep ? parseXMLTVDate(readAttr(node, "start")) : new Date(NaN),
                    stop,
                    keep,
                    title: "",
                    desc: ""
                };
                programmeCount++;
            } else if ((node.name === "title" || node.name === "desc") && programme?.keep) {
                capture = node.name; text = "";
            }
        });
        parser.on("text", appendText);
        parser.on("cdata", appendText);
        parser.on("closetag", name => {
            if (name === capture) {
                const value = text.trim();
                if (name === "display-name" && channel && value) channel.names.push(value);
                if (name === "title" && programme) programme.title = value;
                if (name === "desc" && programme) programme.desc = value;
                capture = null; text = "";
            }
            if (name === "channel" && channel) {
                if (channel.id) channelDefs.set(channel.id, channel.names);
                channel = null;
            }
            if (name === "programme" && programme) {
                if (programme.keep && programme.channel && !Number.isNaN(programme.start.getTime())) {
                    const list = programmesById.get(programme.channel) || [];
                    list.push({ start: programme.start, stop: programme.stop, title: programme.title || "Programma senza titolo", desc: programme.desc });
                    programmesById.set(programme.channel, list);
                }
                programme = null;
            }
        });
        parser.on("error", reject);
        input.on("error", reject);
        parser.on("end", () => resolve({ channelDefs, programmesById, programmeCount }));
        input.pipe(parser);
    });
}

function parseXMLTVDate(str) {
    const m = String(str || "").match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-])(\d{2})(\d{2}))?/);
    if (!m) return new Date(str);
    const [, Y, Mo, D, H, Mi, S, sign, offH, offM] = m;
    if (sign) {
        const offsetMs = ((Number(offH) * 60) + Number(offM)) * 60 * 1000 * (sign === "+" ? 1 : -1);
        return new Date(Date.UTC(Number(Y), Number(Mo) - 1, Number(D), Number(H), Number(Mi), Number(S)) - offsetMs);
    }
    return zonedWallClockToDate(Number(Y), Number(Mo), Number(D), Number(H), Number(Mi), Number(S), EPG_TIME_ZONE);
}

function zonedWallClockToDate(year, month, day, hour, minute, second, timeZone) {
    const wallClockUtc = Date.UTC(year, month - 1, day, hour, minute, second);
    let instant = wallClockUtc;
    // Two passes cover CET/CEST changes: the first estimate selects the seasonal
    // offset, the second applies it to the intended Italian wall-clock time.
    for (let i = 0; i < 2; i++) instant = wallClockUtc - getTimeZoneOffsetMs(new Date(instant), timeZone);
    return new Date(instant);
}

function getTimeZoneOffsetMs(date, timeZone) {
    const parts = Object.fromEntries(getTimeZoneOffsetFormatter(timeZone)
        .formatToParts(date)
        .filter(part => part.type !== "literal")
        .map(part => [part.type, Number(part.value)]));
    return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second) - date.getTime();
}

const timeZoneOffsetFormatters = new Map();
function getTimeZoneOffsetFormatter(timeZone) {
    let formatter = timeZoneOffsetFormatters.get(timeZone);
    if (!formatter) {
        formatter = new Intl.DateTimeFormat("en-CA", {
            timeZone, year: "numeric", month: "2-digit", day: "2-digit",
            hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"
        });
        timeZoneOffsetFormatters.set(timeZone, formatter);
    }
    return formatter;
}

function formatTime(d) {
    return new Intl.DateTimeFormat("it-IT", {
        timeZone: EPG_TIME_ZONE, hour: "2-digit", minute: "2-digit", hourCycle: "h23"
    }).format(d);
}
function selectProgrammeWindow(programmes) {
    const now = new Date();
    const sorted = programmes.slice().sort((a, b) => a.start - b.start);
    const currentIndex = sorted.findIndex(p => now >= p.start && now <= p.stop);
    const current = currentIndex >= 0 ? sorted[currentIndex] : null;
    const next = current ? sorted[currentIndex + 1] : sorted.find(p => p.start > now);
    return { current, next };
}

function formatEpgDescription(programmes) {
    const { current, next } = selectProgrammeWindow(programmes);
    const lines = [];
    if (current) lines.push(formatProgramme("🔴 In Onda", current));
    if (next) lines.push(formatProgramme("🔵 A Seguire", next));
    return lines.join(nbsp("  ║  "));
}

function formatProgramme(label, programme) {
    const description = String(programme.desc || "").slice(0, 500).trim().replace(/\.+$/, "");
    return nbsp(`${label}: ${String(programme.title || "").toUpperCase()} (${formatTime(programme.start)} - ${formatTime(programme.stop)}) | Trama: ${description}`);
}

function nbsp(text) {
    return String(text || "").replace(/ /g, "\u00a0");
}

function getEpgMatchKeys(values) {
    const keys = new Set();
    const add = value => {
        let text = String(value || "").trim();
        if (!text) return;
        text = text.replace(/\.it$/i, "").replace(/^\s*IT\s*:\s*/i, "");
        addNormalizedEpgKey(keys, text);
        addNormalizedEpgKey(keys, text.replace(/\b(?:FHD|FULL\s*HD|HD|HEVC|UHD|4K|SD|H\.?26[45])\b/gi, " "));
        addNormalizedEpgKey(keys, text.replace(/\b(?:FHD|FULL\s*HD|HD|HEVC|UHD|4K|SD)\s+\d{3,4}\b/gi, " "));
        addNormalizedEpgKey(keys, text.replace(/\s+\d{3,4}\s*$/, " "));
    };
    (Array.isArray(values) ? values : [values]).forEach(add);
    return [...keys];
}

function addNormalizedEpgKey(keys, value) {
    const key = String(value || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "");
    if (!key) return;
    keys.add(key);
    const aliases = {
        "20mediaset": "20", canale20: "20",
        discoverymotortrend: "motortrend",
        nationalgeografic: "nationalgeographic",
        raisportplus: "raisport"
    };
    if (aliases[key]) keys.add(aliases[key]);
    if (key.startsWith("discovery") && key.length > "discovery".length) keys.add(key.slice("discovery".length));
}

function findEpgMatch(epgData, channel) {
    if (!epgData?.byKey) return null;
    for (const key of getEpgMatchKeys([channel.tvgId, channel.name])) {
        const entries = epgData.byKey.get(key);
        if (entries?.length) {
            const entry = entries.find(item => formatEpgDescription(item.programmes));
            if (entry) return { ...entry, description: formatEpgDescription(entry.programmes) };
        }
    }
    return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Playlist fetching + parsing
// ─────────────────────────────────────────────────────────────────────────────
function getConfiguredLists(config) {
    if (Array.isArray(config.l) && config.l.length) {
        return config.l
            .map((list, i) => ({
                name: String(list.n || `Lista ${i + 1}`).trim() || `Lista ${i + 1}`,
                url: String(list.u || "").trim()
            }))
            .filter(l => l.url);
    }
    return [{
        name: String(config.ln || "Canali TV").trim() || "Canali TV",
        url: String(config.u || "").trim()
    }].filter(l => l.url);
}

async function fetchPlaylist(sourceUrl, options = {}) {
    console.log("[FETCH PLAYLIST]", sourceUrl);
    const deadline = Date.now() + (options.retryWindow || PLAYLIST_RETRY_WINDOW_MS);
    let attempt = 0;
    let lastErr = null;
    while (Date.now() < deadline) {
        attempt++;
        try {
            const waitMs = Math.max(1, deadline - Date.now());
            const response = await withUpstream(() => axios.get(sourceUrl, {
                timeout: Math.max(1, Math.min(options.timeout || PLAYLIST_REQUEST_TIMEOUT, deadline - Date.now())),
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus: s => s >= 200 && s < 300
            }), true, waitMs);
            const data = String(response.data || "");
            if (!data.trimStart().startsWith("#EXTM3U")) throw new Error("Invalid M3U playlist from upstream");
            console.log("[FETCH PLAYLIST OK]", sourceUrl, "size=" + data.length, "attempt=" + attempt);
            return response.data;
        } catch (err) {
            lastErr = err;
            console.warn(`[FETCH PLAYLIST RETRY] attempt=${attempt} reason=${err.message}`);
        }
        const sleepMs = Math.min(PLAYLIST_RETRY_DELAY_MS, deadline - Date.now());
        if (sleepMs > 0) await sleep(sleepMs);
    }
    throw lastErr || new Error("Playlist fetch failed");
}

function parseM3UChannels(data, source = {}) {
    const lines = String(data || "").split("\n");
    const channels = [];
    let cur = null;
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.startsWith("#EXTINF:")) {
            const name = (line.match(/,(.+)$/) || [, "Canale Sconosciuto"])[1].trim();
            const group = (line.match(/group-title="([^"]+)"/) || [, "Altri Canali"])[1].trim();
            const logoMatch = line.match(/tvg-logo="([^"]+)"/);
            const tvgId = (line.match(/tvg-id="([^"]+)"/) || [, null])[1];
            cur = {
                name, group,
                logo: logoMatch ? logoMatch[1] : "",
                tvgId,
                sourceName: source.name || "Kronos",
                sourceUrl: source.url || ""
            };
        } else if (line.startsWith("http") && cur) {
            cur.url = line;
            cur.id = "channel_" + crypto.createHash("sha1").update(`${source.url || ""}|${line}`).digest("hex").slice(0, 20);
            channels.push(cur);
            cur = null;
        }
    }
    return channels;
}

function stripInitialCountryPrefix(name) { return String(name || "").replace(/^\s*IT:\s*/i, "").trim(); }
function getListAbbreviation(name) {
    const clean = String(name || "LST").replace(/[^a-z0-9]/gi, "");
    return (clean || "LST").slice(0, 3);
}
function decorateChannelName(channel, totalLists, mode) {
    const display = stripInitialCountryPrefix(channel.name);
    if (totalLists <= 1 || mode !== "filter") return display;
    const base = display.replace(/\s*\([^)]*\)\s*$/g, "").trim();
    return `${base} (${getListAbbreviation(channel.sourceName)})`;
}
function normalizeGroupName(g) { return String(g || "").trim().toLowerCase(); }
function normalizeSearchText(v) {
    return String(v || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase()
        .replace(/&/g, " e ").replace(/[^a-z0-9]+/gi, " ").replace(/\s+/g, " ").trim();
}
function matchesChannelSearch(channel, raw) {
    const q = normalizeSearchText(raw); if (!q) return true;
    const h = normalizeSearchText(channel.name);
    const ch = h.replace(/\s+/g, ""); const cq = q.replace(/\s+/g, "");
    if (h.includes(q) || ch.includes(cq)) return true;
    return q.split(" ").filter(Boolean).every(t => h.includes(t));
}

function getExtraParams(extra) {
    const params = {};
    if (!extra) return params;
    decodeURIComponent(String(extra)).replace(/\.json$/i, "").split("&").forEach(pair => {
        const i = pair.indexOf("="); if (i === -1) return;
        const n = pair.slice(0, i); const v = pair.slice(i + 1);
        if (n) params[n] = decodeURIComponent(v || "");
    });
    return params;
}
function getCatalogSourceName(id) {
    if (!String(id || "").startsWith("kronos_list_")) return null;
    return Buffer.from(id.replace("kronos_list_", ""), "hex").toString("utf8");
}
function toCatalogId(name) { return `kronos_list_${Buffer.from(name).toString("hex")}`; }
function sortChannelsByName(list) {
    return list.slice().sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
}
function buildChannelIndex(channels) {
    return channels.reduce((idx, c) => { idx[c.id] = c; return idx; }, {});
}

async function fetchAndProcessChannels(configKey, config, options = {}) {
    if (memoryCache.channelInflight[configKey] && !options.force) return memoryCache.channelInflight[configKey];
    const promise = (async () => {
        memoryCache.isUpdating[configKey] = true;
        subscribeConfigToEPG(configKey, config);
        const epgPromise = config.e
            ? ensureEPGRefresh(config.e).catch(err => {
                console.error("[EPG REFRESH]", err.message);
                return null;
            })
            : null;
        const lists = getConfiguredLists(config);
        const selectedGroups = Array.isArray(config.g) ? config.g : [];
        const selectedSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";

        const parsedGroups = await Promise.all(lists.map(async list => {
            const data = await fetchPlaylist(list.url);
            return parseM3UChannels(data, list);
        }));

        const rawChannels = parsedGroups.flat()
            .filter(c => {
                if (config.gm === "list" || config.gm === "bucket") return true;
                if (selectedSet.size === 0) return true;
                return selectedSet.has(normalizeGroupName(c.group));
            })
            .map(c => {
                return {
                    ...c,
                    name: decorateChannelName(c, lists.length, config.gm),
                    group: config.gm === "bucket" ? bucketGroup : c.group
                };
            });
        let epgData = getCachedEPG(config.e);
        if (config.e && !epgData && epgPromise && EPG_FIRST_CATALOG_WAIT_MS > 0) {
            console.log(`[EPG WAIT] upTo=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s`);
            epgData = await Promise.race([
                epgPromise,
                sleep(EPG_FIRST_CATALOG_WAIT_MS).then(() => null)
            ]);
            if (!epgData) epgData = getCachedEPG(config.e);
        }
        const { channels, matched: epgMatched } = attachEPGToChannels(rawChannels, epgData);
        if (config.e && !epgData) console.log("[EPG PENDING] catalog served without guide; background refresh active");

        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.epgMatchStats[configKey] = {
            matched: epgMatched,
            total: channels.length,
            feedChannels: epgData?.channelCount || 0
        };
        memoryCache.lastUpdate[configKey] = Date.now();
        if (config.e) console.log(`[EPG MATCH] matched=${epgMatched}/${channels.length} feedChannels=${epgData?.channelCount || 0}`);
        console.log(`[CATALOG OK] channels=${channels.length} lists=${lists.length}`);
        return channels;
    })();
    memoryCache.channelInflight[configKey] = promise;
    try { return await promise; }
    finally {
        delete memoryCache.channelInflight[configKey];
        memoryCache.isUpdating[configKey] = false;
    }
}

async function getChannelsFromCache(configKey, config) {
    const cached = memoryCache.channelItems[configKey];
    if (!cached) return await fetchAndProcessChannels(configKey, config);
    if (!memoryCache.channelIndex[configKey]) memoryCache.channelIndex[configKey] = buildChannelIndex(cached);
    if (Date.now() - (memoryCache.lastUpdate[configKey] || 0) > CATALOG_TTL) {
        // Catalog refreshes are not time-critical. Defer them while a channel is
        // active so a slow M3U download cannot occupy the provider's single slot.
        if (!isPlaybackActive()) {
            fetchAndProcessChannels(configKey, config).catch(err => console.error("[CATALOG REFRESH]", err.message));
        }
    }
    return cached;
}

async function getChannelById(configKey, config, id) {
    let ch = memoryCache.channelIndex[configKey]?.[id];
    if (ch) return ch;
    const channels = await getChannelsFromCache(configKey, config);
    ch = channels.find(c => c.id === id);
    if (ch) return ch;
    await fetchAndProcessChannels(configKey, config, { force: true });
    return memoryCache.channelIndex[configKey]?.[id] || null;
}

function isPlaybackActive() {
    const rt = activeChannelUrl ? channels.get(activeChannelUrl) : null;
    return !!rt?.running;
}

// ─────────────────────────────────────────────────────────────────────────────
// HLS relay
// ─────────────────────────────────────────────────────────────────────────────
// ── Upstream connection gate (provider max_connections=1) ───────────────────────
// EVERY upstream request (playlist polls, segment prefetch, player-demand fetch)
// goes through this single-slot mutex, so we NEVER open two connections at once
// (which the provider aborts → cascade → player crash). Player-demand and playlist
// polls take priority over background prefetch.
let upstreamBusy = false;
const upstreamWaiters = [];   // { resolve, prio }
function acquireUpstream(prio, timeoutMs = 0) {
    if (!upstreamBusy) { upstreamBusy = true; return Promise.resolve(); }
    return new Promise((resolve, reject) => {
        const w = {
            resolve: () => {
                if (w.timer) clearTimeout(w.timer);
                resolve();
            },
            prio: !!prio,
            timer: null
        };
        if (prio) {
            const i = upstreamWaiters.findIndex(x => !x.prio);
            if (i === -1) upstreamWaiters.push(w);
            else upstreamWaiters.splice(i, 0, w);
        } else {
            upstreamWaiters.push(w);
        }
        if (timeoutMs > 0) {
            w.timer = setTimeout(() => {
                const i = upstreamWaiters.indexOf(w);
                if (i !== -1) upstreamWaiters.splice(i, 1);
                reject(new Error("Upstream gate timeout"));
            }, timeoutMs);
            if (w.timer.unref) w.timer.unref();
        }
    });
}
function releaseUpstream() {
    const w = upstreamWaiters.shift();
    if (w) w.resolve();        // hand the slot to the next waiter (stays busy)
    else upstreamBusy = false;
}
async function withUpstream(fn, prio = false, timeoutMs = 0) {
    await acquireUpstream(prio, timeoutMs);
    try { return await fn(); }
    finally { releaseUpstream(); }
}

// Fetch a manifest for at most MANIFEST_RETRY_WINDOW_MS. There is no separate
// retry count: keep trying every MANIFEST_RETRY_DELAY_MS while time remains.
async function fetchUpstreamHLS(sourceUrl, label = "stream", signal = null) {
    const deadline = Date.now() + MANIFEST_RETRY_WINDOW_MS;
    let lastErr = null;
    let attempt = 0;

    while (Date.now() < deadline && !signal?.aborted) {
        attempt++;
        try {
            const r = await withUpstream(() => {
                return axios.get(sourceUrl, {
                    timeout: HLS_REQUEST_TIMEOUT,
                    signal,
                    maxRedirects: 5,
                    headers: {
                        "User-Agent": UPSTREAM_UA,
                        "Accept": "application/x-mpegURL, application/vnd.apple.mpegurl, audio/mpegurl, text/plain, */*",
                        "Accept-Encoding": "gzip, deflate",
                        "Connection": "keep-alive"
                    },
                    validateStatus: s => s >= 200 && s < 300
                });
            }, true);
            const finalUrl = r.request?.res?.responseUrl || sourceUrl;
            const text = String(r.data || "").trim();
            if (!text.startsWith("#EXTM3U")) {
                lastErr = new Error("Invalid HLS manifest from upstream");
                console.warn(`[HLS RETRY] channel="${label}" attempt=${attempt} reason=invalid-body`);
            } else {
                const info = analyzeHLS(text);
                if (attempt > 1) console.log(`[HLS RECOVERED] channel="${label}" after=${attempt} attempts`);
                return { data: r.data, finalUrl, info };
            }
        } catch (err) {
            lastErr = err;
            const status = getHttpStatus(err);
            if (status) err.kronosStatus = status;
            console.warn(`[HLS RETRY] channel="${label}" attempt=${attempt} reason=${err.message}`);
            // A 401/403 is normally a provider-side cooldown or session lock.
            // Retrying it every few seconds only extends the denial window and
            // steals the single upstream slot from cached segment playback.
            if (status === 401 || status === 403) throw err;
        }
        const sleepMs = Math.min(MANIFEST_RETRY_DELAY_MS, deadline - Date.now());
        if (sleepMs > 0) await sleep(sleepMs);
    }
    if (signal?.aborted) throw new Error("Manifest fetch aborted");
    throw lastErr || new Error("Manifest fetch failed");
}

// Stable identity for a segment, ignoring rotating query tokens and Xtream's
// rotating /hlsr/ path. The final .ts filename is stable for the physical segment.
const segRegistry = new Map();    // segId -> latest real upstream URL (with current token)
const SEG_REGISTRY_MAX = Number(process.env.SEG_REGISTRY_MAX || 4000);

function getStableSegmentKey(absUrl, scope = "") {
    try {
        const u = new URL(absUrl);
        const parts = u.pathname.split("/").filter(Boolean);
        const pathname = parts[0] === "hlsr" && parts.length > 1
            ? `/hlsr/${parts[parts.length - 1]}`
            : u.pathname;
        return `${scope}|${u.origin}${pathname}`;
    }
    catch { return `${scope}|${String(absUrl).split("?")[0]}`; }
}

function segIdFor(absUrl, scope = "") {
    try { return hashKey(getStableSegmentKey(absUrl, scope)); }
    catch { return hashKey(String(absUrl).split("?")[0]); }
}

function registerSeg(id, absUrl) {
    const previous = segRegistry.get(id);
    segRegistry.set(id, absUrl);
    if (previous !== absUrl) deadSegIds.delete(id);
    if (segRegistry.size > SEG_REGISTRY_MAX) {
        const oldest = segRegistry.keys().next().value;   // oldest insertion = slid-out segment
        if (oldest !== undefined && oldest !== id) {
            segRegistry.delete(oldest);
            deadSegIds.delete(oldest);
        }
    }
}

// Returns { rewritten, segIds } — segIds are stable segment ids in playlist order.
function rewriteHLSUrls(playlist, baseUrl, hostBase, configKey, scope = baseUrl) {
    const plUrl = abs => `${hostBase}/${configKey}/proxy/pl?u=${encodeProxyUrl(abs)}`;
    const segIds = [];
    const pick = abs => {
        if (isHlsUrl(abs)) return plUrl(abs);
        const id = segIdFor(abs, scope);
        registerSeg(id, abs);              // remember the freshest token-bearing URL
        segIds.push(id);
        return `${hostBase}/${configKey}/proxy/seg?s=${id}`;   // STABLE across refreshes
    };

    const rewritten = String(playlist || "").split(/\r?\n/).map(line => {
        const t = line.trim();
        if (!t) return line;
        if (t.startsWith("#")) {
            return line.replace(/URI=("([^"]+)"|'([^']+)')/g, (_, quoted, d, s) => {
                const uri = d || s;
                const q = quoted.startsWith("'") ? "'" : '"';
                return `URI=${q}${pick(toAbsoluteUrl(uri, baseUrl))}${q}`;
            });
        }
        return pick(toAbsoluteUrl(t, baseUrl));
    }).join("\n");

    return { rewritten, segIds };
}

// ── Segment cache, keyed by stable segId (full 200 only; Range bypasses) ────────
const segCache = new Map();       // segId -> { buffer, headers, status, size, fetchedAt }
const segInflight = new Map();    // segId -> Promise<entry|null>
const deadSegIds = new Set();     // segIds that upstream already expired (403/404/410)
let segCacheBytes = 0;
let prefetchActive = 0;
const prefetchQueue = [];
const playerDemandIds = new Set();
const prefetchControllers = new Map();

function getSegFromCache(id) {
    const e = segCache.get(id);
    if (!e) return null;
    if (Date.now() - e.fetchedAt > SEGMENT_CACHE_TTL) {
        segCache.delete(id);
        segCacheBytes -= e.size;
        return null;
    }
    return e;
}

function storeSeg(id, buffer, headers, status) {
    if (status !== 200) return;                       // never cache 206/partial
    if (!Buffer.isBuffer(buffer) || buffer.length <= 0 || buffer.length > SEGMENT_CACHE_MAX_ITEM_BYTES) return;
    deadSegIds.delete(id);
    const existing = segCache.get(id);
    if (existing) segCacheBytes -= existing.size;
    const keep = {};
    ["content-type", "cache-control", "last-modified", "etag"].forEach(h => { if (headers[h]) keep[h] = headers[h]; });
    segCache.set(id, { buffer, headers: keep, status, size: buffer.length, fetchedAt: Date.now() });
    segCacheBytes += buffer.length;
    evictSegCache();
}

function markDeadSeg(id, reason) {
    if (!id || deadSegIds.has(id)) return;
    deadSegIds.add(id);
    const idx = prefetchQueue.indexOf(id);
    if (idx >= 0) prefetchQueue.splice(idx, 1);
    console.warn(`[SEG DEAD] ${getSegmentLabel(segRegistry.get(id) || id)} reason=${reason}`);
}

function evictSegCache() {
    while (segCacheBytes > SEGMENT_CACHE_MAX_BYTES || segCache.size > SEGMENT_CACHE_MAX_ITEMS) {
        const oldestKey = segCache.keys().next().value;   // Map preserves insertion order
        if (oldestKey === undefined) break;
        const e = segCache.get(oldestKey);
        segCache.delete(oldestKey);
        if (e) segCacheBytes -= e.size;
    }
}

async function fetchSegToCache(id, url, prio = false, signal = null) {
    const hit = getSegFromCache(id);
    if (hit) return hit;
    if (segInflight.has(id)) return segInflight.get(id);   // dedup: never two fetches of same seg
    if (!url) return null;

    const p = (async () => {
        const started = Date.now();
        const r = await withUpstream(() => axios.get(url, {
            responseType: "arraybuffer",
            timeout: SEG_REQUEST_TIMEOUT,
            signal,
            maxRedirects: 5,
            headers: { "User-Agent": UPSTREAM_UA, "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive" },
            decompress: false,
            maxContentLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            maxBodyLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            validateStatus: s => s === 200
        }), prio);
        const buf = Buffer.from(r.data);
        storeSeg(id, buf, r.headers, 200);
        noteSegmentFetchSuccess(id);
        const ms = Date.now() - started;
        console.log(`[SEG FETCH${prio ? " *" : ""}] ${getSegmentLabel(url)} bytes=${formatBytes(buf.length)} time=${ms}ms speed=${formatSpeed(buf.length, ms)}`);
        return getSegFromCache(id);
    })();

    segInflight.set(id, p);
    try { return await p; }
    finally { segInflight.delete(id); }
}

async function fetchSegForPlayer(id) {
    playerDemandIds.add(id);
    let preempted = 0;
    prefetchControllers.forEach((controller, prefetchId) => {
        if (prefetchId === id || controller.signal.aborted) return;
        controller.abort();
        preempted++;
    });
    if (preempted > 0) console.log(`[PREFETCH PREEMPT] demand=${getSegmentLabel(segRegistry.get(id) || id)} aborted=${preempted}`);
    try {
        let lastErr = null;
        for (let attempt = 0; attempt <= SEGMENT_PLAYER_RETRIES; attempt++) {
            try {
                const entry = await fetchSegToCache(id, segRegistry.get(id), true);
                if (entry) return entry;
                lastErr = new Error("Segment fetch returned empty");
            } catch (err) {
                lastErr = err;
                const status = getHttpStatus(err);
                if (status === 401 || status === 403 || status === 404 || status === 410) {
                    markDeadSeg(id, `status-${status}`);
                    noteSegmentFetchFailure(id, status);
                    break;
                }
            }
            console.warn(`[SEG RETRY] ${getSegmentLabel(segRegistry.get(id) || id)} attempt=${attempt + 1}/${SEGMENT_PLAYER_RETRIES + 1} reason=${lastErr.message}`);
            if (attempt < SEGMENT_PLAYER_RETRIES) await sleep(SEGMENT_PLAYER_RETRY_DELAY_MS);
        }
        throw lastErr || new Error("Segment fetch failed");
    } finally {
        playerDemandIds.delete(id);
        pumpPrefetch();
    }
}

function queuePrefetch(ids) {
    const candidates = [...new Set(ids)].filter(id => !deadSegIds.has(id));
    prefetchQueue.splice(0, prefetchQueue.length, ...prefetchQueue.filter(id => candidates.includes(id)));
    candidates.forEach(id => {
        if (deadSegIds.has(id) || getSegFromCache(id) || segInflight.has(id) || prefetchQueue.includes(id)) return;
        prefetchQueue.push(id);
    });
    pumpPrefetch();
}

function getRuntimePrefetchIds(rt) {
    const ids = rt.segs.map(s => s.id);
    if (!ids.length) return [];
    if (!rt.primedOnce) return ids.slice(-MIN_START_SEGMENTS).filter(id => !deadSegIds.has(id));

    let startIdx;
    if (rt.lastRequestedSeq != null) {
        startIdx = Math.max(0, rt.lastRequestedSeq - rt.seqBase + 1);
    } else if (rt.servedEdge != null) {
        startIdx = Math.max(0, rt.servedEdge - rt.seqBase - MIN_VISIBLE_SEGMENTS + 1);
    } else {
        startIdx = Math.max(0, ids.length - MIN_START_SEGMENTS);
    }
    const from = Math.min(startIdx, ids.length);
    return ids.slice(from, from + PREFETCH_WINDOW_SEGMENTS).filter(id => !deadSegIds.has(id));
}

function queueRuntimePrefetch(rt) {
    queuePrefetch(getRuntimePrefetchIds(rt));
}

function isActivePrefetchCandidate(id) {
    const rt = activeChannelUrl ? channels.get(activeChannelUrl) : null;
    return !!rt?.running && getRuntimePrefetchIds(rt).includes(id);
}

function pumpPrefetch() {
    if (playerDemandIds.size > 0) return;
    while (prefetchActive < SEGMENT_PREFETCH_CONCURRENCY && prefetchQueue.length > 0) {
        const id = prefetchQueue.shift();
        if (deadSegIds.has(id) || getSegFromCache(id) || segInflight.has(id)) continue;
        const url = segRegistry.get(id);
        if (!url) continue;
        const controller = new AbortController();
        prefetchControllers.set(id, controller);
        prefetchActive++;
        fetchSegToCache(id, url, false, controller.signal)
            .catch(async err => {
                console.warn(`[PREFETCH ERR] ${getSegmentLabel(url)} ${err.message}`);
                const status = getHttpStatus(err);
                if (status === 401 || status === 403 || status === 404 || status === 410) {
                    markDeadSeg(id, `status-${status}`);
                    noteSegmentFetchFailure(id, status);
                    return;
                }
                if (isActivePrefetchCandidate(id) && !prefetchQueue.includes(id)) {
                    // Keep chronological order after a transient failure. Fetching a
                    // newer tail first creates cache holes that later stall players.
                    // Player-demand preemption is also retried unless the segment slid
                    // out of the active channel while the request was in flight.
                    prefetchQueue.unshift(id);
                    await sleep(PREFETCH_RETRY_DELAY_MS);
                }
            })
            .finally(() => {
                prefetchControllers.delete(id);
                prefetchActive--;
                pumpPrefetch();
            });
    }
}

function cancelPrefetch(reason) {
    prefetchQueue.length = 0;
    if (!prefetchControllers.size) return;
    prefetchControllers.forEach(controller => controller.abort());
    prefetchControllers.clear();
    console.log(`[PREFETCH CANCEL] reason=${reason}`);
}

function formatSpeed(bytes, ms) {
    if (!ms) return "n/a";
    const mbps = (bytes * 8 / 1e6) / (ms / 1000);
    return `${mbps.toFixed(1)}Mbps`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Logo helpers + stream/meta builders
// ─────────────────────────────────────────────────────────────────────────────
async function getLogoDataUri(logoUrl) {
    if (!isHttpUrl(logoUrl)) return "";
    if (memoryCache.logoData[logoUrl]) return memoryCache.logoData[logoUrl];
    try {
        const r = await axios.get(logoUrl, {
            responseType: "arraybuffer", timeout: 10000, maxContentLength: 2 * 1024 * 1024,
            headers: {
                "User-Agent": `Kronos/${RELEASE_VERSION}`,
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
            }
        });
        const ct = String(r.headers["content-type"] || "image/png").split(";")[0];
        const dataUri = `data:${ct};base64,${Buffer.from(r.data).toString("base64")}`;
        memoryCache.logoData[logoUrl] = dataUri;
        return dataUri;
    } catch { return ""; }
}

function buildStream(channel, host, configKey) {
    if (isHlsUrl(channel.url)) {
        return {
            title: channel.name, name: "TV",
            url: `${host}/${configKey}/hls/${channel.id}/index.m3u8`,
            behaviorHints: { notWebReady: true, bingeGroup: `kronos-${channel.id}` }
        };
    }
    if (isPlayableHttpUrl(channel.url)) {
        return {
            title: channel.name, name: "TV",
            url: `${host}/${configKey}/proxy/seg?u=${encodeProxyUrl(channel.url)}`,
            behaviorHints: { notWebReady: true, bingeGroup: `kronos-${channel.id}` }
        };
    }
    return { title: `${channel.name} - sorgente web`, name: "TV", externalUrl: channel.url };
}

function toMeta(channel, host, configKey = "") {
    const fallbackLogo = `${host}/logo.svg`;
    const poster = configKey
        ? `${host}/${configKey}/poster/${channel.id}.svg?v=${encodeURIComponent(RELEASE_VERSION)}`
        : (channel.logo || fallbackLogo);
    const logo = channel.logo || poster || fallbackLogo;
    const stream = configKey ? buildStream(channel, host, configKey) : null;
    return {
        id: channel.id, type: ADDON_TYPE, name: channel.name,
        poster, logo, description: channel.description, posterShape: "square", background: poster,
        genres: channel.group ? [channel.group] : undefined,
        behaviorHints: { defaultVideoId: channel.id, hasScheduledVideos: false },
        videos: [{
            id: channel.id, title: channel.name, released: new Date(0).toISOString(),
            thumbnail: poster, overview: channel.description, available: true,
            streams: stream ? [stream] : undefined
        }]
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Static / configurator
// ─────────────────────────────────────────────────────────────────────────────
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
        status: "ok", version: RELEASE_VERSION, uptime: process.uptime(),
        memory: process.memoryUsage(), timestamp: new Date().toISOString()
    });
});

app.get("/configure", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/:base64Config/configure", (req, res) =>
    res.redirect(`/?config=${encodeURIComponent(req.params.base64Config)}`));

// ─────────────────────────────────────────────────────────────────────────────
// Manifest
// ─────────────────────────────────────────────────────────────────────────────
app.get("/:base64Config/manifest.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        const host = getPublicHost(req);

        const catalogs = getConfiguredLists(config).map(list => {
            const catalogChannels = channels.filter(c => c.sourceName === list.name);
            const groups = [...new Set(catalogChannels.map(c => c.group))]
                .filter(g => g && g.trim())
                .sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" }));
            const extra = [{ name: "search", isRequired: false }];
            if (groups.length > 0) extra.push({ name: "genre", options: groups, isRequired: false });
            return { id: toCatalogId(list.name), type: ADDON_TYPE, name: list.name, extra };
        });

        res.json({
            id: "org.stremio.kronos.channel",
            version: RELEASE_VERSION,
            name: "TV", description: "TV",
            logo: `${host}/logo.svg`,
            resources: ["catalog", "meta", "stream"],
            types: [ADDON_TYPE],
            idPrefixes: ["channel_"],
            behaviorHints: { configurable: true, configurationRequired: false },
            catalogs
        });
    } catch (err) {
        console.error("[MANIFEST ERROR]", err.message);
        res.status(500).json({ error: "Errore Token" });
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Configurator API (used by public/index.html)
// ─────────────────────────────────────────────────────────────────────────────
app.post("/api/analyze-link", async (req, res) => {
    try {
        const data = await fetchPlaylist(req.body.url);
        const channels = parseM3UChannels(data, { name: req.body.name || "Lista", url: req.body.url });
        const map = new Map();
        channels.forEach(c => map.set(c.group, (map.get(c.group) || 0) + 1));
        const groups = [...map.entries()].map(([name, count]) => ({ name, count }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
        res.json({ totalChannels: channels.length, groups });
    } catch { res.status(400).json({ error: "Impossibile analizzare la lista M3U" }); }
});

app.post("/api/analyze-lists", async (req, res) => {
    try {
        const lists = getConfiguredLists({ l: req.body.lists || [] });
        const parsed = await Promise.all(lists.map(async l => parseM3UChannels(await fetchPlaylist(l.url), l)));
        const channels = parsed.flat();
        const map = new Map();
        channels.forEach(c => {
            const cur = map.get(c.group) || { name: c.group, count: 0, sources: new Set() };
            cur.count += 1; cur.sources.add(c.sourceName);
            map.set(c.group, cur);
        });
        const groups = [...map.values()]
            .map(g => ({ name: g.name, count: g.count, sources: [...g.sources] }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
        res.json({ totalChannels: channels.length, totalLists: lists.length, groups });
    } catch { res.status(400).json({ error: "Impossibile analizzare le liste M3U" }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// Catalog / meta / poster
// ─────────────────────────────────────────────────────────────────────────────
async function catalogResponse(req, res) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const params = {
            ...getExtraParams(req.params.extra),
            ...Object.fromEntries(Object.entries(req.query || {}).map(([k, v]) => [k, Array.isArray(v) ? v[0] : v]))
        };
        const targetGroup = params.genre || null;
        const targetSource = getCatalogSourceName(req.params.id);
        const search = params.search ? String(params.search).trim() : null;
        const host = getPublicHost(req);
        const channels = await getChannelsFromCache(configKey, config);
        const filtered = sortChannelsByName(channels.filter(c => {
            const matchSrc = targetSource ? c.sourceName === targetSource : true;
            const matchGrp = targetGroup ? normalizeGroupName(c.group) === normalizeGroupName(targetGroup) : true;
            const matchSearch = matchesChannelSearch(c, search);
            return matchSrc && matchGrp && matchSearch;
        }));
        res.json({ metas: filtered.map(c => toMeta(c, host, configKey)) });
    } catch (err) {
        console.error("[CATALOG ERROR]", err.message);
        res.status(500).json({ metas: [] });
    }
}
app.get("/:base64Config/catalog/:type/:id.json", catalogResponse);
app.get("/:base64Config/catalog/:type/:id/:extra.json", catalogResponse);
app.get("/:base64Config/catalog/:type/:id/:extra", catalogResponse);

app.get("/:base64Config/meta/:type/:id.json", async (req, res) => {
    const configKey = req.params.base64Config;
    const config = decodeConfig(configKey);
    const ch = await getChannelById(configKey, config, req.params.id);
    if (!ch) return res.status(404).json({ meta: null });
    res.json({ meta: toMeta(ch, getPublicHost(req), configKey) });
});

app.get("/:base64Config/poster/:id.svg", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        const logoUri = await getLogoDataUri(ch?.logo || "");
        const name = stripInitialCountryPrefix(ch?.name || "Kronos");
        const initials = name.replace(/\([^)]*\)/g, "").split(/\s+/).filter(Boolean).slice(0, 2)
            .map(p => p[0]).join("").toUpperCase() || "TV";
        const logoMarkup = logoUri
            ? `<image href="${escapeXml(logoUri)}" x="58" y="74" width="396" height="286" preserveAspectRatio="xMidYMid meet"/>`
            : `<text x="256" y="274" text-anchor="middle" fill="#111827" font-family="Arial, sans-serif" font-size="86" font-weight="800">${escapeXml(initials)}</text>`;
        res.setHeader("Content-Type", "image/svg+xml");
        res.setHeader("Cache-Control", "public, max-age=3600");
        res.send(`
            <svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
                <defs><linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
                    <stop offset="0" stop-color="#111827"/><stop offset="1" stop-color="#050814"/>
                </linearGradient></defs>
                <rect width="512" height="512" rx="56" fill="url(#bg)"/>
                <rect x="28" y="28" width="456" height="456" rx="44" fill="rgba(255,255,255,0.05)" stroke="rgba(255,255,255,0.16)"/>
                <rect x="46" y="58" width="420" height="318" rx="32" fill="#d9dee7"/>
                ${logoMarkup}
                <text x="256" y="424" text-anchor="middle" fill="#f5f7fb" font-family="Arial, sans-serif" font-size="30" font-weight="700">${escapeXml(name.slice(0, 34))}</text>
            </svg>
        `);
    } catch { res.status(404).send(""); }
});

// ─────────────────────────────────────────────────────────────────────────────
// Playback — background poller + retained buffer + non-blocking serve
// ─────────────────────────────────────────────────────────────────────────────
const channels = new Map();      // channelUrl -> runtime
let activeChannelUrl = null;     // only ONE channel polls upstream (max_connections=1)

function getActiveRuntimeForSegment(id) {
    const rt = activeChannelUrl ? channels.get(activeChannelUrl) : null;
    return rt?.running && rt.byId?.has(id) ? rt : null;
}

function noteSegmentFetchSuccess(id) {
    const rt = getActiveRuntimeForSegment(id);
    if (!rt) return;
    rt.segmentAuthFailures = 0;
    rt.firstSegmentAuthFailureAt = null;
    rt.lastSuccessfulSegmentFetchAt = Date.now();
}

function noteSegmentFetchFailure(id, status) {
    if (status !== 401 && status !== 403) return;
    const rt = getActiveRuntimeForSegment(id);
    if (!rt?.primedOnce) return;

    const now = Date.now();
    rt.segmentAuthFailures = (rt.segmentAuthFailures || 0) + 1;
    rt.firstSegmentAuthFailureAt = rt.firstSegmentAuthFailureAt || now;
    rt.lastSegmentAuthFailureAt = now;

    const failureMs = now - rt.firstSegmentAuthFailureAt;
    const warmRun = getPlayableWarmRun(rt, MIN_VISIBLE_SEGMENTS).count;
    if (warmRun >= MIN_START_SEGMENTS) return;
    if (rt.segmentAuthFailures < SEGMENT_AUTH_FAILURE_LIMIT) return;
    if (failureMs < SEGMENT_AUTH_FAILURE_MIN_MS) return;
    recoverChannelSession(rt, `segment-status-${status}`);
}

function recoverChannelSession(rt, reason) {
    if (!rt?.running) return;
    const now = Date.now();
    if (now - (rt.lastSegmentRecoveryAt || 0) < SEGMENT_AUTH_RECOVERY_COOLDOWN_MS) return;

    const holdPlaylist = rt.lastServedPlaylist || rt.holdPlaylist;
    const nextSeqBase = Number.isFinite(holdPlaylist?.edge)
        ? holdPlaylist.edge + 1
        : rt.seqBase + rt.segs.length;

    rt.lastSegmentRecoveryAt = now;
    rt.segmentAuthFailures = 0;
    rt.firstSegmentAuthFailureAt = null;
    rt.lastSegmentAuthFailureAt = null;
    rt.generation++;
    rt.seqBase = nextSeqBase;
    rt.segs = [];
    rt.byId = new Set();
    rt.lastSeq = -1;
    rt.servedEdge = null;
    rt.primedOnce = false;
    rt.pendingDiscontinuity = true;
    rt.lastRequestedSeq = null;
    rt.lastUpstreamEndSeq = null;
    rt.lastServedPlaylist = holdPlaylist || null;
    rt.holdPlaylist = holdPlaylist || null;
    cancelPrefetch(`segment-recover-${reason}`);
    if (rt.pollAbort) { rt.pollAbort.abort(); rt.pollAbort = null; }
    if (rt.timer) { clearTimeout(rt.timer); rt.timer = null; }
    console.warn(`[CHANNEL RECOVER] channel="${rt.label}" reason=${reason} seqBase=${rt.seqBase} gen=${rt.generation}`);
    rt.timer = setTimeout(() => pollChannelLoop(rt), 0);
    if (rt.timer.unref) rt.timer.unref();
}

function parseSegments(playlist, baseUrl, scope = baseUrl, mediaSequence = null) {
    const out = [];
    let dur = 0;
    let discontinuity = false;
    let segmentOffset = 0;
    for (const raw of String(playlist || "").split(/\r?\n/)) {
        const t = raw.trim();
        if (!t || t.startsWith("#EXT-X-ENDLIST")) continue;
        if (t === "#EXT-X-DISCONTINUITY") {
            discontinuity = true;
            continue;
        }
        if (t.startsWith("#EXTINF")) {
            const m = t.match(/#EXTINF:([0-9.]+)/);
            dur = m ? Number(m[1]) : 0;
            continue;
        }
        if (t.startsWith("#")) continue;
        if (isHlsUrl(t)) continue;   // nested playlist (handled elsewhere)
        const abs = toAbsoluteUrl(t, baseUrl);
        const upstreamSeq = Number.isFinite(mediaSequence) ? mediaSequence + segmentOffset : null;
        out.push({ segId: segIdFor(abs, scope), realUrl: abs, duration: dur || 6, discontinuity, upstreamSeq });
        segmentOffset++;
        dur = 0;
        discontinuity = false;
    }
    return out;
}

function stopPoller(rt, reason) {
    rt.running = false;
    if (rt.timer) { clearTimeout(rt.timer); rt.timer = null; }
    if (rt.pollAbort) { rt.pollAbort.abort(); rt.pollAbort = null; }
    cancelPrefetch(reason);
    rt.segs = []; rt.byId = new Set(); rt.seqBase = 0; rt.lastSeq = -1; rt.servedEdge = null; rt.primedOnce = false; rt.lastServedPlaylist = null; rt.holdPlaylist = null;
    rt.discontinuityBase = 0; rt.pendingDiscontinuity = false;
    rt.lastManifestAt = null; rt.lastSegmentAt = null; rt.segmentRequests = 0;
    rt.lastRequestedSeq = null; rt.lastUpstreamEndSeq = null;
    rt.lastPollStatus = null; rt.pollFailures = 0; rt.lastManifestErrorAt = null;
    rt.segmentAuthFailures = 0; rt.firstSegmentAuthFailureAt = null; rt.lastSegmentAuthFailureAt = null;
    rt.lastSegmentRecoveryAt = null; rt.lastSuccessfulSegmentFetchAt = null;
    rt.generation++;
    console.log(`[POLLER STOP] channel="${rt.label}" reason=${reason}`);
}

function ensureChannel(url, label) {
    // max_connections=1: stop any other channel's poller before starting this one.
    if (activeChannelUrl && activeChannelUrl !== url) {
        const other = channels.get(activeChannelUrl);
        if (other && other.running) stopPoller(other, "channel-switch");
        prefetchQueue.length = 0;
    }
    activeChannelUrl = url;

    let rt = channels.get(url);
    if (!rt) {
        rt = {
            url, label, segs: [], byId: new Set(), seqBase: 0, generation: 0,
            target: 6, lastSeq: -1, isMaster: false, lastPlayerAt: Date.now(),
            running: false, timer: null, pollAbort: null, servedEdge: null, primedOnce: false, lastServedPlaylist: null, holdPlaylist: null,
            discontinuityBase: 0, pendingDiscontinuity: false,
            lastManifestAt: null, lastSegmentAt: null, segmentRequests: 0,
            lastRequestedSeq: null, lastUpstreamEndSeq: null,
            lastPollStatus: null, pollFailures: 0, lastManifestErrorAt: null,
            segmentAuthFailures: 0, firstSegmentAuthFailureAt: null, lastSegmentAuthFailureAt: null,
            lastSegmentRecoveryAt: null, lastSuccessfulSegmentFetchAt: null
        };
        channels.set(url, rt);
    }
    rt.label = label;
    rt.lastPlayerAt = Date.now();
    if (!rt.running && !rt.isMaster) {
        rt.running = true;
        console.log(`[POLLER START] channel="${label}"`);
        pollChannelLoop(rt);
    }
    return rt;
}

async function pollChannelLoop(rt) {
    if (!rt.running) return;
    if (Date.now() - rt.lastPlayerAt > POLLER_IDLE_STOP_MS) {
        stopPoller(rt, "player-idle");
        if (activeChannelUrl === rt.url) activeChannelUrl = null;
        return;
    }
    let nextDelay = null;
    try {
        const controller = new AbortController();
        rt.pollAbort = controller;
        const { data, finalUrl, info } = await fetchUpstreamHLS(rt.url, rt.label, controller.signal);
        if (rt.pollAbort === controller) rt.pollAbort = null;
        rt.lastManifestAt = Date.now();
        rt.lastPollStatus = 200;
        rt.pollFailures = 0;
        if (info.isMaster) {
            rt.isMaster = true;
            stopPoller(rt, "master-playlist");   // master is served passthrough, no buffer
            return;
        }
        // Some on-demand IPTV encoders briefly return a one-segment ENDLIST while
        // starting. It is still useful media and should not be discarded.
        if (info.segmentCount > 0) ingestPlaylist(rt, data, finalUrl, info);
    } catch (e) {
        rt.pollAbort = null;
        if (rt.running) {
            const status = getHttpStatus(e);
            rt.lastPollStatus = status;
            rt.lastManifestErrorAt = Date.now();
            rt.pollFailures++;
            nextDelay = status === 401 || status === 403
                ? MANIFEST_FORBIDDEN_BACKOFF_MS
                : POLL_ERROR_BACKOFF_MS;
            console.warn(`[POLLER ERR] channel="${rt.label}" ${e.message} retryIn=${nextDelay}ms`);
        }
    }
    if (!rt.running) return;
    if (nextDelay == null) {
        const targetMs = Math.max(1, rt.target || 6) * 1000;
        const factor = rt.primedOnce ? 0.75 : 0.4;
        nextDelay = Math.max(POLL_MIN_MS, Math.min(POLL_MAX_MS, Math.round(targetMs * factor)));
    }
    rt.timer = setTimeout(() => pollChannelLoop(rt), nextDelay);
    if (rt.timer.unref) rt.timer.unref();
}

function ingestPlaylist(rt, playlist, baseUrl, info) {
    rt.target = info.targetDuration || rt.target;
    const seq = Number(info.mediaSequence || 0);
    // Restart detection: an on-demand stream that was torn down restarts at a LOWER
    // media-sequence. Same segment paths now hold DIFFERENT content, so we bump a
    // generation (mixed into the id) to avoid serving stale cached segments.
    if (rt.lastSeq >= 0 && seq < rt.lastSeq) {
        rt.generation++;
        cancelPrefetch("channel-restart");
        rt.holdPlaylist = rt.lastServedPlaylist || rt.holdPlaylist;
        // Continue the absolute sequence forward (don't reset to 0) so the player's
        // MEDIA-SEQUENCE never goes backward → it rejoins live moving forward instead
        // of "jumping to the beginning". New generation keeps stale cache out.
        rt.seqBase = rt.seqBase + rt.segs.length;
        rt.segs = []; rt.byId = new Set(); rt.primedOnce = false; rt.pendingDiscontinuity = true;
        rt.lastRequestedSeq = null; rt.lastUpstreamEndSeq = null; rt.servedEdge = null;
        console.log(`[CHANNEL RESTART] channel="${rt.label}" seq ${rt.lastSeq}->${seq} gen=${rt.generation} continuing seqBase=${rt.seqBase}`);
    }
    rt.lastSeq = seq;

    const parsed = parseSegments(playlist, baseUrl, rt.url, Number.isFinite(info.mediaSequence) ? info.mediaSequence : null);
    const firstUpstreamSeq = parsed.find(s => s.upstreamSeq != null)?.upstreamSeq;
    const lastUpstreamSeq = [...parsed].reverse().find(s => s.upstreamSeq != null)?.upstreamSeq;
    if (rt.lastUpstreamEndSeq != null && firstUpstreamSeq != null && firstUpstreamSeq > rt.lastUpstreamEndSeq + 1) {
        const missing = firstUpstreamSeq - rt.lastUpstreamEndSeq - 1;
        rt.pendingDiscontinuity = true;
        console.warn(`[CHANNEL GAP] channel="${rt.label}" missing=${missing} upstreamSeq=${rt.lastUpstreamEndSeq}->${firstUpstreamSeq}`);
    }

    let added = 0;
    for (const s of parsed) {
        const id = `g${rt.generation}:${s.segId}`;
        registerSeg(id, s.realUrl);          // always refresh the token-bearing URL
        if (!rt.byId.has(id)) {
            rt.byId.add(id);
            rt.segs.push({ id, duration: s.duration, discontinuity: s.discontinuity || rt.pendingDiscontinuity });
            rt.pendingDiscontinuity = false;
            added++;
        }
    }
    if (lastUpstreamSeq != null) {
        rt.lastUpstreamEndSeq = Math.max(rt.lastUpstreamEndSeq ?? lastUpstreamSeq, lastUpstreamSeq);
    }
    while (rt.segs.length > RETAIN_SEGMENTS) {
        const dropped = rt.segs.shift();
        rt.byId.delete(dropped.id);
        if (dropped.discontinuity) rt.discontinuityBase++;
        rt.seqBase++;
    }
    if (added > 0) {
        queueRuntimePrefetch(rt);
        console.log(`[BUFFER] channel="${rt.label}" +${added} depth=${rt.segs.length}/${RETAIN_SEGMENTS} seqBase=${rt.seqBase} cache=${segCache.size}/${formatBytes(segCacheBytes)} prefetch=${prefetchActive}+${prefetchQueue.length}`);
    }
}

function buildServedPlaylist(rt, hostBase, configKey) {
    const lo = rt.seqBase;
    const hi = rt.seqBase + rt.segs.length - 1;
    const warmRun = getPlayableWarmRun(rt, MIN_VISIBLE_SEGMENTS);
    if (warmRun.count < MIN_VISIBLE_SEGMENTS) {
        return { hold: true, reason: "short-warm-run", warmRun: warmRun.count };
    }
    const desiredIdx = getDelayedWarmEdgeIndex(warmRun);
    const delayedHi = rt.seqBase + Math.max(0, desiredIdx);

    // ANTI-JUMP: advance the served live edge by at most MAX_EDGE_ADVANCE per request,
    // capped at a deliberately delayed CONTIGUOUS warm edge. If a cache hole exists,
    // hold the current edge instead of advertising a segment that is not ready.
    const servedIdx = rt.servedEdge == null ? -1 : rt.servedEdge - rt.seqBase;
    const servedRun = getWarmRunContainingIndex(rt, servedIdx);
    const servedIsWarm = servedRun && servedRun.count >= MIN_VISIBLE_SEGMENTS;
    let gapJumped = false;
    if (rt.servedEdge == null || rt.servedEdge > hi || rt.servedEdge < lo || !servedIsWarm) {
        const previousEdge = rt.servedEdge;
        if (rt.primedOnce && previousEdge != null && delayedHi > previousEdge + 1 && warmRun.start >= 0) {
            gapJumped = true;
            console.warn(`[HLS GAP JUMP] channel="${rt.label}" from=${previousEdge} to=${delayedHi}`);
        }
        rt.servedEdge = delayedHi;
    }
    else if (delayedHi > rt.servedEdge) rt.servedEdge = Math.min(delayedHi, rt.servedEdge + MAX_EDGE_ADVANCE);

    const endIdx = rt.servedEdge - rt.seqBase;
    const activeRun = getWarmRunContainingIndex(rt, endIdx) || warmRun;
    if (activeRun.count < MIN_VISIBLE_SEGMENTS) {
        return { hold: true, reason: "short-active-run", warmRun: activeRun.count };
    }
    const startIdx = Math.max(activeRun.start, endIdx - SERVE_SEGMENTS + 1);
    if (gapJumped && rt.segs[startIdx]) rt.segs[startIdx].discontinuity = true;
    const win = rt.segs.slice(startIdx, endIdx + 1);
    const mediaSeq = rt.seqBase + startIdx;
    const discontinuitySequence = rt.discontinuityBase
        + rt.segs.slice(0, startIdx).filter(s => s.discontinuity).length;
    const target = Math.max(1, Math.ceil(Math.max(rt.target || 6, ...win.map(s => s.duration || 0))));
    const lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        `#EXT-X-TARGETDURATION:${target}`,
        `#EXT-X-MEDIA-SEQUENCE:${mediaSeq}`,
        `#EXT-X-DISCONTINUITY-SEQUENCE:${discontinuitySequence}`
    ];
    for (const s of win) {
        if (s.discontinuity) lines.push("#EXT-X-DISCONTINUITY");
        lines.push(`#EXTINF:${Number(s.duration || rt.target || 6).toFixed(3)},`);
        lines.push(`${hostBase}/${configKey}/proxy/seg?s=${encodeURIComponent(s.id)}`);
    }
    return {
        playlist: lines.join("\n") + "\n",
        count: win.length, mediaSeq, edge: rt.servedEdge, hi, warmRun: warmRun.count,
        readyAhead: Math.max(0, activeRun.end - endIdx)
    };
}

function getServeReadiness(rt) {
    const warmRun = getPlayableWarmRun(rt, MIN_VISIBLE_SEGMENTS);
    if (warmRun.count <= 0) return { ready: false, window: 0, readyAhead: 0 };
    const edgeIdx = getDelayedWarmEdgeIndex(warmRun);
    const window = edgeIdx - warmRun.start + 1;
    const readyAhead = Math.max(0, warmRun.end - edgeIdx);
    return {
        ready: window >= MIN_VISIBLE_SEGMENTS
            && warmRun.count >= MIN_START_SEGMENTS
            && readyAhead >= LIVE_DELAY_SEGMENTS,
        window,
        readyAhead
    };
}

function getStartupReadiness(rt) {
    const warmRun = getPlayableWarmRun(rt, STARTUP_REAL_SEGMENTS);
    if (warmRun.count <= 0) return { ready: false, window: 0, readyAhead: 0 };
    const edgeIdx = Math.min(warmRun.end, warmRun.start + STARTUP_REAL_SEGMENTS - 1);
    const window = edgeIdx - warmRun.start + 1;
    return {
        ready: window >= STARTUP_REAL_SEGMENTS,
        window,
        readyAhead: Math.max(0, warmRun.end - edgeIdx)
    };
}

function setPlaylistHeaders(res) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
}

function getLatestWarmIndex(rt) {
    for (let i = rt.segs.length - 1; i >= 0; i--) {
        if (getSegFromCache(rt.segs[i].id)) return i;
    }
    return -1;
}

function getLatestWarmRun(rt) {
    const end = getLatestWarmIndex(rt);
    if (end < 0) return { start: -1, end: -1, count: 0 };
    let start = end;
    while (start > 0 && getSegFromCache(rt.segs[start - 1].id)) start--;
    return { start, end, count: end - start + 1 };
}

function getLatestWarmRunAtLeast(rt, minCount) {
    for (let end = rt.segs.length - 1; end >= 0; end--) {
        if (!getSegFromCache(rt.segs[end]?.id)) continue;
        let start = end;
        while (start > 0 && getSegFromCache(rt.segs[start - 1].id)) start--;
        const count = end - start + 1;
        if (count >= minCount) return { start, end, count };
        end = start;
    }
    return null;
}

function getWarmRunContainingIndex(rt, index) {
    if (index < 0 || index >= rt.segs.length || !getSegFromCache(rt.segs[index]?.id)) {
        return null;
    }
    let start = index;
    let end = index;
    while (start > 0 && getSegFromCache(rt.segs[start - 1].id)) start--;
    while (end + 1 < rt.segs.length && getSegFromCache(rt.segs[end + 1].id)) end++;
    return { start, end, count: end - start + 1 };
}

function getPlaybackWarmRun(rt) {
    const servedIdx = rt.servedEdge == null ? -1 : rt.servedEdge - rt.seqBase;
    const servedRun = getWarmRunContainingIndex(rt, servedIdx);
    if (servedRun) return servedRun;

    const requestedIdx = rt.lastRequestedSeq == null ? -1 : rt.lastRequestedSeq - rt.seqBase;
    const requestedRun = getWarmRunContainingIndex(rt, requestedIdx);
    return requestedRun || getLatestWarmRun(rt);
}

function getPlayableWarmRun(rt, minCount = MIN_VISIBLE_SEGMENTS) {
    const preferred = getPlaybackWarmRun(rt);
    if (preferred.count >= minCount) return preferred;
    return getLatestWarmRunAtLeast(rt, minCount) || preferred;
}

function getDelayedWarmEdgeIndex(warmRun) {
    if (!warmRun || warmRun.count <= 0) return -1;
    let edge = Math.max(warmRun.start, warmRun.end - LIVE_DELAY_SEGMENTS);
    const minVisibleEdge = warmRun.start + MIN_VISIBLE_SEGMENTS - 1;
    if (edge < minVisibleEdge) edge = Math.min(warmRun.end, minVisibleEdge);
    return edge;
}

app.get("/:base64Config/hls/:id/index.m3u8", async (req, res) => {
    const t0 = Date.now();
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        if (!ch) return res.status(404).send("#EXTM3U\n#EXT-X-ENDLIST\n");
        const host = getPublicHost(req);

        const rt = ensureChannel(ch.url, ch.name);
        const recoveryPlaylist = rt.lastServedPlaylist || rt.holdPlaylist;
        const coldStart = !recoveryPlaylist && rt.segmentRequests === 0 && rt.servedEdge == null && !rt.primedOnce;
        const warmRun = getPlayableWarmRun(rt, MIN_VISIBLE_SEGMENTS).count;
        const fresh = coldStart && !rt.isMaster;
        const readiness = fresh ? getStartupReadiness(rt) : getServeReadiness(rt);
        const warmTarget = fresh ? STARTUP_REAL_SEGMENTS : MIN_START_SEGMENTS;
        console.log(`[HLS OPEN] channel="${ch.name}" resolve=${Date.now() - t0}ms depth=${rt.segs.length} warmRun=${warmRun}/${warmTarget} serveWindow=${readiness.window}/${MIN_VISIBLE_SEGMENTS} readyAhead=${readiness.readyAhead}/${LIVE_DELAY_SEGMENTS} ${fresh ? "(priming real…)" : "(buffer ready)"}`);

        // Firestick/Stremio can stick forever to synthetic startup media. On the
        // first open only, wait for real cached media so the player joins the real
        // timeline on its first successful manifest. During encoder restarts, keep
        // holding the previous real playlist instead of turning the request into a
        // new cold start.
        if (fresh && !readiness.ready) {
            const startedWait = Date.now();
            while (rt.running && !rt.isMaster && !getStartupReadiness(rt).ready) {
                if (Date.now() - startedWait >= STARTUP_REAL_WAIT_MS) break;
                rt.lastPlayerAt = Date.now();
                await sleep(50);
            }
        }

        if (rt.isMaster) {
            const { data, finalUrl } = await fetchUpstreamHLS(ch.url, ch.name);
            const { rewritten } = rewriteHLSUrls(data, finalUrl, host, configKey, ch.url);
            setPlaylistHeaders(res);
            console.log(`[HLS SERVE] channel="${ch.name}" type=master`);
            return res.send(rewritten);
        }

        const readyAfterWait = fresh ? getStartupReadiness(rt) : getServeReadiness(rt);
        const holdPlaylist = rt.lastServedPlaylist || rt.holdPlaylist;
        if (!rt.segs.length || getLatestWarmIndex(rt) < 0) {
            if (holdPlaylist) {
                setPlaylistHeaders(res);
                console.warn(`[HLS HOLD] channel="${ch.name}" reason=no-warm-cache seq=${holdPlaylist.mediaSeq} edge=${holdPlaylist.edge} warmRun=${getPlaybackWarmRun(rt).count}/${MIN_START_SEGMENTS} waited=${Date.now() - t0}ms`);
                return res.send(holdPlaylist.playlist);
            }
            const retryAfterSeconds = 2;
            setPlaylistHeaders(res);
            res.setHeader("Retry-After", String(retryAfterSeconds));
            console.warn(`[HLS NOT READY] channel="${ch.name}" warmRun=${getPlaybackWarmRun(rt).count}/${warmTarget} waited=${Date.now() - t0}ms`);
            return res.status(503).send("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n");
        }
        if (!readyAfterWait.ready && holdPlaylist) {
            setPlaylistHeaders(res);
            console.warn(`[HLS HOLD] channel="${ch.name}" reason=short-window seq=${holdPlaylist.mediaSeq} edge=${holdPlaylist.edge} warmRun=${getPlayableWarmRun(rt, MIN_VISIBLE_SEGMENTS).count}/${MIN_VISIBLE_SEGMENTS} window=${readyAfterWait.window}/${MIN_VISIBLE_SEGMENTS} waited=${Date.now() - t0}ms`);
            return res.send(holdPlaylist.playlist);
        }
        if (!rt.primedOnce && !readyAfterWait.ready) {
            console.warn(`[HLS START PARTIAL] channel="${ch.name}" warmRun=${getPlaybackWarmRun(rt).count}/${STARTUP_REAL_SEGMENTS} waited=${Date.now() - t0}ms`);
        }
        rt.primedOnce = true;

        const served = buildServedPlaylist(rt, host, configKey);
        if (served.hold) {
            if (holdPlaylist) {
                setPlaylistHeaders(res);
                console.warn(`[HLS HOLD] channel="${ch.name}" reason=${served.reason} seq=${holdPlaylist.mediaSeq} edge=${holdPlaylist.edge} warmRun=${served.warmRun}/${MIN_VISIBLE_SEGMENTS} waited=${Date.now() - t0}ms`);
                return res.send(holdPlaylist.playlist);
            }
            const retryAfterSeconds = 2;
            setPlaylistHeaders(res);
            res.setHeader("Retry-After", String(retryAfterSeconds));
            console.warn(`[HLS NOT READY] channel="${ch.name}" reason=${served.reason} warmRun=${served.warmRun}/${MIN_VISIBLE_SEGMENTS} waited=${Date.now() - t0}ms`);
            return res.status(503).send("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n");
        }
        const { playlist, count, mediaSeq, edge, hi, warmRun: servedWarmRun, readyAhead } = served;
        rt.lastServedPlaylist = { playlist, mediaSeq, edge };
        rt.holdPlaylist = rt.lastServedPlaylist;
        setPlaylistHeaders(res);
        const segmentIdle = rt.lastSegmentAt ? `${Date.now() - rt.lastSegmentAt}ms` : "never";
        const degraded = readyAhead < LIVE_DELAY_SEGMENTS;
        console.log(`[HLS SERVE] channel="${ch.name}" window=${count} seq=${mediaSeq} edge=${edge}/${hi} held=${hi - edge}/${LIVE_DELAY_SEGMENTS} readyAhead=${readyAhead} warmRun=${servedWarmRun}${degraded ? " degraded=1" : ""} gen=${rt.generation} segmentIdle=${segmentIdle} segmentReq=${rt.segmentRequests} cache=${segCache.size}/${formatBytes(segCacheBytes)} prefetch=${prefetchActive}+${prefetchQueue.length} waited=${Date.now() - t0}ms`);
        res.send(playlist);
    } catch (err) {
        console.error(`[HLS ERROR] ${err.message}`);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/pl", async (req, res) => {
    // Nested/variant playlist (only reached for master streams): simple passthrough.
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");
        const sourceUrl = decodeProxyUrl(req.query.u);
        const { data, finalUrl } = await fetchUpstreamHLS(sourceUrl, "nested-playlist");
        const { rewritten } = rewriteHLSUrls(data, finalUrl, getPublicHost(req), configKey, sourceUrl);
        setPlaylistHeaders(res);
        res.send(rewritten);
    } catch (err) {
        console.error(`[PL ERROR] ${err.message}`);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/seg", async (req, res) => {
    const started = Date.now();
    let sourceUrl = null;
    let segId = null;

    try {
        decodeConfig(req.params.base64Config);

        if (req.query.s) {
            // HLS segment: stable id → resolve freshest token-bearing URL.
            segId = String(req.query.s);
            sourceUrl = segRegistry.get(segId);
            if (!sourceUrl) {
                // Unknown id (server restarted / aged out). Tell the player to refresh
                // the playlist rather than serve garbage.
                console.warn(`[SEG STALE-ID] ${segId} not in registry; player must refresh playlist`);
                return res.status(410).end();
            }
        } else if (req.query.u) {
            // Direct (non-HLS) stream URL — continuous, never cached.
            sourceUrl = decodeProxyUrl(req.query.u);
        } else {
            return res.status(400).end();
        }

        // HLS segment: serve from cache (Range-sliceable, always a full 200). On a
        // miss, fetch it through the SERIAL upstream gate with PRIORITY — never open a
        // competing connection (that's what the provider aborts under max_connections=1).
        // fetchSegToCache dedups with any in-flight prefetch, so we just await it.
        if (segId) {
            const activeRt = activeChannelUrl ? channels.get(activeChannelUrl) : null;
            const activeOwnsSegment = activeRt && (activeRt.isMaster || activeRt.byId.has(segId));
            // Cached segment reads still count as activity for the current player.
            // Late reads from a previously selected channel must not keep the new one alive.
            if (activeOwnsSegment) {
                activeRt.lastPlayerAt = Date.now();
                activeRt.lastSegmentAt = Date.now();
                activeRt.segmentRequests++;
                const segmentIdx = activeRt.segs.findIndex(segment => segment.id === segId);
                if (segmentIdx >= 0) {
                    const requestedSeq = activeRt.seqBase + segmentIdx;
                    activeRt.lastRequestedSeq = Math.max(activeRt.lastRequestedSeq ?? requestedSeq, requestedSeq);
                }
                queueRuntimePrefetch(activeRt);
            }
            const cached = getSegFromCache(segId);
            if (cached) return sendCachedSeg(req, res, cached, segId, "HIT");

            if (!activeOwnsSegment) {
                // A late request from the previous channel may still arrive after
                // zapping. A cached response is harmless; reopening that old upstream
                // segment is not, because the provider only permits one connection.
                console.warn(`[SEG STALE] ${getSegmentLabel(sourceUrl)} refresh playlist`);
                return res.status(410).end();
            }
            const wasInflight = segInflight.has(segId);
            const entry = await fetchSegForPlayer(segId);
            if (entry) return sendCachedSeg(req, res, entry, segId, wasInflight ? "WAIT" : "MISS");
            return res.status(502).end();
        }

        // Direct (non-HLS) continuous stream: straight passthrough (its own single
        // connection, no prefetch competing).
        if (activeChannelUrl) {
            const rt = channels.get(activeChannelUrl);
            if (rt?.running) stopPoller(rt, "direct-stream");
            activeChannelUrl = null;
        }
        await withUpstream(() => streamSegment(req, res, sourceUrl, segId, started), true);
    } catch (err) {
        console.error(`[SEG ERROR] ${getSegmentLabel(sourceUrl)} ${err.message}`);
        if (!res.headersSent) res.status(502).end();
        else if (!res.destroyed) res.destroy(err);
    }
});

function sendCachedSeg(req, res, entry, segId, label) {
    const buf = entry.buffer;
    const total = buf.length;
    Object.entries(entry.headers).forEach(([k, v]) => res.setHeader(k, v));
    res.setHeader("Content-Type", entry.headers["content-type"] || "video/mp2t");
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("X-Kronos-Relay", "1");
    res.setHeader("X-Kronos-Cache", label);
    const name = getSegmentLabel(segRegistry.get(segId) || segId);

    const range = req.headers.range;
    if (range) {
        const m = String(range).match(/bytes=(\d*)-(\d*)/);
        if (m) {
            let start = m[1] === "" ? 0 : Number(m[1]);
            let end = m[2] === "" ? total - 1 : Number(m[2]);
            if (!Number.isFinite(start)) start = 0;
            if (!Number.isFinite(end) || end >= total) end = total - 1;
            if (start > end || start >= total) {
                res.status(416);
                res.setHeader("Content-Range", `bytes */${total}`);
                return res.end();
            }
            const chunk = buf.subarray(start, end + 1);
            res.status(206);
            res.setHeader("Content-Range", `bytes ${start}-${end}/${total}`);
            res.setHeader("Content-Length", chunk.length);
            console.log(`[SEG ${label}] ${name} range=${start}-${end}/${total}`);
            return res.end(chunk);
        }
    }

    res.setHeader("Content-Length", total);
    console.log(`[SEG ${label}] ${name} bytes=${formatBytes(total)}`);
    return res.end(buf);
}

// Stream a segment straight through, caching it (by segId) on the way if full 200.
async function streamSegment(req, res, sourceUrl, segId, started) {
    const headers = { "User-Agent": UPSTREAM_UA, "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive" };
    if (req.headers.range) headers.Range = req.headers.range;

    const response = await axios.get(sourceUrl, {
        responseType: "stream",
        timeout: SEG_REQUEST_TIMEOUT,
        maxRedirects: 5,
        headers,
        decompress: false,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        validateStatus: s => s >= 200 && s < 400
    });

    const upstream = response.data;
    ["content-type", "content-length", "content-range", "accept-ranges",
     "last-modified", "etag", "cache-control", "expires"].forEach(h => {
        if (response.headers[h]) res.setHeader(h, response.headers[h]);
    });
    res.setHeader("X-Kronos-Relay", "1");
    res.setHeader("X-Kronos-Cache", "MISS");
    res.status(response.status);

    // Only cache full 200 HLS segments (segId present, no Range).
    const collect = (segId && response.status === 200 && !req.headers.range);
    const chunks = collect ? [] : null;
    let bytes = 0;
    let overflow = false;

    upstream.on("data", chunk => {
        bytes += chunk.length;
        if (chunks && !overflow) {
            if (bytes > SEGMENT_CACHE_MAX_ITEM_BYTES) { overflow = true; chunks.length = 0; }
            else chunks.push(chunk);
        }
    });

    res.on("finish", () => {
        const ms = Date.now() - started;
        if (chunks && !overflow && chunks.length) storeSeg(segId, Buffer.concat(chunks), response.headers, 200);
        const slow = ms >= SLOW_SEGMENT_MS ? " SLOW" : "";
        console.log(`[SEG] ${getSegmentLabel(sourceUrl)} status=${response.status} bytes=${formatBytes(bytes)} time=${ms}ms speed=${formatSpeed(bytes, ms)}${slow}`);
    });

    res.on("close", () => { if (!res.writableEnded && upstream?.destroy) upstream.destroy(); });
    await new Promise(resolve => {
        let settled = false;
        const done = () => {
            if (settled) return;
            settled = true;
            resolve();
        };
        res.once("finish", done);
        res.once("close", done);
        upstream.on("error", err => {
            if (!res.headersSent) res.status(502).end();
            else res.destroy(err);
            done();
        });
        upstream.pipe(res);
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream + stats + debug
// ─────────────────────────────────────────────────────────────────────────────
app.get("/:base64Config/stream/:type/:id.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        if (!ch) return res.status(404).json({ streams: [] });
        res.json({ streams: [buildStream(ch, getPublicHost(req), configKey)] });
    } catch (err) {
        console.error("[STREAM ERROR]", err.message);
        res.status(500).json({ streams: [] });
    }
});

app.get("/:base64Config/stats", (req, res) => {
    res.json({
        version: RELEASE_VERSION,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        cache: {
            channels: Object.values(memoryCache.channelItems).reduce((s, l) => s + l.length, 0),
            epgMaps: Object.keys(memoryCache.epgData).length,
            logos: Object.keys(memoryCache.logoData).length,
            segments: segCache.size,
            segmentBytes: segCacheBytes,
            segmentBytesHuman: formatBytes(segCacheBytes),
            segRegistry: segRegistry.size,
            prefetchActive,
            prefetchQueue: prefetchQueue.length
        },
        channels: [...channels.values()].map(rt => ({
            label: rt.label, depth: rt.segs.length, seqBase: rt.seqBase, generation: rt.generation,
            running: rt.running, isMaster: rt.isMaster, idleMs: Date.now() - rt.lastPlayerAt,
            servedEdge: rt.servedEdge, knownEdge: rt.seqBase + rt.segs.length - 1,
            warmRun: getPlaybackWarmRun(rt).count, primedOnce: rt.primedOnce,
            manifestIdleMs: rt.lastManifestAt ? Date.now() - rt.lastManifestAt : null,
            manifestErrorIdleMs: rt.lastManifestErrorAt ? Date.now() - rt.lastManifestErrorAt : null,
            lastPollStatus: rt.lastPollStatus, pollFailures: rt.pollFailures,
            segmentIdleMs: rt.lastSegmentAt ? Date.now() - rt.lastSegmentAt : null,
            successfulSegmentFetchIdleMs: rt.lastSuccessfulSegmentFetchAt ? Date.now() - rt.lastSuccessfulSegmentFetchAt : null,
            segmentAuthFailures: rt.segmentAuthFailures || 0,
            segmentAuthFailureMs: rt.firstSegmentAuthFailureAt ? Date.now() - rt.firstSegmentAuthFailureAt : null,
            lastSegmentRecoveryIdleMs: rt.lastSegmentRecoveryAt ? Date.now() - rt.lastSegmentRecoveryAt : null,
            segmentRequests: rt.segmentRequests, lastRequestedSeq: rt.lastRequestedSeq
        })),
        activeChannel: activeChannelUrl ? (channels.get(activeChannelUrl)?.label || "?") : null
    });
});

app.get("/:base64Config/debug", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        res.json({
            version: RELEASE_VERSION,
            mode: "buffered-hls-relay",
            config: {
                hasUrl: !!config.u,
                hasMultiLists: Array.isArray(config.l),
                listCount: getConfiguredLists(config).length,
                groupMode: config.gm,
                selectedGroups: config.g
            },
            cache: {
                channelCount: channels.length,
                lastUpdate: memoryCache.lastUpdate[configKey],
                isUpdating: memoryCache.isUpdating[configKey],
                epg: memoryCache.epgMatchStats[configKey] || null,
                epgFetch: config.e ? {
                    ...(memoryCache.epgStatus[config.e] || { state: "idle" }),
                    inflight: !!memoryCache.epgInflight[config.e]
                } : null
            },
            sampleChannels: channels.slice(0, 5).map(c => ({
                id: c.id, name: c.name, group: c.group, sourceName: c.sourceName,
                hasUrl: !!c.url, hasLogo: !!c.logo, hasEpg: !!c.description, epgId: c.epgId
            })),
            groups: [...new Set(channels.map(c => c.group))].slice(0, 50),
            constants: {
                ADDON_TYPE, RELEASE_VERSION, HLS_REQUEST_TIMEOUT, SEG_REQUEST_TIMEOUT,
                LIVE_DELAY_SEGMENTS, MIN_START_SEGMENTS, MIN_VISIBLE_SEGMENTS, STARTUP_REAL_SEGMENTS, STARTUP_REAL_WAIT_MS,
                PRIME_WAIT_MS, MANIFEST_TYPE_WAIT_MS, RETAIN_SEGMENTS, SERVE_SEGMENTS, PREFETCH_WINDOW_SEGMENTS,
                SEGMENT_AUTH_FAILURE_LIMIT, SEGMENT_AUTH_FAILURE_MIN_MS, SEGMENT_AUTH_RECOVERY_COOLDOWN_MS,
                MANIFEST_FORBIDDEN_BACKOFF_MS, POLL_ERROR_BACKOFF_MS, POLL_MIN_MS, POLL_MAX_MS
            }
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
    console.log("\n" + "=".repeat(60));
    console.log(`K.R.O.N.O.S. ${RELEASE_VERSION} - buffered HLS relay`);
    console.log("=".repeat(60));
    console.log(`🌐 http://0.0.0.0:${PORT}`);
    console.log(`📦 Node ${process.version}`);
    console.log(`🔁 manifest retryWindow=${MANIFEST_RETRY_WINDOW_MS / 1000}s delay=${MANIFEST_RETRY_DELAY_MS / 1000}s`);
    console.log(`⛔ manifest forbiddenBackoff=${MANIFEST_FORBIDDEN_BACKOFF_MS / 1000}s pollErrorBackoff=${POLL_ERROR_BACKOFF_MS / 1000}s poll=${POLL_MIN_MS}-${POLL_MAX_MS}ms`);
    console.log(`📋 playlist retryWindow=${PLAYLIST_RETRY_WINDOW_MS / 1000}s delay=${PLAYLIST_RETRY_DELAY_MS / 1000}s`);
    console.log(`⏩ prefetch concurrency=${SEGMENT_PREFETCH_CONCURRENCY} cacheTTL=${SEGMENT_CACHE_TTL / 1000}s`);
    console.log(`🩹 segment playerRetry=${SEGMENT_PLAYER_RETRIES} delay=${SEGMENT_PLAYER_RETRY_DELAY_MS}ms`);
    console.log(`🔐 segment authRecovery=${SEGMENT_AUTH_FAILURE_LIMIT}/${SEGMENT_AUTH_FAILURE_MIN_MS / 1000}s cooldown=${SEGMENT_AUTH_RECOVERY_COOLDOWN_MS / 1000}s`);
    console.log(`🪟 buffer: retain/serve=${RETAIN_SEGMENTS}/${SERVE_SEGMENTS} liveDelay=${LIVE_DELAY_SEGMENTS}segs prime=${MIN_START_SEGMENTS}segs startup=${STARTUP_REAL_SEGMENTS}segs/${STARTUP_REAL_WAIT_MS / 1000}s prefetchWindow=${PREFETCH_WINDOW_SEGMENTS} wait=${PRIME_WAIT_MS / 1000}s typeWait=${MANIFEST_TYPE_WAIT_MS / 1000}s minVisible=${MIN_VISIBLE_SEGMENTS} idleStop=${POLLER_IDLE_STOP_MS / 1000}s`);
    console.log(`📺 epg timezone=${EPG_TIME_ZONE} timeout=${EPG_REQUEST_TIMEOUT / 1000}s firstWait=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s retryDelay=${EPG_RETRY_DELAY_MS / 1000}s cacheTTL=${EPG_CACHE_TTL / 1000}s refresh=${EPG_REFRESH_INTERVAL_MS / 1000}s preload=${EPG_PRELOAD_URL ? "on" : "off"} startupWatch=${EPG_STARTUP_WATCH_MS / 1000}s`);
    console.log(`📚 catalog preload=${CATALOG_PRELOAD_CONFIGS.length}`);
    console.log("=".repeat(60) + "\n");
    startEPGStartupPreload();
    startCatalogStartupPreload();
});
