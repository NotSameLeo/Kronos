const express = require("express");
const axios = require("axios");
const sax = require("sax");
const zlib = require("zlib");
const { Readable } = require("stream");
const http = require("http");
const https = require("https");
const path = require("path");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 7000;

const LOG_TIME_ZONE = process.env.LOG_TIME_ZONE || process.env.EPG_TIME_ZONE || "Europe/Rome";
const logTimeFormatter = new Intl.DateTimeFormat("it-IT", {
    timeZone: LOG_TIME_ZONE,
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
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
const RELEASE_VERSION = "3.3.9";
const ADDON_TYPE = "tv";
const PLAYBACK_MODE = "uhf-direct-relay";
const CATALOG_TTL = 30 * 60 * 1000;
const EPG_CACHE_TTL = Number(process.env.EPG_CACHE_TTL || 6 * 60 * 60 * 1000);
const EPG_REQUEST_TIMEOUT = Number(process.env.EPG_REQUEST_TIMEOUT || 20000);
const EPG_MAX_BYTES = Number(process.env.EPG_MAX_BYTES || 160 * 1024 * 1024);
const EPG_RETRY_DELAY_MS = Number(process.env.EPG_RETRY_DELAY_MS || 15000);
const EPG_FIRST_CATALOG_WAIT_MS = Number(process.env.EPG_FIRST_CATALOG_WAIT_MS || 0);
const EPG_REFRESH_INTERVAL_MS = Number(process.env.EPG_REFRESH_INTERVAL_MS || EPG_CACHE_TTL);
const DEFAULT_EPG_PRELOAD_URL = "https://iptv-epg.org/files/epg-it.xml.gz";
const EPG_PRELOAD_URL = String(process.env.EPG_PRELOAD_URL || DEFAULT_EPG_PRELOAD_URL).trim();
const EPG_PRELOAD_URLS = parseUrlList(process.env.EPG_PRELOAD_URLS || EPG_PRELOAD_URL);
const EPG_STARTUP_WATCH_MS = Number(process.env.EPG_STARTUP_WATCH_MS || 30000);
const CATALOG_PRELOAD_CONFIGS = parseCatalogPreloadConfigs(process.env.CATALOG_PRELOAD_CONFIGS || process.env.KRONOS_PRELOAD_CONFIGS || "");
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 10000);
const HLS_UPSTREAM_RETRIES = Math.max(0, Number(process.env.HLS_UPSTREAM_RETRIES || 1));
const HLS_UPSTREAM_RETRY_DELAY_MS = Math.max(0, Number(process.env.HLS_UPSTREAM_RETRY_DELAY_MS || 250));
const HLS_LIVE_MIN_VISIBLE_SEGMENTS = Math.max(1, Number(process.env.HLS_LIVE_MIN_VISIBLE_SEGMENTS || 2));
const HLS_LIVE_HOLDBACK_SEGMENTS = Math.max(0, Number(process.env.HLS_LIVE_HOLDBACK_SEGMENTS ?? 1));
const HLS_LIVE_MIN_SEGMENTS = Math.max(
    HLS_LIVE_MIN_VISIBLE_SEGMENTS + HLS_LIVE_HOLDBACK_SEGMENTS,
    Number(process.env.HLS_LIVE_MIN_SEGMENTS || 3)
);
const HLS_LIVE_WARMUP_WAIT_MS = Math.max(0, Number(process.env.HLS_LIVE_WARMUP_WAIT_MS || 35000));
const HLS_LIVE_WARMUP_POLL_MS = Math.max(250, Number(process.env.HLS_LIVE_WARMUP_POLL_MS || 2000));
const ACTIVE_STREAM_TTL_MS = Math.max(0, Number(process.env.ACTIVE_STREAM_TTL_MS || 120000));
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);
const SEGMENT_UPSTREAM_CONCURRENCY = Math.max(1, Number(process.env.SEGMENT_UPSTREAM_CONCURRENCY || 2));
const SEGMENT_UPSTREAM_RETRIES = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRIES || 1));
const SEGMENT_UPSTREAM_RETRY_DELAY_MS = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRY_DELAY_MS || 250));
const HLS_STALE_MANIFEST_TTL_MS = Math.max(0, Number(process.env.HLS_STALE_MANIFEST_TTL_MS || 90000));
const HLS_FORBIDDEN_BACKOFF_MS = Math.max(0, Number(process.env.HLS_FORBIDDEN_BACKOFF_MS || 15000));
const UPSTREAM_KEEPALIVE_MAX_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_SOCKETS || 8));
const UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS || 4));
const UPSTREAM_KEEPALIVE_MS = Math.max(1000, Number(process.env.UPSTREAM_KEEPALIVE_MS || 60000));
const PLAYLIST_REQUEST_TIMEOUT = Number(process.env.PLAYLIST_REQUEST_TIMEOUT || 20000);
const PLAYLIST_RETRY_WINDOW_MS = Number(process.env.PLAYLIST_RETRY_WINDOW_MS || 30000);
const PLAYLIST_RETRY_DELAY_MS = Number(process.env.PLAYLIST_RETRY_DELAY_MS || 2000);
const EPG_TIME_ZONE = process.env.EPG_TIME_ZONE || "Europe/Rome";

const SLOW_SEGMENT_MS = Number(process.env.SLOW_SEGMENT_MS || 4000);

const upstreamHttpAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: UPSTREAM_KEEPALIVE_MS,
    maxSockets: UPSTREAM_KEEPALIVE_MAX_SOCKETS,
    maxFreeSockets: UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS,
    scheduling: "lifo"
});
const upstreamHttpsAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: UPSTREAM_KEEPALIVE_MS,
    maxSockets: UPSTREAM_KEEPALIVE_MAX_SOCKETS,
    maxFreeSockets: UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS,
    scheduling: "lifo"
});

function upstreamAgentOptions() {
    return { httpAgent: upstreamHttpAgent, httpsAgent: upstreamHttpsAgent };
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory catalog, EPG and logo caches. Playback is intentionally stateless.
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

function parseUrlList(raw) {
    const urls = String(raw || "")
        .split(/[,\s]+/)
        .map(value => value.trim())
        .filter(Boolean);
    return [...new Set(urls)];
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

function deriveXtreamEpgUrl(sourceUrl) {
    try {
        const parsed = new URL(sourceUrl);
        const username = parsed.searchParams.get("username");
        const password = parsed.searchParams.get("password");
        if (!username || !password) return "";
        const epg = new URL("/xmltv.php", parsed.origin);
        epg.searchParams.set("username", username);
        epg.searchParams.set("password", password);
        return epg.toString();
    } catch {
        return "";
    }
}

function isLegacyGuideProxyUrl(value) {
    try {
        const parsed = new URL(value);
        return /\/guide\.gzip$/i.test(parsed.pathname);
    } catch {
        return false;
    }
}

function getEffectiveEpgUrl(config, lists = []) {
    const configured = String(config?.e || "").trim();
    const derived = lists.map(list => deriveXtreamEpgUrl(list.url)).filter(Boolean);
    if (configured && !isLegacyGuideProxyUrl(configured)) return configured;
    if (EPG_PRELOAD_URLS.length) return EPG_PRELOAD_URLS[0];
    if (isLegacyGuideProxyUrl(configured) && derived.length) return derived[0];
    if (!configured && derived.length) return derived[0];
    return configured;
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
function makeAbortController(res) {
    const controller = new AbortController();
    res.on("close", () => {
        if (!res.writableEnded) controller.abort();
    });
    return controller;
}

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

function getSegmentQueueKey(sourceUrl) {
    try {
        const u = new URL(sourceUrl);
        const dir = u.pathname.replace(/\/[^/]*$/, "/");
        return `${u.protocol}//${u.host}${dir}`;
    } catch {
        return String(sourceUrl || "").split("?")[0].replace(/\/[^/]*$/, "/");
    }
}

function getErrorStatus(err) {
    const status = Number(err?.response?.status || err?.status || 0);
    return Number.isFinite(status) && status > 0 ? status : null;
}

function isRetryableSegmentError(err) {
    const status = getErrorStatus(err);
    if ([408, 425, 429, 500, 502, 503, 504].includes(status)) return true;
    return ["ECONNRESET", "ECONNABORTED", "ETIMEDOUT", "EPIPE"].includes(err?.code);
}

function isRetryableHlsError(err) {
    const status = getErrorStatus(err);
    if ([408, 425, 429, 500, 502, 503, 504].includes(status)) return true;
    return ["ECONNRESET", "ECONNABORTED", "ETIMEDOUT", "EPIPE"].includes(err?.code)
        || /timeout/i.test(String(err?.message || ""));
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
    if (!EPG_PRELOAD_URLS.length) return;
    console.log(`[EPG PRELOAD] urls=${EPG_PRELOAD_URLS.length}`);
    EPG_PRELOAD_URLS.forEach(epgUrl => startEPGBackgroundRefresh(epgUrl, "startup"));

    if (EPG_STARTUP_WATCH_MS <= 0) return;
    const started = Date.now();
    const timer = setInterval(() => {
        const pending = EPG_PRELOAD_URLS.filter(epgUrl => !getCachedEPG(epgUrl));
        if (!pending.length || Date.now() - started >= EPG_STARTUP_WATCH_MS) {
            clearInterval(timer);
            return;
        }
        pending.forEach(epgUrl => startEPGBackgroundRefresh(epgUrl, "startup-watch"));
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
            ...upstreamAgentOptions(),
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
    const description = stripRepeatedTitlePrefix(programme.desc || "", programme.title).slice(0, 500).trim().replace(/\.+$/, "");
    return nbsp(`${label}: ${String(programme.title || "").toUpperCase()} (${formatTime(programme.start)} - ${formatTime(programme.stop)}) | Trama: ${description}`);
}

function stripRepeatedTitlePrefix(description, title) {
    const text = String(description || "").trim();
    const candidates = getEpgTitlePrefixCandidates(title);
    if (!text || !candidates.length) return text;

    for (const candidate of candidates) {
        const cleaned = stripComparablePrefix(text, candidate);
        if (cleaned) return cleaned;
    }
    return text;
}

function getEpgTitlePrefixCandidates(title) {
    const raw = String(title || "").trim();
    if (!raw) return [];
    const values = [raw];
    for (const part of raw.split(/\s[-–—:|]\s/u)) {
        const text = part.trim();
        if (epgComparableKey(text).length >= 8) values.push(text);
    }
    return [...new Map(values
        .map(value => [epgComparableKey(value), value])
        .filter(([key]) => key.length >= 8)
    ).values()].sort((a, b) => epgComparableKey(b).length - epgComparableKey(a).length);
}

function stripComparablePrefix(text, candidate) {
    const target = epgComparableKey(candidate);
    let seen = "";
    for (let i = 0; i < text.length; i++) {
        const part = epgComparableKey(text[i]);
        if (!part) continue;
        seen += part;
        if (seen.length < target.length) {
            if (!target.startsWith(seen)) break;
            continue;
        }
        if (seen === target) {
            const cleaned = text.slice(i + 1).replace(/^[\s:;.,\-–—|]+/u, "").trim();
            if (!cleaned) return null;
            // If the remainder starts lowercase, we probably stripped only part of
            // a longer title/sentence (e.g. "Tutto quello che non ti ho detto" ->
            // "ti ho detto. - ..."). Keeping the original is less harmful.
            if (/^\p{Ll}/u.test(cleaned)) return null;
            return cleaned;
        }
        break;
    }
    return null;
}

function epgComparableKey(value) {
    return String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "");
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
            const response = await axios.get(sourceUrl, {
                timeout: Math.max(1, Math.min(options.timeout || PLAYLIST_REQUEST_TIMEOUT, deadline - Date.now())),
                ...upstreamAgentOptions(),
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus: s => s >= 200 && s < 300
            });
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
        const lists = getConfiguredLists(config);
        const epgUrl = getEffectiveEpgUrl(config, lists);
        const effectiveConfig = epgUrl === config.e ? config : { ...config, e: epgUrl };
        if (config.e && epgUrl && epgUrl !== config.e) console.log(`[EPG RESOLVE] configured=${config.e} effective=${epgUrl}`);
        subscribeConfigToEPG(configKey, effectiveConfig);
        const epgPromise = epgUrl
            ? ensureEPGRefresh(epgUrl).catch(err => {
                console.error("[EPG REFRESH]", err.message);
                return null;
            })
            : null;
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
        let epgData = getCachedEPG(epgUrl);
        if (epgUrl && !epgData && epgPromise && EPG_FIRST_CATALOG_WAIT_MS > 0) {
            console.log(`[EPG WAIT] upTo=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s`);
            epgData = await Promise.race([
                epgPromise,
                sleep(EPG_FIRST_CATALOG_WAIT_MS).then(() => null)
            ]);
            if (!epgData) epgData = getCachedEPG(epgUrl);
        }
        const { channels, matched: epgMatched } = attachEPGToChannels(rawChannels, epgData);
        if (epgUrl && !epgData) console.log("[EPG PENDING] catalog served without guide; background refresh active");

        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.epgMatchStats[configKey] = {
            matched: epgMatched,
            total: channels.length,
            feedChannels: epgData?.channelCount || 0
        };
        memoryCache.lastUpdate[configKey] = Date.now();
        if (epgUrl) console.log(`[EPG MATCH] matched=${epgMatched}/${channels.length} feedChannels=${epgData?.channelCount || 0}`);
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
        fetchAndProcessChannels(configKey, config).catch(err => console.error("[CATALOG REFRESH]", err.message));
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

// ─────────────────────────────────────────────────────────────────────────────
// HLS relay
// ─────────────────────────────────────────────────────────────────────────────
const hlsLastGoodManifests = new Map();
const hlsManifestBackoffs = new Map();

function rememberGoodHLS(sourceUrl, result) {
    if (!HLS_STALE_MANIFEST_TTL_MS || !result?.info) return;
    if (!result.info.isMaster && result.info.isLive && result.info.segmentCount < HLS_LIVE_MIN_VISIBLE_SEGMENTS) return;
    hlsLastGoodManifests.set(hashKey(sourceUrl), { ...result, storedAt: Date.now() });
    hlsManifestBackoffs.delete(hashKey(sourceUrl));
}

function isStaleHlsEligibleError(err) {
    const status = getErrorStatus(err);
    if (err?.code === "HLS_BACKOFF") return true;
    if ([403, 408, 425, 429, 500, 502, 503, 504].includes(status)) return true;
    return ["ECONNRESET", "ECONNABORTED", "ETIMEDOUT", "EPIPE"].includes(err?.code)
        || /timeout/i.test(String(err?.message || ""));
}

function getStaleHLS(sourceUrl, label, err, warmupMs = 0, warmupPolls = 0) {
    if (!HLS_STALE_MANIFEST_TTL_MS || !isStaleHlsEligibleError(err)) return null;
    const key = hashKey(sourceUrl);
    const entry = hlsLastGoodManifests.get(key);
    if (!entry) return null;
    const age = Date.now() - entry.storedAt;
    if (age > HLS_STALE_MANIFEST_TTL_MS) {
        hlsLastGoodManifests.delete(key);
        return null;
    }
    const status = getErrorStatus(err) || err?.code || "n/a";
    console.warn(`[HLS STALE MANIFEST] channel="${label}" age=${age}ms status=${status} reason=${err.message}`);
    return {
        ...entry,
        attempts: 0,
        warmupMs,
        warmupPolls,
        staleManifestMs: age,
        staleManifestReason: status
    };
}

function noteHlsBackoff(sourceUrl, err) {
    if (!HLS_FORBIDDEN_BACKOFF_MS || getErrorStatus(err) !== 403) return;
    hlsManifestBackoffs.set(hashKey(sourceUrl), { until: Date.now() + HLS_FORBIDDEN_BACKOFF_MS, status: 403 });
}

function assertNoHlsBackoff(sourceUrl) {
    if (!HLS_FORBIDDEN_BACKOFF_MS) return;
    const key = hashKey(sourceUrl);
    const backoff = hlsManifestBackoffs.get(key);
    if (!backoff) return;
    if (Date.now() >= backoff.until) {
        hlsManifestBackoffs.delete(key);
        return;
    }
    const err = new Error(`Upstream HLS manifest temporarily backed off after status ${backoff.status}`);
    err.code = "HLS_BACKOFF";
    err.status = backoff.status;
    throw err;
}

// Fetch the manifest requested by the player without inventing live state. A
// short bounded retry absorbs transient Xtream 5xx/timeouts without hiding a
// genuinely broken upstream behind synthetic playlists.
async function fetchUpstreamHLS(sourceUrl, label = "stream", signal = null) {
    assertNoHlsBackoff(sourceUrl);
    let lastErr = null;
    for (let attempt = 1; attempt <= HLS_UPSTREAM_RETRIES + 1; attempt++) {
        try {
            const r = await axios.get(sourceUrl, {
                timeout: HLS_REQUEST_TIMEOUT,
                signal,
                ...upstreamAgentOptions(),
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "application/x-mpegURL, application/vnd.apple.mpegurl, audio/mpegurl, text/plain, */*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus: s => s >= 200 && s < 300
            });
            const finalUrl = r.request?.res?.responseUrl || sourceUrl;
            const text = String(r.data || "").trim();
            if (!text.startsWith("#EXTM3U")) {
                throw new Error(`Invalid HLS manifest from upstream for ${label}`);
            }
            const result = { data: r.data, finalUrl, info: analyzeHLS(text), attempts: attempt };
            rememberGoodHLS(sourceUrl, result);
            return result;
        } catch (err) {
            lastErr = err;
            if (signal?.aborted || !isRetryableHlsError(err) || attempt > HLS_UPSTREAM_RETRIES) {
                noteHlsBackoff(sourceUrl, err);
                throw err;
            }
            console.warn(`[HLS RETRY] channel="${label}" attempt=${attempt} reason=${err.message}`);
            if (HLS_UPSTREAM_RETRY_DELAY_MS > 0) await sleep(HLS_UPSTREAM_RETRY_DELAY_MS);
        }
    }
    throw lastErr || new Error(`HLS manifest fetch failed for ${label}`);
}

function needsLiveWarmup(info) {
    if (!info || info.isMaster || !info.isLive) return false;
    if (info.segmentCount >= HLS_LIVE_MIN_SEGMENTS) return false;
    return (info.mediaSequence ?? 0) <= 1;
}

async function fetchPlayableHLS(sourceUrl, label = "stream", signal = null) {
    const started = Date.now();
    let result;
    try {
        result = await fetchUpstreamHLS(sourceUrl, label, signal);
    } catch (err) {
        const stale = getStaleHLS(sourceUrl, label, err);
        if (stale) return stale;
        throw err;
    }
    if (!needsLiveWarmup(result.info) || HLS_LIVE_WARMUP_WAIT_MS <= 0) return { ...result, warmupMs: 0, warmupPolls: 0 };

    let polls = 0;
    console.warn(`[HLS WARMUP] channel="${label}" media=${result.info.segmentCount}/${HLS_LIVE_MIN_SEGMENTS} seq=${result.info.mediaSequence ?? "n/a"} target=${result.info.targetDuration ?? "n/a"}`);
    while (!signal?.aborted && Date.now() - started < HLS_LIVE_WARMUP_WAIT_MS) {
        polls++;
        await sleep(HLS_LIVE_WARMUP_POLL_MS);
        try {
            result = await fetchUpstreamHLS(sourceUrl, label, signal);
        } catch (err) {
            const stale = getStaleHLS(sourceUrl, label, err, Date.now() - started, polls);
            if (stale) return stale;
            throw err;
        }
        if (!needsLiveWarmup(result.info)) {
            const warmupMs = Date.now() - started;
            console.log(`[HLS WARMUP READY] channel="${label}" media=${result.info.segmentCount} seq=${result.info.mediaSequence ?? "n/a"} wait=${warmupMs}ms polls=${polls}`);
            return { ...result, warmupMs, warmupPolls: polls };
        }
    }

    console.warn(`[HLS WARMUP GIVEUP] channel="${label}" media=${result.info.segmentCount}/${HLS_LIVE_MIN_SEGMENTS} seq=${result.info.mediaSequence ?? "n/a"} wait=${Date.now() - started}ms polls=${polls}`);
    return { ...result, warmupMs: Date.now() - started, warmupPolls: polls };
}

const hlsWarmups = new Map();
const activeStreams = new Map();

async function fetchPlayableHLSShared(sourceUrl, label = "stream") {
    const key = hashKey(sourceUrl);
    const current = hlsWarmups.get(key);
    if (current) return await current;
    const promise = fetchPlayableHLS(sourceUrl, label);
    hlsWarmups.set(key, promise);
    try { return await promise; }
    finally { hlsWarmups.delete(key); }
}

function setActiveStream(configKey, streamKey, label) {
    if (!ACTIVE_STREAM_TTL_MS) return;
    activeStreams.set(configKey, { streamKey, label, updatedAt: Date.now() });
}

function isStaleStream(configKey, streamKey) {
    if (!ACTIVE_STREAM_TTL_MS || !streamKey) return false;
    const current = activeStreams.get(configKey);
    if (!current) return false;
    if (Date.now() - current.updatedAt > ACTIVE_STREAM_TTL_MS) {
        activeStreams.delete(configKey);
        return false;
    }
    return current.streamKey !== streamKey;
}

const HLS_SEGMENT_SCOPED_TAGS = [
    "#EXTINF",
    "#EXT-X-BITRATE",
    "#EXT-X-BYTERANGE",
    "#EXT-X-DISCONTINUITY",
    "#EXT-X-GAP",
    "#EXT-X-KEY",
    "#EXT-X-MAP",
    "#EXT-X-PROGRAM-DATE-TIME"
];

function isSegmentScopedHlsTag(line) {
    return HLS_SEGMENT_SCOPED_TAGS.some(prefix => line.startsWith(prefix));
}

function splitHlsMediaPlaylist(playlist) {
    const head = [];
    const items = [];
    let pending = [];
    let seenMedia = false;

    for (const line of String(playlist || "").split(/\r?\n/)) {
        const t = line.trim();
        if (!t) {
            if (seenMedia || pending.length) pending.push(line);
            else head.push(line);
            continue;
        }
        if (t.startsWith("#")) {
            if (isSegmentScopedHlsTag(t) || pending.length || seenMedia) pending.push(line);
            else head.push(line);
            continue;
        }
        items.push({ pre: pending, uri: line });
        pending = [];
        seenMedia = true;
    }

    return { head, items, trailer: pending };
}

function applyLiveHoldback(playlist, info) {
    const sourceMedia = info?.segmentCount ?? 0;
    if (!info || info.isMaster || !info.isLive || HLS_LIVE_HOLDBACK_SEGMENTS <= 0) {
        return { playlist, heldBack: 0, sourceMedia };
    }

    const parsed = splitHlsMediaPlaylist(playlist);
    const maxHoldback = Math.max(0, parsed.items.length - HLS_LIVE_MIN_VISIBLE_SEGMENTS);
    const heldBack = Math.min(HLS_LIVE_HOLDBACK_SEGMENTS, maxHoldback);
    if (heldBack <= 0) return { playlist, heldBack: 0, sourceMedia: parsed.items.length || sourceMedia };

    const kept = parsed.items.slice(0, parsed.items.length - heldBack);
    const lines = [...parsed.head];
    for (const item of kept) lines.push(...item.pre, item.uri);
    return {
        playlist: lines.join("\n").replace(/\n+$/g, "") + "\n",
        heldBack,
        sourceMedia: parsed.items.length
    };
}

// Transparent HLS relay: do not invent media-sequences, segment ids or cache
// state. This mirrors a normal IPTV player behind the server-side VPN: every
// refreshed manifest carries the current token-bearing segment URLs.
function rewriteHLSUrlsDirect(playlist, baseUrl, hostBase, configKey, streamKey, info = null) {
    const held = applyLiveHoldback(playlist, info);
    const queueKey = hashKey(baseUrl);
    const activeParam = streamKey ? `&a=${encodeURIComponent(streamKey)}` : "";
    const plUrl = abs => `${hostBase}/${configKey}/proxy/live.m3u8?u=${encodeProxyUrl(abs)}${activeParam}`;
    const segUrl = abs => `${hostBase}/${configKey}/proxy/seg?u=${encodeProxyUrl(abs)}&q=${queueKey}${activeParam}`;
    const pick = value => {
        const abs = toAbsoluteUrl(value, baseUrl);
        if (!isHttpUrl(abs)) return value;
        return isHlsUrl(abs) ? plUrl(abs) : segUrl(abs);
    };
    let mediaUrls = 0;
    let firstMedia = "";
    let lastMedia = "";

    const rewritten = String(held.playlist || "").split(/\r?\n/).map(line => {
        const t = line.trim();
        if (!t) return line;
        if (t.startsWith("#")) {
            return line.replace(/URI=("([^"]+)"|'([^']+)')/g, (_, quoted, d, s) => {
                const uri = d || s;
                const q = quoted.startsWith("'") ? "'" : '"';
                return `URI=${q}${pick(uri)}${q}`;
            });
        }
        mediaUrls++;
        if (!firstMedia) firstMedia = getSegmentLabel(toAbsoluteUrl(t, baseUrl));
        lastMedia = getSegmentLabel(toAbsoluteUrl(t, baseUrl));
        return pick(t);
    }).join("\n");

    return {
        rewritten,
        mediaUrls,
        upstreamMediaUrls: held.sourceMedia,
        heldBack: held.heldBack,
        firstMedia,
        lastMedia,
        queueKey
    };
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
            ...upstreamAgentOptions(),
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
            url: `${host}/${configKey}/proxy/live.m3u8?u=${encodeProxyUrl(channel.url)}&label=${encodeURIComponent(channel.name)}`,
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
// Playback — UHF-like direct HLS relay
// ─────────────────────────────────────────────────────────────────────────────
function setPlaylistHeaders(res) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
}

const segmentQueues = new Map();

function acquireSegmentSlot(key, signal) {
    const requestedAt = Date.now();
    let state = segmentQueues.get(key);
    if (!state) {
        state = { active: 0, waiters: [] };
        segmentQueues.set(key, state);
    }

    return new Promise((resolve, reject) => {
        let settled = false;
        let waiter = null;

        const cleanupAbort = () => signal?.removeEventListener?.("abort", onAbort);
        const release = () => {
            state.active = Math.max(0, state.active - 1);
            const next = state.waiters.shift();
            if (next) next();
            else if (state.active === 0) segmentQueues.delete(key);
        };
        const grant = () => {
            if (settled) return;
            settled = true;
            cleanupAbort();
            state.active++;
            resolve({ release, waitMs: Date.now() - requestedAt, queueSize: state.waiters.length });
        };
        const onAbort = () => {
            if (settled) return;
            settled = true;
            if (waiter) {
                const i = state.waiters.indexOf(waiter);
                if (i !== -1) state.waiters.splice(i, 1);
            }
            cleanupAbort();
            if (state.active === 0 && state.waiters.length === 0) segmentQueues.delete(key);
            reject(new Error("Segment request aborted"));
        };

        if (signal?.aborted) return onAbort();
        if (state.active < SEGMENT_UPSTREAM_CONCURRENCY) return grant();
        waiter = grant;
        state.waiters.push(waiter);
        signal?.addEventListener?.("abort", onAbort, { once: true });
    });
}

async function withSegmentSlot(queueKey, signal, fn) {
    const slot = await acquireSegmentSlot(queueKey, signal);
    try { return await fn(slot); }
    finally { slot.release(); }
}

app.get("/:base64Config/proxy/live.m3u8", async (req, res) => {
    const started = Date.now();
    const controller = makeAbortController(res);
    let label = "direct-hls";
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");
        const sourceUrl = decodeProxyUrl(req.query.u);
        label = String(req.query.label || "direct-hls");
        const streamKey = String(req.query.a || hashKey(sourceUrl));
        if (req.query.label) setActiveStream(configKey, streamKey, label);
        else if (isStaleStream(configKey, streamKey)) {
            console.warn(`[HLS STALE] channel="${label}" stream=${streamKey}`);
            return res.status(409).type("text/plain").send("Stale HLS stream\n");
        }
        const { data, finalUrl, info, attempts, warmupMs, warmupPolls, staleManifestMs, staleManifestReason } = await fetchPlayableHLSShared(sourceUrl, label);
        if (controller.signal.aborted) return;
        const { rewritten, mediaUrls, upstreamMediaUrls, heldBack, firstMedia, lastMedia, queueKey } = rewriteHLSUrlsDirect(data, finalUrl, getPublicHost(req), configKey, streamKey, info);
        setPlaylistHeaders(res);
        console.log(`[HLS DIRECT SERVE] channel="${label}" master=${info.isMaster ? 1 : 0} media=${mediaUrls}/${upstreamMediaUrls ?? mediaUrls} held=${heldBack || 0} seq=${info.mediaSequence ?? "n/a"} target=${info.targetDuration ?? "n/a"} first=${firstMedia || "n/a"} last=${lastMedia || "n/a"} q=${queueKey} attempts=${attempts} warmup=${warmupMs || 0}ms polls=${warmupPolls || 0} stale=${staleManifestMs || 0}ms staleReason=${staleManifestReason || "n/a"} time=${Date.now() - started}ms`);
        res.send(rewritten);
    } catch (err) {
        if (controller.signal.aborted) return;
        const timeout = err?.code === "ECONNABORTED" || /timeout/i.test(String(err?.message || ""));
        const status = timeout ? 504 : 502;
        console.error(`[HLS DIRECT ERROR] channel="${label}" status=${status} time=${Date.now() - started}ms reason=${err.message}`);
        if (!res.headersSent) res.status(status).type("text/plain").send("Upstream HLS manifest unavailable\n");
    }
});

app.get("/:base64Config/proxy/seg", async (req, res) => {
    const started = Date.now();
    const controller = makeAbortController(res);
    let sourceUrl = null;
    let queueKey = "";

    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).end();
        sourceUrl = decodeProxyUrl(req.query.u);
        const streamKey = String(req.query.a || "");
        if (streamKey && isStaleStream(configKey, streamKey)) {
            console.warn(`[SEG STALE] ${getSegmentLabel(sourceUrl)} stream=${streamKey}`);
            return res.status(409).end();
        }
        queueKey = req.query.q ? `${configKey}:${String(req.query.q)}` : getSegmentQueueKey(sourceUrl);
        await withSegmentSlot(queueKey, controller.signal, slot => streamSegment(req, res, sourceUrl, started, controller.signal, slot));
    } catch (err) {
        if (controller.signal.aborted) {
            console.warn(`[SEG ABORT] ${getSegmentLabel(sourceUrl)} queue=${queueKey || "n/a"} time=${Date.now() - started}ms`);
            return;
        }
        console.error(`[SEG ERROR] ${getSegmentLabel(sourceUrl)} ${err.message}`);
        if (!res.headersSent) res.status(502).end();
        else if (!res.destroyed) res.destroy(err);
    }
});

// Stream a segment straight through. Kronos must behave like a normal IPTV
// client behind the server-side VPN: no segment cache, no synthetic ids.
async function fetchSegmentResponse(sourceUrl, headers, signal = null) {
    let lastErr = null;
    for (let attempt = 1; attempt <= SEGMENT_UPSTREAM_RETRIES + 1; attempt++) {
        try {
            return await axios.get(sourceUrl, {
                responseType: "stream",
                timeout: SEG_REQUEST_TIMEOUT,
                signal,
                ...upstreamAgentOptions(),
                maxRedirects: 5,
                headers,
                decompress: false,
                maxBodyLength: Infinity,
                maxContentLength: Infinity,
                validateStatus: s => s >= 200 && s < 400
            });
        } catch (err) {
            lastErr = err;
            if (signal?.aborted || !isRetryableSegmentError(err) || attempt > SEGMENT_UPSTREAM_RETRIES) throw err;
            console.warn(`[SEG RETRY] ${getSegmentLabel(sourceUrl)} attempt=${attempt} reason=${err.message}`);
            if (SEGMENT_UPSTREAM_RETRY_DELAY_MS > 0) await sleep(SEGMENT_UPSTREAM_RETRY_DELAY_MS);
        }
    }
    throw lastErr || new Error("Segment fetch failed");
}

async function streamSegment(req, res, sourceUrl, started, signal = null, slot = null) {
    const headers = { "User-Agent": UPSTREAM_UA, "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive" };
    if (req.headers.range) headers.Range = req.headers.range;

    const response = await fetchSegmentResponse(sourceUrl, headers, signal);

    const upstream = response.data;
    ["content-type", "content-length", "content-range", "accept-ranges",
     "last-modified", "etag", "cache-control", "expires"].forEach(h => {
        if (response.headers[h]) res.setHeader(h, response.headers[h]);
    });
    res.setHeader("X-Kronos-Relay", "1");
    res.status(response.status);

    let bytes = 0;

    upstream.on("data", chunk => {
        bytes += chunk.length;
    });

    res.on("finish", () => {
        const ms = Date.now() - started;
        const waitMs = slot?.waitMs || 0;
        const slow = ms >= SLOW_SEGMENT_MS ? " SLOW" : "";
        console.log(`[SEG DIRECT] ${getSegmentLabel(sourceUrl)} status=${response.status} bytes=${formatBytes(bytes)} wait=${waitMs}ms time=${ms}ms speed=${formatSpeed(bytes, Math.max(1, ms - waitMs))}${slow}`);
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
        mode: PLAYBACK_MODE,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        cache: {
            channels: Object.values(memoryCache.channelItems).reduce((s, l) => s + l.length, 0),
            epgMaps: Object.keys(memoryCache.epgData).length,
            logos: Object.keys(memoryCache.logoData).length
        },
        upstream: {
            directRelay: true,
            segmentConcurrency: SEGMENT_UPSTREAM_CONCURRENCY,
            segmentQueues: segmentQueues.size,
            hlsWarmups: hlsWarmups.size,
            hlsLastGoodManifests: hlsLastGoodManifests.size,
            hlsManifestBackoffs: hlsManifestBackoffs.size,
            activeStreams: activeStreams.size
        }
    });
});

app.get("/:base64Config/debug", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        const effectiveConfig = memoryCache.configByKey[configKey] || config;
        const effectiveEpgUrl = effectiveConfig.e || "";
        res.json({
            version: RELEASE_VERSION,
            mode: PLAYBACK_MODE,
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
                epgFetch: effectiveEpgUrl ? {
                    url: effectiveEpgUrl,
                    ...(memoryCache.epgStatus[effectiveEpgUrl] || { state: "idle" }),
                    inflight: !!memoryCache.epgInflight[effectiveEpgUrl]
                } : null
            },
            sampleChannels: channels.slice(0, 5).map(c => ({
                id: c.id, name: c.name, group: c.group, sourceName: c.sourceName,
                hasUrl: !!c.url, hasLogo: !!c.logo, hasEpg: !!c.description, epgId: c.epgId
            })),
            groups: [...new Set(channels.map(c => c.group))].slice(0, 50),
            constants: {
                ADDON_TYPE, RELEASE_VERSION, PLAYBACK_MODE, HLS_REQUEST_TIMEOUT, SEG_REQUEST_TIMEOUT,
                HLS_UPSTREAM_RETRIES, HLS_UPSTREAM_RETRY_DELAY_MS,
                HLS_LIVE_MIN_SEGMENTS, HLS_LIVE_MIN_VISIBLE_SEGMENTS, HLS_LIVE_HOLDBACK_SEGMENTS,
                HLS_LIVE_WARMUP_WAIT_MS, HLS_LIVE_WARMUP_POLL_MS,
                HLS_STALE_MANIFEST_TTL_MS, HLS_FORBIDDEN_BACKOFF_MS,
                ACTIVE_STREAM_TTL_MS,
                SEGMENT_UPSTREAM_CONCURRENCY, SEGMENT_UPSTREAM_RETRIES, SEGMENT_UPSTREAM_RETRY_DELAY_MS,
                UPSTREAM_KEEPALIVE_MAX_SOCKETS, UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS, UPSTREAM_KEEPALIVE_MS,
                PLAYLIST_REQUEST_TIMEOUT, PLAYLIST_RETRY_WINDOW_MS, PLAYLIST_RETRY_DELAY_MS
            }
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
    console.log("\n" + "=".repeat(60));
    console.log(`K.R.O.N.O.S. ${RELEASE_VERSION} - UHF-like direct relay`);
    console.log("=".repeat(60));
    console.log(`🌐 http://0.0.0.0:${PORT}`);
    console.log(`📦 Node ${process.version}`);
    console.log(`🎬 playback=${PLAYBACK_MODE} noBuffer=1 noPrefetch=1 noSegmentCache=1`);
    console.log(`🔁 manifest directFetch=1 timeout=${HLS_REQUEST_TIMEOUT / 1000}s retries=${HLS_UPSTREAM_RETRIES} liveMin=${HLS_LIVE_MIN_SEGMENTS} visible=${HLS_LIVE_MIN_VISIBLE_SEGMENTS} holdback=${HLS_LIVE_HOLDBACK_SEGMENTS} warmupWait=${HLS_LIVE_WARMUP_WAIT_MS / 1000}s staleTTL=${HLS_STALE_MANIFEST_TTL_MS / 1000}s forbiddenBackoff=${HLS_FORBIDDEN_BACKOFF_MS / 1000}s activeTtl=${ACTIVE_STREAM_TTL_MS / 1000}s`);
    console.log(`🔌 upstream keepAlive=1 sockets=${UPSTREAM_KEEPALIVE_MAX_SOCKETS} free=${UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS} ms=${UPSTREAM_KEEPALIVE_MS}`);
    console.log(`📋 playlist retryWindow=${PLAYLIST_RETRY_WINDOW_MS / 1000}s delay=${PLAYLIST_RETRY_DELAY_MS / 1000}s`);
    console.log(`🎞️ segment timeout=${SEG_REQUEST_TIMEOUT / 1000}s rangeForward=1 upstreamConcurrency=${SEGMENT_UPSTREAM_CONCURRENCY} retries=${SEGMENT_UPSTREAM_RETRIES}`);
    console.log(`📺 epg timezone=${EPG_TIME_ZONE} timeout=${EPG_REQUEST_TIMEOUT / 1000}s firstWait=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s retryDelay=${EPG_RETRY_DELAY_MS / 1000}s cacheTTL=${EPG_CACHE_TTL / 1000}s refresh=${EPG_REFRESH_INTERVAL_MS / 1000}s preload=${EPG_PRELOAD_URLS.length} startupWatch=${EPG_STARTUP_WATCH_MS / 1000}s`);
    console.log(`📚 catalog preload=${CATALOG_PRELOAD_CONFIGS.length}`);
    console.log("=".repeat(60) + "\n");
    startEPGStartupPreload();
    startCatalogStartupPreload();
});
