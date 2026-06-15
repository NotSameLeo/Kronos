const express = require("express");
const axios = require("axios");
const sax = require("sax");
const zlib = require("zlib");
const { Readable } = require("stream");
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

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
const RELEASE_VERSION = "4.0.0";
const ADDON_TYPE = "tv";
const PLAYBACK_MODE = "ffmpeg-restream";
const CATALOG_TTL = 30 * 60 * 1000;
// Periodically re-fetch the preloaded (frontend) playlist so the catalog stays
// warm even with no requests, and so frontend changes are picked up. 0 disables.
const CATALOG_REFRESH_INTERVAL_MS = Math.max(0, Number(process.env.CATALOG_REFRESH_INTERVAL_MS || CATALOG_TTL));
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
const FRONTEND_PRELOAD_FILE = String(process.env.FRONTEND_PRELOAD_FILE || "").trim();
// Denylist of upstream playlists (substring match on the URL, e.g. an old host).
// Blocked playlists are filtered out everywhere — never fetched, never saved as
// the frontend preload, never shown — so a stale client that still references an
// old playlist can't resurrect it. Set via KRONOS_BLOCKED_PLAYLISTS (comma/space).
const BLOCKED_PLAYLISTS = String(process.env.KRONOS_BLOCKED_PLAYLISTS || "")
    .split(/[\s,]+/).map(s => s.trim().toLowerCase()).filter(Boolean);
function isBlockedPlaylist(url) {
    if (!url || !BLOCKED_PLAYLISTS.length) return false;
    const u = String(url).toLowerCase();
    return BLOCKED_PLAYLISTS.some(b => u.includes(b));
}
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 15000);
const HLS_UPSTREAM_RETRIES = Math.max(0, Number(process.env.HLS_UPSTREAM_RETRIES || 2));
const HLS_UPSTREAM_RETRY_DELAY_MS = Math.max(0, Number(process.env.HLS_UPSTREAM_RETRY_DELAY_MS || 1000));
const HLS_COLD_START_WAIT_MS = Math.max(0, Number(process.env.HLS_COLD_START_WAIT_MS || 25000));
const HLS_COLD_START_RETRY_MS = Math.max(250, Number(process.env.HLS_COLD_START_RETRY_MS || 1000));
const HLS_LIVE_MIN_VISIBLE_SEGMENTS = Math.max(1, Number(process.env.HLS_LIVE_MIN_VISIBLE_SEGMENTS || 2));
const HLS_LIVE_HOLDBACK_SEGMENTS = Math.max(0, Number(process.env.HLS_LIVE_HOLDBACK_SEGMENTS ?? 2));
const HLS_STALE_HOLDBACK_RELEASE_TARGETS = Math.max(1, Number(process.env.HLS_STALE_HOLDBACK_RELEASE_TARGETS || 1));
const HLS_START_OFFSET_TARGETS = Math.max(0, Number(process.env.HLS_START_OFFSET_TARGETS || 2));
const HLS_STREAM_PREFLIGHT = String(process.env.HLS_STREAM_PREFLIGHT ?? "1") !== "0";
const HLS_LIVE_MIN_SEGMENTS = Math.max(HLS_LIVE_MIN_VISIBLE_SEGMENTS, Number(process.env.HLS_LIVE_MIN_SEGMENTS || 3));
const HLS_LIVE_WARMUP_WAIT_MS = Math.max(0, Number(process.env.HLS_LIVE_WARMUP_WAIT_MS || 35000));
const HLS_LIVE_WARMUP_POLL_MS = Math.max(250, Number(process.env.HLS_LIVE_WARMUP_POLL_MS || 2000));
const ACTIVE_STREAM_TTL_MS = Math.max(0, Number(process.env.ACTIVE_STREAM_TTL_MS || 120000));
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);
const SEGMENT_UPSTREAM_CONCURRENCY = Math.max(1, Number(process.env.SEGMENT_UPSTREAM_CONCURRENCY || 2));
const SEGMENT_UPSTREAM_RETRIES = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRIES || 1));
const SEGMENT_UPSTREAM_RETRY_DELAY_MS = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRY_DELAY_MS || 250));
const HLS_STALE_MANIFEST_TTL_MS = Math.max(0, Number(process.env.HLS_STALE_MANIFEST_TTL_MS || 90000));
const HLS_STALE_LIVE_TARGETS = Math.max(0, Number(process.env.HLS_STALE_LIVE_TARGETS || 6));
const HLS_STALE_FORBIDDEN_TARGETS = Math.max(0, Number(process.env.HLS_STALE_FORBIDDEN_TARGETS || 6));
const HLS_FORBIDDEN_BACKOFF_MS = Math.max(0, Number(process.env.HLS_FORBIDDEN_BACKOFF_MS || 15000));
const HLS_FORBIDDEN_SOURCE_REFRESH = String(process.env.HLS_FORBIDDEN_SOURCE_REFRESH ?? "1") !== "0";
const HLS_WAITING_MANIFEST_ON_ERROR = String(process.env.HLS_WAITING_MANIFEST_ON_ERROR ?? "1") !== "0";
const HLS_WAITING_TARGET_DURATION = Math.max(1, Number(process.env.HLS_WAITING_TARGET_DURATION || 2));
const PLAYBACK_ACTIVITY_TTL_MS = Math.max(60 * 1000, Number(process.env.PLAYBACK_ACTIVITY_TTL_MS || 30 * 60 * 1000));
const PLAYBACK_ACTIVITY_MAX = Math.max(16, Number(process.env.PLAYBACK_ACTIVITY_MAX || 512));
const UPSTREAM_KEEPALIVE_MAX_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_SOCKETS || 8));
const UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS || 4));
const UPSTREAM_KEEPALIVE_MS = Math.max(1000, Number(process.env.UPSTREAM_KEEPALIVE_MS || 60000));
const PLAYLIST_REQUEST_TIMEOUT = Number(process.env.PLAYLIST_REQUEST_TIMEOUT || 20000);
const PLAYLIST_RETRY_WINDOW_MS = Number(process.env.PLAYLIST_RETRY_WINDOW_MS || 30000);
const PLAYLIST_RETRY_DELAY_MS = Number(process.env.PLAYLIST_RETRY_DELAY_MS || 2000);
const EPG_TIME_ZONE = process.env.EPG_TIME_ZONE || "Europe/Rome";

// ── ffmpeg live ingest configuration ──────────────────────────────────────────
// We run one ffmpeg per active channel. ffmpeg behaves like a real IPTV client
// (like UHF): it follows token rotation, reconnects on upstream errors, and
// remuxes (-c copy, NO transcode -> ~0 CPU, safe on a GPU-less VPS) into a clean
// local rolling HLS playlist. The player only ever reads stable local files,
// decoupled from upstream instability, sitting a cushion behind the live edge.
const FFMPEG_BIN = process.env.FFMPEG_BIN || "ffmpeg";
const INGEST_ROOT = process.env.INGEST_ROOT || "/tmp/kronos-ingest";
const INGEST_SEGMENT_SECONDS = Math.max(1, Number(process.env.INGEST_SEGMENT_SECONDS || 4));
const INGEST_WINDOW_SEGMENTS = Math.max(6, Number(process.env.INGEST_WINDOW_SEGMENTS || 36));
// Live cushion: the served edge is held this many seconds behind ffmpeg's real
// edge. This local reserve absorbs ffmpeg bursts (it grows) and stalls (it
// shrinks) so the player timeline stays smooth and never starves on a short
// upstream hiccup. We also prebuffer at least this much before the first serve.
const INGEST_LIVE_OFFSET_SECONDS = Math.max(0, Number(process.env.INGEST_LIVE_OFFSET_SECONDS || 20));
const INGEST_PREBUFFER_SECONDS = Math.max(0, Number(process.env.INGEST_PREBUFFER_SECONDS ?? (INGEST_LIVE_OFFSET_SECONDS + 6)));
// How many segments to list in the served playlist (a sliding DVR window) and
// the max reserve before we force a catch-up (bounds latency; rarely hit).
const INGEST_VISIBLE_SEGMENTS = Math.max(4, Number(process.env.INGEST_VISIBLE_SEGMENTS || 20));
const INGEST_MAX_RESERVE_SECONDS = Math.max(INGEST_LIVE_OFFSET_SECONDS + 10, Number(process.env.INGEST_MAX_RESERVE_SECONDS || 45));
const INGEST_IDLE_TIMEOUT_MS = Math.max(10000, Number(process.env.INGEST_IDLE_TIMEOUT_MS || 45000));
const INGEST_FIRST_SEGMENT_TIMEOUT_MS = Math.max(5000, Number(process.env.INGEST_FIRST_SEGMENT_TIMEOUT_MS || 25000));
const INGEST_REAP_INTERVAL_MS = Math.max(5000, Number(process.env.INGEST_REAP_INTERVAL_MS || 10000));
const INGEST_RECONNECT_DELAY_MAX = Math.max(1, Number(process.env.INGEST_RECONNECT_DELAY_MAX || 5));
const INGEST_READ_TIMEOUT_US = Math.max(1000000, Number(process.env.INGEST_READ_TIMEOUT_US || 15000000));
// One upstream connection at a time by default — these IPTV accounts usually cap
// concurrent streams (a 2nd stream 403s). Opening a channel stops the others, so
// zapping never overlaps and never trips the upstream's connection limit.
const INGEST_MAX_CONCURRENT = Math.max(1, Number(process.env.INGEST_MAX_CONCURRENT || 1));
// ffmpeg sometimes exits on a transient upstream blip ("Failed to reload
// playlist"). Respawn while the channel is still wanted, with backoff, and a
// longer backoff on 403 so we never hammer the upstream into a temp ban.
// Respawn after a transient exit. On a 403 the upstream is briefly refusing a
// new connection (its single slot from the previous channel hasn't been freed):
// retry GENTLY so we don't keep its anti-abuse lock alive (hammering livelocks
// it). On other errors retry fast. Keep retrying while the channel is wanted —
// the idle reaper stops it once nobody is watching.
const INGEST_RESPAWN_DELAY_MS = Math.max(500, Number(process.env.INGEST_RESPAWN_DELAY_MS || 2000));
const INGEST_FORBIDDEN_RESPAWN_DELAY_MS = Math.max(2000, Number(process.env.INGEST_FORBIDDEN_RESPAWN_DELAY_MS || 5000));
// When zapping, wait for the previous channel's ffmpeg to fully close before
// connecting the new one, to minimise the upstream connection-limit 403.
const INGEST_DRAIN_TIMEOUT_MS = Math.max(500, Number(process.env.INGEST_DRAIN_TIMEOUT_MS || 4000));
const INGEST_DRAIN_GRACE_MS = Math.max(0, Number(process.env.INGEST_DRAIN_GRACE_MS || 3000));
const INGEST_HEALTHY_RUN_MS = Math.max(5000, Number(process.env.INGEST_HEALTHY_RUN_MS || 25000));
const INGEST_MAX_RESTARTS = Math.max(1, Number(process.env.INGEST_MAX_RESTARTS || 100));
const INGEST_HTTP_RETRY_CODES = String(process.env.INGEST_HTTP_RETRY_CODES || "429,500,502,503,504,520,521,522,524");
const INGEST_PREFLIGHT = String(process.env.INGEST_PREFLIGHT ?? "0") !== "0";

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

function encodeConfig(config) {
    return Buffer.from(JSON.stringify(config), "utf8").toString("base64url");
}

function parseCatalogPreloadConfigs(raw) {
    return String(raw || "")
        .split(/[,\s]+/)
        .map(extractConfigKey)
        .filter(Boolean);
}

function readFrontendPreloadConfigKey() {
    if (!FRONTEND_PRELOAD_FILE) return "";
    try {
        const payload = JSON.parse(fs.readFileSync(FRONTEND_PRELOAD_FILE, "utf8"));
        const configKey = extractConfigKey(payload.configKey || payload.manifestUrl || "");
        if (!configKey) return "";
        decodeConfig(configKey);
        return configKey;
    } catch (err) {
        if (err?.code !== "ENOENT") console.warn(`[FRONTEND PRELOAD READ] ${err.message}`);
        return "";
    }
}

function saveFrontendPreloadConfig(configKey, config, reason = "frontend") {
    if (!FRONTEND_PRELOAD_FILE || !configKey) return false;
    const lists = getConfiguredLists(config);
    if (!lists.length) return false;
    const payload = {
        configKey,
        savedAt: new Date().toISOString(),
        reason,
        lists: lists.map(list => ({ name: list.name, url: list.url }))
    };
    try {
        fs.mkdirSync(path.dirname(FRONTEND_PRELOAD_FILE), { recursive: true });
        const tmp = `${FRONTEND_PRELOAD_FILE}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify(payload, null, 2));
        fs.renameSync(tmp, FRONTEND_PRELOAD_FILE);
        console.log(`[FRONTEND PRELOAD SAVE] reason=${reason} lists=${lists.length} config=${hashKey(configKey)}`);
        return true;
    } catch (err) {
        console.warn(`[FRONTEND PRELOAD SAVE ERROR] ${err.message}`);
        return false;
    }
}

function getStartupPreloadConfigs() {
    const frontendConfig = readFrontendPreloadConfigKey();
    if (frontendConfig) return { source: "frontend", configs: [frontendConfig] };
    return { source: "env", configs: CATALOG_PRELOAD_CONFIGS };
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

function hashKey(value) { return crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, 20); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function isHttpUrl(v) { return /^https?:\/\//i.test(String(v || "")); }
function isHlsUrl(v) { return /\.m3u8(?:[?#].*)?$/i.test(String(v || "")); }
function isPlayableHttpUrl(v) { return /^https?:\/\//i.test(String(v || "")); }

function escapeXml(value) {
    return String(value || "")
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function getPublicHost(req) {
    const proto = req.get("x-forwarded-proto") || req.protocol;
    const host = req.get("x-forwarded-host") || req.get("host");
    return `${proto}://${host}`;
}

function getErrorStatus(err) {
    const status = Number(err?.response?.status || err?.status || 0);
    return Number.isFinite(status) && status > 0 ? status : null;
}

function getHttpFallbackUrl(sourceUrl) {
    try {
        const u = new URL(sourceUrl);
        if (u.protocol !== "https:") return "";
        u.protocol = "http:";
        return u.toString();
    } catch { return ""; }
}

function isHttpsPlainHttpError(err) {
    const message = String(err?.message || "");
    return err?.code === "EPROTO" && /wrong version number|ssl3_get_record/i.test(message);
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
    const preload = getStartupPreloadConfigs();
    if (!preload.configs.length) return;
    console.log(`[CATALOG PRELOAD] source=${preload.source} configs=${preload.configs.length}`);
    preload.configs.forEach(configKey => {
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

// Keep the preloaded (frontend) catalog warm: re-fetch it on a timer, independent
// of incoming requests, and re-read the frontend config each time so a playlist
// changed on the website is picked up automatically.
function startCatalogPeriodicRefresh() {
    if (CATALOG_REFRESH_INTERVAL_MS <= 0) return;
    console.log(`[CATALOG REFRESH SCHEDULED] interval=${CATALOG_REFRESH_INTERVAL_MS / 1000}s`);
    const timer = setInterval(() => {
        const preload = getStartupPreloadConfigs();
        preload.configs.forEach(configKey => {
            try {
                const config = decodeConfig(configKey);
                fetchAndProcessChannels(configKey, config, { force: true })
                    .then(channels => console.log(`[CATALOG REFRESH OK] channels=${channels.length}`))
                    .catch(err => console.error(`[CATALOG REFRESH] ${err.message}`));
            } catch (err) {
                console.error(`[CATALOG REFRESH] invalid config: ${err.message}`);
            }
        });
    }, CATALOG_REFRESH_INTERVAL_MS);
    timer.unref?.();
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
            .filter(l => l.url && !isBlockedPlaylist(l.url));
    }
    return [{
        name: String(config.ln || "Canali TV").trim() || "Canali TV",
        url: String(config.u || "").trim()
    }].filter(l => l.url && !isBlockedPlaylist(l.url));
}

async function fetchPlaylist(sourceUrl, options = {}) {
    if (isBlockedPlaylist(sourceUrl)) {
        console.warn("[PLAYLIST BLOCKED]", sourceUrl);
        throw new Error("Playlist is blocked");
    }
    console.log("[FETCH PLAYLIST]", sourceUrl);
    const deadline = Date.now() + (options.retryWindow || PLAYLIST_RETRY_WINDOW_MS);
    let attempt = 0;
    let lastErr = null;
    let requestUrl = sourceUrl;
    let usedHttpFallback = false;
    while (Date.now() < deadline) {
        attempt++;
        try {
            const response = await axios.get(requestUrl, {
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
            console.log("[FETCH PLAYLIST OK]", requestUrl, "size=" + data.length, "attempt=" + attempt);
            return response.data;
        } catch (err) {
            lastErr = err;
            const fallbackUrl = !usedHttpFallback && isHttpsPlainHttpError(err) ? getHttpFallbackUrl(requestUrl) : "";
            if (fallbackUrl) {
                usedHttpFallback = true;
                requestUrl = fallbackUrl;
                console.warn(`[FETCH PLAYLIST HTTP FALLBACK] ${sourceUrl} -> ${fallbackUrl} reason=${err.message}`);
                continue;
            }
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

let playbackRequestCounter = 0;

function nextPlaybackRequestId(prefix) {
    playbackRequestCounter = (playbackRequestCounter + 1) % 1000000;
    return `${prefix}${playbackRequestCounter.toString(36)}`;
}

function playbackClientTag(req) {
    return hashKey(`${req.ip || req.socket?.remoteAddress || ""}|${req.get("user-agent") || ""}`).slice(0, 10);
}

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
    if (isHlsUrl(channel.url) || isPlayableHttpUrl(channel.url)) {
        return {
            title: channel.name, name: "TV",
            url: `${host}/${configKey}/live/${encodeURIComponent(channel.id)}/index.m3u8`,
            behaviorHints: { notWebReady: true, bingeGroup: `kronos-${channel.id}` }
        };
    }
    return { title: `${channel.name} - sorgente web`, name: "TV", externalUrl: channel.url };
}

// Pre-warm the ffmpeg ingest as soon as Stremio asks for the stream list, so the
// first segment is usually ready by the time the user presses play.
function preflightHLSStream(channel, configKey) {
    if (!INGEST_PREFLIGHT || !configKey || !channel?.url || !channel?.id) return;
    if (!(isHlsUrl(channel.url) || isPlayableHttpUrl(channel.url))) return;
    try {
        ensureIngest(`${configKey}:${channel.id}`, channel.url, channel.name);
    } catch (err) {
        console.warn(`[INGEST PREFLIGHT FAIL] channel="${channel.name}" reason=${err.message}`);
    }
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
        saveFrontendPreloadConfig(configKey, config, "manifest");
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
app.post("/api/preload-config", async (req, res) => {
    try {
        const configKey = extractConfigKey(req.body.token || req.body.configKey || req.body.manifestUrl || "");
        const config = req.body.config || (configKey ? decodeConfig(configKey) : null);
        if (!config || typeof config !== "object") throw new Error("Missing config");
        const effectiveKey = configKey || encodeConfig(config);
        decodeConfig(effectiveKey);
        const saved = saveFrontendPreloadConfig(effectiveKey, config, "frontend");
        fetchAndProcessChannels(effectiveKey, config).catch(err => console.error("[FRONTEND PRELOAD WARM]", err.message));
        res.json({ ok: true, saved });
    } catch (err) {
        res.status(400).json({ ok: false, error: "Configurazione non valida" });
    }
});

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

// ─────────────────────────────────────────────────────────────────────────────
// ffmpeg live ingest manager
// ─────────────────────────────────────────────────────────────────────────────
const ingests = new Map(); // ingestKey -> ingest record
let ingestReaperStarted = false;

function ingestDirFor(ingestKey) {
    return path.join(INGEST_ROOT, hashKey(ingestKey));
}

function ingestIndexPath(ingest) {
    return path.join(ingest.dir, "index.m3u8");
}

function ffmpegArgs(url, dir, startNumber = 0) {
    return [
        "-nostdin", "-hide_banner", "-loglevel", "warning",
        "-user_agent", UPSTREAM_UA,
        // Survive transient upstream blips on both the byte stream AND the HLS
        // playlist reloads (the latter is what was killing ffmpeg before).
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_on_network_error", "1",
        "-reconnect_on_http_error", INGEST_HTTP_RETRY_CODES,
        "-reconnect_delay_max", String(INGEST_RECONNECT_DELAY_MAX),
        "-rw_timeout", String(INGEST_READ_TIMEOUT_US),
        "-i", url,
        "-map", "0:v:0", "-map", "0:a?",
        "-c", "copy",
        "-sn", "-dn",
        "-f", "hls",
        "-hls_time", String(INGEST_SEGMENT_SECONDS),
        "-hls_list_size", String(INGEST_WINDOW_SEGMENTS),
        "-hls_flags", "delete_segments+omit_endlist+independent_segments+temp_file",
        "-hls_segment_type", "mpegts",
        "-hls_allow_cache", "0",
        "-start_number", String(startNumber),
        "-hls_segment_filename", path.join(dir, "seg_%05d.ts"),
        path.join(dir, "index.m3u8")
    ];
}

function spawnIngest(ingest) {
    const dir = ingest.dir;
    // Cancel any pending respawn so we never end up with two ffmpeg (two upstream
    // connections) for the same channel.
    if (ingest.respawnTimer) { clearTimeout(ingest.respawnTimer); ingest.respawnTimer = null; }
    if (ingest.proc && !ingest.exited) return; // already running
    try { fs.mkdirSync(dir, { recursive: true }); } catch {}

    // Seamless if we've produced segments before (regardless of whether the
    // respawn was triggered by the backoff timer or by a fresh request).
    const respawn = (ingest.lastSegNumber || 0) > 0;
    let startNumber = 0;
    if (respawn) {
        // Seamless respawn: continue segment numbering forward (no backward
        // MEDIA-SEQUENCE reset) and KEEP the existing reserve files on disk so the
        // player keeps playing through the ~few-second restart. A small index gap
        // marks a discontinuity in the served playlist. Base the new number on the
        // highest file actually on disk to avoid colliding with straggler segments.
        let maxIdx = ingest.lastSegNumber || 0;
        try {
            for (const f of fs.readdirSync(dir)) {
                const m = f.match(/^seg_(\d+)\.ts$/);
                if (m) maxIdx = Math.max(maxIdx, parseInt(m[1], 10));
            }
        } catch {}
        startNumber = maxIdx + 5;
    } else {
        // First start: clean slate.
        try {
            for (const f of fs.readdirSync(dir)) {
                if (f === "index.m3u8" || /^seg_\d+\.ts/.test(f) || f.endsWith(".tmp")) {
                    try { fs.unlinkSync(path.join(dir, f)); } catch {}
                }
            }
        } catch {}
        ingest.served = false; // first start -> prebuffer the cushion before serving
        ingest.pace = null;
        ingest.segDur = new Map();
        ingest.lastSegNumber = 0;
    }

    const proc = spawn(FFMPEG_BIN, ffmpegArgs(ingest.url, dir, startNumber), { stdio: ["ignore", "ignore", "pipe"] });
    ingest.proc = proc;
    ingest.pid = proc.pid;
    ingest.startedAt = Date.now();
    ingest.firstSegmentAt = 0;
    ingest.exited = false;
    ingest.stopping = false;
    ingest.stderrTail = "";

    proc.stderr.on("data", chunk => {
        ingest.stderrTail = (ingest.stderrTail + String(chunk)).slice(-2000);
    });
    proc.on("exit", (code, signal) => {
        ingest.exited = true;
        ingest.exitCode = code;
        if (ingest.stopping) return;
        const ranMs = Date.now() - ingest.startedAt;
        const tail = (ingest.stderrTail || "").trim().split("\n").pop() || "";
        console.warn(`[INGEST EXIT] channel="${ingest.label}" key=${ingest.shortKey} pid=${ingest.pid} code=${code} signal=${signal || "none"} ran=${ranMs}ms tail=${tail}`);
        scheduleRespawn(ingest, ranMs, tail);
    });
    proc.on("error", err => {
        ingest.exited = true;
        ingest.spawnError = err.message;
        console.error(`[INGEST SPAWN ERROR] channel="${ingest.label}" key=${ingest.shortKey} reason=${err.message}`);
    });

    console.log(`[INGEST START] channel="${ingest.label}" key=${ingest.shortKey} pid=${ingest.pid} restarts=${ingest.restarts} seg=${INGEST_SEGMENT_SECONDS}s window=${INGEST_WINDOW_SEGMENTS} cushion=${INGEST_LIVE_OFFSET_SECONDS}s`);
}

// Keep a wanted channel alive across ffmpeg deaths, but with backoff so we never
// hammer the upstream (which would turn a transient error into a 403 temp-ban).
function scheduleRespawn(ingest, ranMs, tail) {
    if (!ingests.has(ingest.key)) return; // stopped/zapped away
    if (Date.now() - ingest.lastAccess > INGEST_IDLE_TIMEOUT_MS) return; // nobody watching

    if (ranMs >= INGEST_HEALTHY_RUN_MS) ingest.failures = 0; // it ran fine, just hiccuped
    ingest.failures = (ingest.failures || 0) + 1;

    if (ingest.failures > INGEST_MAX_RESTARTS) {
        console.error(`[INGEST GIVEUP] channel="${ingest.label}" key=${ingest.shortKey} failures=${ingest.failures} lastTail=${tail}`);
        stopIngest(ingest, `giveup after ${ingest.failures} failures`);
        return;
    }

    const forbidden = /403|Forbidden|access denied/i.test(tail);
    const delay = forbidden ? INGEST_FORBIDDEN_RESPAWN_DELAY_MS : INGEST_RESPAWN_DELAY_MS;
    ingest.restarts++;
    console.log(`[INGEST RESPAWN] channel="${ingest.label}" key=${ingest.shortKey} in=${delay}ms failures=${ingest.failures} forbidden=${forbidden ? 1 : 0}`);
    ingest.respawnTimer = setTimeout(() => {
        ingest.respawnTimer = null;
        if (!ingests.has(ingest.key)) return;
        if (ingest.proc && !ingest.exited) return; // already respawned (e.g. by a request)
        if (Date.now() - ingest.lastAccess > INGEST_IDLE_TIMEOUT_MS) { stopIngest(ingest, "idle before respawn"); return; }
        spawnIngest(ingest);
    }, delay);
    ingest.respawnTimer.unref?.();
}

// Enforce the upstream's concurrent-connection budget: stop the least-recently
// used OTHER ingests, then WAIT for their ffmpeg (and upstream socket) to close
// before the caller connects the new channel — otherwise the new one gets a 403.
async function freeConnectionSlot(keepKey) {
    const others = [...ingests.values()].filter(i => i.key !== keepKey);
    if (others.length + 1 <= INGEST_MAX_CONCURRENT) return;
    others.sort((a, b) => a.lastAccess - b.lastAccess); // oldest first
    const toStop = others.slice(0, others.length + 1 - INGEST_MAX_CONCURRENT);
    for (const i of toStop) stopIngest(i, "freeing connection slot (zap)");
    const deadline = Date.now() + INGEST_DRAIN_TIMEOUT_MS;
    while (Date.now() < deadline && toStop.some(i => !i.exited)) await sleep(100);
    if (INGEST_DRAIN_GRACE_MS) await sleep(INGEST_DRAIN_GRACE_MS); // let the upstream register the disconnect
}

function ensureIngest(ingestKey, url, label) {
    let ingest = ingests.get(ingestKey);
    if (ingest && !ingest.exited && ingest.proc) {
        ingest.lastAccess = Date.now();
        ingest.url = url;
        if (label) ingest.label = label;
        return ingest;
    }
    if (!ingest) {
        ingest = {
            key: ingestKey,
            shortKey: hashKey(ingestKey).slice(0, 10),
            url,
            label: label || "live",
            dir: ingestDirFor(ingestKey),
            lastAccess: Date.now(),
            restarts: 0,
            failures: 0
        };
        ingests.set(ingestKey, ingest);
    } else {
        ingest.url = url;
        if (label) ingest.label = label;
        ingest.lastAccess = Date.now();
    }
    spawnIngest(ingest);
    startIngestReaper();
    return ingest;
}

function readIngestPlaylist(ingest) {
    try {
        const raw = fs.readFileSync(ingestIndexPath(ingest), "utf8");
        return /seg_\d+\.ts/.test(raw) ? raw : null;
    } catch {
        return null;
    }
}

function playlistDurationSeconds(raw) {
    let total = 0;
    const re = /#EXTINF:([0-9.]+)/g;
    let m;
    while ((m = re.exec(raw))) total += parseFloat(m[1]) || 0;
    return total;
}

// Block the first serve until we have a real cushion of locally-ingested content
// (or we hit the timeout). This is what gives the player runway from frame one,
// instead of starting glued to the live edge and micro-buffering. Stremio
// tolerates a slow first manifest. Later requests return immediately.
async function waitForPrebuffer(ingest, targetSeconds, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let raw = null;
    while (Date.now() < deadline) {
        raw = readIngestPlaylist(ingest);
        if (raw) {
            if (!ingest.firstSegmentAt) ingest.firstSegmentAt = Date.now();
            if (playlistDurationSeconds(raw) >= targetSeconds) return raw;
        } else if (ingest.exited && !ingest.respawnTimer) {
            return null;
        }
        await sleep(250);
    }
    return raw; // serve whatever we managed to buffer
}

// Record each segment's real duration from ffmpeg's index into a per-ingest
// registry, so we keep accurate durations even across ffmpeg respawns (whose
// fresh index only lists the new run's segments).
function refreshSegDurations(ingest) {
    if (!ingest.segDur) ingest.segDur = new Map();
    let raw;
    try { raw = fs.readFileSync(ingestIndexPath(ingest), "utf8"); } catch { return; }
    let dur = INGEST_SEGMENT_SECONDS;
    for (const line of raw.split(/\r?\n/)) {
        const m = line.match(/^#EXTINF:([0-9.]+)/);
        if (m) { dur = parseFloat(m[1]) || INGEST_SEGMENT_SECONDS; continue; }
        const s = line.match(/^seg_(\d+)\.ts/);
        if (s) { const idx = parseInt(s[1], 10); ingest.segDur.set(idx, dur); if (idx > (ingest.lastSegNumber || 0)) ingest.lastSegNumber = idx; dur = INGEST_SEGMENT_SECONDS; }
    }
}

// List the actual segment files on disk (survives ffmpeg respawns), newest last.
function listSegFiles(ingest) {
    let files;
    try { files = fs.readdirSync(ingest.dir); } catch { return []; }
    const segs = [];
    for (const f of files) {
        const m = f.match(/^seg_(\d+)\.ts$/);
        if (m) { const idx = parseInt(m[1], 10); segs.push({ idx, name: f, dur: (ingest.segDur && ingest.segDur.get(idx)) || INGEST_SEGMENT_SECONDS }); }
    }
    segs.sort((a, b) => a.idx - b.idx);
    return segs;
}

// Pace the served live edge so it advances at ~real time and NEVER jumps forward,
// regardless of how bursty ffmpeg's output is, or whether ffmpeg just respawned.
// ffmpeg produces in bursts (stall, then catch-up) and on a transient upstream
// error it exits and we respawn it; if we exposed that raw edge the player would
// skip on the jumps and stall on the gaps. Instead we keep the served edge
// INGEST_LIVE_OFFSET behind the newest local segment: that reserve grows on
// bursts and is spent on stalls/respawns, keeping the player's timeline smooth.
// We read the segment FILES on disk (which survive a respawn) so the reserve is
// never wiped, and mark any index gap (a respawn boundary) with a DISCONTINUITY.
function buildPacedPlaylist(ingest) {
    refreshSegDurations(ingest);
    const segs = listSegFiles(ingest);
    if (!segs.length) return null;
    pruneOldSegFiles(ingest, segs);

    const lastPos = segs.length - 1;
    const realEdge = segs[lastPos].idx;
    const durAt = p => (segs[p]?.dur || INGEST_SEGMENT_SECONDS);
    const reserveFromPos = p => { let s = 0; for (let i = p + 1; i <= lastPos; i++) s += durAt(i); return s; };
    const posBehind = seconds => { let acc = 0, p = lastPos; while (p > 0 && acc < seconds) { acc += durAt(p); p--; } return p; };

    // Resolve our current edge (stored as an absolute segment index) to a position.
    let curPos;
    if (ingest.pace == null) curPos = posBehind(INGEST_LIVE_OFFSET_SECONDS);
    else {
        curPos = segs.findIndex(s => s.idx >= ingest.pace.edgeIdx);
        if (curPos < 0) curPos = lastPos;
    }

    const now = Date.now();
    let jumped = false;
    if (ingest.pace == null) {
        ingest.pace = { edgeIdx: segs[curPos].idx, at: now };
    } else if (reserveFromPos(curPos) > INGEST_MAX_RESERVE_SECONDS) {
        curPos = posBehind(INGEST_LIVE_OFFSET_SECONDS); // latency too high -> snap back
        ingest.pace = { edgeIdx: segs[curPos].idx, at: now };
        jumped = true;
    } else {
        let budget = now - ingest.pace.at;
        let at = ingest.pace.at;
        while (curPos < lastPos) {
            const dms = durAt(curPos + 1) * 1000;
            if (budget >= dms) { curPos++; budget -= dms; at += dms; } else break;
        }
        ingest.pace = { edgeIdx: segs[curPos].idx, at };
    }

    const startPos = Math.max(0, curPos - INGEST_VISIBLE_SEGMENTS + 1);
    let maxDur = 0;
    const body = [];
    for (let p = startPos; p <= curPos; p++) {
        const s = segs[p];
        if (p > startPos && s.idx !== segs[p - 1].idx + 1) body.push("#EXT-X-DISCONTINUITY"); // respawn gap
        if (s.dur > maxDur) maxDur = s.dur;
        body.push(`#EXTINF:${s.dur.toFixed(3)},`, s.name);
    }
    if (!body.length) return null;
    const playlist = [
        "#EXTM3U",
        "#EXT-X-VERSION:6",
        `#EXT-X-TARGETDURATION:${Math.max(1, Math.ceil(maxDur || INGEST_SEGMENT_SECONDS))}`,
        `#EXT-X-MEDIA-SEQUENCE:${segs[startPos].idx}`,
        "#EXT-X-DISCONTINUITY-SEQUENCE:0",
        "#EXT-X-INDEPENDENT-SEGMENTS",
        ...body,
        ""
    ].join("\n");
    return { playlist, shown: curPos - startPos + 1, edge: segs[curPos].idx, realEdge, reserveSec: reserveFromPos(curPos), jumped };
}

// Delete segment files (and registry entries) well below the served window. Needed
// because after a respawn the previous run's files are no longer managed by
// ffmpeg's own delete_segments.
function pruneOldSegFiles(ingest, segs) {
    const keep = INGEST_VISIBLE_SEGMENTS + INGEST_WINDOW_SEGMENTS;
    if (segs.length <= keep) return;
    const cutoff = segs.length - keep;
    for (let i = 0; i < cutoff; i++) {
        try { fs.unlinkSync(path.join(ingest.dir, segs[i].name)); } catch {}
        if (ingest.segDur) ingest.segDur.delete(segs[i].idx);
    }
}

function stopIngest(ingest, reason) {
    ingest.stopping = true;
    if (ingest.respawnTimer) { clearTimeout(ingest.respawnTimer); ingest.respawnTimer = null; }
    const proc = ingest.proc;
    try { if (proc && !ingest.exited) proc.kill("SIGTERM"); } catch {}
    setTimeout(() => { try { if (proc && !ingest.exited) proc.kill("SIGKILL"); } catch {} }, 4000);
    ingests.delete(ingest.key);
    setTimeout(() => { try { fs.rmSync(ingest.dir, { recursive: true, force: true }); } catch {} }, 5000);
    console.log(`[INGEST STOP] channel="${ingest.label}" key=${ingest.shortKey} reason=${reason}`);
}

function startIngestReaper() {
    if (ingestReaperStarted) return;
    ingestReaperStarted = true;
    const timer = setInterval(() => {
        const now = Date.now();
        for (const ingest of [...ingests.values()]) {
            if (now - ingest.lastAccess > INGEST_IDLE_TIMEOUT_MS) {
                stopIngest(ingest, `idle ${Math.round((now - ingest.lastAccess) / 1000)}s`);
            }
        }
    }, INGEST_REAP_INTERVAL_MS);
    timer.unref?.();
}

// Player fetches the channel manifest -> ensure ffmpeg is ingesting (stopping any
// other channel), prebuffer the cushion, then serve a clean local playlist.
app.get("/:base64Config/live/:cid/index.m3u8", async (req, res) => {
    const started = Date.now();
    const reqId = nextPlaybackRequestId("L");
    const configKey = req.params.base64Config;
    const cid = req.params.cid;
    const client = playbackClientTag(req);
    let label = "live";
    try {
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, cid);
        if (!ch || !ch.url) return res.status(404).type("text/plain").send("#EXTM3U\n");
        label = ch.name || "live";
        const ingestKey = `${configKey}:${cid}`;
        // Zapping: stop any other channel and wait for its upstream connection to
        // close before we connect this one (the upstream allows only N at once).
        if (!ingests.has(ingestKey) || ingests.get(ingestKey)?.exited) await freeConnectionSlot(ingestKey);
        const ingest = ensureIngest(ingestKey, ch.url, label);
        ingest.lastAccess = Date.now();

        // First serve: prebuffer the cushion. Afterwards (incl. across a seamless
        // respawn) serve straight from the reserve files on disk.
        let warmupMs = 0;
        if (!ingest.served) {
            const w0 = Date.now();
            const pre = await waitForPrebuffer(ingest, INGEST_PREBUFFER_SECONDS, INGEST_FIRST_SEGMENT_TIMEOUT_MS);
            warmupMs = Date.now() - w0;
            ingest.lastAccess = Date.now();
            if (!pre && !listSegFiles(ingest).length) {
                console.error(`[LIVE TIMEOUT] id=${reqId} channel="${label}" key=${ingest.shortKey} warmup=${warmupMs}ms exited=${ingest.exited ? 1 : 0} tail=${(ingest.stderrTail || "").trim().split("\n").pop() || ""}`);
                return res.status(504).type("text/plain").send("#EXTM3U\n");
            }
            ingest.served = true;
        }
        const paced = buildPacedPlaylist(ingest);
        if (!paced) {
            console.error(`[LIVE EMPTY] id=${reqId} channel="${label}" key=${ingest.shortKey} warmup=${warmupMs}ms`);
            return res.status(504).type("text/plain").send("#EXTM3U\n");
        }
        setPlaylistHeaders(res);
        res.send(paced.playlist);
        console.log(`[LIVE SERVE] id=${reqId} channel="${label}" key=${ingest.shortKey} shown=${paced.shown} edge=${paced.edge}/${paced.realEdge} reserve=${paced.reserveSec.toFixed(0)}s${paced.jumped ? " JUMP" : ""} warmup=${warmupMs}ms client=${client} time=${Date.now() - started}ms`);
    } catch (err) {
        console.error(`[LIVE ERROR] id=${reqId} channel="${label}" cid=${cid} reason=${err.message}`);
        if (!res.headersSent) res.status(502).type("text/plain").send("#EXTM3U\n");
    }
});

// Serve a locally-produced segment file. No upstream contact here: ffmpeg already
// pulled it via the VPN and remuxed it.
app.get("/:base64Config/live/:cid/:seg", (req, res) => {
    const { base64Config: configKey, cid, seg } = req.params;
    if (!/^seg_\d+\.ts$/.test(seg)) return res.status(400).end();
    const ingestKey = `${configKey}:${cid}`;
    const ingest = ingests.get(ingestKey);
    if (ingest) ingest.lastAccess = Date.now();
    const file = path.join(ingestDirFor(ingestKey), seg);
    fs.stat(file, (err, st) => {
        if (err || !st.isFile()) return res.status(404).end();
        res.setHeader("Content-Type", "video/mp2t");
        res.setHeader("Cache-Control", "no-store");
        res.setHeader("Content-Length", st.size);
        const stream = fs.createReadStream(file);
        stream.on("error", () => { if (!res.headersSent) res.status(404).end(); else res.destroy(); });
        stream.pipe(res);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Stream + stats + debug
// ─────────────────────────────────────────────────────────────────────────────
app.get("/:base64Config/stream/:type/:id.json", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        if (!ch) return res.status(404).json({ streams: [] });
        preflightHLSStream(ch, configKey);
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
        ingest: {
            engine: "ffmpeg",
            active: ingests.size,
            maxConcurrent: INGEST_MAX_CONCURRENT,
            cushionSeconds: INGEST_LIVE_OFFSET_SECONDS,
            prebufferSeconds: INGEST_PREBUFFER_SECONDS,
            channels: [...ingests.values()].map(i => ({
                label: i.label,
                pid: i.pid,
                up: !i.exited,
                restarts: i.restarts,
                failures: i.failures || 0,
                ageSeconds: Math.round((Date.now() - (i.startedAt || Date.now())) / 1000),
                idleSeconds: Math.round((Date.now() - i.lastAccess) / 1000)
            }))
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
                HLS_COLD_START_WAIT_MS, HLS_COLD_START_RETRY_MS,
                HLS_LIVE_MIN_SEGMENTS, HLS_LIVE_MIN_VISIBLE_SEGMENTS, HLS_LIVE_HOLDBACK_SEGMENTS,
                HLS_STALE_HOLDBACK_RELEASE_TARGETS, HLS_START_OFFSET_TARGETS, HLS_STREAM_PREFLIGHT,
                HLS_LIVE_WARMUP_WAIT_MS, HLS_LIVE_WARMUP_POLL_MS,
                HLS_STALE_MANIFEST_TTL_MS, HLS_STALE_LIVE_TARGETS, HLS_STALE_FORBIDDEN_TARGETS,
                HLS_FORBIDDEN_BACKOFF_MS, HLS_FORBIDDEN_SOURCE_REFRESH,
                HLS_WAITING_MANIFEST_ON_ERROR, HLS_WAITING_TARGET_DURATION,
                PLAYBACK_ACTIVITY_TTL_MS, PLAYBACK_ACTIVITY_MAX,
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
    console.log(`🎬 playback=${PLAYBACK_MODE} engine=ffmpeg(-c copy) bin=${FFMPEG_BIN}`);
    console.log(`🎥 ingest seg=${INGEST_SEGMENT_SECONDS}s window=${INGEST_WINDOW_SEGMENTS} cushion=${INGEST_LIVE_OFFSET_SECONDS}s prebuffer=${INGEST_PREBUFFER_SECONDS}s visible=${INGEST_VISIBLE_SEGMENTS} maxReserve=${INGEST_MAX_RESERVE_SECONDS}s paced=1 maxConcurrent=${INGEST_MAX_CONCURRENT} idle=${INGEST_IDLE_TIMEOUT_MS / 1000}s firstSegTimeout=${INGEST_FIRST_SEGMENT_TIMEOUT_MS / 1000}s preflight=${INGEST_PREFLIGHT ? 1 : 0}`);
    console.log(`🔁 ingest respawn=${INGEST_RESPAWN_DELAY_MS}ms forbidden=${INGEST_FORBIDDEN_RESPAWN_DELAY_MS}ms drain=${INGEST_DRAIN_GRACE_MS}ms healthyRun=${INGEST_HEALTHY_RUN_MS / 1000}s maxRestarts=${INGEST_MAX_RESTARTS} httpRetry=${INGEST_HTTP_RETRY_CODES}`);
    console.log(`🔌 upstream keepAlive=1 sockets=${UPSTREAM_KEEPALIVE_MAX_SOCKETS} free=${UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS} ms=${UPSTREAM_KEEPALIVE_MS}`);
    console.log(`🩺 playback telemetry=1 ttl=${PLAYBACK_ACTIVITY_TTL_MS / 1000}s max=${PLAYBACK_ACTIVITY_MAX}`);
    console.log(`📋 playlist retryWindow=${PLAYLIST_RETRY_WINDOW_MS / 1000}s delay=${PLAYLIST_RETRY_DELAY_MS / 1000}s`);
    console.log(`🎞️ segment timeout=${SEG_REQUEST_TIMEOUT / 1000}s rangeForward=1 upstreamConcurrency=${SEGMENT_UPSTREAM_CONCURRENCY} retries=${SEGMENT_UPSTREAM_RETRIES}`);
    console.log(`📺 epg timezone=${EPG_TIME_ZONE} timeout=${EPG_REQUEST_TIMEOUT / 1000}s firstWait=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s retryDelay=${EPG_RETRY_DELAY_MS / 1000}s cacheTTL=${EPG_CACHE_TTL / 1000}s refresh=${EPG_REFRESH_INTERVAL_MS / 1000}s preload=${EPG_PRELOAD_URLS.length} startupWatch=${EPG_STARTUP_WATCH_MS / 1000}s`);
    console.log(`📚 catalog preload=${CATALOG_PRELOAD_CONFIGS.length}`);
    console.log("=".repeat(60) + "\n");
    startEPGStartupPreload();
    startCatalogStartupPreload();
    startCatalogPeriodicRefresh();
});
