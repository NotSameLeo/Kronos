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
const RELEASE_VERSION = "4.3.4";
const ADDON_TYPE = "tv";
const PLAYBACK_MODE = "transparent-relay";
const CATALOG_TTL = 30 * 60 * 1000;
const CATALOG_PAGE_SIZE = Math.max(24, Math.min(300, Number(process.env.CATALOG_PAGE_SIZE || 100)));
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
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);
const HLS_MANIFEST_RETRIES = Math.max(0, Number(process.env.HLS_MANIFEST_RETRIES || 2));
const HLS_MANIFEST_RETRY_DELAY_MS = Math.max(0, Number(process.env.HLS_MANIFEST_RETRY_DELAY_MS || 700));
const HLS_STALE_MANIFEST_TTL_MS = Math.max(0, Number(process.env.HLS_STALE_MANIFEST_TTL_MS || 120000));
const SEGMENT_UPSTREAM_RETRIES = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRIES || 2));
const SEGMENT_UPSTREAM_RETRY_DELAY_MS = Math.max(0, Number(process.env.SEGMENT_UPSTREAM_RETRY_DELAY_MS || 500));
const UPSTREAM_KEEPALIVE_MAX_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_SOCKETS || 16));
const UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS = Math.max(1, Number(process.env.UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS || 8));
const UPSTREAM_KEEPALIVE_MS = Math.max(1000, Number(process.env.UPSTREAM_KEEPALIVE_MS || 60000));
const PLAYLIST_REQUEST_TIMEOUT = Number(process.env.PLAYLIST_REQUEST_TIMEOUT || 20000);
const PLAYLIST_RETRY_WINDOW_MS = Number(process.env.PLAYLIST_RETRY_WINDOW_MS || 30000);
const PLAYLIST_RETRY_DELAY_MS = Number(process.env.PLAYLIST_RETRY_DELAY_MS || 2000);
const EPG_TIME_ZONE = process.env.EPG_TIME_ZONE || "Europe/Rome";
const LOGO_REQUEST_TIMEOUT = Number(process.env.LOGO_REQUEST_TIMEOUT || 4000);
const LOGO_FAILURE_TTL_MS = Number(process.env.LOGO_FAILURE_TTL_MS || 60 * 60 * 1000);
const LOGO_MAX_CACHE_ITEMS = Math.max(50, Number(process.env.LOGO_MAX_CACHE_ITEMS || 500));

// ── Transparent relay configuration ───────────────────────────────────────────
// No VPN and no ffmpeg re-mux: Kronos keeps the stream HLS-native, but exposes a
// slightly delayed live edge so Stremio does not glue itself to the last segment.
const PROXY_START_OFFSET_SECONDS = Math.max(0, Number(process.env.PROXY_START_OFFSET_SECONDS || 18));
const LIVE_EDGE_HOLD_BACK_SECONDS = Math.max(0, Number(process.env.LIVE_EDGE_HOLD_BACK_SECONDS || 16));
const LIVE_EDGE_MIN_SEGMENTS = Math.max(1, Number(process.env.LIVE_EDGE_MIN_SEGMENTS || 3));
const LIVE_EDGE_BATCH_SEGMENTS = Math.max(1, Number(process.env.LIVE_EDGE_BATCH_SEGMENTS || 2));
const LIVE_EDGE_BATCH_MAX_WAIT_MS = Math.max(0, Number(process.env.LIVE_EDGE_BATCH_MAX_WAIT_MS || 35000));
const LIVE_EDGE_BATCH_MIN_WAIT_MS = Math.max(0, Number(process.env.LIVE_EDGE_BATCH_MIN_WAIT_MS || 20000));
const SEGMENT_TOKEN_HEALING = String(process.env.SEGMENT_TOKEN_HEALING || "1") !== "0";

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
    logoFailures: {},     // logoUrl -> timestamp of last failed fetch
    hlsManifestData: {},  // hash(configKey|url) -> { text, updatedAt }
    hlsManifestInflight: {},
    hlsEdgeState: {},     // hash(configKey|playlist) -> last exposed stable edge
    hlsSegmentMap: {},    // hash(configKey|playlist) -> stable segment id -> latest URL
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

function redactUrl(value) {
    try {
        const parsed = new URL(value);
        for (const key of ["username", "password"]) {
            if (parsed.searchParams.has(key)) parsed.searchParams.set(key, "***");
        }
        const parts = parsed.pathname.split("/");
        if (parts.length >= 5 && parts[1] === "live") {
            parts[2] = "***";
            parts[3] = "***";
            parsed.pathname = parts.join("/");
        }
        return parsed.toString();
    } catch {
        return String(value || "").replace(/(password=)[^&\s]+/gi, "$1***").replace(/(username=)[^&\s]+/gi, "$1***");
    }
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
function normalizeSourceType(value) {
    const t = String(value || "auto").trim().toLowerCase();
    return ["auto", "m3u", "xtream"].includes(t) ? t : "auto";
}

function getConfiguredLists(config) {
    if (Array.isArray(config.l) && config.l.length) {
        return config.l
            .map((list, i) => ({
                name: String(list.n || `Lista ${i + 1}`).trim() || `Lista ${i + 1}`,
                url: String(list.u || "").trim(),
                type: normalizeSourceType(list.t)
            }))
            .filter(l => l.url && !isBlockedPlaylist(l.url));
    }
    return [{
        name: String(config.ln || "Canali TV").trim() || "Canali TV",
        url: String(config.u || "").trim(),
        type: normalizeSourceType(config.t)
    }].filter(l => l.url && !isBlockedPlaylist(l.url));
}

async function fetchPlaylist(sourceUrl, options = {}) {
    if (isBlockedPlaylist(sourceUrl)) {
        console.warn("[PLAYLIST BLOCKED]", redactUrl(sourceUrl));
        throw new Error("Playlist is blocked");
    }
    console.log("[FETCH PLAYLIST]", redactUrl(sourceUrl));
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
            console.log("[FETCH PLAYLIST OK]", redactUrl(requestUrl), "size=" + data.length, "attempt=" + attempt);
            return response.data;
        } catch (err) {
            lastErr = err;
            const fallbackUrl = !usedHttpFallback && isHttpsPlainHttpError(err) ? getHttpFallbackUrl(requestUrl) : "";
            if (fallbackUrl) {
                usedHttpFallback = true;
                requestUrl = fallbackUrl;
                console.warn(`[FETCH PLAYLIST HTTP FALLBACK] ${redactUrl(sourceUrl)} -> ${redactUrl(fallbackUrl)} reason=${err.message}`);
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
            const group = cleanGroupName((line.match(/group-title="([^"]+)"/) || [, "Altri Canali"])[1]);
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
    return channels.filter(channel => !isDividerChannelName(channel.name));
}

function isDividerChannelName(name) {
    return /^#{2,}/.test(String(name || "").trim());
}

function cleanGroupName(group) {
    const cleaned = String(group || "").trim()
        .replace(/^\s*IT\s*\|\s*/i, "")
        .replace(/^\s*IT\s*:\s*/i, "")
        .trim();
    return cleaned || "Altri Canali";
}

function parseXtreamConfig(sourceUrl) {
    try {
        const parsed = new URL(sourceUrl);
        const pathParts = parsed.pathname.split("/").filter(Boolean);
        let username = parsed.searchParams.get("username") || "";
        let password = parsed.searchParams.get("password") || "";
        if ((!username || !password) && pathParts.length >= 3 && pathParts[0] === "live") {
            username = username || decodeURIComponent(pathParts[1]);
            password = password || decodeURIComponent(pathParts[2]);
        }
        if (!username || !password) return null;
        return { origin: parsed.origin, username, password };
    } catch {
        return null;
    }
}

function xtreamApiUrl(xtream, action = "") {
    const url = new URL("/player_api.php", xtream.origin);
    url.searchParams.set("username", xtream.username);
    url.searchParams.set("password", xtream.password);
    if (action) url.searchParams.set("action", action);
    return url.toString();
}

function xtreamLiveUrl(xtream, streamId) {
    return `${xtream.origin}/live/${encodeURIComponent(xtream.username)}/${encodeURIComponent(xtream.password)}/${encodeURIComponent(streamId)}.m3u8`;
}

async function fetchXtreamApi(xtream, action = "") {
    const url = xtreamApiUrl(xtream, action);
    const response = await axios.get(url, {
        timeout: PLAYLIST_REQUEST_TIMEOUT,
        ...upstreamAgentOptions(),
        maxRedirects: 5,
        headers: {
            "User-Agent": UPSTREAM_UA,
            "Accept": "application/json, text/plain, */*",
            "Connection": "keep-alive"
        },
        validateStatus: s => s >= 200 && s < 300
    });
    return typeof response.data === "string" ? JSON.parse(response.data) : response.data;
}

async function fetchXtreamChannels(source) {
    const xtream = parseXtreamConfig(source.url);
    if (!xtream) throw new Error("Missing Xtream credentials");
    console.log("[FETCH XTREAM]", redactUrl(xtreamApiUrl(xtream)));

    const [account, categories, streams] = await Promise.all([
        fetchXtreamApi(xtream).catch(err => ({ error: err.message })),
        fetchXtreamApi(xtream, "get_live_categories"),
        fetchXtreamApi(xtream, "get_live_streams")
    ]);
    if (account?.user_info && Number(account.user_info.auth) !== 1) throw new Error("Xtream account not authorized");
    if (!Array.isArray(streams)) throw new Error("Invalid Xtream live stream response");

    const categoryNames = new Map((Array.isArray(categories) ? categories : [])
        .map(c => [String(c.category_id), cleanGroupName(c.category_name)])
        .filter(([, name]) => name));

    const channels = streams
        .filter(item => item && item.stream_id != null && String(item.name || "").trim())
        .filter(item => !isDividerChannelName(item.name))
        .map(item => {
            const streamId = String(item.stream_id);
            const categoryId = String(item.category_id || item.category_ids?.[0] || "");
            const url = isHttpUrl(item.direct_source) ? item.direct_source : xtreamLiveUrl(xtream, streamId);
            return {
                id: "channel_" + crypto.createHash("sha1").update(`${source.url || ""}|xtream:${streamId}`).digest("hex").slice(0, 20),
                name: String(item.name || "Canale Sconosciuto").trim(),
                group: categoryNames.get(categoryId) || cleanGroupName(source.name || "Xtream"),
                logo: String(item.stream_icon || "").trim(),
                tvgId: item.epg_channel_id ? String(item.epg_channel_id) : null,
                url,
                sourceName: source.name || "Xtream",
                sourceUrl: source.url || "",
                sourceType: "xtream"
            };
        });

    console.log(`[FETCH XTREAM OK] channels=${channels.length} categories=${categoryNames.size}`);
    return channels;
}

async function fetchChannelsFromSource(source) {
    const type = normalizeSourceType(source.type);
    const canUseXtream = !!parseXtreamConfig(source.url);
    if (type === "xtream" || (type === "auto" && canUseXtream)) {
        try {
            return await fetchXtreamChannels(source);
        } catch (err) {
            if (type === "xtream") throw err;
            console.warn(`[FETCH XTREAM FALLBACK] ${err.message}; trying M3U`);
        }
    }
    const data = await fetchPlaylist(source.url);
    return parseM3UChannels(data, source);
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
function normalizeGroupName(g) { return cleanGroupName(g).toLowerCase(); }
function getCategoryCustomizations(config) {
    if (!Array.isArray(config?.cg)) return [];
    return config.cg
        .map(item => ({
            id: cleanGroupName(item?.id || item?.name || ""),
            name: cleanGroupName(item?.name || item?.id || "")
        }))
        .filter(item => item.id && item.name);
}
function getSelectedGroups(config) {
    const custom = getCategoryCustomizations(config);
    if (custom.length) return custom.map(item => item.id);
    return Array.isArray(config.g) ? config.g : [];
}
function getDisplayGroupName(config, group) {
    const normalized = normalizeGroupName(group);
    const custom = getCategoryCustomizations(config).find(item => normalizeGroupName(item.id) === normalized);
    return custom?.name || cleanGroupName(group);
}
function sortGroupsForManifest(config, groups) {
    const order = new Map();
    getCategoryCustomizations(config).forEach((item, index) => {
        order.set(normalizeGroupName(item.id), index);
        order.set(normalizeGroupName(item.name), index);
    });
    return groups.slice().sort((a, b) => {
        const ai = order.has(normalizeGroupName(a)) ? order.get(normalizeGroupName(a)) : Number.MAX_SAFE_INTEGER;
        const bi = order.has(normalizeGroupName(b)) ? order.get(normalizeGroupName(b)) : Number.MAX_SAFE_INTEGER;
        return ai - bi || a.localeCompare(b, "it", { sensitivity: "base" });
    });
}
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
    String(extra).replace(/\.json$/i, "").split("&").forEach(pair => {
        const i = pair.indexOf("="); if (i === -1) return;
        const n = decodeURIComponent(pair.slice(0, i)); const v = pair.slice(i + 1);
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
    if (memoryCache.channelInflight[configKey]) return memoryCache.channelInflight[configKey];
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
        const selectedGroups = getSelectedGroups(config);
        const selectedSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";

        const parsedGroups = await Promise.all(lists.map(fetchChannelsFromSource));

        const rawChannels = parsedGroups.flat()
            .filter(c => !isDividerChannelName(c.name))
            .filter(c => {
                if (config.gm === "list" || config.gm === "bucket") return true;
                if (selectedSet.size === 0) return true;
                return selectedSet.has(normalizeGroupName(c.group));
            })
            .map(c => {
                return {
                    ...c,
                    name: decorateChannelName(c, lists.length, config.gm),
                    group: config.gm === "bucket" ? cleanGroupName(bucketGroup) : getDisplayGroupName(config, c.group)
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
    const hadCache = !!memoryCache.channelItems[configKey];
    const channels = await getChannelsFromCache(configKey, config);
    ch = channels.find(c => c.id === id);
    if (ch) return ch;
    if (!hadCache) return null;
    await fetchAndProcessChannels(configKey, config, { force: true });
    return memoryCache.channelIndex[configKey]?.[id] || null;
}

async function getLogoDataUri(logoUrl) {
    if (!isHttpUrl(logoUrl)) return "";
    if (memoryCache.logoData[logoUrl]) return memoryCache.logoData[logoUrl];
    const failedAt = memoryCache.logoFailures[logoUrl] || 0;
    if (failedAt && Date.now() - failedAt < LOGO_FAILURE_TTL_MS) return "";
    try {
        const r = await axios.get(logoUrl, {
            responseType: "arraybuffer", timeout: LOGO_REQUEST_TIMEOUT, maxContentLength: 2 * 1024 * 1024,
            ...upstreamAgentOptions(),
            headers: {
                "User-Agent": `Kronos/${RELEASE_VERSION}`,
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
            }
        });
        const ct = String(r.headers["content-type"] || "image/png").split(";")[0];
        const dataUri = `data:${ct};base64,${Buffer.from(r.data).toString("base64")}`;
        if (Object.keys(memoryCache.logoData).length >= LOGO_MAX_CACHE_ITEMS) {
            delete memoryCache.logoData[Object.keys(memoryCache.logoData)[0]];
        }
        memoryCache.logoData[logoUrl] = dataUri;
        delete memoryCache.logoFailures[logoUrl];
        return dataUri;
    } catch {
        memoryCache.logoFailures[logoUrl] = Date.now();
        return "";
    }
}

function buildStream(channel, host, configKey) {
    if (isHlsUrl(channel.url)) {
        return {
            title: channel.name, name: "TV",
            url: `${host}/${configKey}/proxy/live.m3u8?u=${encodeProxyUrl(channel.url)}`,
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

function toMeta(channel, host, configKey = "", options = {}) {
    const fallbackLogo = `${host}/logo.svg`;
    const poster = configKey
        ? `${host}/${configKey}/poster/${channel.id}.svg?v=${encodeURIComponent(RELEASE_VERSION)}`
        : (channel.logo || fallbackLogo);
    const logo = channel.logo || poster || fallbackLogo;
    const includeVideos = options.includeVideos !== false;
    const stream = includeVideos && configKey ? buildStream(channel, host, configKey) : null;
    const meta = {
        id: channel.id, type: ADDON_TYPE, name: channel.name,
        poster, logo, description: channel.description, posterShape: "square", background: poster,
        genres: channel.group ? [channel.group] : undefined,
        behaviorHints: { defaultVideoId: channel.id, hasScheduledVideos: false }
    };
    if (includeVideos) {
        meta.videos = [{
            id: channel.id, title: channel.name, released: new Date(0).toISOString(),
            thumbnail: poster, overview: channel.description, available: true,
            streams: stream ? [stream] : undefined
        }];
    }
    return meta;
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
            const groups = sortGroupsForManifest(config, [...new Set(catalogChannels.map(c => c.group))]
                .filter(g => g && g.trim()));
            const extra = [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }];
            if (groups.length > 0) extra.push({ name: "genre", options: groups, isRequired: false });
            return { id: toCatalogId(list.name), type: ADDON_TYPE, name: list.name, extra };
        });

        res.json({
            id: "org.stremio.kronos.channel",
            version: RELEASE_VERSION,
            name: "Canali TV", description: "Canali TV",
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
        const source = {
            name: req.body.name || "Lista",
            url: req.body.url,
            type: normalizeSourceType(req.body.type)
        };
        const channels = await fetchChannelsFromSource(source);
        const map = new Map();
        channels.forEach(c => map.set(c.group, (map.get(c.group) || 0) + 1));
        const groups = [...map.entries()].map(([name, count]) => ({ name, count }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
        res.json({ totalChannels: channels.length, groups });
    } catch { res.status(400).json({ error: "Impossibile analizzare questa sorgente" }); }
});

app.post("/api/analyze-lists", async (req, res) => {
    try {
        const lists = getConfiguredLists({ l: req.body.lists || [] });
        const parsed = await Promise.all(lists.map(fetchChannelsFromSource));
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
    } catch { res.status(400).json({ error: "Impossibile analizzare le sorgenti" }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// Catalog / meta / poster
// ─────────────────────────────────────────────────────────────────────────────
async function catalogResponse(req, res) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    try {
        const startedAt = Date.now();
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
        const skip = Math.max(0, Number.parseInt(params.skip, 10) || 0);
        const page = filtered.slice(skip, skip + CATALOG_PAGE_SIZE);
        const elapsed = Date.now() - startedAt;
        if (elapsed > 100 || filtered.length > CATALOG_PAGE_SIZE) {
            console.log(`[CATALOG SERVE] id=${req.params.id} total=${filtered.length} skip=${skip} page=${page.length} ms=${elapsed}`);
        }
        res.setHeader("X-Kronos-Total", String(filtered.length));
        res.setHeader("X-Kronos-Skip", String(skip));
        res.setHeader("X-Kronos-Limit", String(CATALOG_PAGE_SIZE));
        res.json({ metas: page.map(c => toMeta(c, host, configKey, { includeVideos: false })) });
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
// Transparent relay. The playlist is NOT geo-blocked anymore, so Kronos no longer
// needs a VPN, an ffmpeg re-mux, or a local buffer. It just relays bytes upstream
// with the right User-Agent and lets Stremio's own player do the HLS work. The
// only tuning is a small EXT-X-START so playback begins a touch behind the live
// edge (a stable runway) instead of glued to the edge.
// ─────────────────────────────────────────────────────────────────────────────
function encodeProxyUrl(url) { return Buffer.from(String(url), "utf8").toString("base64url"); }
function decodeProxyUrl(enc) { return Buffer.from(String(enc), "base64url").toString("utf8"); }
function toAbsoluteUrl(value, baseUrl) { try { return new URL(value, baseUrl).toString(); } catch { return value; } }

const RELAY_HEADERS = { "User-Agent": UPSTREAM_UA, "Accept": "*/*" };

function hlsManifestCacheKey(configKey, upstream) {
    return hashKey(`${configKey}|${upstream}`);
}

function hlsSegmentMapKey(configKey, parentPlaylistUrl) {
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
        const pathId = decodeURIComponent(parsed.pathname.split("/").filter(Boolean).pop() || parsed.pathname || "");
        return `${parsed.hostname}${parsed.pathname}${stableParams.length ? `?${stableParams.join("&")}` : ""}` || pathId || hashKey(url);
    } catch {
        return hashKey(url);
    }
}

function rememberSegmentUrl(configKey, parentPlaylistUrl, segmentUrl) {
    if (!SEGMENT_TOKEN_HEALING || !isHttpUrl(parentPlaylistUrl) || !isHttpUrl(segmentUrl)) return;
    const key = hlsSegmentMapKey(configKey, parentPlaylistUrl);
    const map = memoryCache.hlsSegmentMap[key] || (memoryCache.hlsSegmentMap[key] = {});
    map[segmentIdentity(segmentUrl)] = segmentUrl;
    const keys = Object.keys(map);
    if (keys.length > 500) {
        for (const staleKey of keys.slice(0, keys.length - 500)) delete map[staleKey];
    }
}

function getRememberedSegmentUrl(configKey, parentPlaylistUrl, identity) {
    if (!SEGMENT_TOKEN_HEALING || !identity) return "";
    return memoryCache.hlsSegmentMap[hlsSegmentMapKey(configKey, parentPlaylistUrl)]?.[identity] || "";
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
    return retryOperation("manifest", HLS_MANIFEST_RETRIES, HLS_MANIFEST_RETRY_DELAY_MS, async attempt => {
        const response = await axios.get(upstream, {
            responseType: "text", timeout: HLS_REQUEST_TIMEOUT, maxRedirects: 5,
            headers: RELAY_HEADERS, ...upstreamAgentOptions(), validateStatus: s => s >= 200 && s < 300
        });
        const data = String(response.data || "");
        if (!data.trimStart().startsWith("#EXTM3U")) throw new Error("Invalid HLS manifest from upstream");
        if (attempt > 0) console.warn(`[PROXY M3U8 RECOVERED] attempt=${attempt + 1}`);
        return { data, finalUrl: response.request?.res?.responseUrl || upstream };
    });
}

async function getRewrittenManifest(configKey, upstream, host) {
    const key = hlsManifestCacheKey(configKey, upstream);
    if (memoryCache.hlsManifestInflight[key]) return memoryCache.hlsManifestInflight[key];

    const promise = (async () => {
        try {
            const fetched = await fetchUpstreamManifest(upstream);
            const text = rewriteManifest(fetched.data, fetched.finalUrl, host, configKey, upstream);
            memoryCache.hlsManifestData[key] = { text, updatedAt: Date.now(), upstream };
            return { text, stale: false };
        } catch (err) {
            const cached = memoryCache.hlsManifestData[key];
            const age = cached ? Date.now() - cached.updatedAt : Infinity;
            if (cached && age <= HLS_STALE_MANIFEST_TTL_MS) {
                console.warn(`[PROXY M3U8 STALE] age=${Math.round(age / 1000)}s reason=${err.message}`);
                return { text: cached.text, stale: true };
            }
            throw err;
        }
    })();

    memoryCache.hlsManifestInflight[key] = promise;
    try { return await promise; }
    finally { delete memoryCache.hlsManifestInflight[key]; }
}

async function fetchSegmentStream(upstream, headers) {
    return retryOperation("segment", SEGMENT_UPSTREAM_RETRIES, SEGMENT_UPSTREAM_RETRY_DELAY_MS, async attempt => {
        const response = await axios.get(upstream, {
            responseType: "stream", timeout: SEG_REQUEST_TIMEOUT, maxRedirects: 5, headers,
            ...upstreamAgentOptions(), decompress: false, maxContentLength: Infinity, maxBodyLength: Infinity,
            validateStatus: s => s >= 200 && s < 300
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

async function fetchSegmentWithHealing(configKey, upstream, headers, req) {
    try {
        return await fetchSegmentStream(upstream, headers);
    } catch (err) {
        const parentPlaylistUrl = decodeProxyUrl(req.query.p || "");
        const identity = String(req.query.s || segmentIdentity(upstream));
        if (!SEGMENT_TOKEN_HEALING || !isHttpUrl(parentPlaylistUrl) || !isRecoverableSegmentError(err)) throw err;

        try {
            await getRewrittenManifest(configKey, parentPlaylistUrl, getPublicHost(req));
            const healedUrl = getRememberedSegmentUrl(configKey, parentPlaylistUrl, identity);
            if (healedUrl && healedUrl !== upstream) {
                console.warn(`[PROXY SEG HEALED] status=${err?.response?.status || err?.code || "error"}`);
                return await fetchSegmentStream(healedUrl, headers);
            }
        } catch (healErr) {
            console.warn(`[PROXY SEG HEAL FAILED] ${healErr.message}`);
        }

        throw err;
    }
}

function segmentProxyUrl(host, configKey, abs, parentPlaylistUrl = "", sequence = null) {
    const params = new URLSearchParams({ u: encodeProxyUrl(abs) });
    if (SEGMENT_TOKEN_HEALING && parentPlaylistUrl && isHttpUrl(parentPlaylistUrl)) {
        params.set("p", encodeProxyUrl(parentPlaylistUrl));
        params.set("s", segmentIdentity(abs));
    }
    if (Number.isFinite(sequence)) params.set("q", String(sequence));
    return `${host}/${configKey}/proxy/seg?${params.toString()}`;
}

function manifestProxyUrl(host, configKey, abs) {
    return `${host}/${configKey}/proxy/live.m3u8?u=${encodeProxyUrl(abs)}`;
}

function rewriteUriAttributes(line, baseUrl, makeUrl) {
    return line.replace(/URI=(["'])(.*?)\1/gi, (_m, q, uri) => `URI=${q}${makeUrl(toAbsoluteUrl(uri, baseUrl))}${q}`);
}

function isSegmentPreludeTag(line) {
    return /^#EXT-X-(PROGRAM-DATE-TIME|DISCONTINUITY|BYTERANGE|GAP|KEY|MAP|PART|PRELOAD-HINT)\b/i.test(line);
}

function parseMediaPlaylist(text) {
    const lines = String(text || "").split(/\r?\n/);
    const preamble = [];
    const segments = [];
    const footer = [];
    let pending = [];
    let current = null;
    let targetDuration = 6;
    let mediaSequence = 0;
    let sawSegment = false;

    for (const line of lines) {
        const t = line.trim();
        const targetMatch = t.match(/^#EXT-X-TARGETDURATION:(\d+(?:\.\d+)?)/i);
        if (targetMatch) targetDuration = Number(targetMatch[1]) || targetDuration;
        const seqMatch = t.match(/^#EXT-X-MEDIA-SEQUENCE:(\d+)/i);
        if (seqMatch) mediaSequence = Number(seqMatch[1]) || 0;

        if (/^#EXTINF:/i.test(t)) {
            const duration = Number((t.match(/^#EXTINF:([\d.]+)/i) || [])[1]) || targetDuration;
            current = {
                lines: [...pending, line],
                duration,
                uri: "",
                sequence: mediaSequence + segments.length
            };
            pending = [];
            continue;
        }

        if (current) {
            current.lines.push(line);
            if (t && !t.startsWith("#")) {
                current.uri = t;
                segments.push(current);
                current = null;
                sawSegment = true;
            }
            continue;
        }

        if (sawSegment && isSegmentPreludeTag(t)) {
            pending.push(line);
        } else if (sawSegment) {
            footer.push(...pending, line);
            pending = [];
        } else if (isSegmentPreludeTag(t) && !/^#EXT-X-(KEY|MAP)\b/i.test(t)) {
            pending.push(line);
        } else {
            preamble.push(line);
        }
    }

    if (current?.lines.length) footer.push(...current.lines);
    if (pending.length) footer.push(...pending);
    return { preamble, segments, footer, targetDuration, mediaSequence, isEnded: /#EXT-X-ENDLIST/i.test(text) };
}

function applyLiveEdgeHoldBack(segments, targetDuration, isEnded) {
    if (isEnded || LIVE_EDGE_HOLD_BACK_SECONDS <= 0 || segments.length <= LIVE_EDGE_MIN_SEGMENTS) return segments;
    let keep = segments.length;
    let hiddenDuration = 0;
    while (keep > LIVE_EDGE_MIN_SEGMENTS && hiddenDuration < LIVE_EDGE_HOLD_BACK_SECONDS) {
        keep -= 1;
        hiddenDuration += Number(segments[keep]?.duration) || targetDuration || 6;
    }
    return segments.slice(0, keep);
}

function edgeStateKey(configKey, parentPlaylistUrl) {
    return hlsManifestCacheKey(configKey, `${parentPlaylistUrl}|edge`);
}

function rememberEdgeSegmentRequest(configKey, parentPlaylistUrl, sequence) {
    if (!Number.isFinite(sequence) || !isHttpUrl(parentPlaylistUrl)) return;
    const key = edgeStateKey(configKey, parentPlaylistUrl);
    const state = memoryCache.hlsEdgeState[key];
    if (!state || !Number.isFinite(state.endSeq) || sequence < state.endSeq) return;
    if (state.edgeWaitFromSeq === sequence) return;
    state.edgeWaitFromSeq = sequence;
    state.edgeWaitStartedAt = Date.now();
    state.edgeWaitLogged = false;
    console.warn(`[HLS EDGE HIT] seq=${sequence} target=${sequence + LIVE_EDGE_BATCH_SEGMENTS} minWait=${Math.round(LIVE_EDGE_BATCH_MIN_WAIT_MS / 1000)}s`);
}

function applyLiveEdgeBatch(configKey, parentPlaylistUrl, segments, isEnded) {
    if (isEnded || LIVE_EDGE_BATCH_SEGMENTS <= 1 || LIVE_EDGE_BATCH_MAX_WAIT_MS <= 0 || !segments.length) return segments;
    const key = edgeStateKey(configKey, parentPlaylistUrl);
    const endSeq = segments[segments.length - 1]?.sequence;
    if (!Number.isFinite(endSeq)) return segments;
    const now = Date.now();
    const state = memoryCache.hlsEdgeState[key];

    if (!state || endSeq < state.endSeq) {
        memoryCache.hlsEdgeState[key] = { endSeq, servedAt: now, segments };
        return segments;
    }

    if (Number.isFinite(state.edgeWaitFromSeq)) {
        const advanced = endSeq - state.edgeWaitFromSeq;
        const waited = now - (state.edgeWaitStartedAt || now);
        const minWaitPending = waited < LIVE_EDGE_BATCH_MIN_WAIT_MS;
        if ((advanced < LIVE_EDGE_BATCH_SEGMENTS || minWaitPending) && waited < LIVE_EDGE_BATCH_MAX_WAIT_MS && state.segments?.length) {
            if (!state.edgeWaitLogged) {
                state.edgeWaitLogged = true;
                console.warn(`[HLS EDGE WAIT] from=${state.edgeWaitFromSeq} current=${endSeq} need=${LIVE_EDGE_BATCH_SEGMENTS} minWait=${Math.round(LIVE_EDGE_BATCH_MIN_WAIT_MS / 1000)}s`);
            }
            return state.segments;
        }
        console.warn(`[HLS EDGE RELEASE] from=${state.edgeWaitFromSeq} current=${endSeq} advanced=${advanced} waited=${Math.round(waited / 1000)}s`);
    }

    memoryCache.hlsEdgeState[key] = { endSeq, servedAt: now, segments };
    return segments;
}

function rewritePreambleForSegments(preamble, segments, baseUrl, makeSegmentUrl) {
    const firstSeq = segments[0]?.sequence;
    let wroteMediaSequence = false;
    const out = preamble
        .filter(line => !/^#EXT-X-START\b/i.test(line.trim()))
        .map(line => {
            const t = line.trim();
            if (/^#EXT-X-MEDIA-SEQUENCE:/i.test(t) && Number.isFinite(firstSeq)) {
                wroteMediaSequence = true;
                return `#EXT-X-MEDIA-SEQUENCE:${firstSeq}`;
            }
            return t.startsWith("#") ? rewriteUriAttributes(line, baseUrl, makeSegmentUrl) : line;
        });

    if (!wroteMediaSequence && Number.isFinite(firstSeq)) {
        const insertAt = out[0]?.trim() === "#EXTM3U" ? 1 : 0;
        out.splice(insertAt, 0, `#EXT-X-MEDIA-SEQUENCE:${firstSeq}`);
    }
    return out;
}

function rewriteMediaManifest(text, baseUrl, host, configKey, parentPlaylistUrl = baseUrl) {
    const parsed = parseMediaPlaylist(text);
    for (const segment of parsed.segments) {
        if (segment.uri) rememberSegmentUrl(configKey, parentPlaylistUrl, toAbsoluteUrl(segment.uri, baseUrl));
    }

    const stableSegments = applyLiveEdgeBatch(
        configKey,
        parentPlaylistUrl,
        applyLiveEdgeHoldBack(parsed.segments, parsed.targetDuration, parsed.isEnded),
        parsed.isEnded
    );

    const makeSegmentUrl = abs => segmentProxyUrl(host, configKey, abs, parentPlaylistUrl);
    const out = rewritePreambleForSegments(parsed.preamble, stableSegments, baseUrl, makeSegmentUrl);

    if (!parsed.isEnded && PROXY_START_OFFSET_SECONDS > 0) {
        out.push(`#EXT-X-START:TIME-OFFSET=-${PROXY_START_OFFSET_SECONDS},PRECISE=YES`);
    }

    for (const segment of stableSegments) {
        for (const line of segment.lines) {
            const t = line.trim();
            if (!t) { out.push(line); continue; }
            if (t.startsWith("#")) {
                out.push(rewriteUriAttributes(line, baseUrl, makeSegmentUrl));
            } else {
                out.push(segmentProxyUrl(host, configKey, toAbsoluteUrl(t, baseUrl), parentPlaylistUrl, segment.sequence));
            }
        }
    }

    for (const line of parsed.footer) {
        if (/^#EXT-X-START\b/i.test(line.trim())) continue;
        out.push(line.trim().startsWith("#") ? rewriteUriAttributes(line, baseUrl, makeSegmentUrl) : line);
    }

    return out.join("\n");
}

// Rewrite an upstream HLS manifest so every URL points back through our relay.
// Master variants are kept as manifests; media segments and key URIs go through
// /proxy/seg with enough parent context to recover if the provider rotates tokens.
function rewriteManifest(text, baseUrl, host, configKey, parentPlaylistUrl = baseUrl) {
    const isMaster = /#EXT-X-STREAM-INF/i.test(text);
    if (!isMaster && /#EXTINF:/i.test(text)) {
        return rewriteMediaManifest(text, baseUrl, host, configKey, parentPlaylistUrl);
    }

    const out = [];
    for (const line of text.split(/\r?\n/)) {
        const t = line.trim();
        if (!t) { out.push(line); continue; }
        if (t.startsWith("#")) {
            out.push(/URI=/i.test(t)
                ? rewriteUriAttributes(line, baseUrl, abs => segmentProxyUrl(host, configKey, abs, parentPlaylistUrl))
                : line);
            continue;
        }
        const abs = toAbsoluteUrl(t, baseUrl);
        if (isMaster || isHlsUrl(abs)) { out.push(manifestProxyUrl(host, configKey, abs)); continue; }
        out.push(segmentProxyUrl(host, configKey, abs, parentPlaylistUrl));
    }
    return out.join("\n");
}

// Relay (and rewrite) an HLS manifest.
app.get("/:base64Config/proxy/live.m3u8", async (req, res) => {
    const configKey = req.params.base64Config;
    try {
        decodeConfig(configKey);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).type("text/plain").send("#EXTM3U\n");
        const manifest = await getRewrittenManifest(configKey, upstream, getPublicHost(req));
        setPlaylistHeaders(res);
        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Cache", manifest.stale ? "stale" : "fresh");
        res.send(manifest.text);
    } catch (err) {
        console.error(`[PROXY M3U8] ${err?.message || err}`);
        if (!res.headersSent) res.status(502).type("text/plain").send("#EXTM3U\n");
    }
});

// Relay a segment (or any raw stream) byte-for-byte, forwarding Range for seeking.
app.get("/:base64Config/proxy/seg", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).end();
        const parentPlaylistUrl = decodeProxyUrl(req.query.p || "");
        const sequence = Number(req.query.q);
        rememberEdgeSegmentRequest(configKey, parentPlaylistUrl, sequence);
        const headers = { ...RELAY_HEADERS };
        if (req.headers.range) headers.Range = req.headers.range;
        const r = await fetchSegmentWithHealing(configKey, upstream, headers, req);
        res.status(r.status);
        for (const h of ["content-type", "content-length", "content-range", "accept-ranges", "last-modified", "etag"]) {
            if (r.headers[h]) res.setHeader(h, r.headers[h]);
        }
        res.setHeader("Cache-Control", "no-store");
        res.setHeader("X-Kronos-Relay", "1");
        r.data.on("error", () => { if (!res.headersSent) res.status(502).end(); else res.destroy(); });
        res.on("close", () => { try { r.data.destroy(); } catch {} });
        r.data.pipe(res);
    } catch (err) {
        if (!res.headersSent) res.status(502).end();
    }
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
            logos: Object.keys(memoryCache.logoData).length,
            hlsManifests: Object.keys(memoryCache.hlsManifestData).length
        },
        playback: {
            mode: "transparent-relay",
            vpn: false,
            startOffsetSeconds: PROXY_START_OFFSET_SECONDS,
            liveEdgeHoldBackSeconds: LIVE_EDGE_HOLD_BACK_SECONDS,
            liveEdgeMinSegments: LIVE_EDGE_MIN_SEGMENTS,
            liveEdgeBatchSegments: LIVE_EDGE_BATCH_SEGMENTS,
            liveEdgeBatchMaxWaitMs: LIVE_EDGE_BATCH_MAX_WAIT_MS,
            liveEdgeBatchMinWaitMs: LIVE_EDGE_BATCH_MIN_WAIT_MS,
            segmentTokenHealing: SEGMENT_TOKEN_HEALING,
            manifestRetries: HLS_MANIFEST_RETRIES,
            staleManifestTtlMs: HLS_STALE_MANIFEST_TTL_MS,
            segmentRetries: SEGMENT_UPSTREAM_RETRIES
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
                sourceTypes: getConfiguredLists(config).map(list => list.type),
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
                sourceType: c.sourceType || "m3u", hasUrl: !!c.url, hasLogo: !!c.logo, hasEpg: !!c.description, epgId: c.epgId
            })),
            groups: [...new Set(channels.map(c => c.group))].slice(0, 50),
            constants: {
                ADDON_TYPE, RELEASE_VERSION, PLAYBACK_MODE,
                HLS_REQUEST_TIMEOUT, SEG_REQUEST_TIMEOUT, PROXY_START_OFFSET_SECONDS,
                LIVE_EDGE_HOLD_BACK_SECONDS, LIVE_EDGE_MIN_SEGMENTS, LIVE_EDGE_BATCH_SEGMENTS,
                LIVE_EDGE_BATCH_MAX_WAIT_MS, LIVE_EDGE_BATCH_MIN_WAIT_MS, SEGMENT_TOKEN_HEALING,
                HLS_MANIFEST_RETRIES, HLS_MANIFEST_RETRY_DELAY_MS, HLS_STALE_MANIFEST_TTL_MS,
                SEGMENT_UPSTREAM_RETRIES, SEGMENT_UPSTREAM_RETRY_DELAY_MS,
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
    console.log(`🎬 playback=transparent-relay vpn=off remux=off startOffset=${PROXY_START_OFFSET_SECONDS}s hlsTimeout=${HLS_REQUEST_TIMEOUT / 1000}s segTimeout=${SEG_REQUEST_TIMEOUT / 1000}s`);
    console.log(`🛡️ live edge holdBack=${LIVE_EDGE_HOLD_BACK_SECONDS}s minSegments=${LIVE_EDGE_MIN_SEGMENTS} edgeCatchupSegments=${LIVE_EDGE_BATCH_SEGMENTS} edgeCatchupMinWait=${LIVE_EDGE_BATCH_MIN_WAIT_MS / 1000}s edgeCatchupMaxWait=${LIVE_EDGE_BATCH_MAX_WAIT_MS / 1000}s tokenHealing=${SEGMENT_TOKEN_HEALING ? 1 : 0}`);
    console.log(`🔌 upstream keepAlive=1 sockets=${UPSTREAM_KEEPALIVE_MAX_SOCKETS} free=${UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS} ms=${UPSTREAM_KEEPALIVE_MS}`);
    console.log(`📋 playlist retryWindow=${PLAYLIST_RETRY_WINDOW_MS / 1000}s delay=${PLAYLIST_RETRY_DELAY_MS / 1000}s`);
    console.log(`🎞️ hls retries=${HLS_MANIFEST_RETRIES} staleTtl=${HLS_STALE_MANIFEST_TTL_MS / 1000}s segmentRetries=${SEGMENT_UPSTREAM_RETRIES} rangeForward=1`);
    console.log(`📺 epg timezone=${EPG_TIME_ZONE} timeout=${EPG_REQUEST_TIMEOUT / 1000}s firstWait=${EPG_FIRST_CATALOG_WAIT_MS / 1000}s retryDelay=${EPG_RETRY_DELAY_MS / 1000}s cacheTTL=${EPG_CACHE_TTL / 1000}s refresh=${EPG_REFRESH_INTERVAL_MS / 1000}s preload=${EPG_PRELOAD_URLS.length} startupWatch=${EPG_STARTUP_WATCH_MS / 1000}s`);
    console.log(`📚 catalog preload=${CATALOG_PRELOAD_CONFIGS.length}`);
    console.log("=".repeat(60) + "\n");
    startEPGStartupPreload();
    startCatalogStartupPreload();
    startCatalogPeriodicRefresh();
});
