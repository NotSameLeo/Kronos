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

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────
const UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const RELEASE_VERSION = "2.0.0";
const ADDON_TYPE = "tv";
const CATALOG_TTL = 30 * 60 * 1000;
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 20000);
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 45000);

// Manifest retry — fixes Xtream "invalid manifest on first hit" (stream spins up
// server-side on first request, returns garbage, is valid on retry).
const MANIFEST_RETRIES = Number(process.env.MANIFEST_RETRIES || 3);
const MANIFEST_RETRY_DELAY = Number(process.env.MANIFEST_RETRY_DELAY || 700);
const MANIFEST_RETRY_DEADLINE = Number(process.env.MANIFEST_RETRY_DEADLINE || 28000);

// Light read-ahead segment cache — absorbs upstream jitter, does NOT create
// bandwidth. Kept small and simple on purpose (no Range caching = no corruption).
const SEGMENT_PREFETCH_AHEAD = Number(process.env.SEGMENT_PREFETCH_AHEAD || 3);
// Provider exposes max_connections=1, so concurrency 1 by default: in steady state
// the prefetcher is the ONLY upstream connection and the player reads from cache.
// (Raising this only helps if your provider tolerates parallel segment fetches.)
const SEGMENT_PREFETCH_CONCURRENCY = Number(process.env.SEGMENT_PREFETCH_CONCURRENCY || 1);
const SEGMENT_CACHE_TTL = Number(process.env.SEGMENT_CACHE_TTL || 90 * 1000);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_BYTES || 256 * 1024 * 1024);
const SEGMENT_CACHE_MAX_ITEMS = Number(process.env.SEGMENT_CACHE_MAX_ITEMS || 48);
const SEGMENT_CACHE_MAX_ITEM_BYTES = Number(process.env.SEGMENT_CACHE_MAX_ITEM_BYTES || 24 * 1024 * 1024);
const SLOW_SEGMENT_MS = Number(process.env.SLOW_SEGMENT_MS || 4000);

// Window warm-up — for channels whose upstream exposes a too-shallow live window
// (e.g. starts fresh at seq=0 with 1 segment). We wait for the window to accumulate
// a minimum amount of content before handing it to the player, so it starts with a
// real cushion instead of starving at the live edge. Self-gating: does nothing when
// the window is already deep, and bails if the window isn't growing.
// Warm-up target is SEGMENT-COUNT based (robust to the provider's changing
// targetDuration). We hold the channel open until the live window is at least
// this many segments deep, so the player attaches BEHIND the live edge (cushion)
// instead of pinned to the bleeding edge of a freshly-started stream.
const MIN_START_SEGMENTS = Number(process.env.MIN_START_SEGMENTS || 4);
const WINDOW_WARM_TIMEOUT = Number(process.env.WINDOW_WARM_TIMEOUT || 45000);
const WINDOW_WARM_POLL = Number(process.env.WINDOW_WARM_POLL || 2500);
// Only warm a channel when it's a fresh (re)open. If the same channel was served
// within this window, it's continuous playback → skip the wait, just refresh.
const REWARM_AFTER_MS = Number(process.env.REWARM_AFTER_MS || 45000);

// ─────────────────────────────────────────────────────────────────────────────
// In-memory caches (catalog/EPG/logos only — no playback buffering anymore)
// ─────────────────────────────────────────────────────────────────────────────
const memoryCache = {
    channelItems: {},     // configKey -> channels[]
    channelIndex: {},     // configKey -> { id -> channel }
    channelInflight: {},  // configKey -> Promise
    lastUpdate: {},       // configKey -> timestamp
    epgData: {},          // epgUrl -> map
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

// ─────────────────────────────────────────────────────────────────────────────
// EPG
// ─────────────────────────────────────────────────────────────────────────────
async function updateEPGCache(epgUrl) {
    if (!epgUrl) return {};
    try {
        const response = await axios.get(epgUrl, { timeout: 15000 });
        const parser = new xml2js.Parser({ explicitArray: false, ignoreAttrs: false });
        const result = await parser.parseStringPromise(response.data);
        const byChannel = {};
        if (result.tv && result.tv.programme) {
            const programmes = Array.isArray(result.tv.programme) ? result.tv.programme : [result.tv.programme];
            programmes.forEach(prog => {
                if (!prog.$ || !prog.$.channel || !prog.$.start || !prog.$.stop) return;
                const start = parseXMLTVDate(prog.$.start);
                const stop = parseXMLTVDate(prog.$.stop);
                if (Number.isNaN(start.getTime()) || Number.isNaN(stop.getTime())) return;
                const key = normalizeEpgId(prog.$.channel);
                if (!byChannel[key]) byChannel[key] = [];
                byChannel[key].push({
                    start, stop,
                    title: getXmlText(prog.title) || "Programma senza titolo",
                    desc: getXmlText(prog.desc)
                });
            });
        }
        const map = {};
        Object.entries(byChannel).forEach(([k, list]) => {
            const sel = selectBestProgramme(list);
            if (!sel) return;
            const label = sel.isLive ? "In onda" : "EPG disponibile";
            const desc = sel.desc ? ` - ${sel.desc}` : "";
            map[k] = `${label}: ${sel.title} (${formatTime(sel.start)} - ${formatTime(sel.stop)})${desc}`;
        });
        memoryCache.epgData[epgUrl] = map;
        return map;
    } catch {
        return memoryCache.epgData[epgUrl] || {};
    }
}
function parseXMLTVDate(str) {
    const m = String(str || "").match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-]\d{4}))?/);
    if (!m) return new Date(str);
    const [, Y, Mo, D, H, Mi, S, off] = m;
    const tz = off ? `${off.slice(0, 3)}:${off.slice(3)}` : "Z";
    return new Date(`${Y}-${Mo}-${D}T${H}:${Mi}:${S}${tz}`);
}
function getXmlText(v) { if (!v) return ""; if (typeof v === "string") return v; if (typeof v === "object" && v._) return v._; return ""; }
function formatTime(d) { return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`; }
function selectBestProgramme(programmes) {
    const now = new Date();
    const sorted = programmes.slice().sort((a, b) => a.start - b.start);
    const live = sorted.find(p => now >= p.start && now <= p.stop);
    if (live) return { ...live, isLive: true };
    const future = sorted.find(p => p.start > now);
    if (future) return { ...future, isLive: false };
    const latest = sorted[sorted.length - 1];
    return latest ? { ...latest, isLive: false } : null;
}
function normalizeEpgId(id) {
    let k = String(id || "").toLowerCase().trim();
    k = k.replace(/\.it$/i, "").replace(/[^a-z0-9]/g, "").replace(/hd$/i, "");
    if (k === "20mediaset") return "20";
    return k;
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
    const response = await axios.get(sourceUrl, {
        timeout: options.timeout || 60000,
        maxRedirects: 5,
        headers: {
            "User-Agent": UPSTREAM_UA,
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        },
        validateStatus: s => s >= 200 && s < 300
    });
    console.log("[FETCH PLAYLIST OK]", sourceUrl, "size=" + response.data.length);
    return response.data;
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
        if (config.e) await updateEPGCache(config.e);
        const lists = getConfiguredLists(config);
        const selectedGroups = Array.isArray(config.g) ? config.g : [];
        const selectedSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";

        const parsedGroups = await Promise.all(lists.map(async list => {
            const data = await fetchPlaylist(list.url);
            return parseM3UChannels(data, list);
        }));

        const channels = parsedGroups.flat()
            .filter(c => {
                if (config.gm === "list" || config.gm === "bucket") return true;
                if (selectedSet.size === 0) return true;
                return selectedSet.has(normalizeGroupName(c.group));
            })
            .map(c => ({
                ...c,
                name: decorateChannelName(c, lists.length, config.gm),
                group: config.gm === "bucket" ? bucketGroup : c.group,
                description: ""
            }));

        memoryCache.channelItems[configKey] = channels;
        memoryCache.channelIndex[configKey] = buildChannelIndex(channels);
        memoryCache.lastUpdate[configKey] = Date.now();
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
// HLS proxy — transparent: fetch, rewrite URLs, return as-is.
// No buffering, no startup phases, no playlist rebuild, no session machinery.
// ─────────────────────────────────────────────────────────────────────────────
// Fetch a manifest with bounded retries. Retries on invalid body (Xtream stream
// spin-up) and on network/timeout errors, until MANIFEST_RETRIES or the deadline.
async function fetchUpstreamHLS(sourceUrl, label = "stream") {
    const deadline = Date.now() + MANIFEST_RETRY_DEADLINE;
    let lastErr = null;

    for (let attempt = 0; attempt <= MANIFEST_RETRIES; attempt++) {
        if (attempt > 0) {
            if (Date.now() >= deadline) break;
            await sleep(MANIFEST_RETRY_DELAY);
        }
        try {
            const r = await axios.get(sourceUrl, {
                timeout: HLS_REQUEST_TIMEOUT,
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
                lastErr = new Error("Invalid HLS manifest from upstream");
                console.warn(`[HLS RETRY] channel="${label}" attempt=${attempt + 1}/${MANIFEST_RETRIES + 1} reason=invalid-body`);
                continue;
            }
            const info = analyzeHLS(text);
            if (attempt > 0) console.log(`[HLS RECOVERED] channel="${label}" after=${attempt + 1} attempts`);
            return { data: r.data, finalUrl, info };
        } catch (err) {
            lastErr = err;
            console.warn(`[HLS RETRY] channel="${label}" attempt=${attempt + 1}/${MANIFEST_RETRIES + 1} reason=${err.message}`);
        }
    }
    throw lastErr || new Error("Manifest fetch failed");
}

// Stable identity for a segment, ignoring rotating query tokens. The upstream
// (Xtream) rotates the token on every playlist refresh, so the same physical
// segment arrives under a different URL each time. We key everything on the
// token-free path so the player sees a STABLE proxied URL (no phantom re-downloads
// / timeline jumps) and the cache deduplicates correctly.
const segRegistry = new Map();    // segId -> latest real upstream URL (with current token)
const SEG_REGISTRY_MAX = Number(process.env.SEG_REGISTRY_MAX || 4000);

function segIdFor(absUrl) {
    try { const u = new URL(absUrl); return hashKey(u.origin + u.pathname); }
    catch { return hashKey(String(absUrl).split("?")[0]); }
}

function registerSeg(id, absUrl) {
    segRegistry.set(id, absUrl);
    if (segRegistry.size > SEG_REGISTRY_MAX) {
        const oldest = segRegistry.keys().next().value;   // oldest insertion = slid-out segment
        if (oldest !== undefined && oldest !== id) segRegistry.delete(oldest);
    }
}

// Returns { rewritten, segIds } — segIds are stable segment ids in playlist order.
function rewriteHLSUrls(playlist, baseUrl, hostBase, configKey) {
    const plUrl = abs => `${hostBase}/${configKey}/proxy/pl?u=${encodeProxyUrl(abs)}`;
    const segIds = [];
    const pick = abs => {
        if (isHlsUrl(abs)) return plUrl(abs);
        const id = segIdFor(abs);
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
let segCacheBytes = 0;
let prefetchActive = 0;
const prefetchQueue = [];

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
    const existing = segCache.get(id);
    if (existing) segCacheBytes -= existing.size;
    const keep = {};
    ["content-type", "cache-control", "last-modified", "etag"].forEach(h => { if (headers[h]) keep[h] = headers[h]; });
    segCache.set(id, { buffer, headers: keep, status, size: buffer.length, fetchedAt: Date.now() });
    segCacheBytes += buffer.length;
    evictSegCache();
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

async function fetchSegToCache(id, url) {
    const hit = getSegFromCache(id);
    if (hit) return hit;
    if (segInflight.has(id)) return segInflight.get(id);
    if (!url) return null;

    const p = (async () => {
        const started = Date.now();
        const r = await axios.get(url, {
            responseType: "arraybuffer",
            timeout: SEG_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers: { "User-Agent": UPSTREAM_UA, "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive" },
            decompress: false,
            maxContentLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            maxBodyLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            validateStatus: s => s === 200
        });
        const buf = Buffer.from(r.data);
        storeSeg(id, buf, r.headers, 200);
        const ms = Date.now() - started;
        console.log(`[PREFETCH OK] ${getSegmentLabel(url)} bytes=${formatBytes(buf.length)} time=${ms}ms speed=${formatSpeed(buf.length, ms)}`);
        return getSegFromCache(id);
    })();

    segInflight.set(id, p);
    try { return await p; }
    finally { segInflight.delete(id); }
}

function queuePrefetch(ids) {
    ids.forEach(id => {
        if (getSegFromCache(id) || segInflight.has(id) || prefetchQueue.includes(id)) return;
        prefetchQueue.push(id);
    });
    pumpPrefetch();
}

function pumpPrefetch() {
    while (prefetchActive < SEGMENT_PREFETCH_CONCURRENCY && prefetchQueue.length > 0) {
        const id = prefetchQueue.shift();
        if (getSegFromCache(id) || segInflight.has(id)) continue;
        const url = segRegistry.get(id);
        if (!url) continue;
        prefetchActive++;
        fetchSegToCache(id, url)
            .catch(err => console.warn(`[PREFETCH ERR] ${getSegmentLabel(url)} ${err.message}`))
            .finally(() => { prefetchActive--; pumpPrefetch(); });
    }
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
// Playback — pass-through HLS
// ─────────────────────────────────────────────────────────────────────────────
// Fetch the manifest; if it's a live media playlist with a too-shallow window
// (a freshly-started on-demand stream), keep re-polling until it accumulates at
// least MIN_START_SEGMENTS, so the player attaches BEHIND the live edge with a
// real cushion. Bails only if the window is sliding (capped) and won't deepen.
const lastWarmServe = new Map();   // sourceUrl -> last served timestamp (set only AFTER serving)
const warmInflight = new Map();    // sourceUrl -> in-progress fetchWithWarmWindow promise

function noteWarmServe(url) {
    lastWarmServe.set(url, Date.now());
    if (lastWarmServe.size > 2000) {
        const oldest = lastWarmServe.keys().next().value;
        if (oldest !== undefined && oldest !== url) lastWarmServe.delete(oldest);
    }
}

function fetchWithWarmWindow(sourceUrl, label) {
    // Dedup: concurrent player polls during a warm share the SAME warm — they all
    // receive the deep window instead of one of them slipping through with segs=1.
    if (warmInflight.has(sourceUrl)) return warmInflight.get(sourceUrl);
    const p = (async () => {
        const last = lastWarmServe.get(sourceUrl) || 0;
        const skipWarm = Date.now() - last < REWARM_AFTER_MS;   // continuous playback → don't wait

        let best = await fetchUpstreamHLS(sourceUrl, label);
        if (skipWarm || best.info.isMaster || !best.info.isLive || best.info.segmentCount >= MIN_START_SEGMENTS) {
            noteWarmServe(sourceUrl);
            return best;
        }

        const startSegs = best.info.segmentCount;
        const started = Date.now();
        console.log(`[HLS WARM START] channel="${label}" segs=${startSegs}/${MIN_START_SEGMENTS} — building cushion before the player attaches`);

        while (best.info.segmentCount < MIN_START_SEGMENTS && Date.now() - started < WINDOW_WARM_TIMEOUT) {
            await sleep(WINDOW_WARM_POLL);
            const next = await fetchUpstreamHLS(sourceUrl, label);
            if (next.info.isMaster || !next.info.isLive) { best = next; break; }

            const grew = next.info.segmentCount > best.info.segmentCount;
            // The window is SLIDING (capped) if media-sequence advances. Only then is
            // it "as deep as it gets" — bail. If it's NOT sliding and just hasn't grown
            // yet, the encoder simply hasn't produced the next segment: keep waiting.
            const sliding = Number(next.info.mediaSequence || 0) > Number(best.info.mediaSequence || 0);
            console.log(`[HLS WARM] channel="${label}" segs=${next.info.segmentCount}/${MIN_START_SEGMENTS} seq=${next.info.mediaSequence} waited=${Date.now() - started}ms`);
            best = next;
            if (!grew && sliding) {
                console.log(`[HLS WARM CAPPED] channel="${label}" window won't deepen (sliding at ${best.info.segmentCount} segs) — serving what we have`);
                break;
            }
        }
        noteWarmServe(sourceUrl);
        console.log(`[HLS WARM DONE] channel="${label}" segs=${startSegs}→${best.info.segmentCount} waited=${Date.now() - started}ms`);
        return best;
    })();

    warmInflight.set(sourceUrl, p);
    p.finally(() => { if (warmInflight.get(sourceUrl) === p) warmInflight.delete(sourceUrl); });
    return p;
}

async function servePlaylist(res, sourceUrl, host, configKey, label, doWarm = true) {
    const { data, finalUrl, info } = doWarm
        ? await fetchWithWarmWindow(sourceUrl, label)
        : await fetchUpstreamHLS(sourceUrl, label);
    const { rewritten, segIds } = rewriteHLSUrls(data, finalUrl, host, configKey);

    // Single-connection provider: keep the prefetcher focused ONLY on the current
    // channel. Drop pending prefetch from a previously-watched channel so a switch
    // never holds two upstream connections at once. Prefetch the WHOLE served window
    // in playlist order (oldest first) so the player gets cache HITs no matter where
    // it attaches — with concurrency 1 this is the single upstream connection while
    // the player reads from RAM.
    if (!info.isMaster && info.isLive && segIds.length) {
        prefetchQueue.length = 0;
        queuePrefetch(segIds);
    }

    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
    console.log(
        `[HLS SERVE] channel="${label}" type=${info.isMaster ? "master" : "media"} live=${info.isLive}` +
        ` segs=${info.segmentCount}${info.targetDuration ? ` target=${info.targetDuration}s` : ""}` +
        `${info.mediaSequence != null ? ` seq=${info.mediaSequence}` : ""}` +
        ` cache=${segCache.size}/${formatBytes(segCacheBytes)} prefetch=${prefetchActive}+${prefetchQueue.length}`
    );
    res.send(rewritten);
}

app.get("/:base64Config/hls/:id/index.m3u8", async (req, res) => {
    const t0 = Date.now();
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        if (!ch) return res.status(404).send("#EXTM3U\n#EXT-X-ENDLIST\n");
        console.log(`[HLS OPEN] channel="${ch.name}" — request hit Kronos (resolve=${Date.now() - t0}ms)`);
        await servePlaylist(res, ch.url, getPublicHost(req), configKey, ch.name, true);
    } catch (err) {
        console.error(`[HLS ERROR] ${err.message}`);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/pl", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");
        // Nested/variant playlist: no warm-up (it's polled repeatedly, not a fresh open).
        await servePlaylist(res, decodeProxyUrl(req.query.u), getPublicHost(req), configKey, "nested-playlist", false);
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
                console.warn(`[SEG MISS-ID] ${segId} not in registry`);
                return res.status(404).end();
            }
        } else if (req.query.u) {
            // Direct (non-HLS) stream URL — continuous, never cached.
            sourceUrl = decodeProxyUrl(req.query.u);
        } else {
            return res.status(400).end();
        }

        // Cache path for known segIds. A cached entry is ALWAYS a full 200, so we can
        // safely satisfy Range requests by slicing it (no extra upstream connection —
        // important with max_connections=1).
        if (segId) {
            const cached = getSegFromCache(segId);
            if (cached) return sendCachedSeg(req, res, cached, segId, "HIT");

            // A prefetch for this exact segment is already in flight: WAIT for it
            // (long enough to cover a slow segment) instead of opening a second
            // upstream connection — essential with max_connections=1.
            if (segInflight.has(segId)) {
                const e = await Promise.race([segInflight.get(segId), sleep(SEG_REQUEST_TIMEOUT).then(() => null)]);
                if (e) return sendCachedSeg(req, res, e, segId, "HIT-WAIT");
            }
        }

        await streamSegment(req, res, sourceUrl, segId, started);
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
    upstream.on("error", err => {
        if (!res.headersSent) res.status(502).end();
        else res.destroy(err);
    });

    upstream.pipe(res);
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
        }
    });
});

app.get("/:base64Config/debug", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const channels = await getChannelsFromCache(configKey, config);
        res.json({
            version: RELEASE_VERSION,
            mode: "transparent-proxy",
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
                isUpdating: memoryCache.isUpdating[configKey]
            },
            sampleChannels: channels.slice(0, 5).map(c => ({
                id: c.id, name: c.name, group: c.group, sourceName: c.sourceName,
                hasUrl: !!c.url, hasLogo: !!c.logo
            })),
            groups: [...new Set(channels.map(c => c.group))].slice(0, 50),
            constants: { ADDON_TYPE, RELEASE_VERSION, HLS_REQUEST_TIMEOUT, SEG_REQUEST_TIMEOUT }
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
    console.log("\n" + "=".repeat(60));
    console.log(`K.R.O.N.O.S. ${RELEASE_VERSION} - transparent-proxy mode`);
    console.log("=".repeat(60));
    console.log(`🌐 http://0.0.0.0:${PORT}`);
    console.log(`📦 Node ${process.version}`);
    console.log(`🔁 manifest retry=${MANIFEST_RETRIES} delay=${MANIFEST_RETRY_DELAY}ms`);
    console.log(`⏩ prefetch ahead=${SEGMENT_PREFETCH_AHEAD} concurrency=${SEGMENT_PREFETCH_CONCURRENCY} cacheTTL=${SEGMENT_CACHE_TTL / 1000}s`);
    console.log(`🪟 window warm-up: target=${MIN_START_SEGMENTS} segs timeout=${WINDOW_WARM_TIMEOUT / 1000}s poll=${WINDOW_WARM_POLL / 1000}s rewarm=${REWARM_AFTER_MS / 1000}s`);
    console.log("=".repeat(60) + "\n");
});
