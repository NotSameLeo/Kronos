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
const RELEASE_VERSION = "4.0.0";
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
// Provider exposes max_connections=1, so concurrency 1: the prefetcher is the ONLY
// upstream connection and the player reads from cache.
const SEGMENT_PREFETCH_CONCURRENCY = Number(process.env.SEGMENT_PREFETCH_CONCURRENCY || 1);
// Cache TTL must exceed the retained-buffer duration so retained segments are never
// evicted out from under the player (then re-fetched).
const SEGMENT_CACHE_TTL = Number(process.env.SEGMENT_CACHE_TTL || 300 * 1000);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_BYTES || 400 * 1024 * 1024);
const SEGMENT_CACHE_MAX_ITEMS = Number(process.env.SEGMENT_CACHE_MAX_ITEMS || 40);
const SEGMENT_CACHE_MAX_ITEM_BYTES = Number(process.env.SEGMENT_CACHE_MAX_ITEM_BYTES || 24 * 1024 * 1024);
const SLOW_SEGMENT_MS = Number(process.env.SLOW_SEGMENT_MS || 4000);

// ── Background poller + retained buffer ─────────────────────────────────────────
// These channels are ON-DEMAND: the upstream starts a fresh encoder (seq=0, 1 seg)
// when first requested and tears it down when nobody fetches → on resume it restarts
// from scratch. To make playback seamless we run a background poller per active
// channel that (a) keeps the upstream stream ALIVE, (b) accumulates a deep retained
// buffer, and (c) prefetches segments. The player is then served a deep window
// IMMEDIATELY (non-blocking) from our buffer — no manifest is ever held hostage, so
// the player can never time out / crash waiting. Only the very first open does a
// short, capped prime wait to build the initial cushion.
const RETAIN_SEGMENTS = Number(process.env.RETAIN_SEGMENTS || 18);   // ~3min retained & served
// HARD anti-jump guarantee: the served live edge advances by at most this many
// segments per request, even if the upstream buffer leapt ahead → no forward jump.
const MAX_EDGE_ADVANCE = Number(process.env.MAX_EDGE_ADVANCE || 2);
const MIN_START_SEGMENTS = Number(process.env.MIN_START_SEGMENTS || 3); // initial cushion depth
const PRIME_TIMEOUT_MS = Number(process.env.PRIME_TIMEOUT_MS || 22000);  // max FIRST-open wait
// How far behind the live edge the player is told to start (deterministic cushion
// via #EXT-X-START). The retained buffer is much deeper, for stall insurance.
const TARGET_CUSHION_SECONDS = Number(process.env.TARGET_CUSHION_SECONDS || 30);
const POLLER_IDLE_STOP_MS = Number(process.env.POLLER_IDLE_STOP_MS || 90000); // stop if player gone
const POLL_MIN_MS = Number(process.env.POLL_MIN_MS || 2000);
const POLL_MAX_MS = Number(process.env.POLL_MAX_MS || 5000);

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
// ── Upstream connection gate (provider max_connections=1) ───────────────────────
// EVERY upstream request (playlist polls, segment prefetch, player-demand fetch)
// goes through this single-slot mutex, so we NEVER open two connections at once
// (which the provider aborts → cascade → player crash). Player-demand and playlist
// polls take priority over background prefetch.
let upstreamBusy = false;
const upstreamWaiters = [];   // { resolve, prio }
function acquireUpstream(prio) {
    if (!upstreamBusy) { upstreamBusy = true; return Promise.resolve(); }
    return new Promise(resolve => {
        const w = { resolve };
        if (prio) {
            const i = upstreamWaiters.findIndex(x => !x.prio);
            if (i === -1) upstreamWaiters.push(Object.assign(w, { prio: true }));
            else upstreamWaiters.splice(i, 0, Object.assign(w, { prio: true }));
        } else {
            upstreamWaiters.push(Object.assign(w, { prio: false }));
        }
    });
}
function releaseUpstream() {
    const w = upstreamWaiters.shift();
    if (w) w.resolve();        // hand the slot to the next waiter (stays busy)
    else upstreamBusy = false;
}
async function withUpstream(fn, prio = false) {
    await acquireUpstream(prio);
    try { return await fn(); }
    finally { releaseUpstream(); }
}

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
            const r = await withUpstream(() => axios.get(sourceUrl, {
                timeout: HLS_REQUEST_TIMEOUT,
                maxRedirects: 5,
                headers: {
                    "User-Agent": UPSTREAM_UA,
                    "Accept": "application/x-mpegURL, application/vnd.apple.mpegurl, audio/mpegurl, text/plain, */*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus: s => s >= 200 && s < 300
            }), true);
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

async function fetchSegToCache(id, url, prio = false) {
    const hit = getSegFromCache(id);
    if (hit) return hit;
    if (segInflight.has(id)) return segInflight.get(id);   // dedup: never two fetches of same seg
    if (!url) return null;

    const p = (async () => {
        const started = Date.now();
        const r = await withUpstream(() => axios.get(url, {
            responseType: "arraybuffer",
            timeout: SEG_REQUEST_TIMEOUT,
            maxRedirects: 5,
            headers: { "User-Agent": UPSTREAM_UA, "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive" },
            decompress: false,
            maxContentLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            maxBodyLength: SEGMENT_CACHE_MAX_ITEM_BYTES,
            validateStatus: s => s === 200
        }), prio);
        const buf = Buffer.from(r.data);
        storeSeg(id, buf, r.headers, 200);
        const ms = Date.now() - started;
        console.log(`[SEG FETCH${prio ? " *" : ""}] ${getSegmentLabel(url)} bytes=${formatBytes(buf.length)} time=${ms}ms speed=${formatSpeed(buf.length, ms)}`);
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
// Playback engine — single-connection poller + DVR buffer + smooth serve
//
// Built for slow, tokenized, on-demand HLS (Comedy Central is the benchmark):
//  • segment identity = FILENAME (token-independent) → no phantom re-downloads
//  • absolute, monotonic sequence numbers → the player timeline never jumps back
//  • ONE serialized upstream connection (max_connections=1) → no aborts
//  • deep retained buffer + #EXT-X-START cushion + smooth edge → no forward jumps
//  • restart-aware (on-demand teardown) → continues forward with a fresh generation
// ─────────────────────────────────────────────────────────────────────────────
const channels = new Map();     // channelUrl -> runtime
let activeChannelUrl = null;    // only ONE channel touches the upstream at a time

function shortLabel(s) { return String(s || "stream").replace(/\s*\([^)]*\)\s*$/, "").trim().slice(0, 24); }
function fileNameOf(url) {
    try { return (decodeURIComponent(new URL(url).pathname).split("/").filter(Boolean).pop() || "").toLowerCase(); }
    catch { return (String(url).split("?")[0].split("#")[0].split("/").filter(Boolean).pop() || "").toLowerCase(); }
}
function segIndexOf(filename) {
    const m = String(filename).match(/(\d+)\.[a-z0-9]+$/i);   // trailing number before extension
    return m ? Number(m[1]) : -1;
}

// Parse a media playlist into ordered segments. Identity is the token-free FILENAME.
function parseMediaPlaylist(playlist, baseUrl) {
    const segs = [];
    let dur = 0;
    for (const raw of String(playlist || "").split(/\r?\n/)) {
        const t = raw.trim();
        if (!t || t.startsWith("#EXT-X-ENDLIST")) continue;
        if (t.startsWith("#EXTINF")) { const m = t.match(/#EXTINF:([0-9.]+)/); dur = m ? Number(m[1]) : 0; continue; }
        if (t.startsWith("#")) continue;
        if (isHlsUrl(t)) continue;
        const abs = toAbsoluteUrl(t, baseUrl);
        const filename = fileNameOf(abs);
        segs.push({ filename, realUrl: abs, duration: dur || 0, index: segIndexOf(filename) });
        dur = 0;
    }
    return segs;
}

function newRuntime(url, label) {
    return {
        url, label, key: hashKey(url).slice(0, 8),
        segs: [],            // [{ id, absSeq, duration }] chronological, absSeq ascending
        byId: new Map(),     // id -> seg
        nextAbs: 0,          // monotonic absSeq counter (never reset)
        generation: 0,
        target: 6,
        lastMaxIndex: -1,    // restart detection via filename numbering
        lastUpSeq: -1,       // restart detection via upstream MEDIA-SEQUENCE
        servedEdge: null,    // absSeq of newest segment shown to the player (smoothed)
        isMaster: false,
        lastPlayerAt: Date.now(),
        running: false, timer: null
    };
}

function stopPoller(rt, reason) {
    rt.running = false;
    if (rt.timer) { clearTimeout(rt.timer); rt.timer = null; }
    console.log(`[IDLE] ${shortLabel(rt.label)} — poller stopped (${reason})`);
    // Keep the buffer: if reopened soon, segments are still cached. A genuine upstream
    // restart is handled by the generation bump on the next ingest.
}

function ensureChannel(url, label) {
    // max_connections=1: only one channel may touch the upstream. Stop the previous.
    if (activeChannelUrl && activeChannelUrl !== url) {
        const other = channels.get(activeChannelUrl);
        if (other && other.running) stopPoller(other, "switched channel");
        prefetchQueue.length = 0;
    }
    activeChannelUrl = url;

    let rt = channels.get(url);
    if (!rt) { rt = newRuntime(url, label); channels.set(url, rt); }
    rt.label = label;
    rt.lastPlayerAt = Date.now();
    if (!rt.running) {
        rt.running = true;
        console.log(`[LIVE] ${shortLabel(label)} — poller started`);
        pollChannelLoop(rt);
    }
    return rt;
}

async function pollChannelLoop(rt) {
    if (!rt.running) return;
    if (Date.now() - rt.lastPlayerAt > POLLER_IDLE_STOP_MS) {
        stopPoller(rt, "player gone");
        if (activeChannelUrl === rt.url) activeChannelUrl = null;
        return;
    }
    try {
        const { data, finalUrl, info } = await fetchUpstreamHLS(rt.url, shortLabel(rt.label));
        if (info.isMaster) { rt.isMaster = true; stopPoller(rt, "master playlist"); return; }
        if (info.isLive) ingestPlaylist(rt, data, finalUrl, info);
    } catch (e) {
        console.warn(`[POLL ERR] ${shortLabel(rt.label)} — ${e.message}`);
    }
    if (!rt.running) return;
    const delay = Math.max(POLL_MIN_MS, Math.min(POLL_MAX_MS, Math.round((rt.target || 6) * 500)));
    rt.timer = setTimeout(() => pollChannelLoop(rt), delay);
    if (rt.timer.unref) rt.timer.unref();
}

function ingestPlaylist(rt, playlist, baseUrl, info) {
    rt.target = info.targetDuration || rt.target;
    const parsed = parseMediaPlaylist(playlist, baseUrl);
    if (!parsed.length) return;

    const upSeq = Number(info.mediaSequence || 0);
    const maxIndex = parsed.reduce((m, s) => Math.max(m, s.index), -1);
    // Restart: the on-demand stream was torn down and restarted → filename numbering
    // reset OR upstream MEDIA-SEQUENCE went backward. Bump generation so reused
    // filenames get fresh ids (no stale cache); keep absSeq advancing FORWARD so the
    // player rejoins live moving forward, never "back to the beginning".
    const indexReset = maxIndex >= 0 && rt.lastMaxIndex >= 0 && maxIndex < rt.lastMaxIndex;
    const seqReset = rt.lastUpSeq >= 0 && upSeq < rt.lastUpSeq;
    if (rt.segs.length && (indexReset || seqReset)) {
        rt.generation++;
        rt.lastMaxIndex = -1;
        console.log(`[RESTART] ${shortLabel(rt.label)} — upstream restarted, continuing forward (gen ${rt.generation}, seq ${rt.nextAbs})`);
    }
    rt.lastUpSeq = upSeq;
    if (maxIndex >= 0) rt.lastMaxIndex = Math.max(rt.lastMaxIndex, maxIndex);

    let added = 0;
    for (const s of parsed) {
        const id = `${rt.key}:g${rt.generation}:${s.filename}`;
        registerSeg(id, s.realUrl);                 // refresh the token-bearing URL
        if (!rt.byId.has(id)) {
            const seg = { id, absSeq: rt.nextAbs++, duration: s.duration || rt.target || 6 };
            rt.byId.set(id, seg);
            rt.segs.push(seg);
            added++;
        }
    }
    while (rt.segs.length > RETAIN_SEGMENTS) {
        const dropped = rt.segs.shift();
        rt.byId.delete(dropped.id);
    }
    if (added > 0) {
        queuePrefetch(rt.segs.map(s => s.id));      // keep the whole retained buffer warm
        const lo = rt.segs[0].absSeq, hi = rt.segs[rt.segs.length - 1].absSeq;
        console.log(`[POLL +${added}] ${shortLabel(rt.label)} — buffer ${rt.segs.length}/${RETAIN_SEGMENTS} (seq ${lo}..${hi}) cache ${segCache.size}/${formatBytes(segCacheBytes)}`);
    }
}

function buildServedPlaylist(rt, hostBase, configKey) {
    const lo = rt.segs[0].absSeq;
    const hi = rt.segs[rt.segs.length - 1].absSeq;

    // Smoothly advance the served live edge (≤ MAX_EDGE_ADVANCE per request). Newer
    // segments are held back (still prefetched) so the edge never leaps → no jump.
    if (rt.servedEdge == null || rt.servedEdge > hi) rt.servedEdge = hi;
    else rt.servedEdge = Math.min(hi, rt.servedEdge + MAX_EDGE_ADVANCE);
    if (rt.servedEdge < lo) rt.servedEdge = lo;

    const endPos = rt.servedEdge - lo;
    const win = rt.segs.slice(0, endPos + 1);        // full retained tail up to the edge
    const mediaSeq = win[0].absSeq;
    const target = Math.max(1, Math.ceil(Math.max(rt.target || 6, ...win.map(s => s.duration || 0))));

    // Deterministic cushion: tell the player to start TARGET_CUSHION seconds behind
    // our live edge (bounded by what we actually have). The deep retained tail then
    // absorbs slow segments without the player ever starving at the edge.
    const totalSec = win.reduce((a, s) => a + (s.duration || rt.target || 6), 0);
    const cushion = Math.min(TARGET_CUSHION_SECONDS, Math.max(0, totalSec - (rt.target || 6)));

    const lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        `#EXT-X-TARGETDURATION:${target}`,
        `#EXT-X-MEDIA-SEQUENCE:${mediaSeq}`
    ];
    if (cushion > 0) lines.push(`#EXT-X-START:TIME-OFFSET=-${cushion.toFixed(1)},PRECISE=YES`);
    for (const s of win) {
        lines.push(`#EXTINF:${Number(s.duration || rt.target || 6).toFixed(3)},`);
        lines.push(`${hostBase}/${configKey}/proxy/seg?s=${encodeURIComponent(s.id)}`);
    }
    return { playlist: lines.join("\n") + "\n", count: win.length, mediaSeq, edge: rt.servedEdge, hi, cushion };
}

function setPlaylistHeaders(res) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    res.setHeader("Pragma", "no-cache");
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

        // FIRST open only: brief, capped wait to build the initial cushion. Re-opens
        // and continuous polls are answered instantly (buffer already deep) so the
        // player's manifest request never hangs → no timeout/crash.
        if (!rt.isMaster && rt.segs.length < MIN_START_SEGMENTS) {
            console.log(`[OPEN] ${shortLabel(ch.name)} — priming (buffer ${rt.segs.length})`);
            const deadline = Date.now() + PRIME_TIMEOUT_MS;
            while (rt.running && !rt.isMaster && rt.segs.length < MIN_START_SEGMENTS && Date.now() < deadline) {
                rt.lastPlayerAt = Date.now();
                await sleep(600);
            }
        }

        if (rt.isMaster) {
            const { data, finalUrl } = await fetchUpstreamHLS(ch.url, shortLabel(ch.name));
            const { rewritten } = rewriteHLSUrls(data, finalUrl, host, configKey);
            setPlaylistHeaders(res);
            console.log(`[SERVE] ${shortLabel(ch.name)} — master playlist`);
            return res.send(rewritten);
        }

        if (!rt.segs.length) {
            console.warn(`[OPEN FAIL] ${shortLabel(ch.name)} — no segments after ${Date.now() - t0}ms`);
            return res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
        }

        const { playlist, count, mediaSeq, edge, hi, cushion } = buildServedPlaylist(rt, host, configKey);
        setPlaylistHeaders(res);
        const waited = Date.now() - t0;
        console.log(
            `[SERVE] ${shortLabel(ch.name)} — ${count} segs, seq ${mediaSeq}..${edge}` +
            `${hi > edge ? ` (+${hi - edge} held)` : ""} cushion ${Math.round(cushion)}s` +
            `${waited > 80 ? ` waited ${waited}ms` : ""}`
        );
        res.send(playlist);
    } catch (err) {
        console.error(`[OPEN ERR] ${err.message}`);
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
        const { data, finalUrl } = await fetchUpstreamHLS(sourceUrl, "nested");
        const { rewritten } = rewriteHLSUrls(data, finalUrl, getPublicHost(req), configKey);
        setPlaylistHeaders(res);
        res.send(rewritten);
    } catch (err) {
        console.error(`[PL ERR] ${err.message}`);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/seg", async (req, res) => {
    const t0 = Date.now();
    let sourceUrl = null;
    let segId = null;
    try {
        decodeConfig(req.params.base64Config);

        // Any segment activity keeps the active channel's poller alive (so a 2x stall
        // that pauses playlist polling doesn't let the stream get torn down).
        if (activeChannelUrl) {
            const rt = channels.get(activeChannelUrl);
            if (rt) rt.lastPlayerAt = Date.now();
        }

        if (req.query.s) {
            segId = String(req.query.s);
            sourceUrl = segRegistry.get(segId);
            if (!sourceUrl) {           // unknown id (restart/aged out) → make the player refresh
                console.warn(`[GET 404] ${segId} — unknown segment, player should refresh`);
                return res.status(404).end();
            }
            const cached = getSegFromCache(segId);
            if (cached) return sendCachedSeg(req, res, cached, segId, "cache");

            // Miss → fetch through the SERIAL gate with priority; dedups with any
            // in-flight prefetch. NEVER opens a competing connection (max_connections=1).
            const wasInflight = segInflight.has(segId);
            const entry = await fetchSegToCache(segId, sourceUrl, true);
            if (entry) return sendCachedSeg(req, res, entry, segId, wasInflight ? "wait" : "fetch");
            return res.status(502).end();
        }

        if (req.query.u) {              // direct (non-HLS) continuous stream → passthrough
            sourceUrl = decodeProxyUrl(req.query.u);
            return streamSegment(req, res, sourceUrl, t0);
        }
        return res.status(400).end();
    } catch (err) {
        console.error(`[GET ERR] ${getSegmentLabel(sourceUrl)} ${err.message}`);
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
            console.log(`[GET ${label}] ${name} ${formatBytes(chunk.length)} (range ${start}-${end})`);
            return res.end(chunk);
        }
    }

    res.setHeader("Content-Length", total);
    console.log(`[GET ${label}] ${name} ${formatBytes(total)}`);
    return res.end(buf);
}

// Direct passthrough for non-HLS continuous streams (?u=). Its own single connection.
async function streamSegment(req, res, sourceUrl, started) {
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
    res.status(response.status);

    let bytes = 0;
    upstream.on("data", chunk => { bytes += chunk.length; });
    res.on("finish", () => {
        const ms = Date.now() - started;
        console.log(`[GET direct] ${getSegmentLabel(sourceUrl)} ${formatBytes(bytes)} ${ms}ms ${formatSpeed(bytes, ms)}`);
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
        },
        channels: [...channels.values()].map(rt => ({
            label: rt.label, depth: rt.segs.length,
            seq: rt.segs.length ? `${rt.segs[0].absSeq}..${rt.segs[rt.segs.length - 1].absSeq}` : null,
            servedEdge: rt.servedEdge, generation: rt.generation,
            running: rt.running, isMaster: rt.isMaster, idleMs: Date.now() - rt.lastPlayerAt
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
    console.log(`⏩ prefetch concurrency=${SEGMENT_PREFETCH_CONCURRENCY} (serialized) cacheTTL=${SEGMENT_CACHE_TTL / 1000}s`);
    console.log(`🪟 buffer: retain=${RETAIN_SEGMENTS} cushion=${TARGET_CUSHION_SECONDS}s prime=${MIN_START_SEGMENTS}segs/${PRIME_TIMEOUT_MS / 1000}s edgeStep=${MAX_EDGE_ADVANCE} idleStop=${POLLER_IDLE_STOP_MS / 1000}s`);
    console.log("=".repeat(60) + "\n");
});
