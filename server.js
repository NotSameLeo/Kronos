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
const HLS_REQUEST_TIMEOUT = Number(process.env.HLS_REQUEST_TIMEOUT || 15000);
const SEG_REQUEST_TIMEOUT = Number(process.env.SEG_REQUEST_TIMEOUT || 30000);

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
async function fetchUpstreamHLS(sourceUrl) {
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
    if (!text.startsWith("#EXTM3U")) throw new Error("Invalid HLS manifest from upstream");
    return { data: r.data, finalUrl };
}

function rewriteHLSUrls(playlist, baseUrl, hostBase, configKey) {
    const segUrl = abs => `${hostBase}/${configKey}/proxy/seg?u=${encodeProxyUrl(abs)}`;
    const plUrl = abs => `${hostBase}/${configKey}/proxy/pl?u=${encodeProxyUrl(abs)}`;
    const pick = abs => isHlsUrl(abs) ? plUrl(abs) : segUrl(abs);

    return String(playlist || "").split(/\r?\n/).map(line => {
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
app.get("/:base64Config/hls/:id/index.m3u8", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        const config = decodeConfig(configKey);
        const ch = await getChannelById(configKey, config, req.params.id);
        if (!ch) return res.status(404).send("#EXTM3U\n#EXT-X-ENDLIST\n");

        const host = getPublicHost(req);
        const { data, finalUrl } = await fetchUpstreamHLS(ch.url);
        const rewritten = rewriteHLSUrls(data, finalUrl, host, configKey);

        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
        res.setHeader("Pragma", "no-cache");
        console.log(`[HLS] channel="${ch.name}" bytes=${formatBytes(Buffer.byteLength(rewritten))}`);
        res.send(rewritten);
    } catch (err) {
        console.error("[HLS ERROR]", err.message);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/pl", async (req, res) => {
    try {
        const configKey = req.params.base64Config;
        decodeConfig(configKey);
        if (!req.query.u) return res.status(400).send("#EXTM3U\n");
        const sourceUrl = decodeProxyUrl(req.query.u);
        const host = getPublicHost(req);
        const { data, finalUrl } = await fetchUpstreamHLS(sourceUrl);
        const rewritten = rewriteHLSUrls(data, finalUrl, host, configKey);
        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
        res.send(rewritten);
    } catch (err) {
        console.error("[PL ERROR]", err.message);
        if (!res.headersSent) res.status(502).send("#EXTM3U\n#EXT-X-ENDLIST\n");
    }
});

app.get("/:base64Config/proxy/seg", async (req, res) => {
    let upstream = null;
    const started = Date.now();
    let sourceUrl = null;
    let bytes = 0;

    try {
        decodeConfig(req.params.base64Config);
        if (!req.query.u) return res.status(400).end();
        sourceUrl = decodeProxyUrl(req.query.u);

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
            validateStatus: s => s >= 200 && s < 400
        });

        upstream = response.data;
        ["content-type", "content-length", "content-range", "accept-ranges",
         "last-modified", "etag", "cache-control", "expires"].forEach(h => {
            if (response.headers[h]) res.setHeader(h, response.headers[h]);
        });
        res.setHeader("X-Kronos-Relay", "1");
        res.status(response.status);

        upstream.on("data", chunk => { bytes += chunk.length; });

        res.on("finish", () => {
            const ms = Date.now() - started;
            console.log(`[SEG] ${getSegmentLabel(sourceUrl)} status=${response.status} bytes=${formatBytes(bytes)} time=${ms}ms`);
        });

        res.on("close", () => {
            if (!res.writableEnded && upstream?.destroy) upstream.destroy();
        });

        upstream.on("error", err => {
            if (!res.headersSent) return res.status(502).end();
            res.destroy(err);
        });

        upstream.pipe(res);
    } catch (err) {
        if (upstream?.destroy) upstream.destroy();
        console.error("[SEG ERROR]", getSegmentLabel(sourceUrl), err.message);
        if (!res.headersSent) res.status(502).end();
        else if (!res.destroyed) res.destroy(err);
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
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        cache: {
            channels: Object.values(memoryCache.channelItems).reduce((s, l) => s + l.length, 0),
            epgMaps: Object.keys(memoryCache.epgData).length,
            logos: Object.keys(memoryCache.logoData).length
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
    console.log("=".repeat(60) + "\n");
});
