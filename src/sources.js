const axios = require("axios");
const crypto = require("crypto");
const settings = require("./settings");
const {
    cleanGroupName,
    isBlockedPlaylist,
    isHttpUrl,
    normalizeGroupName,
    normalizeSearchText,
    redactUrl,
    sleep,
    upstreamAgentOptions
} = require("./utils");

function normalizeSourceType(value) {
    const type = String(value || "auto").trim().toLowerCase();
    return ["auto", "m3u", "xtream"].includes(type) ? type : "auto";
}

function normalizeXtreamStreamFormat(value) {
    const format = String(value || "ts").trim().toLowerCase();
    return ["ts", "hls"].includes(format) ? format : "ts";
}

function readXtreamOutputFormat(sourceUrl) {
    try {
        const parsed = new URL(sourceUrl);
        const output = parsed.searchParams.get("output");
        if (output) return normalizeXtreamStreamFormat(output);
        if (/\.m3u8$/i.test(parsed.pathname)) return "hls";
        if (/\.ts$/i.test(parsed.pathname)) return "ts";
        return "ts";
    } catch {
        return "ts";
    }
}

function getConfiguredLists(config) {
    if (Array.isArray(config?.l) && config.l.length) {
        return config.l
            .map((list, index) => ({
                name: String(list?.n || `Lista ${index + 1}`).trim() || `Lista ${index + 1}`,
                url: String(list?.u || "").trim(),
                type: normalizeSourceType(list?.t),
                streamFormat: normalizeXtreamStreamFormat(list?.fmt || list?.f || readXtreamOutputFormat(list?.u || ""))
            }))
            .filter(list => list.url && !isBlockedPlaylist(list.url));
    }

    const url = String(config?.u || "").trim();
    return [{
        name: String(config?.ln || "Canali TV").trim() || "Canali TV",
        url,
        type: normalizeSourceType(config?.t),
        streamFormat: normalizeXtreamStreamFormat(config?.fmt || config?.f || readXtreamOutputFormat(url))
    }].filter(list => list.url && !isBlockedPlaylist(list.url));
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
        return /\/guide\.gzip$/i.test(new URL(value).pathname);
    } catch {
        return false;
    }
}

function getEffectiveEpgUrl(config, lists = getConfiguredLists(config)) {
    const configured = String(config?.e || "").trim();
    const derived = lists.map(list => deriveXtreamEpgUrl(list.url)).filter(Boolean);
    if (configured && !isLegacyGuideProxyUrl(configured)) return configured;
    if (settings.EPG_PRELOAD_URLS.length) return settings.EPG_PRELOAD_URLS[0];
    if (isLegacyGuideProxyUrl(configured) && derived.length) return derived[0];
    if (!configured && derived.length) return derived[0];
    return configured;
}

function getHttpFallbackUrl(sourceUrl) {
    try {
        const parsed = new URL(sourceUrl);
        if (parsed.protocol !== "https:") return "";
        parsed.protocol = "http:";
        return parsed.toString();
    } catch {
        return "";
    }
}

function isHttpsPlainHttpError(err) {
    const message = String(err?.message || "");
    return err?.code === "EPROTO" && /wrong version number|ssl3_get_record/i.test(message);
}

async function fetchPlaylist(sourceUrl, options = {}) {
    if (isBlockedPlaylist(sourceUrl)) throw new Error("Playlist is blocked");

    console.log("[PLAYLIST FETCH]", redactUrl(sourceUrl));
    const deadline = Date.now() + (options.retryWindow || settings.PLAYLIST_RETRY_WINDOW_MS);
    let requestUrl = sourceUrl;
    let usedHttpFallback = false;
    let attempt = 0;
    let lastErr = null;

    while (Date.now() < deadline) {
        attempt++;
        try {
            const response = await axios.get(requestUrl, {
                timeout: Math.max(1, Math.min(options.timeout || settings.PLAYLIST_REQUEST_TIMEOUT, deadline - Date.now())),
                maxRedirects: 5,
                ...upstreamAgentOptions(),
                headers: {
                    "User-Agent": settings.UPSTREAM_UA,
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                },
                validateStatus: status => status >= 200 && status < 300
            });

            const data = String(response.data || "");
            if (!data.trimStart().startsWith("#EXTM3U")) throw new Error("Invalid M3U playlist");
            console.log(`[PLAYLIST OK] channelsSource=${redactUrl(requestUrl)} size=${data.length} attempt=${attempt}`);
            return data;
        } catch (err) {
            lastErr = err;
            const fallbackUrl = !usedHttpFallback && isHttpsPlainHttpError(err) ? getHttpFallbackUrl(requestUrl) : "";
            if (fallbackUrl) {
                usedHttpFallback = true;
                requestUrl = fallbackUrl;
                console.warn(`[PLAYLIST HTTP FALLBACK] ${redactUrl(sourceUrl)} -> ${redactUrl(fallbackUrl)}`);
                continue;
            }
            console.warn(`[PLAYLIST RETRY] attempt=${attempt} reason=${err.message}`);
        }

        const waitMs = Math.min(settings.PLAYLIST_RETRY_DELAY_MS, deadline - Date.now());
        if (waitMs > 0) await sleep(waitMs);
    }

    throw lastErr || new Error("Playlist fetch failed");
}

function parseExtinfAttributes(line) {
    const attrs = {};
    line.replace(/([\w-]+)="([^"]*)"/g, (_match, key, value) => {
        attrs[key.toLowerCase()] = value;
        return "";
    });
    return attrs;
}

function isDividerChannelName(name) {
    return /^#{2,}/.test(String(name || "").trim());
}

function parseM3UChannels(data, source = {}) {
    const channels = [];
    let current = null;

    for (const rawLine of String(data || "").split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line) continue;

        if (line.startsWith("#EXTINF:")) {
            const attrs = parseExtinfAttributes(line);
            const fallbackName = (line.match(/,(.+)$/) || [, "Canale Sconosciuto"])[1].trim();
            current = {
                name: String(attrs["tvg-name"] || fallbackName || "Canale Sconosciuto").trim(),
                group: cleanGroupName(attrs["group-title"] || "Altri Canali"),
                logo: String(attrs["tvg-logo"] || "").trim(),
                tvgId: attrs["tvg-id"] ? String(attrs["tvg-id"]) : null,
                sourceName: source.name || "Kronos",
                sourceUrl: source.url || "",
                sourceType: "m3u"
            };
            continue;
        }

        if (!line.startsWith("#") && current) {
            current.url = line;
            current.id = "channel_" + crypto.createHash("sha1")
                .update(`${source.url || ""}|${line}`)
                .digest("hex")
                .slice(0, 20);
            if (!isDividerChannelName(current.name) && isHttpUrl(current.url)) channels.push(current);
            current = null;
        }
    }

    return channels;
}

function parseXtreamConfig(sourceUrl) {
    try {
        const parsed = new URL(sourceUrl);
        const parts = parsed.pathname.split("/").filter(Boolean);
        let username = parsed.searchParams.get("username") || "";
        let password = parsed.searchParams.get("password") || "";
        if ((!username || !password) && parts.length >= 3 && parts[0] === "live") {
            username = username || decodeURIComponent(parts[1]);
            password = password || decodeURIComponent(parts[2]);
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

function xtreamLiveUrl(xtream, streamId, streamFormat = "ts") {
    const extension = normalizeXtreamStreamFormat(streamFormat) === "hls" ? "m3u8" : "ts";
    return `${xtream.origin}/live/${encodeURIComponent(xtream.username)}/${encodeURIComponent(xtream.password)}/${encodeURIComponent(streamId)}.${extension}`;
}

async function fetchXtreamApi(xtream, action = "") {
    const response = await axios.get(xtreamApiUrl(xtream, action), {
        timeout: settings.PLAYLIST_REQUEST_TIMEOUT,
        maxRedirects: 5,
        ...upstreamAgentOptions(),
        headers: {
            "User-Agent": settings.UPSTREAM_UA,
            "Accept": "application/json, text/plain, */*",
            "Connection": "keep-alive"
        },
        validateStatus: status => status >= 200 && status < 300
    });
    return typeof response.data === "string" ? JSON.parse(response.data) : response.data;
}

async function fetchXtreamChannels(source) {
    const xtream = parseXtreamConfig(source.url);
    if (!xtream) throw new Error("Missing Xtream credentials");

    console.log("[XTREAM FETCH]", redactUrl(xtreamApiUrl(xtream)));
    const [account, categories, streams] = await Promise.all([
        fetchXtreamApi(xtream).catch(err => ({ error: err.message })),
        fetchXtreamApi(xtream, "get_live_categories"),
        fetchXtreamApi(xtream, "get_live_streams")
    ]);

    if (account?.user_info && Number(account.user_info.auth) !== 1) throw new Error("Xtream account not authorized");
    if (!Array.isArray(streams)) throw new Error("Invalid Xtream live stream response");

    const categoryNames = new Map((Array.isArray(categories) ? categories : [])
        .map(category => [String(category.category_id), cleanGroupName(category.category_name)])
        .filter(([, name]) => name));

    const channels = streams
        .filter(item => item && item.stream_id != null && String(item.name || "").trim())
        .filter(item => !isDividerChannelName(item.name))
        .map(item => {
            const streamId = String(item.stream_id);
            const categoryId = String(item.category_id || item.category_ids?.[0] || "");
            const url = isHttpUrl(item.direct_source) ? item.direct_source : xtreamLiveUrl(xtream, streamId, source.streamFormat);
            return {
                id: "channel_" + crypto.createHash("sha1")
                    .update(`${source.url || ""}|xtream:${streamId}`)
                    .digest("hex")
                    .slice(0, 20),
                name: String(item.name || "Canale Sconosciuto").trim(),
                group: categoryNames.get(categoryId) || cleanGroupName(source.name || "Xtream"),
                logo: String(item.stream_icon || "").trim(),
                tvgId: item.epg_channel_id ? String(item.epg_channel_id) : null,
                url,
                sourceName: source.name || "Xtream",
                sourceUrl: source.url || "",
                sourceType: "xtream",
                streamFormat: normalizeXtreamStreamFormat(source.streamFormat)
            };
        });

    console.log(`[XTREAM OK] channels=${channels.length} categories=${categoryNames.size}`);
    return channels;
}

async function fetchChannelsFromSource(source) {
    const type = normalizeSourceType(source.type);
    const looksXtream = !!parseXtreamConfig(source.url);

    if (type === "xtream" || (type === "auto" && looksXtream)) {
        try {
            return await fetchXtreamChannels(source);
        } catch (err) {
            if (type === "xtream") throw err;
            console.warn(`[XTREAM FALLBACK] ${err.message}; trying M3U`);
        }
    }

    const playlist = await fetchPlaylist(source.url);
    return parseM3UChannels(playlist, source);
}

function stripInitialCountryPrefix(name) {
    return String(name || "").replace(/^\s*IT:\s*/i, "").trim();
}

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
    return Array.isArray(config?.g) ? config.g : [];
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

function matchesChannelSearch(channel, raw) {
    const query = normalizeSearchText(raw);
    if (!query) return true;
    const haystack = normalizeSearchText(channel.name);
    const compactHaystack = haystack.replace(/\s+/g, "");
    const compactQuery = query.replace(/\s+/g, "");
    if (haystack.includes(query) || compactHaystack.includes(compactQuery)) return true;
    return query.split(" ").filter(Boolean).every(token => haystack.includes(token));
}

function getExtraParams(extra) {
    const params = {};
    if (!extra) return params;
    String(extra).replace(/\.json$/i, "").split("&").forEach(pair => {
        const index = pair.indexOf("=");
        if (index === -1) return;
        const name = decodeURIComponent(pair.slice(0, index));
        const value = pair.slice(index + 1);
        if (name) params[name] = decodeURIComponent(value || "");
    });
    return params;
}

function getCatalogSourceName(id) {
    if (!String(id || "").startsWith("kronos_list_")) return null;
    return Buffer.from(String(id).replace("kronos_list_", ""), "hex").toString("utf8");
}

function toCatalogId(name) {
    return `kronos_list_${Buffer.from(String(name)).toString("hex")}`;
}

const channelNameCollator = new Intl.Collator("it", {
    numeric: true,
    sensitivity: "base",
    ignorePunctuation: true
});

const QUALITY_ORDER = new Map([
    ["", 0],
    ["hd", 1],
    ["fhd", 2],
    ["fullhd", 2],
    ["uhd", 3],
    ["4k", 3],
    ["8k", 4]
]);

function channelSortParts(name) {
    const text = String(name || "").trim();
    const qualityMatch = text.match(/\b(8K|4K|UHD|FULL\s*HD|FHD|HD)\b\s*$/i);
    const quality = qualityMatch ? qualityMatch[1].toLowerCase().replace(/\s+/g, "") : "";
    const base = qualityMatch ? text.slice(0, qualityMatch.index).trim() : text;
    return {
        base: base || text,
        quality: QUALITY_ORDER.has(quality) ? QUALITY_ORDER.get(quality) : QUALITY_ORDER.get("")
    };
}

function compareChannelNames(a, b) {
    const left = channelSortParts(a);
    const right = channelSortParts(b);
    return channelNameCollator.compare(left.base, right.base)
        || left.quality - right.quality
        || channelNameCollator.compare(String(a || ""), String(b || ""));
}

function sortChannelsByName(list) {
    return list.slice().sort((a, b) => compareChannelNames(a.name, b.name));
}

module.exports = {
    decorateChannelName,
    deriveXtreamEpgUrl,
    fetchChannelsFromSource,
    fetchPlaylist,
    fetchXtreamChannels,
    getCatalogSourceName,
    getConfiguredLists,
    getDisplayGroupName,
    getEffectiveEpgUrl,
    getExtraParams,
    getSelectedGroups,
    isDividerChannelName,
    matchesChannelSearch,
    normalizeXtreamStreamFormat,
    normalizeSourceType,
    parseM3UChannels,
    parseXtreamConfig,
    xtreamLiveUrl,
    sortChannelsByName,
    sortGroupsForManifest,
    stripInitialCountryPrefix,
    toCatalogId
};
