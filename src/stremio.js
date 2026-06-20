const axios = require("axios");
const settings = require("./settings");
const state = require("./state");
const {
    encodeBase64Url,
    escapeXml,
    hashKey,
    isHlsUrl,
    isHttpUrl,
    routeBase,
    upstreamAgentOptions
} = require("./utils");
const {
    getConfiguredLists,
    sortGroupsForManifest,
    stripInitialCountryPrefix,
    toCatalogId
} = require("./sources");

function manifestAddonId(configKey) {
    return `org.stremio.kronos.channel.${hashKey(configKey)}`;
}

function buildManifest(configKey, config, channels, host) {
    const catalogs = getConfiguredLists(config).map(list => {
        const catalogChannels = channels.filter(channel => channel.sourceName === list.name);
        const groups = sortGroupsForManifest(config, [...new Set(catalogChannels.map(channel => channel.group))]
            .filter(group => group && group.trim()));
        const extra = [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }];
        if (groups.length) extra.push({ name: "genre", options: groups, isRequired: false });
        return { id: toCatalogId(list.name), type: settings.ADDON_TYPE, name: list.name, extra };
    });

    return {
        id: manifestAddonId(configKey),
        version: settings.RELEASE_VERSION,
        name: "Canali TV",
        description: "Canali TV",
        logo: `${host}/logo.svg`,
        resources: ["catalog", "meta", "stream"],
        types: [settings.ADDON_TYPE],
        idPrefixes: ["channel_"],
        behaviorHints: { configurable: true, configurationRequired: false },
        catalogs
    };
}

function buildStream(channel, host, routeKey) {
    const base = routeBase(host, routeKey);
    const behaviorHints = { bingeGroup: `kronos-${channel.id}` };

    if (isHlsUrl(channel.url)) {
        const params = new URLSearchParams({
            u: encodeBase64Url(channel.url),
            pg: shouldBlockOfflinePlaceholders(channel) ? "1" : "0"
        });
        return {
            title: channel.name,
            name: "TV",
            url: `${base}/proxy/live.m3u8?${params.toString()}`,
            behaviorHints
        };
    }

    if (isHttpUrl(channel.url)) {
        return {
            title: channel.name,
            name: "TV",
            url: `${base}/proxy/seg?u=${encodeBase64Url(channel.url)}`,
            behaviorHints
        };
    }

    return {
        title: `${channel.name} - sorgente web`,
        name: "TV",
        externalUrl: channel.url
    };
}

function shouldBlockOfflinePlaceholders(channel) {
    const text = `${channel?.name || ""} ${channel?.group || ""}`.toLowerCase();
    return !/\b(?:vetrina|info\s+eventi)\b/.test(text);
}

function toMeta(channel, host, routeKey = "", options = {}) {
    const fallbackLogo = `${host}/logo.svg`;
    const poster = options.shortPoster
        ? `${host}/poster/${channel.id}.svg?v=${encodeURIComponent(settings.RELEASE_VERSION)}`
        : routeKey
            ? `${routeBase(host, routeKey)}/poster/${channel.id}.svg?v=${encodeURIComponent(settings.RELEASE_VERSION)}`
            : channel.logo || fallbackLogo;
    const stream = options.includeVideos !== false && routeKey ? buildStream(channel, host, routeKey) : null;
    const meta = {
        id: channel.id,
        type: settings.ADDON_TYPE,
        name: channel.name,
        poster,
        posterShape: "square"
    };

    if (channel.group) meta.genres = [channel.group];
    if (!options.catalogLite) {
        meta.logo = channel.logo || poster || fallbackLogo;
        meta.description = channel.description || "";
        meta.background = poster;
        meta.behaviorHints = { defaultVideoId: channel.id, hasScheduledVideos: false };
    }

    if (options.includeVideos !== false) {
        meta.videos = [{
            id: channel.id,
            title: channel.name,
            released: new Date(0).toISOString(),
            thumbnail: poster,
            overview: channel.description || "",
            available: true,
            streams: stream ? [stream] : undefined
        }];
    }

    return meta;
}

async function getLogoDataUri(logoUrl) {
    if (!isHttpUrl(logoUrl)) return "";
    if (state.logoData.has(logoUrl)) return state.logoData.get(logoUrl);

    const failedAt = state.logoFailures.get(logoUrl) || 0;
    if (failedAt && Date.now() - failedAt < settings.LOGO_FAILURE_TTL_MS) return "";

    try {
        const response = await axios.get(logoUrl, {
            responseType: "arraybuffer",
            timeout: settings.LOGO_REQUEST_TIMEOUT,
            maxContentLength: 2 * 1024 * 1024,
            ...upstreamAgentOptions(),
            headers: {
                "User-Agent": `Kronos/${settings.RELEASE_VERSION}`,
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
            }
        });
        const contentType = String(response.headers["content-type"] || "image/png").split(";")[0];
        const dataUri = `data:${contentType};base64,${Buffer.from(response.data).toString("base64")}`;
        if (state.logoData.size >= settings.LOGO_MAX_CACHE_ITEMS) {
            state.logoData.delete(state.logoData.keys().next().value);
        }
        state.logoData.set(logoUrl, dataUri);
        state.logoFailures.delete(logoUrl);
        return dataUri;
    } catch {
        state.logoFailures.set(logoUrl, Date.now());
        return "";
    }
}

async function sendPosterSvg(res, channel) {
    const logoUri = await getLogoDataUri(channel?.logo || "");
    const name = stripInitialCountryPrefix(channel?.name || "Kronos");
    const initials = name
        .replace(/\([^)]*\)/g, "")
        .split(/\s+/)
        .filter(Boolean)
        .slice(0, 2)
        .map(part => part[0])
        .join("")
        .toUpperCase() || "TV";
    const logoMarkup = logoUri
        ? `<image href="${escapeXml(logoUri)}" x="58" y="74" width="396" height="286" preserveAspectRatio="xMidYMid meet"/>`
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
            <rect x="28" y="28" width="456" height="456" rx="44" fill="#ffffff" opacity="0.05" stroke="#ffffff" stroke-opacity="0.16"/>
            <rect x="46" y="58" width="420" height="318" rx="32" fill="#d9dee7"/>
            ${logoMarkup}
            <text x="256" y="424" text-anchor="middle" fill="#f5f7fb" font-family="Arial, sans-serif" font-size="30" font-weight="700">${escapeXml(name.slice(0, 34))}</text>
        </svg>
    `);
}

function logoSvg() {
    return `
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
    `;
}

module.exports = {
    buildManifest,
    buildStream,
    logoSvg,
    sendPosterSvg,
    shouldBlockOfflinePlaceholders,
    toMeta
};
