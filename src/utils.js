const crypto = require("crypto");
const http = require("http");
const https = require("https");
const settings = require("./settings");

const httpAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: settings.UPSTREAM_KEEPALIVE_MS,
    maxSockets: settings.UPSTREAM_KEEPALIVE_MAX_SOCKETS,
    maxFreeSockets: settings.UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS,
    scheduling: "lifo"
});

const httpsAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: settings.UPSTREAM_KEEPALIVE_MS,
    maxSockets: settings.UPSTREAM_KEEPALIVE_MAX_SOCKETS,
    maxFreeSockets: settings.UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS,
    scheduling: "lifo"
});

function upstreamAgentOptions() {
    return { httpAgent, httpsAgent };
}

function hashKey(value, length = 20) {
    return crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, length);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function isHttpUrl(value) {
    return /^https?:\/\//i.test(String(value || ""));
}

function isHlsUrl(value) {
    return /\.m3u8(?:[?#].*)?$/i.test(String(value || ""));
}

function encodeBase64Url(value) {
    return Buffer.from(String(value), "utf8").toString("base64url");
}

function decodeBase64Url(value) {
    return Buffer.from(String(value || ""), "base64url").toString("utf8");
}

function toAbsoluteUrl(value, baseUrl) {
    try {
        return new URL(value, baseUrl).toString();
    } catch {
        return value;
    }
}

function routeBase(host, routeKey) {
    return routeKey ? `${host}/${routeKey}` : host;
}

function getPublicHost(req) {
    const proto = req.get("x-forwarded-proto") || req.protocol;
    const host = req.get("x-forwarded-host") || req.get("host");
    return `${proto}://${host}`;
}

function escapeXml(value) {
    return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function cleanGroupName(value) {
    const cleaned = String(value || "").trim()
        .replace(/^\s*IT\s*\|\s*/i, "")
        .replace(/^\s*IT\s*:\s*/i, "")
        .trim();
    return cleaned || "Altri Canali";
}

function normalizeGroupName(value) {
    return cleanGroupName(value).toLowerCase();
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
        return String(value || "")
            .replace(/(password=)[^&\s]+/gi, "$1***")
            .replace(/(username=)[^&\s]+/gi, "$1***");
    }
}

function isBlockedPlaylist(url) {
    if (!url || !settings.BLOCKED_PLAYLISTS.length) return false;
    const normalized = String(url).toLowerCase();
    return settings.BLOCKED_PLAYLISTS.some(blocked => normalized.includes(blocked));
}

module.exports = {
    upstreamAgentOptions,
    hashKey,
    sleep,
    isHttpUrl,
    isHlsUrl,
    encodeBase64Url,
    decodeBase64Url,
    toAbsoluteUrl,
    routeBase,
    getPublicHost,
    escapeXml,
    cleanGroupName,
    normalizeGroupName,
    normalizeSearchText,
    redactUrl,
    isBlockedPlaylist
};
