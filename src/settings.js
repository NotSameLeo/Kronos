const path = require("path");

function numberEnv(name, fallback, min = null, max = null) {
    const value = Number(process.env[name] ?? fallback);
    let next = Number.isFinite(value) ? value : fallback;
    if (min !== null) next = Math.max(min, next);
    if (max !== null) next = Math.min(max, next);
    return next;
}

function parseList(raw) {
    return String(raw || "")
        .split(/[,\s]+/)
        .map(value => value.trim())
        .filter(Boolean);
}

const FRONTEND_PRELOAD_FILE = String(process.env.FRONTEND_PRELOAD_FILE || "").trim();

module.exports = {
    RELEASE_VERSION: "5.0.0",
    ADDON_TYPE: "tv",
    PLAYBACK_MODE: "plain-hls-relay",
    PORT: process.env.PORT || 7000,
    LOG_TIME_ZONE: process.env.LOG_TIME_ZONE || process.env.EPG_TIME_ZONE || "Europe/Rome",
    UPSTREAM_UA: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

    CATALOG_TTL: numberEnv("CATALOG_TTL", 30 * 60 * 1000, 0),
    CATALOG_PAGE_SIZE: numberEnv("CATALOG_PAGE_SIZE", 100, 24, 300),
    CATALOG_REFRESH_INTERVAL_MS: numberEnv("CATALOG_REFRESH_INTERVAL_MS", 30 * 60 * 1000, 0),
    CATALOG_PRELOAD_CONFIGS: parseList(process.env.CATALOG_PRELOAD_CONFIGS || process.env.KRONOS_PRELOAD_CONFIGS || ""),

    EPG_CACHE_TTL: numberEnv("EPG_CACHE_TTL", 6 * 60 * 60 * 1000, 0),
    EPG_REQUEST_TIMEOUT: numberEnv("EPG_REQUEST_TIMEOUT", 20000, 1),
    EPG_MAX_BYTES: numberEnv("EPG_MAX_BYTES", 160 * 1024 * 1024, 1),
    EPG_RETRY_DELAY_MS: numberEnv("EPG_RETRY_DELAY_MS", 15000, 0),
    EPG_FIRST_CATALOG_WAIT_MS: numberEnv("EPG_FIRST_CATALOG_WAIT_MS", 0, 0),
    EPG_REFRESH_INTERVAL_MS: numberEnv("EPG_REFRESH_INTERVAL_MS", 6 * 60 * 60 * 1000, 0),
    EPG_TIME_ZONE: process.env.EPG_TIME_ZONE || "Europe/Rome",
    EPG_PRELOAD_URLS: parseList(process.env.EPG_PRELOAD_URLS || process.env.EPG_PRELOAD_URL || "https://iptv-epg.org/files/epg-it.xml.gz"),
    EPG_STARTUP_WATCH_MS: numberEnv("EPG_STARTUP_WATCH_MS", 30000, 0),

    HLS_REQUEST_TIMEOUT: numberEnv("HLS_REQUEST_TIMEOUT", 15000, 1),
    SEG_REQUEST_TIMEOUT: numberEnv("SEG_REQUEST_TIMEOUT", 45000, 1),
    HLS_MANIFEST_RETRIES: numberEnv("HLS_MANIFEST_RETRIES", 2, 0),
    HLS_MANIFEST_RETRY_DELAY_MS: numberEnv("HLS_MANIFEST_RETRY_DELAY_MS", 700, 0),
    SEGMENT_UPSTREAM_RETRIES: numberEnv("SEGMENT_UPSTREAM_RETRIES", 2, 0),
    SEGMENT_UPSTREAM_RETRY_DELAY_MS: numberEnv("SEGMENT_UPSTREAM_RETRY_DELAY_MS", 500, 0),
    SEGMENT_TOKEN_HEALING: String(process.env.SEGMENT_TOKEN_HEALING || "1") !== "0",

    PLAYLIST_REQUEST_TIMEOUT: numberEnv("PLAYLIST_REQUEST_TIMEOUT", 20000, 1),
    PLAYLIST_RETRY_WINDOW_MS: numberEnv("PLAYLIST_RETRY_WINDOW_MS", 30000, 1),
    PLAYLIST_RETRY_DELAY_MS: numberEnv("PLAYLIST_RETRY_DELAY_MS", 2000, 0),

    UPSTREAM_KEEPALIVE_MAX_SOCKETS: numberEnv("UPSTREAM_KEEPALIVE_MAX_SOCKETS", 16, 1),
    UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS: numberEnv("UPSTREAM_KEEPALIVE_MAX_FREE_SOCKETS", 8, 1),
    UPSTREAM_KEEPALIVE_MS: numberEnv("UPSTREAM_KEEPALIVE_MS", 60000, 1000),

    LOGO_REQUEST_TIMEOUT: numberEnv("LOGO_REQUEST_TIMEOUT", 4000, 1),
    LOGO_FAILURE_TTL_MS: numberEnv("LOGO_FAILURE_TTL_MS", 60 * 60 * 1000, 0),
    LOGO_MAX_CACHE_ITEMS: numberEnv("LOGO_MAX_CACHE_ITEMS", 500, 50),

    FRONTEND_PRELOAD_FILE,
    SHORT_CONFIG_FILE: String(process.env.SHORT_CONFIG_FILE || (FRONTEND_PRELOAD_FILE
        ? path.join(path.dirname(FRONTEND_PRELOAD_FILE), "short-configs.json")
        : "")).trim(),
    BLOCKED_PLAYLISTS: parseList(process.env.KRONOS_BLOCKED_PLAYLISTS || "").map(value => value.toLowerCase())
};
