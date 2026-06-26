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
    RELEASE_VERSION: "5.7.0",
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
    HLS_MANIFEST_COALESCE_MS: numberEnv("HLS_MANIFEST_COALESCE_MS", 2500, 0),
    HLS_STALE_MANIFEST_MAX_MS: numberEnv("HLS_STALE_MANIFEST_MAX_MS", 2 * 60 * 1000, 0),
    SEGMENT_UPSTREAM_RETRIES: numberEnv("SEGMENT_UPSTREAM_RETRIES", 8, 0),
    SEGMENT_UPSTREAM_RETRY_DELAY_MS: numberEnv("SEGMENT_UPSTREAM_RETRY_DELAY_MS", 1000, 0),
    SEGMENT_TOKEN_HEALING: String(process.env.SEGMENT_TOKEN_HEALING || "1") !== "0",
    HLS_CACHE_BUST_SEGMENTS: String(process.env.HLS_CACHE_BUST_SEGMENTS || "1") !== "0",
    HLS_BLOCK_OFFLINE_PLACEHOLDERS: String(process.env.HLS_BLOCK_OFFLINE_PLACEHOLDERS || "1") !== "0",
    HLS_OFFLINE_PLACEHOLDER_MAX_SECONDS: numberEnv("HLS_OFFLINE_PLACEHOLDER_MAX_SECONDS", 45, 1),
    HLS_LIVE_EDGE_DELAY_SECONDS: numberEnv("HLS_LIVE_EDGE_DELAY_SECONDS", 60, 0),
    HLS_LIVE_EDGE_MIN_SEGMENTS: numberEnv("HLS_LIVE_EDGE_MIN_SEGMENTS", 3, 1),
    HLS_PLAYER_HOLD_BACK_SECONDS: numberEnv("HLS_PLAYER_HOLD_BACK_SECONDS", 30, 0, 60),
    HLS_SEGMENT_UPSTREAM_KEEPALIVE: String(process.env.HLS_SEGMENT_UPSTREAM_KEEPALIVE || "0") === "1",
    HLS_DIAGNOSTICS: String(process.env.HLS_DIAGNOSTICS || "1") !== "0",
    TS_SEGMENT_DIAGNOSTICS: String(process.env.TS_SEGMENT_DIAGNOSTICS || "1") !== "0",
    HLS_DIAGNOSTIC_HEADERS: String(process.env.HLS_DIAGNOSTIC_HEADERS || "0") === "1",
    HLS_DIAGNOSTIC_URLS: String(process.env.HLS_DIAGNOSTIC_URLS || "0") === "1",
    SEGMENT_STRICT_NO_CACHE: String(process.env.SEGMENT_STRICT_NO_CACHE || "1") !== "0",
    TRANSCODE_AUTO_ENABLED: String(process.env.TRANSCODE_AUTO_ENABLED || "1") !== "0",
    TRANSCODE_INCLUDE_ORIGINAL_VARIANT: String(process.env.TRANSCODE_INCLUDE_ORIGINAL_VARIANT || "1") !== "0",
    TRANSCODE_FFMPEG_PATH: process.env.TRANSCODE_FFMPEG_PATH || "ffmpeg",
    TRANSCODE_WORK_DIR: process.env.TRANSCODE_WORK_DIR || "/tmp/kronos-transcode",
    TRANSCODE_HEIGHT: numberEnv("TRANSCODE_HEIGHT", 480, 144, 1080),
    TRANSCODE_VIDEO_BITRATE_K: numberEnv("TRANSCODE_VIDEO_BITRATE_K", 900, 150, 8000),
    TRANSCODE_AUDIO_BITRATE_K: numberEnv("TRANSCODE_AUDIO_BITRATE_K", 96, 32, 320),
    TRANSCODE_HLS_TIME: numberEnv("TRANSCODE_HLS_TIME", 4, 2, 10),
    TRANSCODE_HLS_LIST_SIZE: numberEnv("TRANSCODE_HLS_LIST_SIZE", 12, 4, 30),
    TRANSCODE_START_TIMEOUT_MS: numberEnv("TRANSCODE_START_TIMEOUT_MS", 20000, 1000),
    TRANSCODE_IDLE_TIMEOUT_MS: numberEnv("TRANSCODE_IDLE_TIMEOUT_MS", 90000, 10000),
    TRANSCODE_MAX_SESSIONS: numberEnv("TRANSCODE_MAX_SESSIONS", 3, 1, 12),
    TRANSCODE_PRESET: process.env.TRANSCODE_PRESET || "veryfast",
    TS_LIVE_RECONNECT_DELAY_MS: numberEnv("TS_LIVE_RECONNECT_DELAY_MS", 250, 0),
    TS_LIVE_ERROR_RETRY_MS: numberEnv("TS_LIVE_ERROR_RETRY_MS", 1000, 0),

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
