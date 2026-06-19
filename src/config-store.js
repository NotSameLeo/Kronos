const fs = require("fs");
const path = require("path");
const settings = require("./settings");
const state = require("./state");
const { hashKey } = require("./utils");

function decodeConfig(configKey) {
    try {
        const normalized = String(configKey || "").replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
        return JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    } catch {
        throw new Error("Invalid configuration token");
    }
}

function encodeConfig(config) {
    return Buffer.from(JSON.stringify(config), "utf8").toString("base64url");
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

function readJsonFile(file) {
    try {
        return JSON.parse(fs.readFileSync(file, "utf8"));
    } catch (err) {
        if (err?.code !== "ENOENT") console.warn(`[CONFIG READ] ${file}: ${err.message}`);
        return null;
    }
}

function writeJsonFile(file, payload) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(payload, null, 2));
    fs.renameSync(tmp, file);
}

function readShortConfigStore() {
    if (!settings.SHORT_CONFIG_FILE) return {};
    if (state.shortConfigs) return state.shortConfigs;
    const parsed = readJsonFile(settings.SHORT_CONFIG_FILE);
    state.shortConfigs = parsed && typeof parsed === "object" ? parsed : {};
    return state.shortConfigs;
}

function writeShortConfigStore(store) {
    if (!settings.SHORT_CONFIG_FILE) return false;
    try {
        writeJsonFile(settings.SHORT_CONFIG_FILE, store);
        state.shortConfigs = store;
        return true;
    } catch (err) {
        console.warn(`[SHORT CONFIG SAVE] ${err.message}`);
        return false;
    }
}

function saveShortConfig(configKey, config) {
    const short = hashKey(configKey, 12);
    const store = { ...readShortConfigStore() };
    store[short] = { configKey, config, savedAt: new Date().toISOString() };
    writeShortConfigStore(store);
    return short;
}

function getShortConfig(short) {
    const id = String(short || "").trim();
    if (!/^[a-f0-9]{8,20}$/i.test(id)) throw new Error("Invalid short configuration token");
    const entry = readShortConfigStore()[id];
    if (!entry?.configKey) throw new Error("Short configuration not found");
    return {
        configKey: entry.configKey,
        config: entry.config && typeof entry.config === "object" ? entry.config : decodeConfig(entry.configKey),
        routeKey: id
    };
}

function readFrontendPreloadConfigKey() {
    if (!settings.FRONTEND_PRELOAD_FILE) return "";
    const payload = readJsonFile(settings.FRONTEND_PRELOAD_FILE);
    if (!payload) return "";
    const configKey = extractConfigKey(payload.configKey || payload.manifestUrl || "");
    if (!configKey) return "";
    decodeConfig(configKey);
    return configKey;
}

function saveFrontendPreloadConfig(configKey, lists, reason = "frontend") {
    if (!settings.FRONTEND_PRELOAD_FILE || !configKey || !lists?.length) return false;
    try {
        writeJsonFile(settings.FRONTEND_PRELOAD_FILE, {
            configKey,
            savedAt: new Date().toISOString(),
            reason,
            lists: lists.map(list => ({ name: list.name, url: list.url }))
        });
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
    return { source: "env", configs: settings.CATALOG_PRELOAD_CONFIGS.map(extractConfigKey).filter(Boolean) };
}

function getRequestConfig(req) {
    const configKey = req.params.base64Config;
    return {
        configKey,
        config: req.kronosConfig || decodeConfig(configKey),
        routeKey: req.kronosRouteKey === undefined ? configKey : req.kronosRouteKey
    };
}

function attachShortConfig(req, res, next) {
    try {
        const resolved = getShortConfig(req.params.shortConfig);
        req.params.base64Config = resolved.configKey;
        req.kronosConfig = resolved.config;
        req.kronosRouteKey = resolved.routeKey;
        next();
    } catch (err) {
        console.error("[SHORT CONFIG ERROR]", err.message);
        res.status(404).json({ error: "Configurazione non trovata" });
    }
}

function attachDefaultConfig(req, res, next) {
    try {
        const configKey = readFrontendPreloadConfigKey();
        if (!configKey) throw new Error("Default configuration not found");
        req.params.base64Config = configKey;
        req.kronosConfig = decodeConfig(configKey);
        req.kronosRouteKey = "";
        next();
    } catch (err) {
        console.error("[DEFAULT CONFIG ERROR]", err.message);
        res.status(404).json({ error: "Configurazione non trovata" });
    }
}

module.exports = {
    decodeConfig,
    encodeConfig,
    extractConfigKey,
    saveShortConfig,
    getShortConfig,
    readFrontendPreloadConfigKey,
    saveFrontendPreloadConfig,
    getStartupPreloadConfigs,
    getRequestConfig,
    attachShortConfig,
    attachDefaultConfig
};
