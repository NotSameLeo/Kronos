const express = require("express");
const path = require("path");
const settings = require("./src/settings");
const state = require("./src/state");
const {
    attachDefaultConfig,
    attachShortConfig,
    decodeConfig,
    encodeConfig,
    extractConfigKey,
    getRequestConfig,
    getShortConfig,
    saveFrontendPreloadConfig,
    saveShortConfig
} = require("./src/config-store");
const {
    fetchAndProcessChannels,
    findCachedChannelById,
    getChannelById,
    getChannelsFromCache,
    withLiveEPG,
    startCatalogPeriodicRefresh,
    startCatalogStartupPreload
} = require("./src/catalog");
const { startEPGStartupPreload } = require("./src/epg");
const {
    fetchChannelsFromSource,
    getCatalogSourceName,
    getConfiguredLists,
    getEffectiveEpgUrl,
    getExtraParams,
    matchesChannelSearch,
    normalizeSourceType,
    sortChannelsByName
} = require("./src/sources");
const {
    copyResponseHeaders,
    closeUpstreamResponse,
    decodeProxyUrl,
    fetchSegmentWithHealing,
    getRewrittenManifest,
    monitorSegmentTransfer,
    RELAY_HEADERS,
    releaseActiveUpstream,
    setPlaylistHeaders
} = require("./src/proxy");
const {
    adaptiveMasterManifest,
    prewarmTranscode,
    serveTranscodeFile,
    transcodeManifest
} = require("./src/transcode");
const {
    buildManifest,
    buildStream,
    logoSvg,
    sendPosterSvg,
    toMeta
} = require("./src/stremio");
const {
    getPublicHost,
    isHttpUrl,
    normalizeGroupName,
    routeBase
} = require("./src/utils");

installTimestampedConsole();

const app = express();
app.set("trust proxy", true);
app.set("etag", false);
app.use(corsMiddleware);
app.use(dynamicNoCacheMiddleware);
app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));
app.use(requestLogMiddleware);

app.get("/logo.svg", (req, res) => {
    res.setHeader("Content-Type", "image/svg+xml");
    res.send(logoSvg());
});

app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        version: settings.RELEASE_VERSION,
        mode: settings.PLAYBACK_MODE,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString()
    });
});

app.get("/configure", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/manifest.json/configure", (req, res) => res.redirect("/"));
app.get("/:shortConfig([a-f0-9]{8,20})/configure", (req, res) =>
    res.redirect(`/?short=${encodeURIComponent(req.params.shortConfig)}`));
app.get("/c/:shortConfig/configure", (req, res) =>
    res.redirect(`/?short=${encodeURIComponent(req.params.shortConfig)}`));
app.get("/:base64Config/configure", (req, res) =>
    res.redirect(`/?config=${encodeURIComponent(req.params.base64Config)}`));

app.get("/manifest.json", attachDefaultConfig, manifestResponse);
app.get("/:shortConfig([a-f0-9]{8,20})/manifest.json", attachShortConfig, manifestResponse);
app.get("/c/:shortConfig/manifest.json", attachShortConfig, manifestResponse);
app.get("/:base64Config/manifest.json", manifestResponse);

app.post("/api/preload-config", preloadConfigResponse);
app.get("/api/config/:shortConfig", shortConfigResponse);
app.post("/api/analyze-link", analyzeLinkResponse);
app.post("/api/analyze-lists", analyzeListsResponse);

registerCatalogRoutes();
registerMetaRoutes();
registerPosterRoutes();
registerProxyRoutes();
registerStreamRoutes();
registerStatsRoutes();

app.listen(settings.PORT, "0.0.0.0", () => {
    console.log("");
    console.log("============================================================");
    console.log(`K.R.O.N.O.S. ${settings.RELEASE_VERSION} - ${settings.PLAYBACK_MODE}`);
    console.log("============================================================");
    console.log(`http://0.0.0.0:${settings.PORT}`);
    console.log(`Node ${process.version}`);
    console.log(`playback=${settings.PLAYBACK_MODE} playerPositionKnown=0 remux=0 vpn=0`);
    console.log(`hlsDiagnostics=${settings.HLS_DIAGNOSTICS ? 1 : 0} diagnosticUrls=${settings.HLS_DIAGNOSTIC_URLS ? 1 : 0} diagnosticHeaders=${settings.HLS_DIAGNOSTIC_HEADERS ? 1 : 0}`);
    console.log(`segmentStrictNoCache=${settings.SEGMENT_STRICT_NO_CACHE ? 1 : 0}`);
    console.log(`hlsTimeout=${settings.HLS_REQUEST_TIMEOUT / 1000}s segmentTimeout=${settings.SEG_REQUEST_TIMEOUT / 1000}s`);
    console.log(`manifestRetries=${settings.HLS_MANIFEST_RETRIES} manifestCoalesce=${settings.HLS_MANIFEST_COALESCE_MS}ms segmentRetries=${settings.SEGMENT_UPSTREAM_RETRIES}`);
    console.log(`tokenHealing=${settings.SEGMENT_TOKEN_HEALING ? 1 : 0} segmentCacheBust=${settings.HLS_CACHE_BUST_SEGMENTS ? 1 : 0} offlinePlaceholderBlock=${settings.HLS_BLOCK_OFFLINE_PLACEHOLDERS ? 1 : 0} liveEdgeDelay=${settings.HLS_LIVE_EDGE_DELAY_SECONDS}s playerHoldBack=${settings.HLS_PLAYER_HOLD_BACK_SECONDS}s segmentKeepAlive=${settings.HLS_SEGMENT_UPSTREAM_KEEPALIVE ? 1 : 0} rangeForward=1`);
    console.log(`transcodeAuto=${settings.TRANSCODE_AUTO_ENABLED ? 1 : 0} ladder=${settings.TRANSCODE_VARIANTS.map(variant => `${variant.height}p:${variant.videoK}k`).join(",")} sourceVariant=${settings.TRANSCODE_INCLUDE_SOURCE_VARIANT ? 1 : 0}:${settings.TRANSCODE_SOURCE_VIDEO_BITRATE_K}k originalVariant=${settings.TRANSCODE_INCLUDE_ORIGINAL_VARIANT ? 1 : 0} fileDiagnostics=${settings.TRANSCODE_FILE_DIAGNOSTICS ? 1 : 0} ffmpegDiagnostics=${settings.TRANSCODE_FFMPEG_DIAGNOSTICS ? 1 : 0} maxSessions=${settings.TRANSCODE_MAX_SESSIONS}`);
    console.log(`catalogPageSize=${settings.CATALOG_PAGE_SIZE} catalogRefresh=${settings.CATALOG_REFRESH_INTERVAL_MS / 1000}s`);
    console.log(`epgPreload=${settings.EPG_PRELOAD_URLS.length} epgRefresh=${settings.EPG_REFRESH_INTERVAL_MS / 1000}s`);
    console.log("============================================================");
    console.log("");

    startEPGStartupPreload();
    startCatalogStartupPreload();
    startCatalogPeriodicRefresh();
});

function installTimestampedConsole() {
    const formatter = new Intl.DateTimeFormat("it-IT", {
        timeZone: settings.LOG_TIME_ZONE,
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hourCycle: "h23"
    });
    for (const method of ["log", "warn", "error"]) {
        const original = console[method].bind(console);
        console[method] = (...args) => original(`[${formatter.format(new Date())}]`, ...args);
    }
}

function corsMiddleware(req, res, next) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Accept, Range");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges, X-Kronos-Total, X-Kronos-Skip, X-Kronos-Limit");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
}

function dynamicNoCacheMiddleware(req, res, next) {
    const dynamic = isDynamicStremioPath(req.path);
    if (!dynamic) return next();

    delete req.headers["if-none-match"];
    delete req.headers["if-modified-since"];
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.setHeader("Surrogate-Control", "no-store");
    res.removeHeader("ETag");
    res.removeHeader("Last-Modified");
    next();
}

function isDynamicStremioPath(pathname) {
    return /(?:^|\/)(?:manifest\.json|catalog\/|meta\/|stream\/|stats$|debug$|api\/config\/|api\/preload-config|api\/analyze-|proxy\/auto\.m3u8|proxy\/live\.m3u8|proxy\/live\.ts|proxy\/transcode)/i.test(pathname);
}

function requestLogMiddleware(req, res, next) {
    const startedAt = Date.now();
    res.on("finish", () => {
        const ua = String(req.get("user-agent") || "");
        const interesting = /stremio/i.test(ua)
            || /(?:manifest\.json|\/catalog\/|\/stream\/|\/configure$|\/api\/preload-config)/i.test(req.path);
        if (!interesting) return;
        console.log(`[HTTP] ${req.method} ${req.path} status=${res.statusCode} ms=${Date.now() - startedAt} ua=${ua.slice(0, 120) || "-"}`);
    });
    next();
}

async function manifestResponse(req, res) {
    try {
        const { configKey, config } = getRequestConfig(req);
        const lists = getConfiguredLists(config);
        saveFrontendPreloadConfig(configKey, lists, "manifest");
        const channels = await getChannelsFromCache(configKey, config);
        res.json(buildManifest(configKey, config, channels, getPublicHost(req)));
    } catch (err) {
        console.error("[MANIFEST ERROR]", err.message);
        res.status(500).json({ error: "Errore Token" });
    }
}

async function preloadConfigResponse(req, res) {
    try {
        const configKey = extractConfigKey(req.body.token || req.body.configKey || req.body.manifestUrl || "");
        const config = req.body.config || (configKey ? decodeConfig(configKey) : null);
        if (!config || typeof config !== "object") throw new Error("Missing config");

        const effectiveKey = configKey || encodeConfig(config);
        decodeConfig(effectiveKey);
        const lists = getConfiguredLists(config);
        const short = saveShortConfig(effectiveKey, config);
        const saved = saveFrontendPreloadConfig(effectiveKey, lists, "frontend");
        fetchAndProcessChannels(effectiveKey, config).catch(err => console.error("[FRONTEND PRELOAD WARM]", err.message));

        res.json({
            ok: true,
            saved,
            short,
            token: short,
            manifestPath: `/${short}/manifest.json`,
            configurePath: `/${short}/configure`
        });
    } catch (err) {
        console.warn("[PRELOAD CONFIG ERROR]", err.message);
        res.status(400).json({ ok: false, error: "Configurazione non valida" });
    }
}

function shortConfigResponse(req, res) {
    try {
        const resolved = getShortConfig(req.params.shortConfig);
        res.json({ ok: true, config: resolved.config, token: resolved.routeKey });
    } catch {
        res.status(404).json({ ok: false, error: "Configurazione non trovata" });
    }
}

async function analyzeLinkResponse(req, res) {
    try {
        const source = {
            name: req.body.name || "Lista",
            url: req.body.url,
            type: normalizeSourceType(req.body.type)
        };
        const channels = await fetchChannelsFromSource(source);
        res.json({ totalChannels: channels.length, groups: summarizeGroups(channels, false) });
    } catch {
        res.status(400).json({ error: "Impossibile analizzare questa sorgente" });
    }
}

async function analyzeListsResponse(req, res) {
    try {
        const lists = getConfiguredLists({ l: req.body.lists || [] });
        const parsed = await Promise.all(lists.map(fetchChannelsFromSource));
        const channels = parsed.flat();
        res.json({ totalChannels: channels.length, totalLists: lists.length, groups: summarizeGroups(channels, true) });
    } catch {
        res.status(400).json({ error: "Impossibile analizzare le sorgenti" });
    }
}

function summarizeGroups(channels, includeSources) {
    const map = new Map();
    channels.forEach(channel => {
        const current = map.get(channel.group) || {
            name: channel.group,
            count: 0,
            sources: new Set()
        };
        current.count += 1;
        current.sources.add(channel.sourceName);
        map.set(channel.group, current);
    });
    return [...map.values()]
        .map(group => includeSources
            ? { name: group.name, count: group.count, sources: [...group.sources] }
            : { name: group.name, count: group.count })
        .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
}

function registerCatalogRoutes() {
    app.get("/catalog/:type/:id.json", attachDefaultConfig, catalogResponse);
    app.get("/catalog/:type/:id/:extra.json", attachDefaultConfig, catalogResponse);
    app.get("/catalog/:type/:id/:extra", attachDefaultConfig, catalogResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/catalog/:type/:id.json", attachShortConfig, catalogResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/catalog/:type/:id/:extra.json", attachShortConfig, catalogResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/catalog/:type/:id/:extra", attachShortConfig, catalogResponse);
    app.get("/c/:shortConfig/catalog/:type/:id.json", attachShortConfig, catalogResponse);
    app.get("/c/:shortConfig/catalog/:type/:id/:extra.json", attachShortConfig, catalogResponse);
    app.get("/c/:shortConfig/catalog/:type/:id/:extra", attachShortConfig, catalogResponse);
    app.get("/:base64Config/catalog/:type/:id.json", catalogResponse);
    app.get("/:base64Config/catalog/:type/:id/:extra.json", catalogResponse);
    app.get("/:base64Config/catalog/:type/:id/:extra", catalogResponse);
}

async function catalogResponse(req, res) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    try {
        const startedAt = Date.now();
        const { configKey, config, routeKey } = getRequestConfig(req);
        const params = {
            ...getExtraParams(req.params.extra),
            ...Object.fromEntries(Object.entries(req.query || {}).map(([key, value]) => [key, Array.isArray(value) ? value[0] : value]))
        };
        const targetSource = getCatalogSourceName(req.params.id);
        const targetGroup = params.genre || null;
        const search = params.search ? String(params.search).trim() : null;
        const channels = withLiveEPG(configKey, config, await getChannelsFromCache(configKey, config));
        const filtered = sortChannelsByName(channels.filter(channel => {
            const sourceOk = targetSource ? channel.sourceName === targetSource : true;
            const groupOk = targetGroup ? normalizeGroupName(channel.group) === normalizeGroupName(targetGroup) : true;
            return sourceOk && groupOk && matchesChannelSearch(channel, search);
        }));
        const skip = Math.max(0, Number.parseInt(params.skip, 10) || 0);
        const page = filtered.slice(skip, skip + settings.CATALOG_PAGE_SIZE);

        if (Date.now() - startedAt > 100 || filtered.length > settings.CATALOG_PAGE_SIZE) {
            console.log(`[CATALOG SERVE] id=${req.params.id} total=${filtered.length} skip=${skip} page=${page.length} ms=${Date.now() - startedAt}`);
        }
        res.setHeader("X-Kronos-Total", String(filtered.length));
        res.setHeader("X-Kronos-Skip", String(skip));
        res.setHeader("X-Kronos-Limit", String(settings.CATALOG_PAGE_SIZE));
        res.json({
            metas: page.map(channel => toMeta(channel, getPublicHost(req), routeKey, {
                includeVideos: false,
                catalogLite: true,
                shortPoster: true
            }))
        });
    } catch (err) {
        console.error("[CATALOG ERROR]", err.message);
        res.status(500).json({ metas: [] });
    }
}

function registerMetaRoutes() {
    app.get("/meta/:type/:id.json", attachDefaultConfig, metaResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/meta/:type/:id.json", attachShortConfig, metaResponse);
    app.get("/c/:shortConfig/meta/:type/:id.json", attachShortConfig, metaResponse);
    app.get("/:base64Config/meta/:type/:id.json", metaResponse);
}

async function metaResponse(req, res) {
    try {
        const { configKey, config, routeKey } = getRequestConfig(req);
        const channel = withLiveEPG(configKey, config, [await getChannelById(configKey, config, req.params.id)]).filter(Boolean)[0];
        if (!channel) return res.status(404).json({ meta: null });
        res.json({ meta: toMeta(channel, getPublicHost(req), routeKey) });
    } catch (err) {
        console.error("[META ERROR]", err.message);
        res.status(500).json({ meta: null });
    }
}

function registerPosterRoutes() {
    app.get("/poster/:id.svg", async (req, res) => {
        try {
            await sendPosterSvg(res, findCachedChannelById(req.params.id));
        } catch {
            res.status(404).send("");
        }
    });
    app.get("/poster-config/:id.svg", attachDefaultConfig, configPosterResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/poster/:id.svg", attachShortConfig, configPosterResponse);
    app.get("/c/:shortConfig/poster/:id.svg", attachShortConfig, configPosterResponse);
    app.get("/:base64Config/poster/:id.svg", configPosterResponse);
}

async function configPosterResponse(req, res) {
    try {
        const { configKey, config } = getRequestConfig(req);
        const channel = await getChannelById(configKey, config, req.params.id);
        await sendPosterSvg(res, channel);
    } catch {
        res.status(404).send("");
    }
}

function registerProxyRoutes() {
    app.get("/proxy/auto.m3u8", attachDefaultConfig, proxyAutoManifestResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/auto.m3u8", attachShortConfig, proxyAutoManifestResponse);
    app.get("/c/:shortConfig/proxy/auto.m3u8", attachShortConfig, proxyAutoManifestResponse);
    app.get("/:base64Config/proxy/auto.m3u8", proxyAutoManifestResponse);
    app.get("/proxy/live.m3u8", attachDefaultConfig, proxyManifestResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/live.m3u8", attachShortConfig, proxyManifestResponse);
    app.get("/c/:shortConfig/proxy/live.m3u8", attachShortConfig, proxyManifestResponse);
    app.get("/:base64Config/proxy/live.m3u8", proxyManifestResponse);
    app.get("/proxy/transcode.m3u8", attachDefaultConfig, proxyTranscodeManifestResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/transcode.m3u8", attachShortConfig, proxyTranscodeManifestResponse);
    app.get("/c/:shortConfig/proxy/transcode.m3u8", attachShortConfig, proxyTranscodeManifestResponse);
    app.get("/:base64Config/proxy/transcode.m3u8", proxyTranscodeManifestResponse);
    app.get("/proxy/transcode/:session/:file", proxyTranscodeFileResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/transcode/:session/:file", proxyTranscodeFileResponse);
    app.get("/c/:shortConfig/proxy/transcode/:session/:file", proxyTranscodeFileResponse);
    app.get("/:base64Config/proxy/transcode/:session/:file", proxyTranscodeFileResponse);
    app.get("/proxy/live.ts", attachDefaultConfig, proxyLiveTsResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/live.ts", attachShortConfig, proxyLiveTsResponse);
    app.get("/c/:shortConfig/proxy/live.ts", attachShortConfig, proxyLiveTsResponse);
    app.get("/:base64Config/proxy/live.ts", proxyLiveTsResponse);
    app.get("/proxy/seg", attachDefaultConfig, proxySegmentResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/proxy/seg", attachShortConfig, proxySegmentResponse);
    app.get("/c/:shortConfig/proxy/seg", attachShortConfig, proxySegmentResponse);
    app.get("/:base64Config/proxy/seg", proxySegmentResponse);
}

async function proxyAutoManifestResponse(req, res) {
    try {
        const { routeKey } = getRequestConfig(req);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).type("text/plain").send("#EXTM3U\n");
        setPlaylistHeaders(res);
        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Auto-Transcode", settings.TRANSCODE_AUTO_ENABLED ? "1" : "0");
        if (!settings.TRANSCODE_AUTO_ENABLED) {
            return res.redirect(302, `${routeBase(getPublicHost(req), routeKey)}/proxy/live.m3u8?${new URLSearchParams(req.query).toString()}`);
        }
        prewarmTranscode(upstream, req.query.h);
        res.send(adaptiveMasterManifest(getPublicHost(req), routeKey, upstream, {
            blockOfflinePlaceholders: req.query.pg !== "0",
            liveEdgeDelaySeconds: queryNumber(req.query.d),
            startOffsetSeconds: queryNumber(req.query.st),
            holdBackSeconds: queryNumber(req.query.hb)
        }));
    } catch (err) {
        console.error("[PROXY AUTO M3U8]", err.message);
        if (!res.headersSent) res.status(502).type("text/plain").send("#EXTM3U\n");
    }
}

async function proxyManifestResponse(req, res) {
    try {
        const { configKey, routeKey } = getRequestConfig(req);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).type("text/plain").send("#EXTM3U\n");
        const manifest = await getRewrittenManifest(configKey, upstream, getPublicHost(req), routeKey, req);
        setPlaylistHeaders(res);
        res.setHeader("X-Kronos-Relay", "1");
        if (manifest.stale) {
            res.setHeader("X-Kronos-Stale-Manifest", "1");
            res.setHeader("X-Kronos-Stale-Age-Ms", String(manifest.staleAgeMs || 0));
        }
        res.send(manifest.text);
    } catch (err) {
        console.error("[PROXY M3U8]", err.message);
        if (!res.headersSent) {
            if (err.statusCode === 503) {
                setPlaylistHeaders(res);
                res.setHeader("Retry-After", String(err.retryAfter || 2));
                res.setHeader("X-Kronos-Relay", "1");
                res.setHeader("X-Kronos-Blocked-Placeholder", "1");
                return res.status(503).send("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n");
            }
            res.status(502).type("text/plain").send("#EXTM3U\n");
        }
    }
}

async function proxyTranscodeManifestResponse(req, res) {
    try {
        const { routeKey } = getRequestConfig(req);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).type("text/plain").send("#EXTM3U\n");
        const manifest = await transcodeManifest(getPublicHost(req), routeKey, upstream, req);
        setPlaylistHeaders(res);
        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Transcode", "1");
        res.send(manifest);
    } catch (err) {
        console.error("[PROXY TRANSCODE M3U8]", err.message);
        if (!res.headersSent) {
            res.setHeader("Retry-After", "2");
            res.status(err.statusCode || 503).type("text/plain").send("#EXTM3U\n");
        }
    }
}

async function proxyTranscodeFileResponse(req, res) {
    await serveTranscodeFile(req.params.session, req.params.file, res);
}

async function proxyLiveTsResponse(req, res) {
    const { configKey, routeKey } = getRequestConfig(req);
    try {
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream) || !isDirectTsUrl(upstream)) return res.status(400).end();
        const hlsUpstream = upstream.replace(/\.ts([?#].*)?$/i, ".m3u8$1");
        req.query.d ||= "60";
        req.query.st ||= "20";
        const manifest = await getRewrittenManifest(configKey, hlsUpstream, getPublicHost(req), routeKey, req);
        setPlaylistHeaders(res);
        res.setHeader("X-Kronos-Relay", "1");
        res.setHeader("X-Kronos-Ts-Hls-Fallback", "1");
        if (manifest.stale) {
            res.setHeader("X-Kronos-Stale-Manifest", "1");
            res.setHeader("X-Kronos-Stale-Age-Ms", String(manifest.staleAgeMs || 0));
        }
        res.send(manifest.text);
    } catch (err) {
        console.error("[PROXY TS LIVE]", err?.response?.status || err.code || err.message);
        if (!res.headersSent) res.status(502).end();
    }
}

function queryNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

async function proxySegmentResponse(req, res) {
    try {
        const { configKey, routeKey } = getRequestConfig(req);
        const upstream = decodeProxyUrl(req.query.u || "");
        if (!isHttpUrl(upstream)) return res.status(400).end();

        const directTsRequest = isDirectTsProxyRequest(upstream, req);
        const headers = { ...RELAY_HEADERS };
        if (req.headers.range && !directTsRequest) headers.Range = req.headers.range;

        const upstreamResponse = await fetchSegmentWithHealing(configKey, routeKey, upstream, headers, req);
        res.status(directTsRequest && upstreamResponse.status === 206 ? 200 : upstreamResponse.status);
        copyResponseHeaders(upstreamResponse, res);
        if (settings.SEGMENT_STRICT_NO_CACHE) {
            res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
            res.setHeader("Pragma", "no-cache");
            res.setHeader("Expires", "0");
            res.setHeader("Surrogate-Control", "no-store");
            res.setHeader("X-Accel-Buffering", "no");
            res.removeHeader("ETag");
        } else {
            res.setHeader("Cache-Control", "no-store");
        }
        res.setHeader("X-Kronos-Relay", "1");
        monitorSegmentTransfer(upstreamResponse, req, res);
        upstreamResponse.data.on("error", () => {
            if (!res.headersSent) res.status(502).end();
            else res.destroy();
        });
        res.on("close", () => {
            releaseActiveUpstream(upstreamResponse);
            closeUpstreamResponse(upstreamResponse);
        });
        upstreamResponse.data.pipe(res);
    } catch (err) {
        console.error("[PROXY SEG]", err?.response?.status || err.code || err.message);
        if (!res.headersSent) res.status(502).end();
    }
}

function isDirectTsProxyRequest(upstream, req) {
    if (req.query.p || req.query.s) return false;
    return isDirectTsUrl(upstream);
}

function isDirectTsUrl(upstream) {
    try {
        return new URL(upstream).pathname.toLowerCase().endsWith(".ts");
    } catch {
        return false;
    }
}

function registerStreamRoutes() {
    app.get("/stream/:type/:id.json", attachDefaultConfig, streamResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/stream/:type/:id.json", attachShortConfig, streamResponse);
    app.get("/c/:shortConfig/stream/:type/:id.json", attachShortConfig, streamResponse);
    app.get("/:base64Config/stream/:type/:id.json", streamResponse);
}

async function streamResponse(req, res) {
    try {
        const { configKey, config, routeKey } = getRequestConfig(req);
        const channel = await getChannelById(configKey, config, req.params.id);
        if (!channel) return res.status(404).json({ streams: [] });
        res.json({ streams: [buildStream(channel, getPublicHost(req), routeKey)] });
    } catch (err) {
        console.error("[STREAM ERROR]", err.message);
        res.status(500).json({ streams: [] });
    }
}

function registerStatsRoutes() {
    app.get("/stats", attachDefaultConfig, statsResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/stats", attachShortConfig, statsResponse);
    app.get("/c/:shortConfig/stats", attachShortConfig, statsResponse);
    app.get("/:base64Config/stats", statsResponse);
    app.get("/debug", attachDefaultConfig, debugResponse);
    app.get("/:shortConfig([a-f0-9]{8,20})/debug", attachShortConfig, debugResponse);
    app.get("/c/:shortConfig/debug", attachShortConfig, debugResponse);
    app.get("/:base64Config/debug", debugResponse);
}

function statsResponse(req, res) {
    try {
        const { configKey } = getRequestConfig(req);
        res.json(buildStats(configKey));
    } catch {
        res.json(buildStats(""));
    }
}

async function debugResponse(req, res) {
    try {
        const { configKey, config } = getRequestConfig(req);
        const channels = withLiveEPG(configKey, config, await getChannelsFromCache(configKey, config));
        const effectiveConfig = state.configByKey.get(configKey) || config;
        const effectiveEpgUrl = effectiveConfig.e || getEffectiveEpgUrl(config, getConfiguredLists(config));
        res.json({
            ...buildStats(configKey),
            config: {
                hasUrl: !!config.u,
                hasMultiLists: Array.isArray(config.l),
                listCount: getConfiguredLists(config).length,
                sourceTypes: getConfiguredLists(config).map(list => list.type),
                groupMode: config.gm,
                selectedGroups: config.g
            },
            cache: {
                ...buildStats(configKey).cache,
                lastUpdate: state.lastCatalogUpdate.get(configKey) || 0,
                isUpdating: state.updatingCatalog.has(configKey),
                epg: state.epgMatchStats.get(configKey) || null,
                epgFetch: effectiveEpgUrl ? {
                    url: effectiveEpgUrl,
                    ...(state.epgStatus.get(effectiveEpgUrl) || { state: "idle" }),
                    inflight: state.epgInflight.has(effectiveEpgUrl)
                } : null
            },
            sampleChannels: channels.slice(0, 5).map(channel => ({
                id: channel.id,
                name: channel.name,
                group: channel.group,
                sourceName: channel.sourceName,
                sourceType: channel.sourceType || "m3u",
                hasUrl: !!channel.url,
                hasLogo: !!channel.logo,
                hasEpg: !!channel.description,
                epgId: channel.epgId
            })),
            groups: [...new Set(channels.map(channel => channel.group))].slice(0, 50)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
}

function buildStats(configKey) {
    const channelCount = [...state.channels.values()].reduce((sum, channels) => sum + channels.length, 0);
    return {
        version: settings.RELEASE_VERSION,
        mode: settings.PLAYBACK_MODE,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        cache: {
            channels: channelCount,
            currentConfigChannels: configKey ? state.channels.get(configKey)?.length || 0 : 0,
            epgMaps: state.epgData.size,
            logos: state.logoData.size,
            hlsSegmentMaps: state.segmentMaps.size,
            activeSegmentUpstreams: state.activeSegmentUpstreams.size,
            transcodeSessions: state.transcodeSessions.size
        },
        playback: {
            mode: settings.PLAYBACK_MODE,
            vpn: false,
            remux: false,
            playerPositionKnown: false,
            resolutionLimit: false,
            segmentTokenHealing: settings.SEGMENT_TOKEN_HEALING,
            segmentCacheBust: settings.HLS_CACHE_BUST_SEGMENTS,
            offlinePlaceholderBlock: settings.HLS_BLOCK_OFFLINE_PLACEHOLDERS,
            liveEdgeDelaySeconds: settings.HLS_LIVE_EDGE_DELAY_SECONDS,
            liveEdgeMinSegments: settings.HLS_LIVE_EDGE_MIN_SEGMENTS,
            playerHoldBackSeconds: settings.HLS_PLAYER_HOLD_BACK_SECONDS,
            segmentUpstreamKeepAlive: settings.HLS_SEGMENT_UPSTREAM_KEEPALIVE,
            manifestRetries: settings.HLS_MANIFEST_RETRIES,
            manifestCoalesceMs: settings.HLS_MANIFEST_COALESCE_MS,
            segmentRetries: settings.SEGMENT_UPSTREAM_RETRIES,
            hlsDiagnostics: settings.HLS_DIAGNOSTICS,
            hlsDiagnosticUrls: settings.HLS_DIAGNOSTIC_URLS,
            hlsDiagnosticHeaders: settings.HLS_DIAGNOSTIC_HEADERS,
            segmentStrictNoCache: settings.SEGMENT_STRICT_NO_CACHE,
            transcodeAutoEnabled: settings.TRANSCODE_AUTO_ENABLED,
            transcodeVariants: settings.TRANSCODE_VARIANTS,
            transcodeSourceVariant: settings.TRANSCODE_INCLUDE_SOURCE_VARIANT,
            transcodeSourceVideoBitrateK: settings.TRANSCODE_SOURCE_VIDEO_BITRATE_K,
            transcodeSourceAudioBitrateK: settings.TRANSCODE_SOURCE_AUDIO_BITRATE_K,
            transcodeSourceBandwidth: settings.TRANSCODE_SOURCE_BANDWIDTH,
            transcodeHlsInputLiveStartIndex: settings.TRANSCODE_HLS_INPUT_LIVE_START_INDEX,
            transcodeHlsDeleteThreshold: settings.TRANSCODE_HLS_DELETE_THRESHOLD,
            transcodeOriginalVariant: settings.TRANSCODE_INCLUDE_ORIGINAL_VARIANT,
            transcodeFileDiagnostics: settings.TRANSCODE_FILE_DIAGNOSTICS,
            transcodeFfmpegDiagnostics: settings.TRANSCODE_FFMPEG_DIAGNOSTICS,
            transcodeBlackGuardMinSegments: settings.TRANSCODE_BLACK_GUARD_MIN_SEGMENTS,
            transcodeHeight: settings.TRANSCODE_HEIGHT,
            transcodeVideoBitrateK: settings.TRANSCODE_VIDEO_BITRATE_K,
            transcodeAudioBitrateK: settings.TRANSCODE_AUDIO_BITRATE_K,
            transcodeMaxSessions: settings.TRANSCODE_MAX_SESSIONS
        }
    };
}
