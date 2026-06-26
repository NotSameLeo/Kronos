const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { spawn } = require("child_process");
const settings = require("./settings");
const state = require("./state");
const {
    encodeBase64Url,
    hashKey,
    isHttpUrl,
    routeBase,
    sleep
} = require("./utils");

const MASTER_CODECS = "avc1.4d401f,mp4a.40.2";

let cleanupTimerStarted = false;

function adaptiveMasterManifest(host, routeKey, upstream, options = {}) {
    const params = new URLSearchParams({
        u: encodeBase64Url(upstream),
        pg: options.blockOfflinePlaceholders ? "1" : "0"
    });
    if (Number.isFinite(options.liveEdgeDelaySeconds) && options.liveEdgeDelaySeconds > 0) {
        params.set("d", String(Math.round(options.liveEdgeDelaySeconds)));
    }
    if (Number.isFinite(options.startOffsetSeconds) && options.startOffsetSeconds > 0) {
        params.set("st", String(Math.round(options.startOffsetSeconds)));
    }
    if (Number.isFinite(options.holdBackSeconds) && options.holdBackSeconds > 0) {
        params.set("hb", String(Math.round(options.holdBackSeconds)));
    }

    const base = routeBase(host, routeKey);
    const lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-INDEPENDENT-SEGMENTS"
    ];

    for (const variant of settings.TRANSCODE_VARIANTS) {
        const variantParams = new URLSearchParams(params);
        variantParams.set("v", variant.name);
        const peak = Math.round((variant.videoK + variant.audioK) * 1000 * 1.25);
        const average = Math.round((variant.videoK + variant.audioK) * 1000);
        lines.push(
            `#EXT-X-STREAM-INF:BANDWIDTH=${peak},AVERAGE-BANDWIDTH=${average},RESOLUTION=${variant.width}x${variant.height},CODECS="${MASTER_CODECS}",NAME="${variant.label}"`,
            `${base}/proxy/transcode.m3u8?${variantParams.toString()}`
        );
    }

    if (settings.TRANSCODE_INCLUDE_ORIGINAL_VARIANT) {
        lines.push(
            `#EXT-X-STREAM-INF:BANDWIDTH=${settings.TRANSCODE_ORIGINAL_BANDWIDTH},AVERAGE-BANDWIDTH=${settings.TRANSCODE_ORIGINAL_AVERAGE_BANDWIDTH},NAME="Original"`,
            `${base}/proxy/live.m3u8?${params.toString()}`
        );
    }

    return `${lines.join("\n")}\n`;
}

async function transcodeManifest(host, routeKey, upstream, req) {
    if (!settings.TRANSCODE_AUTO_ENABLED) {
        const err = new Error("Transcode disabled");
        err.statusCode = 503;
        throw err;
    }
    if (!isHttpUrl(upstream)) {
        const err = new Error("Invalid transcode upstream");
        err.statusCode = 400;
        throw err;
    }

    startCleanupTimer();
    const variant = selectVariant(req);
    const session = await getOrStartSession(upstream);
    await waitForManifest(session, variant, settings.TRANSCODE_START_TIMEOUT_MS);
    return rewriteTranscodePlaylist(session, variant, host, routeKey);
}

function prewarmTranscode(upstream) {
    if (!settings.TRANSCODE_AUTO_ENABLED || !isHttpUrl(upstream)) return;
    startCleanupTimer();
    getOrStartSession(upstream).catch(err => {
        if (settings.HLS_DIAGNOSTICS) console.warn(`[TRANSCODE PREWARM ERR] ${compact(err.message)}`);
    });
}

async function serveTranscodeFile(sessionId, fileName, res) {
    const session = state.transcodeSessions.get(sessionId);
    if (!session || !isSafeHlsFile(fileName)) {
        res.status(404).end();
        return;
    }
    session.lastAccess = Date.now();
    const filePath = path.join(session.dir, fileName);
    if (!filePath.startsWith(`${session.dir}${path.sep}`)) {
        res.status(404).end();
        return;
    }
    try {
        await fsp.access(filePath, fs.constants.R_OK);
    } catch {
        res.status(404).end();
        return;
    }

    if (/\.m3u8$/i.test(fileName)) {
        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    } else {
        res.setHeader("Content-Type", "video/mp2t");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    }
    res.setHeader("X-Kronos-Transcode", "1");
    res.sendFile(filePath);
}

async function getOrStartSession(upstream) {
    const variants = settings.TRANSCODE_VARIANTS;
    const id = hashKey(`${upstream}|${variantSignature(variants)}`, 20);
    const existing = state.transcodeSessions.get(id);
    if (existing?.process && !existing.exitedAt) {
        existing.lastAccess = Date.now();
        return existing;
    }

    await ensureWorkDir();
    trimTranscodeSessions(settings.TRANSCODE_MAX_SESSIONS - 1);
    const dir = path.join(settings.TRANSCODE_WORK_DIR, id);
    await fsp.rm(dir, { recursive: true, force: true });
    await fsp.mkdir(dir, { recursive: true });
    const session = {
        id,
        upstream,
        variants,
        dir,
        startedAt: Date.now(),
        lastAccess: Date.now(),
        ready: new Set(),
        stderr: ""
    };
    session.process = spawn(settings.TRANSCODE_FFMPEG_PATH, ffmpegArgs(upstream, session), {
        stdio: ["ignore", "ignore", "pipe"]
    });
    session.process.stderr.on("data", chunk => {
        session.stderr = `${session.stderr}${chunk.toString()}`.slice(-4000);
    });
    session.process.once("error", err => {
        session.exitedAt = Date.now();
        session.stderr = `${session.stderr}\n${err.message}`.slice(-4000);
    });
    session.process.once("exit", (code, signal) => {
        session.exitedAt = Date.now();
        session.exitCode = code;
        session.exitSignal = signal;
    });
    state.transcodeSessions.set(id, session);
    logTranscode("START", session, { pid: session.process.pid, variants: variants.map(v => v.name).join(",") });
    return session;
}

function ffmpegArgs(upstream, session) {
    const variants = session.variants;
    const args = [
        "-hide_banner",
        "-loglevel", "warning",
        "-nostdin",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "4",
        "-user_agent", settings.UPSTREAM_UA,
        "-headers", "Cache-Control: no-cache\r\nPragma: no-cache\r\n",
        "-i", upstream,
        "-filter_complex", filterComplex(variants)
    ];

    for (let index = 0; index < variants.length; index++) {
        const variant = variants[index];
        args.push(
            "-map", `[v${index}out]`,
            "-map", "0:a:0?",
            "-sn",
            "-dn",
            "-c:v", "libx264",
            "-preset", settings.TRANSCODE_PRESET,
            "-tune", "zerolatency",
            "-sc_threshold", "0",
            "-force_key_frames", `expr:gte(t,n_forced*${settings.TRANSCODE_HLS_TIME})`,
            "-b:v", `${variant.videoK}k`,
            "-maxrate", `${Math.round(variant.videoK * 1.2)}k`,
            "-bufsize", `${Math.round(variant.videoK * 2)}k`,
            "-c:a", "aac",
            "-b:a", `${variant.audioK}k`,
            "-ac", "2",
            "-f", "hls",
            "-hls_time", String(settings.TRANSCODE_HLS_TIME),
            "-hls_list_size", String(settings.TRANSCODE_HLS_LIST_SIZE),
            "-hls_delete_threshold", "4",
            "-hls_flags", "delete_segments+omit_endlist+program_date_time+independent_segments",
            "-hls_segment_filename", path.join(session.dir, `${variant.name}_seg_%06d.ts`),
            variantManifestPath(session, variant)
        );
    }

    return args;
}

function filterComplex(variants) {
    if (variants.length === 1) return `[0:v:0]scale=-2:${variants[0].height}[v0out]`;
    const splitOutputs = variants.map((_variant, index) => `[v${index}]`).join("");
    const scales = variants
        .map((variant, index) => `[v${index}]scale=-2:${variant.height}[v${index}out]`)
        .join(";");
    return `[0:v:0]split=${variants.length}${splitOutputs};${scales}`;
}

async function waitForManifest(session, variant, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        session.lastAccess = Date.now();
        if (await hasReadableManifest(variantManifestPath(session, variant), variant)) {
            if (!session.ready.has(variant.name)) logTranscode("READY", session, { variant: variant.name });
            session.ready.add(variant.name);
            return;
        }
        if (session.exitedAt) break;
        await sleep(250);
    }
    stopSession(session, "start-timeout");
    const err = new Error(`Transcode manifest not ready for ${variant.name}${session.stderr ? `: ${compact(session.stderr)}` : ""}`);
    err.statusCode = 503;
    throw err;
}

async function hasReadableManifest(manifestPath, variant) {
    try {
        const text = await fsp.readFile(manifestPath, "utf8");
        return text.includes("#EXTM3U") && text.includes(`${variant.name}_seg_`);
    } catch {
        return false;
    }
}

async function rewriteTranscodePlaylist(session, variant, host, routeKey) {
    session.lastAccess = Date.now();
    const text = await fsp.readFile(variantManifestPath(session, variant), "utf8");
    const base = routeBase(host, routeKey);
    return text.split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) return line;
        const fileName = path.basename(trimmed);
        return `${base}/proxy/transcode/${session.id}/${encodeURIComponent(fileName)}`;
    }).join("\n");
}

function startCleanupTimer() {
    if (cleanupTimerStarted) return;
    cleanupTimerStarted = true;
    const timer = setInterval(() => trimTranscodeSessions(settings.TRANSCODE_MAX_SESSIONS), 15000);
    timer.unref?.();
}

function trimTranscodeSessions(maxSessions) {
    const now = Date.now();
    for (const session of state.transcodeSessions.values()) {
        const idleMs = now - (session.lastAccess || session.startedAt || now);
        if (idleMs > settings.TRANSCODE_IDLE_TIMEOUT_MS || session.exitedAt) {
            stopSession(session, session.exitedAt ? "exited" : "idle");
        }
    }

    const sessions = [...state.transcodeSessions.values()]
        .sort((a, b) => (a.lastAccess || 0) - (b.lastAccess || 0));
    while (sessions.length > maxSessions) {
        stopSession(sessions.shift(), "max-sessions");
    }
}

function stopSession(session, reason) {
    if (!session || !state.transcodeSessions.has(session.id)) return;
    state.transcodeSessions.delete(session.id);
    try { session.process?.kill?.("SIGTERM"); } catch {}
    setTimeout(() => {
        try {
            if (session.process && !session.process.killed) session.process.kill("SIGKILL");
        } catch {}
    }, 3000).unref?.();
    fsp.rm(session.dir, { recursive: true, force: true }).catch(() => {});
    logTranscode("STOP", session, { reason });
}

function selectVariant(req) {
    const requested = String(req?.query?.v || req?.query?.h || "").toLowerCase();
    return settings.TRANSCODE_VARIANTS.find(variant =>
        variant.name === requested || String(variant.height) === requested || `${variant.height}p` === requested
    ) || settings.TRANSCODE_VARIANTS[0];
}

function variantManifestPath(session, variant) {
    return path.join(session.dir, `${variant.name}.m3u8`);
}

function variantSignature(variants) {
    return variants.map(variant => `${variant.name}:${variant.height}:${variant.videoK}:${variant.audioK}`).join("|");
}

async function ensureWorkDir() {
    await fsp.mkdir(settings.TRANSCODE_WORK_DIR, { recursive: true });
}

function isSafeHlsFile(fileName) {
    return /^[A-Za-z0-9_.-]+\.(?:m3u8|ts)$/.test(String(fileName || ""));
}

function compact(value) {
    return String(value || "").replace(/\s+/g, " ").slice(0, 220);
}

function logTranscode(label, session, fields = {}) {
    if (!settings.HLS_DIAGNOSTICS) return;
    const parts = {
        sid: session?.id || "-",
        src: session?.upstream ? hashKey(session.upstream, 12) : "-",
        ...fields
    };
    console.log(`[TRANSCODE ${label}] ${Object.entries(parts).map(([key, value]) => `${key}=${value ?? "-"}`).join(" ")}`);
}

module.exports = {
    adaptiveMasterManifest,
    prewarmTranscode,
    serveTranscodeFile,
    transcodeManifest
};
