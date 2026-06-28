const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { execFile, spawn } = require("child_process");
const settings = require("./settings");
const state = require("./state");
const {
    encodeBase64Url,
    hashKey,
    isHlsUrl,
    isHttpUrl,
    routeBase,
    sleep
} = require("./utils");

const MASTER_CODECS = "avc1.4d401f,mp4a.40.2";
const SOURCE_VARIANT_NAME = "source";

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

    for (const variant of playbackVariants()) {
        const variantParams = new URLSearchParams(params);
        variantParams.set("v", variant.name);
        lines.push(masterStreamInfo(variant), `${base}/proxy/transcode.m3u8?${variantParams.toString()}`);
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
    return rewriteTranscodePlaylist(session, variant, host, routeKey, {
        holdBackSeconds: queryNumber(req?.query?.hb),
        startOffsetSeconds: queryNumber(req?.query?.st)
    });
}

function prewarmTranscode(upstream) {
    if (!settings.TRANSCODE_AUTO_ENABLED || !isHttpUrl(upstream)) return;
    startCleanupTimer();
    getOrStartSession(upstream).catch(err => {
        if (settings.HLS_DIAGNOSTICS) console.warn(`[TRANSCODE PREWARM ERR] ${compact(err.message)}`);
    });
}

async function serveTranscodeFile(sessionId, fileName, res) {
    const startedAt = Date.now();
    const session = state.transcodeSessions.get(sessionId);
    if (!session || !isSafeHlsFile(fileName)) {
        logTranscodeFile("MISS", { sid: sessionId, file: fileName, reason: session ? "unsafe" : "no-session" });
        res.status(404).end();
        return;
    }
    session.lastAccess = Date.now();
    const info = transcodeFileInfo(fileName);
    const filePath = path.join(session.dir, fileName);
    if (!filePath.startsWith(`${session.dir}${path.sep}`)) {
        logTranscodeFile("MISS", { session, file: fileName, reason: "path" });
        res.status(404).end();
        return;
    }
    let stat;
    try {
        stat = await fsp.stat(filePath);
    } catch {
        logTranscodeFile("MISS", { session, file: fileName, variant: info.variant, seq: info.seq, reason: "missing" });
        res.status(404).end();
        return;
    }

    if (info.kind === "manifest") {
        res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    } else {
        res.setHeader("Content-Type", "video/mp2t");
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, no-transform");
    }
    res.setHeader("X-Kronos-Transcode", "1");
    logTranscodeFile("REQ", {
        session,
        kind: info.kind,
        variant: info.variant,
        seq: info.seq,
        file: fileName,
        bytes: stat.size,
        ua: compact(res.req?.get?.("user-agent") || "-")
    });
    let closed = false;
    res.on("finish", () => {
        closed = true;
        logTranscodeFile("SENT", {
            session,
            kind: info.kind,
            variant: info.variant,
            seq: info.seq,
            file: fileName,
            status: res.statusCode,
            bytes: stat.size,
            ms: Date.now() - startedAt
        });
    });
    res.on("close", () => {
        if (closed) return;
        logTranscodeFile("CLOSE", {
            session,
            kind: info.kind,
            variant: info.variant,
            seq: info.seq,
            file: fileName,
            status: res.statusCode,
            bytes: stat.size,
            ms: Date.now() - startedAt
        });
    });
    res.sendFile(filePath);
}

async function getOrStartSession(upstream) {
    const variants = playbackVariants();
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
        ffmpegLogLine: "",
        stderr: ""
    };
    session.process = spawn(settings.TRANSCODE_FFMPEG_PATH, ffmpegArgs(upstream, session), {
        stdio: ["ignore", "ignore", "pipe"]
    });
    session.process.stderr.on("data", chunk => {
        const text = chunk.toString();
        session.stderr = `${session.stderr}${text}`.slice(-4000);
        logFfmpegLines(session, text);
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
    const hlsInputArgs = isHlsUrl(upstream)
        ? ["-live_start_index", String(settings.TRANSCODE_HLS_INPUT_LIVE_START_INDEX)]
        : [];
    const args = [
        "-hide_banner",
        "-loglevel", "warning",
        "-nostdin",
        "-vsync", "0",
        "-fflags", "+genpts+discardcorrupt",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_on_network_error", "1",
        "-reconnect_delay_max", "4",
        "-user_agent", settings.UPSTREAM_UA,
        "-headers", "Cache-Control: no-cache\r\nPragma: no-cache\r\n",
        ...hlsInputArgs,
        "-i", upstream
    ];

    const scaledVariants = variants.filter(variant => !variant.source);
    if (scaledVariants.length) {
        args.push("-filter_complex", filterComplex(scaledVariants));
    }

    for (let index = 0; index < scaledVariants.length; index++) {
        const variant = scaledVariants[index];
        pushEncodedHlsOutput(args, `[v${index}out]`, variant, session);
    }

    const sourceVariant = variants.find(variant => variant.source);
    if (sourceVariant) {
        pushEncodedHlsOutput(args, "0:v:0", sourceVariant, session);
    }

    return args;
}

function pushEncodedHlsOutput(args, videoMap, variant, session) {
    args.push(
        "-map", videoMap,
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
        "-hls_delete_threshold", String(settings.TRANSCODE_HLS_DELETE_THRESHOLD),
        "-hls_flags", "delete_segments+omit_endlist+program_date_time+independent_segments+temp_file",
        "-hls_segment_filename", path.join(session.dir, `${variant.name}_seg_%06d.ts`),
        variantManifestPath(session, variant)
    );
}

function playbackVariants() {
    const variants = [...settings.TRANSCODE_VARIANTS];
    if (settings.TRANSCODE_INCLUDE_SOURCE_VARIANT) variants.push({
        name: SOURCE_VARIANT_NAME,
        label: settings.TRANSCODE_SOURCE_LABEL,
        source: true,
        videoK: settings.TRANSCODE_SOURCE_VIDEO_BITRATE_K,
        audioK: settings.TRANSCODE_SOURCE_AUDIO_BITRATE_K,
        bandwidth: settings.TRANSCODE_SOURCE_BANDWIDTH,
        averageBandwidth: settings.TRANSCODE_SOURCE_AVERAGE_BANDWIDTH
    });
    return variants;
}

function masterStreamInfo(variant) {
    if (variant.source) {
        return `#EXT-X-STREAM-INF:BANDWIDTH=${variant.bandwidth},AVERAGE-BANDWIDTH=${variant.averageBandwidth},CODECS="${MASTER_CODECS}",NAME="${variant.label}"`;
    }
    const peak = Math.round((variant.videoK + variant.audioK) * 1000 * 1.25);
    const average = Math.round((variant.videoK + variant.audioK) * 1000);
    return `#EXT-X-STREAM-INF:BANDWIDTH=${peak},AVERAGE-BANDWIDTH=${average},RESOLUTION=${variant.width}x${variant.height},CODECS="${MASTER_CODECS}",NAME="${variant.label}"`;
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
            if (settings.TRANSCODE_BLACK_GUARD && !session.blackGuardChecked?.has(variant.name)) {
                const segmentCount = await variantSegmentCount(session, variant);
                if (segmentCount < settings.TRANSCODE_BLACK_GUARD_MIN_SEGMENTS) {
                    await sleep(250);
                    continue;
                }
                const result = await checkVariantBlack(session, variant);
                session.blackGuardChecked ||= new Set();
                session.blackGuardChecked.add(variant.name);
                if (result.black) {
                    logTranscode("BLACK", session, {
                        variant: variant.name,
                        strict: settings.TRANSCODE_BLACK_GUARD_STRICT ? 1 : 0,
                        blackSeconds: Math.round(result.blackSeconds * 1000) / 1000,
                        durationSeconds: Math.round(result.durationSeconds * 1000) / 1000,
                        file: path.basename(result.file || "")
                    });
                    if (settings.TRANSCODE_BLACK_GUARD_STRICT) {
                        stopSession(session, "black-output");
                        const err = new Error(`Transcode output is black for ${variant.name}`);
                        err.statusCode = 503;
                        throw err;
                    }
                }
            }
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

async function checkVariantBlack(session, variant) {
    const segments = await newestVariantSegments(session, variant, settings.TRANSCODE_BLACK_GUARD_MIN_SEGMENTS);
    if (!segments.length) return { black: false, blackSeconds: 0, durationSeconds: 0, file: "" };

    let durationSeconds = 0;
    let blackSeconds = 0;
    for (const segment of segments) {
        const [duration, black] = await Promise.all([
            mediaDurationSeconds(segment),
            blackDurationSeconds(segment)
        ]);
        durationSeconds += duration || 0;
        blackSeconds += black || 0;
    }
    if (!durationSeconds || !blackSeconds) return {
        black: false,
        blackSeconds,
        durationSeconds,
        file: segments.map(segment => path.basename(segment)).join(",")
    };
    return {
        black: blackSeconds / durationSeconds >= settings.TRANSCODE_BLACK_GUARD_RATIO,
        blackSeconds,
        durationSeconds,
        file: segments.map(segment => path.basename(segment)).join(",")
    };
}

async function newestVariantSegment(session, variant) {
    const segments = await newestVariantSegments(session, variant, 1);
    return segments[0] || "";
}

async function newestVariantSegments(session, variant, count) {
    const matches = await variantSegmentFiles(session, variant);
    return matches.slice(-Math.max(1, count)).map(file => path.join(session.dir, file));
}

async function variantSegmentCount(session, variant) {
    return (await variantSegmentFiles(session, variant)).length;
}

async function variantSegmentFiles(session, variant) {
    const files = await fsp.readdir(session.dir).catch(() => []);
    const prefix = `${variant.name}_seg_`;
    return files
        .filter(file => file.startsWith(prefix) && file.endsWith(".ts"))
        .sort();
}

function mediaDurationSeconds(filePath) {
    return new Promise(resolve => {
        execFile("ffprobe", [
            "-hide_banner",
            "-loglevel", "error",
            "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1",
            filePath
        ], { timeout: 5000 }, (_err, stdout) => {
            const value = Number(String(stdout || "").trim());
            resolve(Number.isFinite(value) && value > 0 ? value : 0);
        });
    });
}

function blackDurationSeconds(filePath) {
    return new Promise(resolve => {
        execFile(settings.TRANSCODE_FFMPEG_PATH, [
            "-hide_banner",
            "-loglevel", "info",
            "-i", filePath,
            "-vf", "blackdetect=d=0.5:pix_th=0.10",
            "-an",
            "-f", "null",
            "-"
        ], { timeout: 8000 }, (_err, _stdout, stderr) => {
            let total = 0;
            for (const match of String(stderr || "").matchAll(/black_duration:([0-9.]+)/g)) {
                total += Number(match[1]) || 0;
            }
            resolve(total);
        });
    });
}

async function rewriteTranscodePlaylist(session, variant, host, routeKey, options = {}) {
    session.lastAccess = Date.now();
    const text = await fsp.readFile(variantManifestPath(session, variant), "utf8");
    const base = routeBase(host, routeKey);
    const rewritten = text.split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) return line;
        const fileName = path.basename(trimmed);
        return `${base}/proxy/transcode/${session.id}/${encodeURIComponent(fileName)}`;
    }).join("\n");
    return applyTranscodeLiveHints(rewritten, options);
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
    return playbackVariants().find(variant =>
        variant.name === requested || String(variant.height || "") === requested || `${variant.height}p` === requested
    ) || playbackVariants()[0];
}

function applyTranscodeLiveHints(text, options = {}) {
    let next = applyServerControlHoldBack(text, options.holdBackSeconds);
    next = applyStartOffset(next, options.startOffsetSeconds);
    return next;
}

function applyStartOffset(text, offsetSeconds) {
    const offset = Number(offsetSeconds || 0);
    if (offset <= 0 || /#EXT-X-START:/i.test(text)) return text;
    const lines = String(text || "").split(/\r?\n/);
    lines.splice(playlistHeaderInsertIndex(lines), 0, `#EXT-X-START:TIME-OFFSET=-${Math.round(offset)},PRECISE=NO`);
    return lines.join("\n");
}

function applyServerControlHoldBack(text, holdBackSeconds) {
    const holdBack = Number(holdBackSeconds || 0);
    if (holdBack <= 0) return text;
    const rounded = Math.round(holdBack * 1000) / 1000;
    const lines = String(text || "").split(/\r?\n/);
    const existing = lines.findIndex(line => /^#EXT-X-SERVER-CONTROL:/i.test(line.trim()));
    if (existing >= 0) {
        if (/HOLD-BACK=/i.test(lines[existing])) return text;
        lines[existing] = `${lines[existing]},HOLD-BACK=${rounded}`;
        return lines.join("\n");
    }
    const versionIndex = lines.findIndex(line => /^#EXT-X-VERSION:/i.test(line.trim()));
    lines.splice(versionIndex >= 0 ? versionIndex + 1 : playlistHeaderInsertIndex(lines), 0, `#EXT-X-SERVER-CONTROL:HOLD-BACK=${rounded}`);
    return lines.join("\n");
}

function playlistHeaderInsertIndex(lines) {
    let index = lines[0]?.trim() === "#EXTM3U" ? 1 : 0;
    while (index < lines.length && /^#EXT-X-(VERSION|SERVER-CONTROL|INDEPENDENT-SEGMENTS):/i.test(lines[index].trim())) {
        index++;
    }
    return index;
}

function queryNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function variantManifestPath(session, variant) {
    return path.join(session.dir, `${variant.name}.m3u8`);
}

function variantSignature(variants) {
    return variants
        .map(variant => `${variant.name}:${variant.height || "native"}:${variant.videoK}:${variant.audioK}:${variant.bandwidth || ""}:${variant.averageBandwidth || ""}`)
        .join("|");
}

async function ensureWorkDir() {
    await fsp.mkdir(settings.TRANSCODE_WORK_DIR, { recursive: true });
}

function isSafeHlsFile(fileName) {
    return /^[A-Za-z0-9_.-]+\.(?:m3u8|ts)$/.test(String(fileName || ""));
}

function transcodeFileInfo(fileName) {
    const value = String(fileName || "");
    const segment = value.match(/^(.+)_seg_(\d+)\.ts$/i);
    if (segment) return { kind: "segment", variant: segment[1], seq: Number(segment[2]) };
    const manifest = value.match(/^(.+)\.m3u8$/i);
    if (manifest) return { kind: "manifest", variant: manifest[1], seq: "-" };
    return { kind: "file", variant: "-", seq: "-" };
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

function logTranscodeFile(label, fields = {}) {
    if (!settings.HLS_DIAGNOSTICS || !settings.TRANSCODE_FILE_DIAGNOSTICS) return;
    const session = fields.session;
    const parts = {
        sid: session?.id || fields.sid || "-",
        src: session?.upstream ? hashKey(session.upstream, 12) : "-",
        kind: fields.kind || "-",
        variant: fields.variant || "-",
        seq: fields.seq ?? "-",
        status: fields.status || "-",
        bytes: fields.bytes ?? "-",
        ms: fields.ms ?? "-",
        file: fields.file || "-",
        reason: fields.reason || "-",
        ua: fields.ua || "-"
    };
    console.log(`[TRANSCODE FILE ${label}] ${Object.entries(parts).map(([key, value]) => `${key}=${value ?? "-"}`).join(" ")}`);
}

function logFfmpegLines(session, text) {
    if (!settings.HLS_DIAGNOSTICS || !settings.TRANSCODE_FFMPEG_DIAGNOSTICS) return;
    const combined = `${session.ffmpegLogLine || ""}${text}`;
    const lines = combined.split(/\r?\n/);
    session.ffmpegLogLine = lines.pop() || "";
    for (const line of lines) {
        const message = compact(line);
        if (!message) continue;
        logTranscode("FFMPEG", session, { msg: message });
    }
}

module.exports = {
    adaptiveMasterManifest,
    prewarmTranscode,
    serveTranscodeFile,
    transcodeManifest
};
