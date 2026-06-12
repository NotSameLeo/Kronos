"use strict";

const assert = require("assert");
const crypto = require("crypto");
const http = require("http");
const path = require("path");
const { spawn } = require("child_process");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = path.resolve(__dirname, "..");
const segmentPeriodMs = 120;

function encodeConfig(value) {
    return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function channelId(sourceUrl, streamUrl) {
    return "channel_" + crypto.createHash("sha1").update(`${sourceUrl}|${streamUrl}`).digest("hex").slice(0, 20);
}

function parsePlaylist(text) {
    const lines = String(text || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const mediaSequence = Number((lines.find(line => line.startsWith("#EXT-X-MEDIA-SEQUENCE:")) || "").split(":")[1]);
    const segments = lines.filter(line => !line.startsWith("#"));
    return { mediaSequence, segments, edge: mediaSequence + segments.length - 1 };
}

async function request(url, options = {}, timeoutMs = 5000) {
    return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

function createMockUpstream() {
    const state = { startedAt: Date.now(), manifests: 0, segments: 0 };
    const server = http.createServer((req, res) => {
        const url = new URL(req.url, "http://mock");
        if (url.pathname === "/playlist.m3u") {
            const origin = `http://127.0.0.1:${server.address().port}`;
            res.writeHead(200, { "content-type": "audio/x-mpegurl" });
            res.end([
                "#EXTM3U",
                '#EXTINF:-1 group-title="IT | TEST",IT: WATCHDOG HD',
                `${origin}/live/A.m3u8`,
                ""
            ].join("\n"));
            return;
        }
        if (url.pathname === "/live/A.m3u8") {
            state.manifests++;
            const edge = 8 + Math.floor((Date.now() - state.startedAt) / segmentPeriodMs);
            const first = Math.max(0, edge - 8);
            const token = `token-${state.manifests}`;
            const body = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:1", `#EXT-X-MEDIA-SEQUENCE:${first}`];
            for (let seq = first; seq <= edge; seq++) {
                body.push(`#EXTINF:${(segmentPeriodMs / 1000).toFixed(3)},`);
                body.push(`/hlsr/${token}/A/0/A_${seq}.ts`);
            }
            res.writeHead(200, { "content-type": "application/x-mpegurl" });
            res.end(body.join("\n") + "\n");
            return;
        }
        const segment = url.pathname.match(/^\/hlsr\/[^/]+\/A\/0\/A_(\d+)\.ts$/);
        if (segment) {
            state.segments++;
            const seq = Number(segment[1]);
            setTimeout(() => {
                res.writeHead(200, { "content-type": "video/mp2t" });
                res.end(Buffer.alloc(32 * 1024, seq % 255));
            }, 20);
            return;
        }
        res.writeHead(404);
        res.end();
    });
    return { server, state };
}

async function main() {
    const mock = createMockUpstream();
    await new Promise(resolve => mock.server.listen(0, "127.0.0.1", resolve));
    const upstreamOrigin = `http://127.0.0.1:${mock.server.address().port}`;
    const sourceUrl = `${upstreamOrigin}/playlist.m3u`;
    const streamUrl = `${upstreamOrigin}/live/A.m3u8`;
    const config = encodeConfig({ l: [{ n: "Watchdog", u: sourceUrl }], g: ["IT | TEST"], gm: "filter" });
    const port = 19000 + Math.floor(Math.random() * 1000);
    const kronosOrigin = `http://127.0.0.1:${port}`;
    const id = channelId(sourceUrl, streamUrl);
    const logs = [];
    const child = spawn(process.execPath, [path.join(root, "server.js")], {
        cwd: root,
        env: {
            ...process.env,
            PORT: String(port),
            MANIFEST_RETRY_WINDOW_MS: "700",
            MANIFEST_RETRY_DELAY_MS: "40",
            HLS_REQUEST_TIMEOUT: "500",
            SEG_REQUEST_TIMEOUT: "700",
            LIVE_DELAY_SEGMENTS: "1",
            MIN_START_SEGMENTS: "3",
            MIN_VISIBLE_SEGMENTS: "2",
            STARTUP_REAL_SEGMENTS: "3",
            STARTUP_REAL_WAIT_MS: "3000",
            PREFETCH_WINDOW_SEGMENTS: "8",
            POLL_MIN_MS: "25",
            POLL_MAX_MS: "40",
            POLLER_IDLE_STOP_MS: "5000",
            RETAIN_SEGMENTS: "18",
            SERVE_SEGMENTS: "5",
            SEGMENT_CACHE_TTL: "10000",
            MANIFEST_ONLY_IDLE_STOP_MS: "700",
            MANIFEST_ONLY_IDLE_MIN_REQUESTS: "4",
            STALE_PLAYBACK_RUN_MS: "30000",
            STALE_PLAYBACK_RUN_GAP: "30"
        },
        stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout.on("data", data => logs.push(String(data)));
    child.stderr.on("data", data => logs.push(String(data)));

    const hlsUrl = `${kronosOrigin}/${config}/hls/${id}/index.m3u8`;
    const statsUrl = `${kronosOrigin}/${config}/stats`;

    const waitForHealth = async () => {
        for (let i = 0; i < 80; i++) {
            try {
                const response = await request(`${kronosOrigin}/health`, {}, 500);
                if (response.ok) return;
            } catch {}
            await sleep(40);
        }
        throw new Error("Kronos did not start");
    };

    const readManifest = async () => {
        const response = await request(hlsUrl, {}, 5000);
        assert.equal(response.status, 200, `manifest returned ${response.status}`);
        return parsePlaylist(await response.text());
    };

    try {
        await waitForHealth();
        let playlist = await readManifest();
        assert(playlist.segments.length >= 2, "startup did not wait for real media");
        const segmentUrl = new URL(playlist.segments.at(-1), kronosOrigin).toString();
        const segment = await request(segmentUrl, { headers: { Range: "bytes=0-" } }, 5000);
        assert(segment.ok, `initial segment returned ${segment.status}`);
        await segment.arrayBuffer();

        const before = await (await request(statsUrl)).json();
        const activeBefore = before.channels.find(channel => channel.label === "WATCHDOG HD");
        assert(activeBefore, "active channel was not visible in stats");
        const generationBefore = activeBefore.generation;

        const cursor = logs.join("").length;
        let restarted = false;
        for (let i = 0; i < 16; i++) {
            playlist = await readManifest();
            const text = logs.join("").slice(cursor);
            if (text.includes("[PLAYER STALE]") && text.includes("reason=manifest-only-idle")) {
                restarted = true;
                break;
            }
            await sleep(150);
        }
        assert(restarted, "manifest-only client did not restart the HLS session");

        for (let i = 0; i < 60; i++) {
            await sleep(60);
            const stats = await (await request(statsUrl)).json();
            const active = stats.channels.find(channel => channel.label === "WATCHDOG HD");
            if (active?.generation > generationBefore) {
                console.log(JSON.stringify({
                    ok: true,
                    upstreamSegments: mock.state.segments,
                    activeChannel: active,
                    exercised: ["manifest-only-watchdog"]
                }, null, 2));
                return;
            }
        }
        throw new Error("manifest-only restart did not advance generation");
    } catch (err) {
        console.error(logs.join(""));
        throw err;
    } finally {
        child.kill("SIGTERM");
        mock.server.close();
    }
}

main().catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
});
