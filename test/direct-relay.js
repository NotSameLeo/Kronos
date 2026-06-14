"use strict";

const assert = require("assert");
const crypto = require("crypto");
const http = require("http");
const path = require("path");
const { spawn } = require("child_process");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = path.resolve(__dirname, "..");

function encodeConfig(value) {
    return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function channelId(sourceUrl, streamUrl) {
    return "channel_" + crypto.createHash("sha1").update(`${sourceUrl}|${streamUrl}`).digest("hex").slice(0, 20);
}

async function request(url, options = {}, timeoutMs = 5000) {
    return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

function createMockUpstream() {
    const state = { manifests: 0, segments: 0 };
    const server = http.createServer((req, res) => {
        const origin = `http://127.0.0.1:${server.address().port}`;
        const url = new URL(req.url, origin);

        if (url.pathname === "/playlist.m3u") {
            res.writeHead(200, { "content-type": "audio/x-mpegurl" });
            res.end([
                "#EXTM3U",
                '#EXTINF:-1 group-title="IT | TEST",IT: DIRECT HD',
                `${origin}/live/direct.m3u8`,
                ""
            ].join("\n"));
            return;
        }

        if (url.pathname === "/live/direct.m3u8") {
            state.manifests++;
            const token = `token-${state.manifests}`;
            res.writeHead(200, { "content-type": "application/x-mpegurl" });
            res.end([
                "#EXTM3U",
                "#EXT-X-VERSION:3",
                "#EXT-X-TARGETDURATION:1",
                "#EXT-X-MEDIA-SEQUENCE:0",
                "#EXTINF:1.000,",
                `/hlsr/${token}/direct_0.ts`,
                ""
            ].join("\n"));
            return;
        }

        if (/^\/hlsr\/[^/]+\/direct_0\.ts$/.test(url.pathname)) {
            state.segments++;
            res.writeHead(200, { "content-type": "video/mp2t", "content-length": "4096" });
            res.end(Buffer.alloc(4096, state.segments));
            return;
        }

        res.writeHead(404);
        res.end();
    });
    return { server, state };
}

async function waitForHealth(origin) {
    for (let i = 0; i < 80; i++) {
        try {
            const response = await request(`${origin}/health`, {}, 500);
            if (response.ok) return;
        } catch {}
        await sleep(40);
    }
    throw new Error("Kronos did not start");
}

async function main() {
    const mock = createMockUpstream();
    await new Promise(resolve => mock.server.listen(0, "127.0.0.1", resolve));
    const upstreamOrigin = `http://127.0.0.1:${mock.server.address().port}`;
    const sourceUrl = `${upstreamOrigin}/playlist.m3u`;
    const streamUrl = `${upstreamOrigin}/live/direct.m3u8`;
    const config = encodeConfig({ l: [{ n: "Direct", u: sourceUrl }], g: ["IT | TEST"], gm: "filter" });
    const id = channelId(sourceUrl, streamUrl);
    const port = 20000 + Math.floor(Math.random() * 1000);
    const kronosOrigin = `http://127.0.0.1:${port}`;
    const logs = [];
    const child = spawn(process.execPath, [path.join(root, "server.js")], {
        cwd: root,
        env: {
            ...process.env,
            PORT: String(port),
            PLAYLIST_RETRY_WINDOW_MS: "1000",
            PLAYLIST_RETRY_DELAY_MS: "40",
            HLS_REQUEST_TIMEOUT: "700",
            HLS_COLD_START_WAIT_MS: "500",
            HLS_COLD_START_RETRY_MS: "50",
            HLS_LIVE_MIN_VISIBLE_SEGMENTS: "1",
            HLS_LIVE_MIN_SEGMENTS: "1",
            HLS_LIVE_HOLDBACK_SEGMENTS: "0",
            SEG_REQUEST_TIMEOUT: "1000"
        },
        stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout.on("data", data => logs.push(String(data)));
    child.stderr.on("data", data => logs.push(String(data)));

    try {
        await waitForHealth(kronosOrigin);

        const streamResponse = await request(`${kronosOrigin}/${config}/stream/tv/${id}.json`, {}, 2000);
        assert.equal(streamResponse.status, 200, `stream returned ${streamResponse.status}`);
        const stream = (await streamResponse.json()).streams[0];
        assert(stream.url.includes("/proxy/live.m3u8?u="), "default HLS stream is not direct relay");
        assert(!stream.url.includes(`/${config}/hls/`), "legacy HLS route leaked into direct mode");

        const manifestResponse = await request(stream.url, {}, 2000);
        assert.equal(manifestResponse.status, 200, `manifest returned ${manifestResponse.status}`);
        const manifest = await manifestResponse.text();
        assert(manifest.includes("/proxy/seg?u="), "direct manifest did not encode segment URLs");
        assert(!manifest.includes("/proxy/seg?s="), "direct manifest used stable segment ids");

        const oldHlsResponse = await request(`${kronosOrigin}/${config}/hls/${id}/index.m3u8`, {}, 2000);
        assert.equal(oldHlsResponse.status, 404, "legacy buffered HLS route is still exposed");

        const oldNestedResponse = await request(`${kronosOrigin}/${config}/proxy/pl?u=${Buffer.from(streamUrl).toString("base64url")}`, {}, 2000);
        assert.equal(oldNestedResponse.status, 404, "legacy stable nested-playlist route is still exposed");

        const segmentLine = manifest.split(/\r?\n/).find(line => line.includes("/proxy/seg?u="));
        const segmentUrl = new URL(segmentLine, kronosOrigin).toString();
        const segmentResponse = await request(segmentUrl, { headers: { Range: "bytes=0-" } }, 2000);
        assert(segmentResponse.ok, `segment returned ${segmentResponse.status}`);
        assert.equal(segmentResponse.headers.get("x-kronos-relay"), "1");
        assert.equal(segmentResponse.headers.get("x-kronos-cache"), null);
        await segmentResponse.arrayBuffer();

        const debug = await (await request(`${kronosOrigin}/${config}/debug`, {}, 2000)).json();
        assert.equal(debug.mode, "uhf-direct-relay", "debug mode did not report UHF-like direct relay");

        console.log(JSON.stringify({
            ok: true,
            mode: debug.mode,
            manifests: mock.state.manifests,
            segments: mock.state.segments,
            exercised: ["uhf-direct-relay"]
        }, null, 2));
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
