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

function decodeProxyUrl(value) {
    return Buffer.from(String(value), "base64url").toString("utf8");
}

function channelId(sourceUrl, streamUrl) {
    return "channel_" + crypto.createHash("sha1").update(`${sourceUrl}|${streamUrl}`).digest("hex").slice(0, 20);
}

async function request(url, options = {}, timeoutMs = 5000) {
    return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

function segmentProxyUrls(manifest, origin) {
    return manifest.split(/\r?\n/)
        .map(line => line.trim())
        .filter(line => line && !line.startsWith("#") && line.includes("/proxy/seg?u="))
        .map(line => new URL(line, origin).toString());
}

function upstreamUrlFromProxy(proxyUrl) {
    return decodeProxyUrl(new URL(proxyUrl).searchParams.get("u"));
}

function keyProxyUrl(manifest, origin) {
    const keyLine = manifest.split(/\r?\n/).find(line => line.trim().startsWith("#EXT-X-KEY"));
    const match = keyLine?.match(/URI="([^"]+)"/);
    return match ? new URL(match[1], origin).toString() : "";
}

function browserLike(req) {
    return /Chrome\/120/i.test(String(req.headers["user-agent"] || ""));
}

function createComplexUpstream() {
    const state = {
        playlistHits: 0,
        manifestHits: { alpha: 0, beta: 0, gamma: 0 },
        segmentHits: { alpha: 0, beta: 0, gamma: 0 },
        failManifests: { alpha: 0, beta: 0, gamma: 0 },
        activeSegments: 0,
        maxActiveSegments: 0,
        userAgents: []
    };

    const server = http.createServer(async (req, res) => {
        const origin = `http://127.0.0.1:${server.address().port}`;
        const url = new URL(req.url, origin);

        if (url.pathname === "/playlist.m3u") {
            state.playlistHits++;
            res.writeHead(200, { "content-type": "audio/x-mpegurl" });
            res.end([
                "#EXTM3U",
                '#EXTINF:-1 group-title="IT | TEST",IT: ALPHA HD',
                `${origin}/live/alpha.m3u8`,
                '#EXTINF:-1 group-title="IT | TEST",IT: BETA HD',
                `${origin}/live/beta.m3u8`,
                '#EXTINF:-1 group-title="IT | TEST",IT: GAMMA HD',
                `${origin}/live/gamma.m3u8`,
                ""
            ].join("\n"));
            return;
        }

        const manifestMatch = url.pathname.match(/^\/live\/(alpha|beta|gamma)\.m3u8$/);
        if (manifestMatch) {
            if (!browserLike(req)) {
                res.writeHead(451, { "content-type": "text/plain" });
                res.end("geo/browser policy rejected this client");
                return;
            }
            const channel = manifestMatch[1];
            state.userAgents.push(String(req.headers["user-agent"] || ""));
            state.manifestHits[channel]++;
            if (state.failManifests[channel] > 0) {
                state.failManifests[channel]--;
                res.writeHead(403, { "content-type": "text/plain" });
                res.end("token temporarily locked");
                return;
            }
            const hit = state.manifestHits[channel];
            const depth = Math.min(3, hit);
            const seq = Math.max(0, hit - 3);
            const token = `${channel}-token-${hit}`;
            const lines = [
                "#EXTM3U",
                "#EXT-X-VERSION:3",
                "#EXT-X-TARGETDURATION:1",
                `#EXT-X-MEDIA-SEQUENCE:${seq}`,
                `#EXT-X-KEY:METHOD=AES-128,URI="/key/${channel}/${token}.bin"`
            ];
            for (let i = 0; i < depth; i++) {
                const number = seq + i;
                lines.push("#EXTINF:1.000,");
                lines.push(`/segments/${channel}/${token}/${number}.ts?expires=${Date.now() + 30000}&geo=it`);
            }
            lines.push("");
            res.writeHead(200, { "content-type": "application/x-mpegurl" });
            res.end(lines.join("\n"));
            return;
        }

        const segmentMatch = url.pathname.match(/^\/segments\/(alpha|beta|gamma)\/([^/]+)\/(\d+)\.ts$/);
        if (segmentMatch) {
            if (!browserLike(req)) {
                res.writeHead(451, { "content-type": "text/plain" });
                res.end("geo/browser policy rejected this client");
                return;
            }
            const channel = segmentMatch[1];
            state.segmentHits[channel]++;
            state.activeSegments++;
            state.maxActiveSegments = Math.max(state.maxActiveSegments, state.activeSegments);
            await sleep(320);
            state.activeSegments--;
            res.writeHead(200, {
                "content-type": "video/mp2t",
                "content-length": "65536",
                "accept-ranges": "bytes"
            });
            res.end(Buffer.alloc(65536, Number(segmentMatch[3]) % 255));
            return;
        }

        if (url.pathname.startsWith("/key/")) {
            res.writeHead(200, { "content-type": "application/octet-stream", "content-length": "16" });
            res.end(Buffer.alloc(16, 1));
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
            const response = await request(`${origin}/health`, {}, 300);
            if (response.ok) return;
        } catch {}
        await sleep(40);
    }
    throw new Error("Kronos did not start");
}

async function main() {
    const mock = createComplexUpstream();
    await new Promise(resolve => mock.server.listen(0, "127.0.0.1", resolve));
    const upstreamOrigin = `http://127.0.0.1:${mock.server.address().port}`;
    const sourceUrl = `${upstreamOrigin}/playlist.m3u`;
    const alphaUrl = `${upstreamOrigin}/live/alpha.m3u8`;
    const betaUrl = `${upstreamOrigin}/live/beta.m3u8`;
    const gammaUrl = `${upstreamOrigin}/live/gamma.m3u8`;
    const config = encodeConfig({ l: [{ n: "Complex", u: sourceUrl }], g: ["IT | TEST"], gm: "filter" });
    const alphaId = channelId(sourceUrl, alphaUrl);
    const betaId = channelId(sourceUrl, betaUrl);
    const gammaId = channelId(sourceUrl, gammaUrl);
    const port = 21000 + Math.floor(Math.random() * 1000);
    const kronosOrigin = `http://127.0.0.1:${port}`;
    const logs = [];
    const child = spawn(process.execPath, [path.join(root, "server.js")], {
        cwd: root,
        env: {
            ...process.env,
            PORT: String(port),
            EPG_PRELOAD_URL: `${upstreamOrigin}/empty.xml`,
            EPG_REQUEST_TIMEOUT: "80",
            EPG_RETRY_DELAY_MS: "10000",
            PLAYLIST_RETRY_WINDOW_MS: "1000",
            PLAYLIST_RETRY_DELAY_MS: "40",
            HLS_REQUEST_TIMEOUT: "1000",
            HLS_UPSTREAM_RETRIES: "0",
            HLS_LIVE_WARMUP_POLL_MS: "50",
            HLS_LIVE_WARMUP_WAIT_MS: "2000",
            HLS_STALE_MANIFEST_TTL_MS: "3000",
            HLS_FORBIDDEN_BACKOFF_MS: "500",
            SEG_REQUEST_TIMEOUT: "2500",
            SEGMENT_UPSTREAM_CONCURRENCY: "2",
            SLOW_SEGMENT_MS: "99999"
        },
        stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout.on("data", data => logs.push(String(data)));
    child.stderr.on("data", data => logs.push(String(data)));

    try {
        await waitForHealth(kronosOrigin);

        const alphaStreamResponse = await request(`${kronosOrigin}/${config}/stream/tv/${alphaId}.json`, {}, 2000);
        assert.equal(alphaStreamResponse.status, 200, `alpha stream returned ${alphaStreamResponse.status}`);
        const alphaStream = (await alphaStreamResponse.json()).streams[0];
        assert(alphaStream.url.includes("/proxy/live.m3u8?u="), "HLS stream did not use direct live relay");

        const coldManifestResponse = await request(alphaStream.url, {}, 3000);
        assert.equal(coldManifestResponse.status, 200, `cold alpha manifest returned ${coldManifestResponse.status}`);
        const coldManifest = await coldManifestResponse.text();
        const alphaSegments = segmentProxyUrls(coldManifest, kronosOrigin);
        assert.equal(mock.state.manifestHits.alpha, 3, "cold live warmup did not wait for a playable window");
        assert.equal(alphaSegments.length, 2, "live holdback should expose two visible segments from a three-segment cold window");
        assert(alphaSegments.every(url => upstreamUrlFromProxy(url).includes("/alpha/alpha-token-3/")), "segment URLs did not preserve the latest tokenized upstream URLs");
        assert(!coldManifest.includes("/proxy/seg?s="), "stable segment registry leaked into direct relay");
        assert(!coldManifest.includes(`/${config}/hls/`), "legacy buffered route leaked into direct relay");
        assert(coldManifest.includes("/proxy/seg?u="), "segment URLs were not proxied directly");
        const keyUrl = keyProxyUrl(coldManifest, kronosOrigin);
        assert(keyUrl.includes("/proxy/seg?u="), "encryption key URI was not proxied directly");
        assert(upstreamUrlFromProxy(keyUrl).includes("/key/alpha/alpha-token-3.bin"), "encryption key URI lost its tokenized upstream URL");

        mock.state.failManifests.alpha = 1;
        const alphaHitsBefore403 = mock.state.manifestHits.alpha;
        const fallbackManifestResponse = await request(alphaStream.url, {}, 3000);
        assert.equal(fallbackManifestResponse.status, 200, `fallback alpha manifest returned ${fallbackManifestResponse.status}`);
        const fallbackManifest = await fallbackManifestResponse.text();
        assert(segmentProxyUrls(fallbackManifest, kronosOrigin).every(url => upstreamUrlFromProxy(url).includes("/alpha/alpha-token-3/")), "fallback manifest did not preserve the last known good tokenized URLs");
        assert.equal(mock.state.manifestHits.alpha, alphaHitsBefore403 + 1, "first fallback did not exercise the upstream 403");

        const alphaHitsDuringBackoff = mock.state.manifestHits.alpha;
        const backoffManifestResponse = await request(alphaStream.url, {}, 3000);
        assert.equal(backoffManifestResponse.status, 200, `backoff alpha manifest returned ${backoffManifestResponse.status}`);
        await backoffManifestResponse.text();
        assert.equal(mock.state.manifestHits.alpha, alphaHitsDuringBackoff, "forbidden manifest backoff still hit upstream");

        const started = Date.now();
        const segmentResponses = await Promise.all(alphaSegments.map(url => request(url, {}, 3000)));
        const elapsed = Date.now() - started;
        assert(segmentResponses.every(response => response.ok), "parallel segment fetch failed");
        await Promise.all(segmentResponses.map(response => response.arrayBuffer()));
        assert(mock.state.maxActiveSegments >= 2, "segment relay still serialized player catch-up requests");
        assert(elapsed < 580, `parallel segment relay was too slow (${elapsed}ms)`);

        const betaStreamResponse = await request(`${kronosOrigin}/${config}/stream/tv/${betaId}.json`, {}, 2000);
        assert.equal(betaStreamResponse.status, 200, `beta stream returned ${betaStreamResponse.status}`);
        const betaStream = (await betaStreamResponse.json()).streams[0];
        const betaManifestResponse = await request(betaStream.url, {}, 3000);
        assert.equal(betaManifestResponse.status, 200, `beta manifest returned ${betaManifestResponse.status}`);
        await betaManifestResponse.text();

        mock.state.failManifests.gamma = 1;
        const gammaStreamResponse = await request(`${kronosOrigin}/${config}/stream/tv/${gammaId}.json`, {}, 2000);
        assert.equal(gammaStreamResponse.status, 200, `gamma stream returned ${gammaStreamResponse.status}`);
        const gammaStream = (await gammaStreamResponse.json()).streams[0];
        const waitingManifestResponse = await request(gammaStream.url, {}, 3000);
        assert.equal(waitingManifestResponse.status, 200, `waiting gamma manifest returned ${waitingManifestResponse.status}`);
        const waitingManifest = await waitingManifestResponse.text();
        assert(waitingManifest.startsWith("#EXTM3U"), "waiting manifest is not valid HLS");
        assert.equal(segmentProxyUrls(waitingManifest, kronosOrigin).length, 0, "waiting manifest should not invent segments");
        assert.equal(mock.state.manifestHits.gamma, 1, "waiting manifest did not exercise the first upstream 403");
        const gammaHitsDuringBackoff = mock.state.manifestHits.gamma;
        const waitingBackoffResponse = await request(gammaStream.url, {}, 3000);
        assert.equal(waitingBackoffResponse.status, 200, `waiting backoff gamma manifest returned ${waitingBackoffResponse.status}`);
        await waitingBackoffResponse.text();
        assert.equal(mock.state.manifestHits.gamma, gammaHitsDuringBackoff, "waiting backoff still hit upstream");
        await sleep(550);
        const gammaReadyResponse = await request(gammaStream.url, {}, 3000);
        assert.equal(gammaReadyResponse.status, 200, `gamma ready manifest returned ${gammaReadyResponse.status}`);
        const gammaReady = await gammaReadyResponse.text();
        assert(segmentProxyUrls(gammaReady, kronosOrigin).length >= 2, "gamma did not recover to real segments after transient startup 403");

        const alphaHitsBeforeStale = mock.state.segmentHits.alpha;
        const staleResponse = await request(alphaSegments[0], {}, 1000);
        assert.equal(staleResponse.status, 409, "old channel segment was not rejected locally after stream switch");
        assert.equal(mock.state.segmentHits.alpha, alphaHitsBeforeStale, "stale segment request still hit the upstream provider");

        await sleep(550);
        const alphaHitsBeforeRefresh = mock.state.manifestHits.alpha;
        const refreshedManifestResponse = await request(alphaStream.url, {}, 3000);
        assert.equal(refreshedManifestResponse.status, 200, `refreshed alpha manifest returned ${refreshedManifestResponse.status}`);
        const refreshedManifest = await refreshedManifestResponse.text();
        const refreshedSegments = segmentProxyUrls(refreshedManifest, kronosOrigin);
        assert(refreshedSegments.length >= 2, "refreshed manifest did not expose playable segments");
        assert(refreshedSegments.every(url => upstreamUrlFromProxy(url).includes(`/alpha/alpha-token-${alphaHitsBeforeRefresh + 1}/`)), "refreshed manifest reused stale tokenized segment URLs");
        assert(mock.state.userAgents.every(ua => /Chrome\/120/.test(ua)), "upstream did not receive the browser-like IPTV user agent");

        const text = logs.join("");
        assert(text.includes("[HLS DIRECT OPEN]"), "HLS request-open telemetry was not emitted");
        assert(text.includes("idleHls="), "playback gap telemetry was not emitted");
        assert(text.includes("media=2/3 held=1"), "holdback was not visible in HLS logs");
        assert(text.includes("[HLS STALE MANIFEST]"), "stale manifest fallback was not exercised");
        assert(text.includes("[HLS WAITING MANIFEST]"), "waiting manifest fallback was not exercised");
        assert(text.includes("staleReason=403"), "HLS serve log did not expose the stale manifest reason");
        assert(text.includes("[SEG STALE]"), "stale stream guard was not exercised");
        assert(text.includes("upstreamConcurrency=2"), "segment concurrency setting was not active");
        assert(text.includes("upstream keepAlive=1"), "upstream keep-alive setting was not active");

        console.log(JSON.stringify({
            ok: true,
            alphaManifests: mock.state.manifestHits.alpha,
            betaManifests: mock.state.manifestHits.beta,
            gammaManifests: mock.state.manifestHits.gamma,
            alphaSegments: mock.state.segmentHits.alpha,
            maxActiveSegments: mock.state.maxActiveSegments,
            parallelMs: elapsed,
            exercised: ["cold-live-warmup", "token-rotating-hls", "live-holdback", "stale-manifest-fallback", "waiting-manifest-fallback", "forbidden-backoff", "parallel-segments", "stale-stream-guard"]
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
