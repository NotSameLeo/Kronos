"use strict";

const assert = require("assert");
const crypto = require("crypto");
const http = require("http");
const path = require("path");
const { spawn } = require("child_process");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = path.resolve(__dirname, "..");
const segmentPeriodMs = 320;

function encodeConfig(value) {
    return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function channelId(sourceUrl, streamUrl) {
    return "channel_" + crypto.createHash("sha1").update(`${sourceUrl}|${streamUrl}`).digest("hex").slice(0, 20);
}

function stableSegmentId(scope, absoluteUrl, generation = 0) {
    const url = new URL(absoluteUrl);
    const parts = url.pathname.split("/").filter(Boolean);
    const pathname = parts[0] === "hlsr" && parts.length > 1
        ? `/hlsr/${parts.at(-1)}`
        : url.pathname;
    const hash = crypto.createHash("sha1").update(`${scope}|${url.origin}${pathname}`).digest("hex").slice(0, 20);
    return `g${generation}:${hash}`;
}

function parsePlaylist(text) {
    const lines = String(text || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const mediaSequence = Number((lines.find(line => line.startsWith("#EXT-X-MEDIA-SEQUENCE:")) || "").split(":")[1]);
    const segments = lines.filter(line => !line.startsWith("#"));
    const discontinuities = lines.filter(line => line === "#EXT-X-DISCONTINUITY").length;
    return { mediaSequence, segments, edge: mediaSequence + segments.length - 1, discontinuities };
}

async function request(url, options = {}, timeoutMs = 5000) {
    return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

function createMockUpstream() {
    const channels = new Map();
    const state = {
        active: 0,
        maxConcurrent: 0,
        concurrencyViolations: 0,
        segmentAttempts: new Map(),
        abortNextSegment: false,
        masterManifests: 0,
        forbiddenUntil: 0,
        manifestRequestsDuringForbidden: 0
    };

    const getChannel = id => {
        if (!channels.has(id)) channels.set(id, { id, startedAt: Date.now(), generation: 0, manifests: 0 });
        return channels.get(id);
    };

    const withSingleConnection = (res, fn) => {
        state.active++;
        state.maxConcurrent = Math.max(state.maxConcurrent, state.active);
        let finished = false;
        const finish = () => {
            if (finished) return;
            finished = true;
            state.active--;
        };
        res.once("finish", finish);
        res.once("close", finish);
        if (state.active > 1) {
            state.concurrencyViolations++;
            res.writeHead(429, { "content-type": "text/plain" });
            res.end("too many upstream connections");
            return;
        }
        fn(finish);
    };

    const server = http.createServer((req, res) => {
        const url = new URL(req.url, "http://mock");

        if (url.pathname === "/playlist.m3u") {
            const origin = `http://127.0.0.1:${server.address().port}`;
            res.writeHead(200, { "content-type": "audio/x-mpegurl" });
            res.end([
                "#EXTM3U",
                '#EXTINF:-1 group-title="IT | TEST",IT: STRESS A HD',
                `${origin}/live/user/secret/A.m3u8`,
                '#EXTINF:-1 group-title="IT | TEST",IT: STRESS B HD',
                `${origin}/live/user/secret/B.m3u8`,
                '#EXTINF:-1 group-title="IT | TEST",IT: STRESS MASTER HD',
                `${origin}/master/M.m3u8`,
                '#EXTINF:-1 group-title="IT | TEST",IT: STRESS DIRECT HD',
                `${origin}/direct/D.ts`,
                ""
            ].join("\n"));
            return;
        }

        if (url.pathname === "/control/abort-next-segment") {
            state.abortNextSegment = true;
            res.writeHead(204);
            res.end();
            return;
        }

        if (url.pathname === "/control/restart") {
            const channel = getChannel(url.searchParams.get("id") || "A");
            channel.startedAt = Date.now();
            channel.generation++;
            channel.manifests = 0;
            res.writeHead(204);
            res.end();
            return;
        }

        if (url.pathname === "/control/forbid-manifest") {
            state.forbiddenUntil = Date.now() + Number(url.searchParams.get("ms") || 0);
            res.writeHead(204);
            res.end();
            return;
        }

        const manifest = url.pathname.match(/^\/live\/user\/secret\/([A-Z])\.m3u8$/);
        if (manifest) {
            withSingleConnection(res, () => {
                const channel = getChannel(manifest[1]);
                channel.manifests++;
                if (Date.now() < state.forbiddenUntil) {
                    state.manifestRequestsDuringForbidden++;
                    res.writeHead(403, { "content-type": "text/plain" });
                    res.end("provider cooldown");
                    return;
                }
                if (channel.manifests === 1) {
                    res.writeHead(200, { "content-type": "text/html" });
                    res.end("<html>encoder starting</html>");
                    return;
                }
                if (channel.manifests === 2 || channel.manifests % 17 === 0) {
                    res.writeHead(403, { "content-type": "text/plain" });
                    res.end("geo blocked briefly");
                    return;
                }
                const edge = 5 + Math.floor((Date.now() - channel.startedAt) / segmentPeriodMs);
                const first = Math.max(0, edge - 5);
                const token = `token-${channel.generation}-${channel.manifests}`;
                const body = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:1", `#EXT-X-MEDIA-SEQUENCE:${first}`];
                for (let seq = first; seq <= edge; seq++) {
                    body.push(`#EXTINF:${(segmentPeriodMs / 1000).toFixed(3)},`);
                    body.push(`/hlsr/${token}/${channel.id}/${channel.generation}/${channel.id}_${seq}.ts`);
                }
                res.writeHead(200, { "content-type": "application/x-mpegurl" });
                res.end(body.join("\n") + "\n");
            });
            return;
        }

        if (url.pathname === "/master/M.m3u8") {
            withSingleConnection(res, () => {
                state.masterManifests++;
                res.writeHead(200, { "content-type": "application/x-mpegurl" });
                res.end("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2500000\n/master/M-low.m3u8\n");
            });
            return;
        }

        if (url.pathname === "/master/M-low.m3u8") {
            withSingleConnection(res, () => {
                res.writeHead(200, { "content-type": "application/x-mpegurl" });
                res.end([
                    "#EXTM3U",
                    "#EXT-X-VERSION:3",
                    "#EXT-X-TARGETDURATION:1",
                    "#EXT-X-MEDIA-SEQUENCE:0",
                    "#EXTINF:0.320,",
                    "/hlsr/master-token/M/0/M_0.ts",
                    ""
                ].join("\n"));
            });
            return;
        }

        if (url.pathname === "/direct/D.ts") {
            withSingleConnection(res, () => {
                res.writeHead(200, { "content-type": "video/mp2t" });
                res.write(Buffer.alloc(1024, 1));
                setTimeout(() => res.end(Buffer.alloc(1024, 2)), 350);
            });
            return;
        }

        const segment = url.pathname.match(/^\/hlsr\/[^/]+\/([A-Z])\/(\d+)\/([A-Z])_(\d+)\.ts$/);
        if (segment) {
            withSingleConnection(res, finish => {
                const [, channelIdValue, generation, , sequence] = segment;
                const key = `${channelIdValue}:${generation}:${sequence}`;
                const attempt = (state.segmentAttempts.get(key) || 0) + 1;
                state.segmentAttempts.set(key, attempt);
                const seq = Number(sequence);
                const mustAbort = state.abortNextSegment || (seq % 11 === 4 && attempt === 1);
                state.abortNextSegment = false;
                const delay = seq % 7 === 3 ? 520 : 55;
                if (mustAbort) {
                    res.writeHead(200, { "content-type": "video/mp2t" });
                    res.write(Buffer.alloc(1024, seq % 255));
                    setTimeout(() => {
                        finish();
                        res.destroy();
                    }, 35);
                    return;
                }
                setTimeout(() => {
                    res.writeHead(200, { "content-type": "video/mp2t" });
                    res.end(Buffer.alloc(64 * 1024, seq % 255));
                }, delay);
            });
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
    const streams = {
        A: `${upstreamOrigin}/live/user/secret/A.m3u8`,
        B: `${upstreamOrigin}/live/user/secret/B.m3u8`,
        M: `${upstreamOrigin}/master/M.m3u8`
    };
    const config = encodeConfig({ l: [{ n: "Stress", u: sourceUrl }], g: ["IT | TEST"], gm: "filter" });
    const port = 18000 + Math.floor(Math.random() * 1000);
    const kronosOrigin = `http://127.0.0.1:${port}`;
    const ids = { A: channelId(sourceUrl, streams.A), B: channelId(sourceUrl, streams.B), M: channelId(sourceUrl, streams.M) };
    const logs = [];
    const child = spawn(process.execPath, [path.join(root, "server.js")], {
        cwd: root,
        env: {
            ...process.env,
            PORT: String(port),
            MANIFEST_RETRY_WINDOW_MS: "1600",
            MANIFEST_RETRY_DELAY_MS: "60",
            HLS_REQUEST_TIMEOUT: "600",
            SEG_REQUEST_TIMEOUT: "900",
            SEGMENT_PLAYER_RETRY_DELAY_MS: "25",
            PREFETCH_RETRY_DELAY_MS: "30",
            MANIFEST_FORBIDDEN_BACKOFF_MS: "220",
            POLL_ERROR_BACKOFF_MS: "80",
            LIVE_DELAY_SEGMENTS: "2",
            MIN_START_SEGMENTS: "4",
            MIN_VISIBLE_SEGMENTS: "2",
            PRIME_WAIT_MS: "120",
            RETAIN_SEGMENTS: "18",
            SERVE_SEGMENTS: "5",
            POLL_MIN_MS: "70",
            POLL_MAX_MS: "110",
            POLLER_IDLE_STOP_MS: "900",
            SEGMENT_CACHE_TTL: "10000"
        },
        stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout.on("data", data => logs.push(String(data)));
    child.stderr.on("data", data => logs.push(String(data)));

    const hlsUrl = id => `${kronosOrigin}/${config}/hls/${ids[id]}/index.m3u8`;
    const statsUrl = `${kronosOrigin}/${config}/stats`;
    let playerErrors = 0;
    let maxServedEdge = -1;
    let maxInitialManifestMs = 0;

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

    const readManifest = async id => {
        const started = Date.now();
        const response = await request(hlsUrl(id), {}, 6000);
        maxInitialManifestMs = Math.max(maxInitialManifestMs, Date.now() - started);
        assert.equal(response.status, 200, `manifest ${id} returned ${response.status}`);
        return parsePlaylist(await response.text());
    };

    const fetchSegment = async (url, expectedCache = null) => {
        const response = await request(url, { headers: { Range: "bytes=0-" } }, 5000);
        if (!response.ok) playerErrors++;
        assert(response.ok, `segment returned ${response.status}`);
        if (expectedCache) assert.equal(response.headers.get("x-kronos-cache"), expectedCache, `segment was not ${expectedCache}`);
        await response.arrayBuffer();
    };

    const assertManifestCached = async playlist => {
        for (const segment of playlist.segments) {
            await fetchSegment(new URL(segment, kronosOrigin).toString(), "HIT");
        }
    };

    const consume = async (id, rounds, intervalMs) => {
        let lastUrl = null;
        for (let i = 0; i < rounds; i++) {
            const playlist = await readManifest(id);
            assert(playlist.segments.length > 0, "served playlist is empty");
            assert(playlist.segments.length <= 5, "served playlist exceeded its visible window");
            assert(playlist.edge >= maxServedEdge || id !== "A", "served timeline moved backward");
            if (id === "A") maxServedEdge = Math.max(maxServedEdge, playlist.edge);
            const newest = playlist.segments.at(-1);
            if (newest !== lastUrl) {
                await fetchSegment(new URL(newest, kronosOrigin).toString());
                lastUrl = newest;
            }
            await sleep(intervalMs);
        }
    };

    const waitForCushion = async id => {
        for (let i = 0; i < 30; i++) {
            await readManifest(id);
            const stats = await (await request(statsUrl)).json();
            const active = stats.channels.find(channel => channel.label === `STRESS ${id} HD`);
            if (active?.warmRun >= 4 && active.knownEdge - active.servedEdge >= 2) return stats;
            await sleep(40);
        }
        throw new Error(`warm cushion did not recover for ${id}`);
    };

    const waitForActivePrefetch = async () => {
        for (let i = 0; i < 100; i++) {
            const stats = await (await request(statsUrl)).json();
            if (stats.cache.prefetchActive > 0) return;
            await sleep(10);
        }
        throw new Error("prefetch did not become active");
    };

    try {
        await waitForHealth();

        const placeholder = await readManifest("A");
        assert.equal(placeholder.segments.length, 3, "startup placeholder has the wrong segment count");
        assert(placeholder.segments.every(segment => segment.includes("/black.ts")), "startup did not return placeholder media");
        assert(maxInitialManifestMs < 700, `initial placeholder took too long (${maxInitialManifestMs}ms)`);
        for (const segment of placeholder.segments) {
            await fetchSegment(new URL(segment, kronosOrigin).toString());
        }

        let stats = await waitForCushion("A");
        const initial = await readManifest("A");
        assert(initial.segments.length > 0, "initial delayed playlist is empty");
        assert(initial.segments.length <= 5, "initial real playlist exceeded its visible window");
        assert(initial.segments.every(segment => !segment.includes("/black.ts")), "real playlist still contains placeholder media");
        assert(initial.discontinuities > 0, "placeholder transition was not marked as a discontinuity");
        await assertManifestCached(initial);
        const active = stats.channels.find(channel => channel.label === "STRESS A HD");
        assert(active, "missing active channel stats");
        assert(active.warmRun >= 4, "startup did not prime a protected warm cushion");

        const demandMissUrl = seq => {
            const upstreamSegment = `${upstreamOrigin}/hlsr/test-token/A/0/A_${seq}.ts`;
            const id = stableSegmentId(streams.A, upstreamSegment);
            return `${kronosOrigin}/${config}/proxy/seg?s=${encodeURIComponent(id)}`;
        };
        await request(`${upstreamOrigin}/control/abort-next-segment`);
        await fetchSegment(demandMissUrl(0), "MISS");
        await waitForActivePrefetch();
        await fetchSegment(demandMissUrl(1), "MISS");

        await consume("A", 22, 80);
        await consume("A", 22, 20);

        // A sustained provider-side 403 advances the upstream timeline beyond its
        // six-segment manifest window. Kronos must keep serving only cached history,
        // recover in chronological order, and mark the skipped timeline as a gap.
        await request(`${upstreamOrigin}/control/forbid-manifest?ms=2300`);
        const forbiddenDeadline = Date.now() + 2500;
        while (Date.now() < forbiddenDeadline) {
            const frozen = await readManifest("A");
            await assertManifestCached(frozen);
            await sleep(80);
        }
        let recoveredGap = null;
        for (let i = 0; i < 50; i++) {
            const candidate = await readManifest("A");
            await assertManifestCached(candidate);
            if (candidate.discontinuities > 0 && candidate.edge > initial.edge) {
                recoveredGap = candidate;
                break;
            }
            await sleep(60);
        }
        assert(recoveredGap, "sustained 403 gap did not recover with a discontinuity");
        assert(mock.state.manifestRequestsDuringForbidden <= 12, "403 cooldown was hammered too aggressively");

        await request(`${upstreamOrigin}/control/restart?id=A`);
        await sleep(250);
        const restarted = await readManifest("A");
        assert(restarted.discontinuities > 0, "encoder restart was not marked as an HLS discontinuity");
        assert(restarted.segments.length >= 2, "encoder restart exposed too small a visible playlist");
        assert(restarted.segments.length <= 5, "encoder restart exceeded the visible playlist limit");
        await consume("A", 12, 70);

        await readManifest("B");
        const staleSequence = initial.mediaSequence + 1;
        const staleAttemptKey = `A:0:${staleSequence}`;
        const segmentAttemptsBeforeStaleRead = mock.state.segmentAttempts.get(staleAttemptKey) || 0;
        const stale = await request(new URL(initial.segments[1], kronosOrigin).toString(), { headers: { Range: "bytes=0-" } }, 5000);
        assert([200, 206, 410].includes(stale.status), `late segment from the previous channel returned ${stale.status}`);
        if (stale.ok) await stale.arrayBuffer();
        const segmentAttemptsAfterStaleRead = mock.state.segmentAttempts.get(staleAttemptKey) || 0;
        assert.equal(segmentAttemptsAfterStaleRead, segmentAttemptsBeforeStaleRead, "late segment from the previous channel reopened upstream after zapping");
        await consume("B", 5, 50);
        await readManifest("A");
        maxServedEdge = -1; // Zapping back opens a fresh player timeline.
        await consume("A", 8, 70);

        stats = await waitForCushion("A");
        let activeAfter = stats.channels.find(channel => channel.label === "STRESS A HD");
        assert(activeAfter.depth <= 18, "retained window exceeded its limit");
        assert(stats.cache.prefetchQueue <= 18, "prefetch queue exceeded its intended bound");
        assert.equal(mock.state.concurrencyViolations, 0, "Kronos opened parallel upstream requests");
        assert.equal(playerErrors, 0, "player observed failed segment responses");

        const idleGeneration = activeAfter.generation;
        let stopped = null;
        for (let i = 0; i < 60; i++) {
            await sleep(50);
            stats = await (await request(statsUrl)).json();
            stopped = stats.channels.find(channel => channel.label === "STRESS A HD");
            if (stopped && !stopped.running) break;
        }
        assert(stopped && !stopped.running, "poller did not stop after player idle timeout");
        maxServedEdge = -1;
        stats = await waitForCushion("A");
        activeAfter = stats.channels.find(channel => channel.label === "STRESS A HD");
        assert(activeAfter.generation > idleGeneration, "idle reopen reused the previous cache generation");

        const directUrl = `${kronosOrigin}/${config}/proxy/seg?u=${Buffer.from(`${upstreamOrigin}/direct/D.ts`, "utf8").toString("base64url")}`;
        const direct = request(directUrl, {}, 5000).then(async response => {
            assert(response.ok, `direct stream returned ${response.status}`);
            await response.arrayBuffer();
        });
        await sleep(40);
        const master = await readManifest("M");
        await direct;
        assert.equal(mock.state.concurrencyViolations, 0, "direct stream released the upstream gate before closing");
        const masterRequests = mock.state.masterManifests;
        await readManifest("M");
        assert.equal(mock.state.masterManifests, masterRequests + 1, "master playlist started a redundant background poll");
        const variant = await request(new URL(master.segments[0], kronosOrigin).toString(), {}, 5000);
        assert(variant.ok, `nested playlist returned ${variant.status}`);
        const nested = parsePlaylist(await variant.text());
        await fetchSegment(new URL(nested.segments[0], kronosOrigin).toString());

        const text = logs.join("");
        assert(text.includes("[HLS PLACEHOLDER]"), "startup placeholder path was not exercised");
        assert(text.includes("[HLS TRANSITION]"), "placeholder transition path was not exercised");
        assert(text.includes("[HLS RETRY]"), "manifest retry path was not exercised");
        assert(text.includes("[POLLER ERR]"), "manifest poller recovery path was not exercised");
        assert(text.includes("[SEG RETRY]"), "segment retry path was not exercised");
        assert(text.includes("[PREFETCH PREEMPT]"), "player-demand prefetch preemption was not exercised");
        assert(text.includes("[CHANNEL GAP]"), "upstream sequence gap was not detected");
        assert(text.includes("[CHANNEL RESTART]"), "encoder restart path was not exercised");
        assert(text.includes("reason=channel-switch"), "zapping cancellation path was not exercised");
        assert(text.includes("reason=player-idle"), "idle cleanup path was not exercised");
        assert(text.includes("reason=direct-stream"), "direct stream cancellation path was not exercised");

        console.log(JSON.stringify({
            ok: true,
            mockMaxConcurrent: mock.state.maxConcurrent,
            playerErrors,
            initialManifestMs: maxInitialManifestMs,
            manifestRequestsDuringForbidden: mock.state.manifestRequestsDuringForbidden,
            cache: stats.cache,
            activeChannel: activeAfter,
            exercised: ["startup-placeholder", "bounded-visible-window", "token-rotation", "invalid-manifest", "geo-403", "sustained-403-gap", "slow-segments", "aborted-segment", "x2-consumption", "encoder-restart", "zapping", "idle-reopen", "direct-stream-gate", "master-playlist"]
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
