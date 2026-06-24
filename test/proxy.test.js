const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { EventEmitter } = require("node:events");
const state = require("../src/state");
const { analyzeManifest, copyResponseHeaders, fetchSegmentWithHealing, getRewrittenManifest, isOfflinePlaceholderManifest, monitorSegmentTransfer, releaseStaleManifest, rewriteManifest, rewriteManifestDetailed, segmentIdentity } = require("../src/proxy");

test("rewriteManifest relays media playlist URLs without live-edge rules", () => {
    const upstream = "http://upstream.example/live/index.m3u8?token=abc";
    const text = [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:6",
        "#EXT-X-KEY:METHOD=AES-128,URI=\"key.key?token=old\"",
        "#EXTINF:6,",
        "seg-1.ts?token=old",
        "#EXTINF:6,",
        "http://cdn.example/seg-2.ts?expires=123&track=a",
        ""
    ].join("\n");

    const out = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "testnonce");
    assert.doesNotMatch(out, /#EXT-X-START/i);
    assert.doesNotMatch(out, /[?&]q=/);
    assert.match(out, /#EXT-X-KEY:METHOD=AES-128,URI="http:\/\/kronos\.test\/short1234\/proxy\/seg\?/);

    const segmentLines = out.split("\n").filter(line => line.includes("/proxy/seg?") && !line.startsWith("#"));
    assert.equal(segmentLines.length, 2);
    const proxied = new URL(segmentLines[0]);
    assert.equal(proxied.pathname, "/short1234/proxy/seg");
    assert.ok(proxied.searchParams.get("u"));
    assert.ok(proxied.searchParams.get("p"));
    assert.ok(proxied.searchParams.get("s"));
    assert.ok(proxied.searchParams.get("m"));
});

test("rewriteManifest gives live segment URLs a fresh manifest nonce", () => {
    const upstream = "http://upstream.example/live/index.m3u8";
    const text = [
        "#EXTM3U",
        "#EXTINF:6,",
        "seg-1.ts"
    ].join("\n");

    const first = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "nonce-a");
    const second = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "nonce-b");
    const firstUrl = new URL(first.split("\n").find(line => line.includes("/proxy/seg?")));
    const secondUrl = new URL(second.split("\n").find(line => line.includes("/proxy/seg?")));

    assert.equal(firstUrl.searchParams.get("u"), secondUrl.searchParams.get("u"));
    assert.notEqual(firstUrl.searchParams.get("m"), secondUrl.searchParams.get("m"));
});

test("rewriteManifest hides newest live segments to keep a stability buffer", () => {
    const upstream = "http://upstream.example/live/index.m3u8";
    const text = [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:50",
        "#EXTINF:10,",
        "seg50.ts",
        "#EXTINF:10,",
        "seg51.ts",
        "#EXTINF:10,",
        "seg52.ts",
        "#EXTINF:10,",
        "seg53.ts",
        "#EXTINF:10,",
        "seg54.ts",
        "#EXTINF:10,",
        "seg55.ts"
    ].join("\n");

    const out = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "nonce-a");
    const segmentLines = out.split("\n").filter(line => line.includes("/proxy/seg?"));
    assert.equal(segmentLines.length, 3);
    assert.match(Buffer.from(new URL(segmentLines.at(-1)).searchParams.get("u"), "base64url").toString(), /seg52\.ts$/);
});

test("stale manifest recovery releases only real reserved segments over time", () => {
    const upstream = "http://upstream.example/live/index.m3u8";
    const text = [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:50",
        "#EXTINF:10,",
        "seg50.ts",
        "#EXTINF:10,",
        "seg51.ts",
        "#EXTINF:10,",
        "seg52.ts",
        "#EXTINF:10,",
        "seg53.ts",
        "#EXTINF:10,",
        "seg54.ts",
        "#EXTINF:10,",
        "seg55.ts"
    ].join("\n");

    const detailed = rewriteManifestDetailed(
        text,
        upstream,
        "http://kronos.test",
        "config-key",
        upstream,
        "short1234",
        "nonce-a",
        true,
        30,
        30,
        30
    );
    const stale = {
        text: detailed.text,
        reserve: detailed.reserve
    };

    const before = releaseStaleManifest(stale, 9999);
    const one = releaseStaleManifest(stale, 10000);
    const two = releaseStaleManifest(stale, 20000);
    assert.equal(before.released, 0);
    assert.equal(one.released, 1);
    assert.equal(two.released, 2);
    assert.equal(before.text.split("\n").filter(line => line.includes("/proxy/seg?")).length, 3);
    assert.equal(one.text.split("\n").filter(line => line.includes("/proxy/seg?")).length, 4);
    assert.equal(two.text.split("\n").filter(line => line.includes("/proxy/seg?")).length, 5);
    assert.match(one.text, /#EXT-X-SERVER-CONTROL:HOLD-BACK=30/);
    assert.match(one.text, /#EXT-X-START:TIME-OFFSET=-30,PRECISE=NO/);
});

test("rewriteManifest can add a playback start offset for delayed live playlists", () => {
    const upstream = "http://upstream.example/live/index.m3u8";
    const text = [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:50",
        "#EXTINF:10,",
        "seg50.ts",
        "#EXTINF:10,",
        "seg51.ts",
        "#EXTINF:10,",
        "seg52.ts"
    ].join("\n");

    const out = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "nonce-a", true, 0, 20);
    assert.match(out, /^#EXTM3U\n#EXT-X-START:TIME-OFFSET=-20,PRECISE=NO/m);
});

test("rewriteManifest can add a conservative live hold-back hint", () => {
    const upstream = "http://upstream.example/live/index.m3u8";
    const text = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:50",
        "#EXTINF:10,",
        "seg50.ts",
        "#EXTINF:10,",
        "seg51.ts",
        "#EXTINF:10,",
        "seg52.ts"
    ].join("\n");

    const out = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234", "nonce-a", true, 0, 30, 30);
    assert.match(out, /^#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-SERVER-CONTROL:HOLD-BACK=30\n#EXT-X-START:TIME-OFFSET=-30,PRECISE=NO/m);
    assert.equal(out.split("\n").filter(line => line.includes("/proxy/seg?")).length, 3);
});

test("rewriteManifest relays master playlist variants as child manifests", () => {
    const text = [
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",URI=\"audio/main.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=3840x2160,FRAME-RATE=60.000",
        "video/4k.m3u8"
    ].join("\n");

    const out = rewriteManifest(text, "http://upstream.example/master.m3u8", "http://kronos.test", "config-key", "http://upstream.example/master.m3u8", "abc12345", "", true, 60, 30, 30);
    assert.match(out, /URI="http:\/\/kronos\.test\/abc12345\/proxy\/live\.m3u8\?/);
    const variant = out.split("\n").find(line => line.includes("video%2F4k") || line.includes("/proxy/live.m3u8?"));
    assert.ok(variant);
    assert.match(out, /http:\/\/kronos\.test\/abc12345\/proxy\/live\.m3u8\?/);
    assert.match(out, /[?&]pg=1/);
    assert.match(out, /[?&]hb=30/);
});

test("segmentIdentity ignores volatile token parameters", () => {
    const first = segmentIdentity("http://cdn.example/path/seg.ts?token=old&track=a&expires=1");
    const second = segmentIdentity("http://cdn.example/path/seg.ts?token=new&track=a&expires=2");
    assert.equal(first, second);
});

test("copyResponseHeaders hides finite range metadata for direct TS streams", () => {
    const headers = new Map();
    const res = {
        setHeader: (key, value) => headers.set(key.toLowerCase(), value),
        removeHeader: key => headers.delete(key.toLowerCase())
    };
    copyResponseHeaders({
        headers: {
            "content-type": "video/mp2t",
            "content-length": "14472616",
            "content-range": "bytes 0-14472615/14472616",
            "accept-ranges": "bytes",
            "etag": "\"file\""
        },
        kronosContext: { streamFormat: "ts-direct" }
    }, res);

    assert.equal(headers.get("content-type"), "video/mp2t");
    assert.equal(headers.has("content-length"), false);
    assert.equal(headers.has("content-range"), false);
    assert.equal(headers.has("accept-ranges"), false);
    assert.equal(headers.has("etag"), false);
});

test("monitorSegmentTransfer completes passive TS diagnostics without affecting the response", () => {
    const data = new EventEmitter();
    const res = new EventEmitter();
    res.writableFinished = false;
    res.writableEnded = false;
    const context = {
        sessionKey: "diagnostic-session",
        routeKey: "route-a",
        streamFormat: "hls-segment",
        urlExtension: "ts",
        upstream: "http://upstream.example/seg.ts",
        movement: "unknown",
        sequenceDelta: null,
        metadata: {
            sequence: 100,
            duration: 10,
            urlHash: "segment100"
        }
    };
    const upstreamResponse = {
        data,
        headers: { "content-length": "188" },
        kronosContext: context
    };
    const lines = [];
    const originalLog = console.log;
    console.log = line => lines.push(String(line));
    try {
        monitorSegmentTransfer(upstreamResponse, {}, res);
        const packet = Buffer.alloc(188, 0xff);
        packet[0] = 0x47;
        packet[1] = 0x1f;
        packet[2] = 0xff;
        packet[3] = 0x10;
        data.emit("data", packet);
        data.emit("end");
        res.writableFinished = true;
        res.emit("finish");
    } finally {
        console.log = originalLog;
    }

    assert.ok(lines.some(line => line.startsWith("[HLS SEG SENT]")));
    assert.ok(lines.some(line => line.startsWith("[TS SEG DIAG]") && line.includes("complete=1")));
});

test("analyzeManifest exposes live sequence diagnostics", () => {
    const text = [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:6",
        "#EXT-X-MEDIA-SEQUENCE:100",
        "#EXTINF:6,",
        "seg100.ts",
        "#EXTINF:6,",
        "seg101.ts"
    ].join("\n");

    const analysis = analyzeManifest(text, "http://upstream.example/live/index.m3u8");
    assert.equal(analysis.kind, "media");
    assert.equal(analysis.mediaSequence, 100);
    assert.equal(analysis.targetDuration, 6);
    assert.equal(analysis.segmentCount, 2);
    assert.equal(analysis.firstSegment.sequence, 100);
    assert.equal(analysis.lastSegment.sequence, 101);
    assert.equal(analysis.totalDuration, 12);
    assert.equal(analysis.endList, false);
});

test("isOfflinePlaceholderManifest detects short ended slate playlists", () => {
    const placeholder = analyzeManifest([
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:15",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXTINF:15,",
        "waiting.ts",
        "#EXT-X-ENDLIST"
    ].join("\n"), "http://upstream.example/live/index.m3u8");

    const liveStartup = analyzeManifest([
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:17",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXTINF:16.733,",
        "live0.ts"
    ].join("\n"), "http://upstream.example/live/index.m3u8");

    assert.equal(isOfflinePlaceholderManifest(placeholder), true);
    assert.equal(isOfflinePlaceholderManifest(liveStartup), false);
});

test("getRewrittenManifest coalesces duplicate upstream manifest fetches", async () => {
    state.manifestInflight.clear();
    state.manifestRawInflight.clear();
    state.manifestRawRecent.clear();
    state.manifestLastGood.clear();

    let hits = 0;
    const server = http.createServer((req, res) => {
        hits++;
        setTimeout(() => {
            res.writeHead(200, { "Content-Type": "application/vnd.apple.mpegurl" });
            res.end([
                "#EXTM3U",
                "#EXT-X-TARGETDURATION:10",
                "#EXT-X-MEDIA-SEQUENCE:200",
                "#EXTINF:10,",
                "seg200.ts",
                "#EXTINF:10,",
                "seg201.ts"
            ].join("\n"));
        }, 50);
    });
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));

    try {
        const upstream = `http://127.0.0.1:${server.address().port}/live.m3u8`;
        const req = {
            query: {},
            headers: {},
            ip: "127.0.0.1",
            get: name => name.toLowerCase() === "user-agent" ? "test-player" : ""
        };

        const [first, second] = await Promise.all([
            getRewrittenManifest("coalesce-config", upstream, "http://kronos-a.test", "route-a", req),
            getRewrittenManifest("coalesce-config", upstream, "http://kronos-b.test", "route-a", req)
        ]);
        const third = await getRewrittenManifest("coalesce-config", upstream, "http://kronos-c.test", "route-a", req);

        assert.equal(hits, 1);
        assert.match(first.text, /http:\/\/kronos-a\.test\/route-a\/proxy\/seg/);
        assert.match(second.text, /http:\/\/kronos-b\.test\/route-a\/proxy\/seg/);
        assert.match(third.text, /http:\/\/kronos-c\.test\/route-a\/proxy\/seg/);
    } finally {
        await new Promise(resolve => server.close(resolve));
        state.manifestInflight.clear();
        state.manifestRawInflight.clear();
        state.manifestRawRecent.clear();
        state.manifestLastGood.clear();
    }
});

test("fetchSegmentWithHealing retries transient provider 530 before exposing an error", async () => {
    let hits = 0;
    const server = http.createServer((req, res) => {
        hits++;
        if (hits <= 3) {
            res.writeHead(530, { "Content-Type": "text/plain" });
            res.end("temporary upstream error");
            return;
        }
        res.writeHead(200, { "Content-Type": "video/mp2t", "Content-Length": "4" });
        res.end("data");
    });
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));

    try {
        const upstream = `http://127.0.0.1:${server.address().port}/seg.ts`;
        const req = {
            query: {},
            headers: {},
            ip: "127.0.0.1",
            get: name => name.toLowerCase() === "user-agent" ? "test-player" : ""
        };
        const response = await fetchSegmentWithHealing("segment-530-config", "route-a", upstream, {}, req);
        assert.equal(response.status, 200);
        assert.equal(hits, 4);
        response.data.destroy();
    } finally {
        await new Promise(resolve => server.close(resolve));
    }
});
