const test = require("node:test");
const assert = require("node:assert/strict");
const { rewriteManifest, segmentIdentity } = require("../src/proxy");

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

    const out = rewriteManifest(text, upstream, "http://kronos.test", "config-key", upstream, "short1234");
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
});

test("rewriteManifest relays master playlist variants as child manifests", () => {
    const text = [
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",URI=\"audio/main.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=3840x2160,FRAME-RATE=60.000",
        "video/4k.m3u8"
    ].join("\n");

    const out = rewriteManifest(text, "http://upstream.example/master.m3u8", "http://kronos.test", "config-key", "http://upstream.example/master.m3u8", "abc12345");
    assert.match(out, /URI="http:\/\/kronos\.test\/abc12345\/proxy\/live\.m3u8\?/);
    const variant = out.split("\n").find(line => line.includes("video%2F4k") || line.includes("/proxy/live.m3u8?"));
    assert.ok(variant);
    assert.match(out, /http:\/\/kronos\.test\/abc12345\/proxy\/live\.m3u8\?/);
});

test("segmentIdentity ignores volatile token parameters", () => {
    const first = segmentIdentity("http://cdn.example/path/seg.ts?token=old&track=a&expires=1");
    const second = segmentIdentity("http://cdn.example/path/seg.ts?token=new&track=a&expires=2");
    assert.equal(first, second);
});
