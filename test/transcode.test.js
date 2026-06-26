const test = require("node:test");
const assert = require("node:assert/strict");
const { adaptiveMasterManifest } = require("../src/transcode");

test("adaptiveMasterManifest advertises a low transcoded variant before original", () => {
    const text = adaptiveMasterManifest("http://kronos.test", "abc", "http://upstream.example/live.m3u8", {
        blockOfflinePlaceholders: true,
        liveEdgeDelaySeconds: 60,
        startOffsetSeconds: 30,
        holdBackSeconds: 30
    });
    const lines = text.trim().split("\n");
    const lowUrl = new URL(lines[4]);
    const originalUrl = new URL(lines[6]);

    assert.match(lines[3], /Kronos Low/);
    assert.equal(lowUrl.pathname, "/abc/proxy/transcode.m3u8");
    assert.equal(lowUrl.searchParams.get("h"), "480");
    assert.equal(lowUrl.searchParams.get("d"), "60");
    assert.equal(lowUrl.searchParams.get("st"), "30");
    assert.equal(originalUrl.pathname, "/abc/proxy/live.m3u8");
    assert.equal(originalUrl.searchParams.get("hb"), "30");
});
