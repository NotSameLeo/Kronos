const test = require("node:test");
const assert = require("node:assert/strict");
const { adaptiveMasterManifest } = require("../src/transcode");

test("adaptiveMasterManifest advertises an ABR ladder before original", () => {
    const text = adaptiveMasterManifest("http://kronos.test", "abc", "http://upstream.example/live.m3u8", {
        blockOfflinePlaceholders: true,
        liveEdgeDelaySeconds: 60,
        startOffsetSeconds: 30,
        holdBackSeconds: 30
    });
    const lines = text.trim().split("\n");
    const urls = lines.filter(line => line.startsWith("http://")).map(line => new URL(line));

    assert.match(text, /Kronos 240p/);
    assert.match(text, /Kronos 360p/);
    assert.match(text, /Kronos 480p/);
    assert.equal(urls[0].pathname, "/abc/proxy/transcode.m3u8");
    assert.equal(urls[0].searchParams.get("v"), "240p");
    assert.equal(urls[1].searchParams.get("v"), "360p");
    assert.equal(urls[2].searchParams.get("v"), "480p");
    assert.equal(urls[0].searchParams.get("d"), "60");
    assert.equal(urls[0].searchParams.get("st"), "30");
    assert.equal(urls.at(-1).pathname, "/abc/proxy/live.m3u8");
    assert.equal(urls.at(-1).searchParams.get("hb"), "30");
});
