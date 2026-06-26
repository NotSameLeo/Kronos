const test = require("node:test");
const assert = require("node:assert/strict");
const { buildStream, shouldBlockOfflinePlaceholders } = require("../src/stremio");

test("buildStream disables offline placeholder blocking for vetrina channels", () => {
    const vetrina = {
        id: "channel_vetrina",
        name: "Sky Primafila Premiere Vetrina HD",
        group: "DAZN",
        url: "http://stream.example/vetrina.m3u8"
    };
    const infoEventi = {
        id: "channel_info_eventi",
        name: "INFO EVENTI 4K",
        group: "Canali UHD/4K",
        url: "http://stream.example/info-eventi.m3u8"
    };
    const primafilaEvent = {
        id: "channel_primafila_event",
        name: "SKY PRIMAFILA 1 4K",
        group: "Canali UHD/4K",
        url: "http://stream.example/primafila-1.m3u8"
    };
    const live = {
        id: "channel_live",
        name: "DAZN WEB 1",
        group: "DAZN Web",
        url: "http://stream.example/live.m3u8"
    };

    assert.equal(shouldBlockOfflinePlaceholders(vetrina), false);
    assert.equal(shouldBlockOfflinePlaceholders(infoEventi), false);
    assert.equal(shouldBlockOfflinePlaceholders(primafilaEvent), true);
    assert.equal(shouldBlockOfflinePlaceholders(live), true);
    assert.equal(new URL(buildStream(vetrina, "http://kronos.test", "abc").url).searchParams.get("pg"), "0");
    assert.equal(new URL(buildStream(infoEventi, "http://kronos.test", "abc").url).searchParams.get("pg"), "0");
    assert.equal(new URL(buildStream(primafilaEvent, "http://kronos.test", "abc").url).searchParams.get("pg"), "1");
    assert.equal(new URL(buildStream(live, "http://kronos.test", "abc").url).searchParams.get("pg"), "1");
});

test("buildStream gives native HLS channels conservative live playback hints", () => {
    const stream = buildStream({
        id: "channel_hls",
        name: "Live HLS",
        group: "Sport",
        url: "http://stream.example/live.m3u8"
    }, "http://kronos.test", "abc");
    const url = new URL(stream.url);

    assert.equal(url.pathname, "/abc/proxy/auto.m3u8");
    assert.equal(url.searchParams.get("d"), "60");
    assert.equal(url.searchParams.get("st"), "30");
    assert.equal(url.searchParams.get("hb"), "30");
});

test("buildStream sends Xtream TS channels through delayed HLS fallback", () => {
    const stream = buildStream({
        id: "channel_ts",
        name: "DAZN WEB 1",
        group: "DAZN",
        url: "http://stream.example/live/user/pass/123.ts",
        sourceType: "xtream",
        streamFormat: "ts"
    }, "http://kronos.test", "abc");

    const url = new URL(stream.url);
    assert.equal(url.pathname, "/abc/proxy/auto.m3u8");
    assert.equal(url.searchParams.get("d"), "60");
    assert.equal(url.searchParams.get("st"), "30");
    assert.equal(url.searchParams.get("hb"), "30");
    assert.ok(url.searchParams.get("u"));
    assert.match(Buffer.from(url.searchParams.get("u"), "base64url").toString(), /123\.m3u8$/);
});
