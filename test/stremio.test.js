const test = require("node:test");
const assert = require("node:assert/strict");
const { buildStream, isXtreamTsLive, shouldBlockOfflinePlaceholders } = require("../src/stremio");

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

test("buildStream uses continuous TS live relay only for Xtream TS live URLs", () => {
    const xtreamTs = {
        id: "channel_ts",
        name: "DAZN WEB 2 4K",
        group: "DAZN Web",
        sourceType: "xtream",
        streamFormat: "ts",
        url: "http://stream.example/live/username/password/12345.ts"
    };
    const xtreamHls = {
        ...xtreamTs,
        streamFormat: "hls",
        url: "http://stream.example/live/username/password/12345.m3u8"
    };
    const genericTs = {
        ...xtreamTs,
        sourceType: "m3u",
        url: "http://stream.example/live.ts"
    };

    assert.equal(isXtreamTsLive(xtreamTs), true);
    assert.equal(isXtreamTsLive(xtreamHls), false);
    assert.equal(isXtreamTsLive(genericTs), false);
    assert.equal(new URL(buildStream(xtreamTs, "http://kronos.test", "abc").url).pathname, "/abc/proxy/live.ts");
    assert.equal(new URL(buildStream(xtreamHls, "http://kronos.test", "abc").url).pathname, "/abc/proxy/live.m3u8");
    assert.equal(new URL(buildStream(genericTs, "http://kronos.test", "abc").url).pathname, "/abc/proxy/seg");
});
