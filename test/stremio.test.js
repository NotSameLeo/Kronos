const test = require("node:test");
const assert = require("node:assert/strict");
const { buildStream, buildStreams, shouldBlockOfflinePlaceholders } = require("../src/stremio");
const { toMeta, sendPosterPng } = require("../src/stremio");

test("live TV metadata uses direct selection and native PNG artwork, never fake episodes", () => {
    const channel = { id: "tv_test", name: "TEST HD", logo: "/channel-logos/test.svg" };
    for (const options of [{}, { catalogLite: true, shortPoster: true, includeVideos: false }]) {
        const meta = toMeta(channel, "https://kronos.test", "abcdef123456", options);
        assert.equal(meta.type, "tv");
        assert.equal(meta.id, channel.id);
        assert.equal(Object.hasOwn(meta, "videos"), false);
        assert.match(meta.poster, /\/poster\/tv_test\.png\?v=/);
        if (!options.catalogLite) {
            assert.equal(meta.behaviorHints.defaultVideoId, channel.id);
            assert.equal(meta.logo, meta.poster);
            assert.equal(meta.background, meta.poster);
        }
    }
});

test("native posters are real 512px PNGs and concurrent requests share rendering", async () => {
    const channel = { id: "png_test", name: "TEST HD", logo: "" };
    const render = async () => {
        let body;
        const headers = {};
        await sendPosterPng({ setHeader: (k, v) => headers[k] = v, send: b => body = b }, channel);
        assert.equal(headers["Content-Type"], "image/png");
        assert.equal(body.subarray(1, 4).toString(), "PNG");
        const info = await require("sharp")(body).metadata();
        assert.equal(info.width, 512);
        assert.equal(info.height, 512);
        return body;
    };
    const [a, b] = await Promise.all([render(), render()]);
    assert.equal(a, b);
});

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

    assert.equal(url.pathname, "/abc/proxy/live.m3u8");
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
    assert.equal(url.pathname, "/abc/proxy/live.m3u8");
    assert.equal(url.searchParams.get("d"), "60");
    assert.equal(url.searchParams.get("st"), "30");
    assert.equal(url.searchParams.get("hb"), "30");
    assert.ok(url.searchParams.get("u"));
    assert.match(Buffer.from(url.searchParams.get("u"), "base64url").toString(), /123\.m3u8$/);
});

test("buildStreams exposes transcode qualities as separate Stremio choices", () => {
    const streams = buildStreams({
        id: "channel_ts",
        name: "DAZN WEB 1",
        group: "DAZN",
        url: "http://stream.example/live/user/pass/123.ts",
        sourceType: "xtream",
        streamFormat: "ts"
    }, "http://kronos.test", "abc");

    assert.deepEqual(streams.map(stream => stream.name), ["🖥 Sorgente", "💿 720p", "📼 480p", "💾 360p"]);
    assert.deepEqual(streams.map(stream => stream.title), ["DAZN WEB 1", "DAZN WEB 1", "DAZN WEB 1", "DAZN WEB 1"]);
    assert.equal(new URL(streams[0].url).pathname, "/abc/proxy/live.m3u8");
    assert.equal(new URL(streams[0].url).searchParams.get("v"), null);
    assert.deepEqual(streams.slice(1).map(stream => new URL(stream.url).searchParams.get("v")), ["720p", "480p", "360p"]);
    assert.ok(streams.slice(1).every(stream => new URL(stream.url).pathname === "/abc/proxy/transcode.m3u8"));
    assert.ok(streams.every(stream => Buffer.from(new URL(stream.url).searchParams.get("u"), "base64url").toString().endsWith("/123.m3u8")));
});
