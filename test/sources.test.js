const test = require("node:test");
const assert = require("node:assert/strict");
const {
    getConfiguredLists,
    normalizeSourceType,
    normalizeXtreamStreamFormat,
    parseM3UChannels,
    parseXtreamConfig,
    sortChannelsByName,
    xtreamLiveUrl
} = require("../src/sources");

test("parseM3UChannels extracts channels and skips divider rows", () => {
    const playlist = [
        "#EXTM3U",
        "#EXTINF:-1 tvg-id=\"rai1.it\" tvg-logo=\"http://img/rai.png\" group-title=\"IT: News\",IT: Rai 1 HD",
        "http://stream.example/rai1.m3u8",
        "#EXTINF:-1 group-title=\"Other\",#### Divider",
        "http://stream.example/divider.m3u8"
    ].join("\n");

    const channels = parseM3UChannels(playlist, { name: "Main", url: "http://list.example/list.m3u" });
    assert.equal(channels.length, 1);
    assert.equal(channels[0].name, "IT: Rai 1 HD");
    assert.equal(channels[0].group, "News");
    assert.equal(channels[0].tvgId, "rai1.it");
    assert.equal(channels[0].sourceName, "Main");
    assert.match(channels[0].id, /^channel_[a-f0-9]{20}$/);
});

test("source type and config list normalization stay conservative", () => {
    assert.equal(normalizeSourceType("xtream"), "xtream");
    assert.equal(normalizeSourceType("M3U"), "m3u");
    assert.equal(normalizeSourceType("weird"), "auto");
    assert.equal(normalizeXtreamStreamFormat("hls"), "hls");
    assert.equal(normalizeXtreamStreamFormat("TS"), "ts");
    assert.equal(normalizeXtreamStreamFormat("weird"), "ts");

    const lists = getConfiguredLists({
        l: [
            { n: "A", u: "http://a.example/list.m3u", t: "m3u", fmt: "hls" },
            { n: "", u: "", t: "xtream" },
            { u: "http://b.example/get.php?username=u&password=p&type=m3u_plus&output=hls" }
        ]
    });
    assert.deepEqual(lists.map(list => [list.name, list.type, list.streamFormat]), [["A", "m3u", "hls"], ["Lista 3", "auto", "hls"]]);
});

test("parseXtreamConfig accepts get.php and live path credentials", () => {
    assert.deepEqual(parseXtreamConfig("http://host.example/get.php?username=user&password=pass&type=m3u_plus"), {
        origin: "http://host.example",
        username: "user",
        password: "pass"
    });
    assert.deepEqual(parseXtreamConfig("http://host.example/live/user/pass/10.m3u8"), {
        origin: "http://host.example",
        username: "user",
        password: "pass"
    });
});

test("xtreamLiveUrl follows the configured stream format", () => {
    const xtream = { origin: "http://host.example", username: "user", password: "pass" };
    assert.equal(xtreamLiveUrl(xtream, "10", "ts"), "http://host.example/live/user/pass/10.ts");
    assert.equal(xtreamLiveUrl(xtream, "10", "hls"), "http://host.example/live/user/pass/10.m3u8");
    assert.equal(xtreamLiveUrl(xtream, "10", "bad"), "http://host.example/live/user/pass/10.ts");
});

test("sortChannelsByName uses natural channel ordering and quality variants", () => {
    const channels = [
        { name: "DAZN WEB 10" },
        { name: "DAZN WEB 1 4K" },
        { name: "DAZN WEB 1 UHD" },
        { name: "DAZN WEB 1 FHD" },
        { name: "DAZN WEB 2" },
        { name: "DAZN WEB 1 HEVC" },
        { name: "DAZN WEB 1 HD" },
        { name: "DAZN WEB 1 SD" },
        { name: "DAZN WEB 1" },
        { name: "DAZN WEB 10 HD" }
    ];

    assert.deepEqual(sortChannelsByName(channels).map(channel => channel.name), [
        "DAZN WEB 1 HEVC",
        "DAZN WEB 1 4K",
        "DAZN WEB 1 UHD",
        "DAZN WEB 1 FHD",
        "DAZN WEB 1 HD",
        "DAZN WEB 1 SD",
        "DAZN WEB 1",
        "DAZN WEB 2",
        "DAZN WEB 10 HD",
        "DAZN WEB 10"
    ]);
});
