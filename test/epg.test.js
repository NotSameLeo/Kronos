const test = require("node:test");
const assert = require("node:assert/strict");
const { attachEPGToChannels, formatEpgDescription, withCurrentEPG } = require("../src/epg");

const programmes = [
    {
        start: new Date("2026-06-20T01:50:00+02:00"),
        stop: new Date("2026-06-20T02:22:00+02:00"),
        title: "Coppa del mondo di mountain bike",
        desc: "Short track femminile"
    },
    {
        start: new Date("2026-06-20T02:22:00+02:00"),
        stop: new Date("2026-06-20T04:40:00+02:00"),
        title: "Coppa del mondo FIFA 2026",
        desc: "Gruppo C"
    }
];

test("formatEpgDescription recalculates the current programme for the request time", () => {
    const beforeSwitch = formatEpgDescription(programmes, new Date("2026-06-20T02:10:00+02:00"));
    assert.match(beforeSwitch, /^In Onda: COPPA DEL MONDO DI MOUNTAIN BIKE/);
    assert.match(beforeSwitch, /A Seguire: COPPA DEL MONDO FIFA 2026/);

    const afterSwitch = formatEpgDescription(programmes, new Date("2026-06-20T02:26:00+02:00"));
    assert.match(afterSwitch, /^In Onda: COPPA DEL MONDO FIFA 2026/);
    assert.doesNotMatch(afterSwitch, /^In Onda: COPPA DEL MONDO DI MOUNTAIN BIKE/);
});

test("withCurrentEPG refreshes cached channel descriptions without refetching the catalog", () => {
    const cachedChannel = {
        id: "channel_test",
        name: "RSI LA 2 HD",
        description: formatEpgDescription(programmes, new Date("2026-06-20T02:10:00+02:00")),
        epgId: "rsi-la2",
        epgProgrammes: programmes
    };

    const refreshed = withCurrentEPG(cachedChannel, null, new Date("2026-06-20T02:26:00+02:00"));
    assert.match(refreshed.description, /^In Onda: COPPA DEL MONDO FIFA 2026/);
});

test("EPG matching tolerates country prefixes and quality suffixes", () => {
    const epgData = {
        byKey: new Map([
            ["rsila2", [{ id: "RSILA2.it", programmes }]]
        ])
    };

    const { channels, matched } = attachEPGToChannels([{ name: "RSI LA 2 HD" }], epgData, new Date("2026-06-20T02:10:00+02:00"));
    assert.equal(matched, 1);
    assert.equal(channels[0].epgId, "RSILA2.it");
    assert.match(channels[0].description, /^In Onda:/);
});

test("EPG matching ignores country prefixes and noisy stream suffixes", () => {
    const epgData = {
        byKey: new Map([
            ["daznweb1", [{ id: "DAZNWeb1.it", programmes }]]
        ])
    };

    const { channels, matched } = attachEPGToChannels([
        { name: "IT - DAZN WEB 1 4K BAR HEVC" },
        { name: "CH: DAZN WEB 1 UHD RAW" }
    ], epgData, new Date("2026-06-20T02:10:00+02:00"));
    assert.equal(matched, 2);
    assert.deepEqual(channels.map(channel => channel.epgId), ["DAZNWeb1.it", "DAZNWeb1.it"]);
});
