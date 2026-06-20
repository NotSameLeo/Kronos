const test = require("node:test");
const assert = require("node:assert/strict");
const { formatEpgDescription, withCurrentEPG } = require("../src/epg");

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
