const test = require("node:test");
const assert = require("node:assert/strict");

process.env.KRONOS_CHANNEL_BRANDING_CONFIGS = "d2bc70d5ec70";
delete require.cache[require.resolve("../src/settings")];
delete require.cache[require.resolve("../src/channel-branding")];

const {
    applyChannelBranding,
    logoForName,
    normalizeChannelName,
    shouldExcludeChannel,
    withoutQualitySuffix
} = require("../src/channel-branding");

test("normalizza i nomi obsoleti senza falsi prefissi Sky", () => {
    assert.equal(normalizeChannelName("Sky DMAX 4k"), "DMAX 4K");
    assert.equal(normalizeChannelName("SKY FOOD NETWORK HEVC"), "FOOD NETWORK HEVC");
    assert.equal(normalizeChannelName("Sky Discovery HD"), "DISCOVERY HD");
    assert.equal(normalizeChannelName("Sky Crime Investigation 4k"), "SKY CRIME 4K");
    assert.equal(normalizeChannelName("Sky Cinema Due 24 UHD"), "SKY CINEMA STORIES UHD");
    assert.equal(normalizeChannelName("MotorTrend HD"), "DISCOVERY TURBO HD");
});

test("rende stabili i nomi degli slot evento e corregge i marchi obsoleti", () => {
    assert.equal(normalizeChannelName("No event streaming now - | 8K exclusive | IT: DAZN PPV 12"), "DAZN EXCLUSIVE 12 8K");
    assert.equal(normalizeChannelName("Next | evento | IT: DAZN PPV 7"), "DAZN EXCLUSIVE 7 8K");
    assert.equal(normalizeChannelName("PRIM 1 4K [LIVE-EVENT]"), "PRIME VIDEO EVENTO 1 4K");
    assert.equal(normalizeChannelName("Prime: Inter TV RAW"), "INTER TV RAW");
    assert.equal(normalizeChannelName("Alice"), "ALMA TV");
    assert.equal(normalizeChannelName("Champions League Infinity 3 HD"), "MEDIASET INFINITY CHAMPIONS 3 HD");
    assert.equal(normalizeChannelName("Adriano Celentano Channel RAW"), "ADRIANO CELENTANO 24/7 RAW");
    assert.equal(normalizeChannelName("American Dad 24H RAW"), "AMERICAN DAD 24/7 RAW");
});

test("rimuove solo i feed obsoleti concordati dal manifest Kronos", () => {
    assert.equal(shouldExcludeChannel("d2bc70d5ec70", { name: "Eleven Sport ciclismo live + replica" }), true);
    assert.equal(shouldExcludeChannel("d2bc70d5ec70", { name: "A3Series" }), true);
    assert.equal(shouldExcludeChannel("d2bc70d5ec70", { name: "Feralpi Salo Girone A" }), true);
    assert.equal(shouldExcludeChannel("d2bc70d5ec70", { name: "DAZN PPV 1" }), false);
    assert.equal(shouldExcludeChannel("un-altro-manifest", { name: "A3Series" }), false);
});

test("porta ogni nome in maiuscolo e conserva le varianti speciali", () => {
    assert.equal(normalizeChannelName("Disney Topolino Paperino e Pippo ᴿᴬᵂ"), "DISNEY TOPOLINO PAPERINO E PIPPO RAW");
    assert.equal(normalizeChannelName("Serie B - Girone A HD"), "SERIE B - GIRONE A HD");
});

test("le varianti di qualità condividono lo stesso logo base", () => {
    assert.equal(logoForName("DMAX 4K"), logoForName("DMAX HEVC"));
    assert.equal(logoForName("SKY CINEMA STORIES HD"), logoForName("SKY CINEMA STORIES UHD"));
    assert.equal(withoutQualitySuffix("SKY SPORT F1 4K HDR"), "SKY SPORT F1");
});

test("applica la cura soltanto al manifest autorizzato", () => {
    const channel = { name: "Sky DMAX 4k", logo: "https://example.test/old.png" };
    assert.deepEqual(applyChannelBranding("un-altro-manifest", channel), channel);
    const branded = applyChannelBranding("d2bc70d5ec70", channel);
    assert.equal(branded.name, "DMAX 4K");
    assert.match(branded.logo, /^\/channel-logos\/italy\/dmax-it\.svg$/);
});
