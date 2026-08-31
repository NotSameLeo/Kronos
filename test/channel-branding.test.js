const test = require("node:test");
const assert = require("node:assert/strict");

process.env.KRONOS_CHANNEL_BRANDING_CONFIGS = "d2bc70d5ec70";
delete require.cache[require.resolve("../src/settings")];
delete require.cache[require.resolve("../src/channel-branding")];

const {
    applyChannelBranding,
    logoForName,
    normalizeChannelName,
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
