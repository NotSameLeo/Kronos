"use strict";

const assert = require("assert");
const crypto = require("crypto");
const http = require("http");
const path = require("path");
const zlib = require("zlib");
const { spawn } = require("child_process");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = path.resolve(__dirname, "..");

function encodeConfig(value) {
    return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function channelId(sourceUrl, streamUrl) {
    return "channel_" + crypto.createHash("sha1").update(`${sourceUrl}|${streamUrl}`).digest("hex").slice(0, 20);
}

function xmltvDate(date) {
    const pad = value => String(value).padStart(2, "0");
    return `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}${pad(date.getUTCHours())}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())} +0000`;
}

function romeWallClockToDate(year, month, day, hour, minute, second) {
    const wallClockUtc = Date.UTC(year, month - 1, day, hour, minute, second);
    let instant = wallClockUtc;
    for (let i = 0; i < 2; i++) {
        const parts = Object.fromEntries(new Intl.DateTimeFormat("en-CA", {
            timeZone: "Europe/Rome", year: "numeric", month: "2-digit", day: "2-digit",
            hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"
        }).formatToParts(new Date(instant)).filter(part => part.type !== "literal").map(part => [part.type, Number(part.value)]));
        instant = wallClockUtc - (Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second) - instant);
    }
    return new Date(instant);
}

async function request(url, options = {}, timeoutMs = 3000) {
    return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

function createMock() {
    const state = { epgHits: 0, guideHits: 0, playlistHits: 0, xtreamEpgHits: 0 };
    const server = http.createServer((req, res) => {
        const origin = `http://127.0.0.1:${server.address().port}`;
        if (req.url.startsWith("/get.php")) {
            state.playlistHits++;
            if (state.playlistHits === 1) {
                res.writeHead(503, { "content-type": "text/plain" });
                res.end("playlist warming up");
                return;
            }
            res.writeHead(200, { "content-type": "audio/x-mpegurl" });
            res.end([
                "#EXTM3U",
                '#EXTINF:-1 tvg-id="stress.it" group-title="IT | TEST",IT: STRESS HD',
                `${origin}/live/stress.m3u8`,
                ""
            ].join("\n"));
            return;
        }
        if (req.url === "/guide.gzip") {
            state.guideHits++;
            res.writeHead(404, { "content-type": "text/plain" });
            res.end("old guide proxy is gone");
            return;
        }
        if (req.url.startsWith("/xmltv.php")) {
            state.xtreamEpgHits++;
            res.writeHead(500, { "content-type": "text/plain" });
            res.end("provider EPG is not usable");
            return;
        }
        if (req.url === "/global.xml") {
            state.epgHits++;
            const now = Date.now();
            const body = [
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<tv>",
                '<channel id="stress.it"><display-name>STRESS</display-name></channel>',
                `<programme start="${xmltvDate(new Date(now - 60000))}" stop="${xmltvDate(new Date(now + 60000))}" channel="stress.it"><title>Film attuale</title><desc>Film attuale: Descrizione attuale.</desc></programme>`,
                `<programme start="${xmltvDate(new Date(now + 60000))}" stop="${xmltvDate(new Date(now + 120000))}" channel="stress.it"><title>Film successivo</title><desc>Film successivo - Descrizione successiva.</desc></programme>`,
                "</tv>"
            ].join("");
            const send = () => {
                if (res.destroyed) return;
                res.writeHead(200, { "content-type": "application/gzip" });
                res.end(zlib.gzipSync(body));
            };
            if (state.epgHits === 1) setTimeout(send, 350);
            else send();
            return;
        }
        res.writeHead(404);
        res.end();
    });
    return { server, state };
}

async function waitForHealth(origin) {
    for (let i = 0; i < 80; i++) {
        try {
            const response = await request(`${origin}/health`, {}, 300);
            if (response.ok) return;
        } catch {}
        await sleep(40);
    }
    throw new Error("Kronos did not start");
}

async function main() {
    assert.equal(romeWallClockToDate(2026, 1, 15, 12, 0, 0).toISOString(), "2026-01-15T11:00:00.000Z", "CET conversion is incorrect");
    assert.equal(romeWallClockToDate(2026, 6, 1, 12, 0, 0).toISOString(), "2026-06-01T10:00:00.000Z", "CEST conversion is incorrect");
    const mock = createMock();
    await new Promise(resolve => mock.server.listen(0, "127.0.0.1", resolve));
    const upstreamOrigin = `http://127.0.0.1:${mock.server.address().port}`;
    const sourceUrl = `${upstreamOrigin}/get.php?username=stress&password=secret&type=m3u_plus&output=hls`;
    const streamUrl = `${upstreamOrigin}/live/stress.m3u8`;
    const config = encodeConfig({ l: [{ n: "Stress", u: sourceUrl }], g: ["IT | TEST"], gm: "filter", e: `${upstreamOrigin}/guide.gzip` });
    const port = 19000 + Math.floor(Math.random() * 1000);
    const kronosOrigin = `http://127.0.0.1:${port}`;
    const id = channelId(sourceUrl, streamUrl);
    const catalogId = `kronos_list_${Buffer.from("Stress").toString("hex")}`;
    const logs = [];
    const child = spawn(process.execPath, [path.join(root, "server.js")], {
        cwd: root,
        env: {
            ...process.env,
            PORT: String(port),
            EPG_PRELOAD_URL: `${upstreamOrigin}/global.xml`,
            EPG_REQUEST_TIMEOUT: "120",
            EPG_RETRY_DELAY_MS: "100",
            PLAYLIST_RETRY_WINDOW_MS: "1000",
            PLAYLIST_RETRY_DELAY_MS: "40"
        },
        stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout.on("data", data => logs.push(String(data)));
    child.stderr.on("data", data => logs.push(String(data)));

    try {
        await waitForHealth(kronosOrigin);
        const started = Date.now();
        const catalogs = await Promise.all(Array.from({ length: 5 }, () =>
            request(`${kronosOrigin}/${config}/catalog/tv/${catalogId}.json`, {}, 1000)
        ));
        const elapsed = Date.now() - started;
        assert(catalogs.every(response => response.ok), "catalog request failed while EPG was unavailable");
        assert(elapsed < 500, `catalog waited for EPG download (${elapsed}ms)`);
        assert.equal(mock.state.playlistHits, 2, "playlist was not recovered by its internal retry");
        assert.equal(mock.state.epgHits, 1, "concurrent catalog requests started duplicate EPG downloads");
        assert.equal(mock.state.guideHits, 0, "legacy guide.gzip was fetched instead of the global EPG");
        assert.equal(mock.state.xtreamEpgHits, 0, "provider-derived EPG was preferred over the global EPG");

        let description = "";
        let debug = null;
        for (let i = 0; i < 50; i++) {
            const meta = await (await request(`${kronosOrigin}/${config}/meta/tv/${id}.json`)).json();
            description = meta.meta?.description || "";
            debug = await (await request(`${kronosOrigin}/${config}/debug`)).json();
            if (description.includes("FILM ATTUALE") && debug.cache?.epg?.matched === 1) break;
            await sleep(50);
        }

        assert(mock.state.epgHits >= 2, "EPG retry did not run automatically");
        assert(description.includes("🔴\u00a0In\u00a0Onda:\u00a0FILM\u00a0ATTUALE\u00a0("), "current programme format is incorrect");
        assert(description.includes(")\u00a0|\u00a0Trama:\u00a0Descrizione\u00a0attuale\u00a0\u00a0║\u00a0\u00a0🔵\u00a0A\u00a0Seguire:\u00a0FILM\u00a0SUCCESSIVO\u00a0("), "inline EPG separator spacing is incorrect");
        assert(description.includes(")\u00a0|\u00a0Trama:\u00a0Descrizione\u00a0successiva"), "next programme format is incorrect");
        assert(!description.includes("▌"), "old test separator is still present");
        assert(!description.includes("Descrizione attuale."), "current description still has a trailing period");
        assert(!description.endsWith("."), "next description still has a trailing period");
        assert(!description.includes("Trama:\u00a0Film\u00a0attuale"), "current description repeats the programme title");
        assert(!description.includes("Trama:\u00a0Film\u00a0successivo"), "next description repeats the programme title");
        assert(!description.includes("€"), "stray euro prefix is still present");
        assert.equal(debug.cache.epgFetch.state, "ready", "EPG did not recover without restart");

        const text = logs.join("");
        assert(text.includes("[EPG PRELOAD]"), "startup EPG preload was not started");
        assert(text.includes("[EPG RESOLVE]"), "legacy EPG URL was not resolved to the global EPG");
        assert(text.includes("[EPG PENDING]"), "background EPG state was not logged");
        assert(text.includes("[EPG ERROR]"), "timeout path was not exercised");
        assert(text.includes("[EPG RETRY SCHEDULED]"), "automatic retry was not scheduled");
        assert(text.includes("[EPG APPLY] matched=1/1"), "recovered EPG was not applied to cached channels");
        assert(text.includes("[FETCH PLAYLIST RETRY] attempt=1"), "playlist retry path was not exercised");

        console.log(JSON.stringify({ ok: true, catalogMs: elapsed, playlistHits: mock.state.playlistHits, epgHits: mock.state.epgHits, epgState: debug.cache.epgFetch.state }, null, 2));
    } catch (err) {
        console.error(logs.join(""));
        throw err;
    } finally {
        child.kill("SIGTERM");
        mock.server.close();
    }
}

main().catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
});
