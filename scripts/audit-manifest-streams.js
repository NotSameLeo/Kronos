#!/usr/bin/env node

const fs = require("fs/promises");

function argument(name, fallback = "") {
    const index = process.argv.indexOf(`--${name}`);
    return index >= 0 ? process.argv[index + 1] || fallback : fallback;
}

const manifestUrl = argument("manifest") || process.argv[2];
const outputFile = argument("out", "/tmp/kronos-stream-audit.json");
const concurrency = Math.max(1, Math.min(24, Number(argument("concurrency", "8")) || 8));
const timeoutMs = Math.max(1500, Math.min(30000, Number(argument("timeout", "8000")) || 8000));

if (!manifestUrl) {
    console.error("Usage: npm run audit:manifest -- --manifest https://host/key/manifest.json [--out file] [--concurrency 8]");
    process.exit(2);
}

function timeoutSignal() {
    return AbortSignal.timeout(timeoutMs);
}

async function fetchChecked(url, options = {}) {
    return fetch(url, {
        redirect: "follow",
        signal: timeoutSignal(),
        headers: {
            "User-Agent": "Kronos-Catalog-Audit/1.0",
            ...(options.headers || {})
        },
        ...options
    });
}

function routeRoot(url) {
    const parsed = new URL(url);
    parsed.pathname = parsed.pathname.replace(/\/manifest\.json$/, "");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString().replace(/\/$/, "");
}

function firstPlaylistUri(text) {
    const lines = text.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const media = lines.find(line => !line.startsWith("#") && /\.(?:ts|m4s|mp4|aac)(?:[?#]|$)/i.test(line));
    return media || lines.find(line => !line.startsWith("#")) || "";
}

async function readLimited(response, limit = 96 * 1024) {
    if (!response.body) return 0;
    const reader = response.body.getReader();
    let bytes = 0;
    try {
        while (bytes < limit) {
            const { done, value } = await reader.read();
            if (done) break;
            bytes += value?.byteLength || 0;
        }
    } finally {
        await reader.cancel().catch(() => {});
    }
    return bytes;
}

async function probePlayback(url) {
    let current = url;
    for (let depth = 0; depth < 3; depth += 1) {
        const response = await fetchChecked(current, {
            headers: depth === 2 ? { Range: "bytes=0-98303" } : {}
        });
        const contentType = String(response.headers.get("content-type") || "").toLowerCase();
        if (!response.ok) return { ok: false, phase: `http-${depth}`, status: response.status, contentType };

        if (contentType.includes("mpegurl") || /\.m3u8(?:[?#]|$)/i.test(current)) {
            const text = await response.text();
            if (!text.includes("#EXTM3U")) return { ok: false, phase: "invalid-playlist", status: response.status, contentType };
            const next = firstPlaylistUri(text);
            if (!next) return { ok: false, phase: "empty-playlist", status: response.status, contentType };
            current = new URL(next, response.url || current).toString();
            continue;
        }

        const bytes = await readLimited(response);
        return { ok: bytes > 0, phase: bytes > 0 ? "media" : "empty-media", status: response.status, contentType, bytes };
    }
    return { ok: false, phase: "playlist-depth", status: 0, contentType: "" };
}

async function loadCatalog(root, manifest) {
    const catalog = manifest.catalogs?.[0];
    if (!catalog) throw new Error("Manifest without catalogs");
    const metas = [];
    let skip = 0;
    while (true) {
        const url = `${root}/catalog/${encodeURIComponent(catalog.type)}/${encodeURIComponent(catalog.id)}/skip=${skip}.json`;
        const response = await fetchChecked(url);
        if (!response.ok) throw new Error(`Catalog HTTP ${response.status} at skip=${skip}`);
        const page = await response.json();
        const items = Array.isArray(page.metas) ? page.metas : [];
        metas.push(...items);
        const total = Number(response.headers.get("x-kronos-total") || 0);
        if (!items.length || (total && metas.length >= total)) break;
        skip += items.length;
    }
    return metas;
}

async function auditChannel(root, meta) {
    const started = Date.now();
    try {
        const streamResponse = await fetchChecked(`${root}/stream/${encodeURIComponent(meta.type || "tv")}/${encodeURIComponent(meta.id)}.json`);
        if (!streamResponse.ok) return { id: meta.id, name: meta.name, ok: false, phase: "stream-json", status: streamResponse.status, ms: Date.now() - started };
        const payload = await streamResponse.json();
        const stream = (payload.streams || []).find(item => item.url) || (payload.streams || [])[0];
        if (!stream?.url) return { id: meta.id, name: meta.name, ok: false, phase: stream?.externalUrl ? "external" : "no-url", status: 0, ms: Date.now() - started };
        const probe = await probePlayback(stream.url);
        return { id: meta.id, name: meta.name, ...probe, ms: Date.now() - started };
    } catch (error) {
        return {
            id: meta.id,
            name: meta.name,
            ok: false,
            phase: error?.name === "TimeoutError" ? "timeout" : "error",
            status: 0,
            error: String(error?.message || error).slice(0, 180),
            ms: Date.now() - started
        };
    }
}

async function runPool(items, worker) {
    const results = new Array(items.length);
    let next = 0;
    let completed = 0;
    const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
        while (true) {
            const index = next++;
            if (index >= items.length) break;
            results[index] = await worker(items[index]);
            completed += 1;
            if (completed % 25 === 0 || completed === items.length) {
                const online = results.filter(Boolean).filter(result => result.ok).length;
                process.stdout.write(`[AUDIT] ${completed}/${items.length} online=${online}\n`);
            }
        }
    });
    await Promise.all(runners);
    return results;
}

async function main() {
    const root = routeRoot(manifestUrl);
    const manifestResponse = await fetchChecked(manifestUrl);
    if (!manifestResponse.ok) throw new Error(`Manifest HTTP ${manifestResponse.status}`);
    const manifest = await manifestResponse.json();
    const channels = await loadCatalog(root, manifest);
    console.log(`[AUDIT START] channels=${channels.length} concurrency=${concurrency} timeoutMs=${timeoutMs}`);
    const startedAt = new Date().toISOString();
    const results = await runPool(channels, meta => auditChannel(root, meta));
    const report = {
        manifest: manifestUrl,
        addonId: manifest.id,
        addonVersion: manifest.version,
        startedAt,
        completedAt: new Date().toISOString(),
        total: results.length,
        online: results.filter(result => result.ok).length,
        offline: results.filter(result => !result.ok).length,
        byPhase: Object.fromEntries([...new Set(results.map(result => result.phase))].sort().map(phase => [phase, results.filter(result => result.phase === phase).length])),
        results
    };
    await fs.writeFile(outputFile, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`[AUDIT DONE] total=${report.total} online=${report.online} offline=${report.offline} report=${outputFile}`);
}

main().catch(error => {
    console.error(`[AUDIT ERROR] ${error.message}`);
    process.exitCode = 1;
});
