#!/usr/bin/env node
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { decodeConfig, getStartupPreloadConfigs } = require("../src/config-store");
const { fetchChannelsFromSource, getConfiguredLists, getSelectedGroups } = require("../src/sources");
const { normalizeGroupName } = require("../src/utils");
const { applyChannelBranding, shouldExcludeChannel, withoutQualitySuffix } = require("../src/channel-branding");

const OUTPUT = process.argv.includes("--out") ? process.argv[process.argv.indexOf("--out") + 1] : "/tmp/kronos-channel-assets.json";
const CONCURRENCY = Math.max(1, Math.min(32, Number(process.env.ASSET_AUDIT_CONCURRENCY || 12)));
const TIMEOUT_MS = Math.max(1000, Number(process.env.ASSET_AUDIT_TIMEOUT_MS || 10000));
const LOGO_ROOT = path.resolve(__dirname, "..", "public");
const CREATED_LOCALLY = new Set([
    "20th-century-studios-it.svg", "aci-sport-tv-it.svg", "adriano-celentano-24-7-it.svg",
    "alberto-sordi-24-7-it.svg", "aldo-giovanni-giacomo-24-7-it.svg", "alessandro-siani-24-7-it.svg",
    "alma-tv-it.svg", "american-dad-24-7-it.svg", "cinema-24-7-it.svg", "comedy-central-kronos-it.svg",
    "euronews-it.svg", "mediaset-infinity-it.svg", "mezzo-it.svg", "motogp-it.svg", "prime-video-it.svg",
    "sky-cinema-action-kronos-it.svg", "sky-cinema-collection-kronos-it.svg", "sky-cinema-comedy-kronos-it.svg",
    "sky-cinema-drama-kronos-it.svg", "sky-cinema-family-kronos-it.svg", "sky-cinema-romance-kronos-it.svg",
    "sky-cinema-stories-kronos-it.svg", "sky-cinema-suspense-kronos-it.svg", "sky-cinema-uno-kronos-it.svg",
    "sky-cinema-uno-plus24-kronos-it.svg"
]);

function dimensions(data, type = "") {
    const prefix = data.subarray(0, 256).toString("utf8");
    const text = type.includes("svg") || prefix.includes("<svg") ? data.toString("utf8") : "";
    if (text) {
        const viewBox = text.match(/viewBox=["']\s*[-\d.]+\s+[-\d.]+\s+([\d.]+)\s+([\d.]+)["']/i);
        const width = text.match(/\bwidth=["']([\d.]+)(?:px)?["']/i);
        const height = text.match(/\bheight=["']([\d.]+)(?:px)?["']/i);
        return { format: "svg", width: Number(viewBox?.[1] || width?.[1] || 0), height: Number(viewBox?.[2] || height?.[1] || 0), vector: true };
    }
    if (data.length >= 24 && data.subarray(1, 4).toString() === "PNG") {
        return { format: "png", width: data.readUInt32BE(16), height: data.readUInt32BE(20), vector: false };
    }
    if (data.length >= 10 && /^GIF8/.test(data.subarray(0, 4).toString())) {
        return { format: "gif", width: data.readUInt16LE(6), height: data.readUInt16LE(8), vector: false };
    }
    if (data.length >= 30 && data.subarray(0, 4).toString("ascii") === "RIFF" && data.subarray(8, 12).toString("ascii") === "WEBP") {
        if (data.subarray(12, 16).toString("ascii") === "VP8X") {
            return { format: "webp", width: 1 + data.readUIntLE(24, 3), height: 1 + data.readUIntLE(27, 3), vector: false };
        }
    }
    if (data.length > 4 && data[0] === 0xff && data[1] === 0xd8) {
        let offset = 2;
        while (offset + 9 < data.length) {
            if (data[offset] !== 0xff) { offset += 1; continue; }
            const marker = data[offset + 1];
            const size = data.readUInt16BE(offset + 2);
            if (marker >= 0xc0 && marker <= 0xc3) {
                return { format: "jpeg", width: data.readUInt16BE(offset + 7), height: data.readUInt16BE(offset + 5), vector: false };
            }
            offset += Math.max(2, size + 2);
        }
    }
    return { format: type.split("/")[1] || "unknown", width: 0, height: 0, vector: false };
}

function resultForData(data, contentType, source) {
    const image = dimensions(data, contentType);
    return {
        ok: data.length > 0, source, contentType, bytes: data.length,
        sha256: crypto.createHash("sha256").update(data).digest("hex"), ...image,
        ratio: image.width && image.height ? Number((image.width / image.height).toFixed(3)) : 0
    };
}

async function inspectLogo(value) {
    const logo = String(value || "").trim();
    if (!logo) return { ok: false, source: "missing", error: "missing" };
    if (logo.startsWith("/channel-logos/")) {
        const candidate = path.resolve(LOGO_ROOT, `.${logo}`);
        if (!candidate.startsWith(`${LOGO_ROOT}${path.sep}`) || !fs.existsSync(candidate)) return { ok: false, source: "local", error: "local-missing" };
        const data = fs.readFileSync(candidate);
        return { ...resultForData(data, path.extname(candidate) === ".svg" ? "image/svg+xml" : "image/png", "local"), file: path.basename(candidate), createdLocally: CREATED_LOCALLY.has(path.basename(candidate)) };
    }
    try {
        const response = await fetch(logo, { redirect: "follow", signal: AbortSignal.timeout(TIMEOUT_MS), headers: { "User-Agent": "KronosAssetAudit/1.0", Accept: "image/*,*/*;q=0.8" } });
        if (!response.ok) return { ok: false, source: "remote", status: response.status, error: `http-${response.status}` };
        const data = Buffer.from(await response.arrayBuffer());
        return { ...resultForData(data, response.headers.get("content-type") || "", "remote"), finalUrlHost: new URL(response.url).hostname };
    } catch (error) {
        return { ok: false, source: "remote", error: error.name || error.message };
    }
}

function reviewReasons(channel, rawAsset, finalAsset) {
    const reasons = [];
    if (!rawAsset.ok) reasons.push(`raw-logo-${rawAsset.error || "unavailable"}`);
    if (!finalAsset.ok) reasons.push(`final-logo-${finalAsset.error || "unavailable"}`);
    if (finalAsset.createdLocally) reasons.push("locally-created-logo");
    // Horizontal broadcaster wordmarks are legitimately shallow. Flag only
    // assets that do not provide enough pixels overall for a 512 px poster.
    if (finalAsset.ok && !finalAsset.vector && (Math.max(finalAsset.width, finalAsset.height) < 256 || (finalAsset.width * finalAsset.height) < 16384)) reasons.push("low-resolution");
    if (finalAsset.ok && finalAsset.ratio && (finalAsset.ratio > 8 || finalAsset.ratio < 0.12)) reasons.push("extreme-aspect-ratio");
    if (/SKY DONNA|THE PET COLLECTIVE|TOP CALCIO|HELBIZ|HIGHLIGHTS|EURO TV|ELIVE|SERIE [ABC] GIRONE/i.test(channel.finalName)) reasons.push("identity-review");
    return reasons;
}

async function mapLimit(items, limit, mapper) {
    const output = new Array(items.length);
    let cursor = 0;
    async function worker() {
        while (cursor < items.length) {
            const index = cursor++;
            output[index] = await mapper(items[index], index);
            if ((index + 1) % 50 === 0) console.log(`[ASSET AUDIT] ${index + 1}/${items.length}`);
        }
    }
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
    return output;
}

(async () => {
    const preload = getStartupPreloadConfigs();
    if (!preload.configs.length) throw new Error("No preloaded configuration found");
    const configKey = preload.configs[0];
    const config = decodeConfig(configKey);
    const lists = getConfiguredLists(config);
    const selected = new Set(getSelectedGroups(config).map(normalizeGroupName));
    const groups = await Promise.all(lists.map(fetchChannelsFromSource));
    const rawChannels = groups.flat().filter(channel => !selected.size || selected.has(normalizeGroupName(channel.group)));
    const channels = rawChannels.map(channel => {
        const excluded = shouldExcludeChannel(configKey, channel);
        const final = applyChannelBranding(configKey, channel);
        return {
            id: channel.id, rawName: channel.name, finalName: final.name, group: channel.group, excluded,
            tvgId: channel.tvgId, rawLogo: channel.logo, finalLogo: final.logo,
            streamFingerprint: crypto.createHash("sha256").update(String(channel.url || "")).digest("hex").slice(0, 16)
        };
    });
    const uniqueLogos = [...new Set(channels.flatMap(channel => [channel.rawLogo, channel.finalLogo]).filter(Boolean))];
    const inspected = await mapLimit(uniqueLogos, CONCURRENCY, inspectLogo);
    const assets = new Map(uniqueLogos.map((logo, index) => [logo, inspected[index]]));
    const hashNames = new Map();
    for (const channel of channels) {
        const asset = assets.get(channel.finalLogo);
        if (!asset?.sha256) continue;
        if (!hashNames.has(asset.sha256)) hashNames.set(asset.sha256, new Set());
        hashNames.get(asset.sha256).add(withoutQualitySuffix(channel.finalName));
    }
    const records = channels.map(channel => {
        const rawAsset = assets.get(channel.rawLogo) || { ok: false, source: "missing", error: "missing" };
        const finalAsset = assets.get(channel.finalLogo) || { ok: false, source: "missing", error: "missing" };
        const duplicateBases = finalAsset.sha256 ? [...(hashNames.get(finalAsset.sha256) || [])].sort() : [];
        const reasons = reviewReasons(channel, rawAsset, finalAsset);
        if (duplicateBases.length >= 4) reasons.push(`shared-by-${duplicateBases.length}-bases`);
        return { ...channel, rawAsset, finalAsset, duplicateBases, reviewReasons: [...new Set(reasons)] };
    });
    const summary = {
        generatedAt: new Date().toISOString(), totalSelected: records.length,
        excluded: records.filter(item => item.excluded).length, visible: records.filter(item => !item.excluded).length,
        missingRawLogo: records.filter(item => !item.rawAsset.ok).length,
        missingFinalLogo: records.filter(item => !item.finalAsset.ok).length,
        locallyCreatedLogo: records.filter(item => item.finalAsset.createdLocally).length,
        lowResolution: records.filter(item => item.reviewReasons.includes("low-resolution")).length,
        duplicateLogoSuspects: records.filter(item => item.reviewReasons.some(reason => reason.startsWith("shared-by-"))).length,
        requiringReview: records.filter(item => item.reviewReasons.length).length,
        uniqueLogoAssets: uniqueLogos.length
    };
    fs.writeFileSync(OUTPUT, JSON.stringify({ summary, records }, null, 2));
    console.log(`[ASSET AUDIT DONE] ${JSON.stringify(summary)} report=${OUTPUT}`);
})().catch(error => { console.error(`[ASSET AUDIT ERROR] ${error.stack || error.message}`); process.exit(1); });
