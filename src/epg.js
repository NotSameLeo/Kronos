const axios = require("axios");
const sax = require("sax");
const zlib = require("zlib");
const { Readable } = require("stream");
const settings = require("./settings");
const state = require("./state");
const { sleep, upstreamAgentOptions } = require("./utils");

function getCachedEPG(epgUrl) {
    return epgUrl ? state.epgData.get(epgUrl) || null : null;
}

function subscribeConfigToEPG(configKey, config) {
    const previous = state.configByKey.get(configKey)?.e;
    if (previous && previous !== config.e) state.epgSubscribers.get(previous)?.delete(configKey);

    state.configByKey.set(configKey, config);
    if (!config.e) return;

    const subscribers = state.epgSubscribers.get(config.e) || new Set();
    subscribers.add(configKey);
    state.epgSubscribers.set(config.e, subscribers);
    startEPGBackgroundRefresh(config.e, "config");
}

function clearEPGRetry(epgUrl) {
    const timer = state.epgRetryTimers.get(epgUrl);
    if (timer) clearTimeout(timer);
    state.epgRetryTimers.delete(epgUrl);
}

function scheduleEPGRetry(epgUrl) {
    if (!epgUrl || state.epgRetryTimers.has(epgUrl) || settings.EPG_RETRY_DELAY_MS <= 0) return;
    const retryAt = Date.now() + settings.EPG_RETRY_DELAY_MS;
    state.epgStatus.set(epgUrl, { ...(state.epgStatus.get(epgUrl) || {}), retryAt });
    const timer = setTimeout(() => {
        state.epgRetryTimers.delete(epgUrl);
        ensureEPGRefresh(epgUrl, { force: true }).catch(() => {});
    }, settings.EPG_RETRY_DELAY_MS);
    timer.unref?.();
    state.epgRetryTimers.set(epgUrl, timer);
}

function scheduleEPGPeriodicRefresh(epgUrl) {
    if (!epgUrl || settings.EPG_REFRESH_INTERVAL_MS <= 0 || state.epgRefreshTimers.has(epgUrl)) return;
    const timer = setTimeout(() => {
        state.epgRefreshTimers.delete(epgUrl);
        ensureEPGRefresh(epgUrl, { force: true }).catch(err => console.error("[EPG REFRESH]", err.message));
    }, settings.EPG_REFRESH_INTERVAL_MS);
    timer.unref?.();
    state.epgRefreshTimers.set(epgUrl, timer);
}

function startEPGBackgroundRefresh(epgUrl, reason = "background") {
    if (!epgUrl) return;
    const retryAt = state.epgStatus.get(epgUrl)?.retryAt || 0;
    if (!getCachedEPG(epgUrl) && !state.epgInflight.has(epgUrl) && Date.now() >= retryAt) {
        console.log(`[EPG BACKGROUND] reason=${reason}`);
        ensureEPGRefresh(epgUrl).catch(err => console.error("[EPG BACKGROUND]", err.message));
    }
    scheduleEPGPeriodicRefresh(epgUrl);
}

function startEPGStartupPreload() {
    if (!settings.EPG_PRELOAD_URLS.length) return;
    console.log(`[EPG PRELOAD] urls=${settings.EPG_PRELOAD_URLS.length}`);
    settings.EPG_PRELOAD_URLS.forEach(epgUrl => startEPGBackgroundRefresh(epgUrl, "startup"));

    if (settings.EPG_STARTUP_WATCH_MS <= 0) return;
    const startedAt = Date.now();
    const timer = setInterval(() => {
        const pending = settings.EPG_PRELOAD_URLS.filter(epgUrl => !getCachedEPG(epgUrl));
        if (!pending.length || Date.now() - startedAt >= settings.EPG_STARTUP_WATCH_MS) {
            clearInterval(timer);
            return;
        }
        pending.forEach(epgUrl => startEPGBackgroundRefresh(epgUrl, "startup-watch"));
    }, 3000);
    timer.unref?.();
}

function buildChannelIndex(channels) {
    const index = new Map();
    channels.forEach(channel => index.set(channel.id, channel));
    return index;
}

function refreshSubscribedChannels(epgUrl, epgData) {
    const subscribers = state.epgSubscribers.get(epgUrl) || new Set();
    subscribers.forEach(configKey => {
        if (state.configByKey.get(configKey)?.e !== epgUrl) return;
        const current = state.channels.get(configKey);
        if (!current) return;

        const { channels, matched } = attachEPGToChannels(current, epgData);
        state.channels.set(configKey, channels);
        state.channelIndex.set(configKey, buildChannelIndex(channels));
        state.epgMatchStats.set(configKey, {
            matched,
            total: channels.length,
            feedChannels: epgData.channelCount
        });
        console.log(`[EPG APPLY] matched=${matched}/${channels.length} feedChannels=${epgData.channelCount}`);
    });
}

async function ensureEPGRefresh(epgUrl, options = {}) {
    if (!epgUrl) return null;

    const cached = state.epgData.get(epgUrl);
    const lastUpdate = state.epgLastUpdate.get(epgUrl) || 0;
    if (cached && !options.force && Date.now() - lastUpdate < settings.EPG_CACHE_TTL) return cached;
    if (state.epgInflight.has(epgUrl)) return state.epgInflight.get(epgUrl);

    const retryAt = state.epgStatus.get(epgUrl)?.retryAt || 0;
    if (!options.force && Date.now() < retryAt) return cached || null;

    const promise = (async () => {
        try {
            const startedAt = Date.now();
            state.epgStatus.set(epgUrl, { state: "fetching", startedAt, retryAt: 0 });
            console.log("[EPG FETCH]", epgUrl);

            const response = await axios.get(epgUrl, {
                timeout: settings.EPG_REQUEST_TIMEOUT,
                signal: AbortSignal.timeout(settings.EPG_REQUEST_TIMEOUT),
                maxContentLength: settings.EPG_MAX_BYTES,
                responseType: "arraybuffer",
                ...upstreamAgentOptions(),
                headers: {
                    "User-Agent": `Kronos/${settings.RELEASE_VERSION}`,
                    "Accept": "application/xml, text/xml, application/gzip, */*"
                }
            });

            const downloadMs = Date.now() - startedAt;
            const parsed = await parseXMLTVBuffer(Buffer.from(response.data));
            const data = indexEPG(parsed);

            state.epgData.set(epgUrl, data);
            state.epgLastUpdate.set(epgUrl, Date.now());
            state.epgStatus.set(epgUrl, { state: "ready", updatedAt: Date.now(), retryAt: 0 });
            clearEPGRetry(epgUrl);
            scheduleEPGPeriodicRefresh(epgUrl);
            refreshSubscribedChannels(epgUrl, data);

            console.log(`[EPG OK] channels=${data.channelCount} programmes=${data.programmeCount} keys=${data.byKey.size} ms=${Date.now() - startedAt} downloadMs=${downloadMs}`);
            return data;
        } catch (err) {
            console.warn("[EPG ERROR]", err.message);
            state.epgStatus.set(epgUrl, { state: "error", error: err.message, failedAt: Date.now(), retryAt: 0 });
            scheduleEPGRetry(epgUrl);
            return cached || null;
        }
    })();

    state.epgInflight.set(epgUrl, promise);
    try {
        return await promise;
    } finally {
        state.epgInflight.delete(epgUrl);
    }
}

function indexEPG({ channelDefs, programmesById, programmeCount }) {
    const byKey = new Map();
    const xmlIds = new Set([...channelDefs.keys(), ...programmesById.keys()]);
    xmlIds.forEach(id => {
        const programmes = programmesById.get(id) || [];
        if (!programmes.length) return;
        const entry = { id, names: channelDefs.get(id) || [], programmes };
        getEpgMatchKeys([id, ...entry.names]).forEach(key => {
            const list = byKey.get(key) || [];
            list.push(entry);
            byKey.set(key, list);
        });
    });
    return { byKey, channelCount: xmlIds.size, programmeCount };
}

function parseXMLTVBuffer(buffer) {
    const isGzip = buffer[0] === 0x1f && buffer[1] === 0x8b;
    const input = isGzip ? Readable.from([buffer]).pipe(zlib.createGunzip()) : Readable.from([buffer]);
    const channelDefs = new Map();
    const programmesById = new Map();
    const keepAfter = Date.now() - 5 * 60 * 1000;

    return new Promise((resolve, reject) => {
        const parser = sax.createStream(true, { trim: false, normalize: false });
        let channel = null;
        let programme = null;
        let capture = null;
        let text = "";
        let programmeCount = 0;

        const readAttr = (node, name) => {
            const value = node.attributes?.[name];
            return typeof value === "string" ? value : value?.value || "";
        };
        const appendText = value => {
            if (capture) text += value;
        };

        parser.on("opentag", node => {
            if (node.name === "channel") {
                channel = { id: readAttr(node, "id"), names: [] };
            } else if (node.name === "display-name" && channel) {
                capture = "display-name";
                text = "";
            } else if (node.name === "programme") {
                const stop = parseXMLTVDate(readAttr(node, "stop"));
                const keep = !Number.isNaN(stop.getTime()) && stop.getTime() >= keepAfter;
                programme = {
                    channel: readAttr(node, "channel"),
                    start: keep ? parseXMLTVDate(readAttr(node, "start")) : new Date(NaN),
                    stop,
                    keep,
                    title: "",
                    desc: ""
                };
                programmeCount++;
            } else if ((node.name === "title" || node.name === "desc") && programme?.keep) {
                capture = node.name;
                text = "";
            }
        });

        parser.on("text", appendText);
        parser.on("cdata", appendText);
        parser.on("closetag", name => {
            if (name === capture) {
                const value = text.trim();
                if (name === "display-name" && channel && value) channel.names.push(value);
                if (name === "title" && programme) programme.title = value;
                if (name === "desc" && programme) programme.desc = value;
                capture = null;
                text = "";
            }
            if (name === "channel" && channel) {
                if (channel.id) channelDefs.set(channel.id, channel.names);
                channel = null;
            }
            if (name === "programme" && programme) {
                if (programme.keep && programme.channel && !Number.isNaN(programme.start.getTime())) {
                    const list = programmesById.get(programme.channel) || [];
                    list.push({
                        start: programme.start,
                        stop: programme.stop,
                        title: programme.title || "Programma senza titolo",
                        desc: programme.desc
                    });
                    programmesById.set(programme.channel, list);
                }
                programme = null;
            }
        });
        parser.on("error", reject);
        input.on("error", reject);
        parser.on("end", () => resolve({ channelDefs, programmesById, programmeCount }));
        input.pipe(parser);
    });
}

function parseXMLTVDate(value) {
    const match = String(value || "").match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-])(\d{2})(\d{2}))?/);
    if (!match) return new Date(value);
    const [, year, month, day, hour, minute, second, sign, offHour, offMinute] = match;
    if (sign) {
        const offsetMs = ((Number(offHour) * 60) + Number(offMinute)) * 60 * 1000 * (sign === "+" ? 1 : -1);
        return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second)) - offsetMs);
    }
    return zonedWallClockToDate(
        Number(year),
        Number(month),
        Number(day),
        Number(hour),
        Number(minute),
        Number(second),
        settings.EPG_TIME_ZONE
    );
}

function zonedWallClockToDate(year, month, day, hour, minute, second, timeZone) {
    const wallClockUtc = Date.UTC(year, month - 1, day, hour, minute, second);
    let instant = wallClockUtc;
    for (let pass = 0; pass < 2; pass++) instant = wallClockUtc - getTimeZoneOffsetMs(new Date(instant), timeZone);
    return new Date(instant);
}

const timeZoneFormatters = new Map();

function getTimeZoneOffsetFormatter(timeZone) {
    let formatter = timeZoneFormatters.get(timeZone);
    if (!formatter) {
        formatter = new Intl.DateTimeFormat("en-CA", {
            timeZone,
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hourCycle: "h23"
        });
        timeZoneFormatters.set(timeZone, formatter);
    }
    return formatter;
}

function getTimeZoneOffsetMs(date, timeZone) {
    const parts = Object.fromEntries(getTimeZoneOffsetFormatter(timeZone)
        .formatToParts(date)
        .filter(part => part.type !== "literal")
        .map(part => [part.type, Number(part.value)]));
    return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second) - date.getTime();
}

function formatTime(date) {
    return new Intl.DateTimeFormat("it-IT", {
        timeZone: settings.EPG_TIME_ZONE,
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23"
    }).format(date);
}

function selectProgrammeWindow(programmes, at = new Date()) {
    const now = at instanceof Date ? at : new Date(at);
    const sorted = programmes.slice().sort((a, b) => a.start - b.start);
    const currentIndex = sorted.findIndex(programme => now >= programme.start && now <= programme.stop);
    const current = currentIndex >= 0 ? sorted[currentIndex] : null;
    const next = current ? sorted[currentIndex + 1] : sorted.find(programme => programme.start > now);
    return { current, next };
}

function formatEpgDescription(programmes, at = new Date()) {
    const { current, next } = selectProgrammeWindow(programmes, at);
    const lines = [];
    if (current) lines.push(formatProgramme("In Onda", current));
    if (next) lines.push(formatProgramme("A Seguire", next));
    return lines.join(" | ");
}

function formatProgramme(label, programme) {
    const description = stripRepeatedTitlePrefix(programme.desc || "", programme.title)
        .slice(0, 500)
        .trim()
        .replace(/\.+$/, "");
    return `${label}: ${String(programme.title || "").toUpperCase()} (${formatTime(programme.start)} - ${formatTime(programme.stop)}) | Trama: ${description}`;
}

function stripRepeatedTitlePrefix(description, title) {
    const text = String(description || "").trim();
    const candidates = getEpgTitlePrefixCandidates(title);
    if (!text || !candidates.length) return text;

    for (const candidate of candidates) {
        const cleaned = stripComparablePrefix(text, candidate);
        if (cleaned) return cleaned;
    }
    return text;
}

function getEpgTitlePrefixCandidates(title) {
    const raw = String(title || "").trim();
    if (!raw) return [];
    const values = [raw];
    for (const part of raw.split(/\s[-:|]\s/u)) {
        const text = part.trim();
        if (epgComparableKey(text).length >= 8) values.push(text);
    }
    return [...new Map(values
        .map(value => [epgComparableKey(value), value])
        .filter(([key]) => key.length >= 8)).values()]
        .sort((a, b) => epgComparableKey(b).length - epgComparableKey(a).length);
}

function stripComparablePrefix(text, candidate) {
    const target = epgComparableKey(candidate);
    let seen = "";
    for (let index = 0; index < text.length; index++) {
        const part = epgComparableKey(text[index]);
        if (!part) continue;
        seen += part;
        if (seen.length < target.length) {
            if (!target.startsWith(seen)) break;
            continue;
        }
        if (seen === target) {
            const cleaned = text.slice(index + 1).replace(/^[\s:;.,\-|]+/u, "").trim();
            if (!cleaned) return null;
            if (/^\p{Ll}/u.test(cleaned)) return null;
            return cleaned;
        }
        break;
    }
    return null;
}

function epgComparableKey(value) {
    return String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "");
}

function getEpgMatchKeys(values) {
    const keys = new Set();
    const add = value => {
        let text = String(value || "").trim();
        if (!text) return;
        text = text.replace(/\.it$/i, "").replace(/^\s*IT\s*:\s*/i, "");
        addNormalizedEpgKey(keys, text);
        addNormalizedEpgKey(keys, text.replace(/\b(?:FHD|FULL\s*HD|HD|HEVC|UHD|4K|SD|H\.?26[45])\b/gi, " "));
        addNormalizedEpgKey(keys, text.replace(/\b(?:FHD|FULL\s*HD|HD|HEVC|UHD|4K|SD)\s+\d{3,4}\b/gi, " "));
        addNormalizedEpgKey(keys, text.replace(/\s+\d{3,4}\s*$/, " "));
    };
    (Array.isArray(values) ? values : [values]).forEach(add);
    return [...keys];
}

function addNormalizedEpgKey(keys, value) {
    const key = epgComparableKey(value);
    if (!key) return;
    keys.add(key);
    const aliases = {
        "20mediaset": "20",
        canale20: "20",
        discoverymotortrend: "motortrend",
        nationalgeografic: "nationalgeographic",
        raisportplus: "raisport"
    };
    if (aliases[key]) keys.add(aliases[key]);
    if (key.startsWith("discovery") && key.length > "discovery".length) keys.add(key.slice("discovery".length));
}

function findEpgMatch(epgData, channel) {
    if (!epgData?.byKey) return null;
    for (const key of getEpgMatchKeys([channel.tvgId, channel.name])) {
        const entries = epgData.byKey.get(key);
        if (!entries?.length) continue;
        const entry = entries.find(item => formatEpgDescription(item.programmes));
        if (entry) return {
            ...entry,
            description: formatEpgDescription(entry.programmes),
            programmes: entry.programmes
        };
    }
    return null;
}

function withCurrentEPG(channel, epgData = null, at = new Date()) {
    if (!channel) return null;
    const epg = channel.epgProgrammes?.length
        ? { id: channel.epgId || null, programmes: channel.epgProgrammes }
        : findEpgMatch(epgData, channel);
    if (!epg?.programmes?.length) return { ...channel, description: "" };
    return {
        ...channel,
        description: formatEpgDescription(epg.programmes, at),
        epgId: epg.id || channel.epgId || null,
        epgProgrammes: epg.programmes
    };
}

function withCurrentEPGForChannels(channels, epgData = null, at = new Date()) {
    return (Array.isArray(channels) ? channels : []).map(channel => withCurrentEPG(channel, epgData, at));
}

function attachEPGToChannels(channels, epgData) {
    let matched = 0;
    const updated = channels.map(channel => {
        const epg = findEpgMatch(epgData, channel);
        if (epg) matched++;
        return {
            ...channel,
            description: epg?.description || "",
            epgId: epg?.id || null,
            epgProgrammes: epg?.programmes || null
        };
    });
    return { channels: updated, matched };
}

async function waitForFirstEPG(epgUrl, epgPromise) {
    if (!epgUrl || !epgPromise || settings.EPG_FIRST_CATALOG_WAIT_MS <= 0) return getCachedEPG(epgUrl);
    const data = await Promise.race([
        epgPromise,
        sleep(settings.EPG_FIRST_CATALOG_WAIT_MS).then(() => null)
    ]);
    return data || getCachedEPG(epgUrl);
}

module.exports = {
    attachEPGToChannels,
    ensureEPGRefresh,
    formatEpgDescription,
    getCachedEPG,
    startEPGStartupPreload,
    subscribeConfigToEPG,
    waitForFirstEPG,
    withCurrentEPG,
    withCurrentEPGForChannels
};
