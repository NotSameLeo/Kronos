const settings = require("./settings");
const state = require("./state");
const {
    attachEPGToChannels,
    ensureEPGRefresh,
    getCachedEPG,
    subscribeConfigToEPG,
    waitForFirstEPG
} = require("./epg");
const {
    decorateChannelName,
    fetchChannelsFromSource,
    getConfiguredLists,
    getDisplayGroupName,
    getEffectiveEpgUrl,
    getSelectedGroups,
    isDividerChannelName
} = require("./sources");
const { cleanGroupName, normalizeGroupName } = require("./utils");
const { decodeConfig, getStartupPreloadConfigs } = require("./config-store");

function buildChannelIndex(channels) {
    const index = new Map();
    channels.forEach(channel => index.set(channel.id, channel));
    return index;
}

async function fetchAndProcessChannels(configKey, config, options = {}) {
    if (state.channelInflight.has(configKey)) return state.channelInflight.get(configKey);

    const promise = (async () => {
        state.updatingCatalog.add(configKey);
        const lists = getConfiguredLists(config);
        const epgUrl = getEffectiveEpgUrl(config, lists);
        const effectiveConfig = epgUrl === config.e ? config : { ...config, e: epgUrl };

        subscribeConfigToEPG(configKey, effectiveConfig);
        const epgPromise = epgUrl
            ? ensureEPGRefresh(epgUrl).catch(err => {
                console.error("[EPG REFRESH]", err.message);
                return null;
            })
            : null;

        const selectedGroups = getSelectedGroups(config);
        const selectedSet = new Set(selectedGroups.map(normalizeGroupName));
        const bucketGroup = selectedGroups[0] || "Kronos";
        const parsedGroups = await Promise.all(lists.map(fetchChannelsFromSource));

        const rawChannels = parsedGroups.flat()
            .filter(channel => !isDividerChannelName(channel.name))
            .filter(channel => {
                if (config.gm === "list" || config.gm === "bucket") return true;
                if (selectedSet.size === 0) return true;
                return selectedSet.has(normalizeGroupName(channel.group));
            })
            .map(channel => ({
                ...channel,
                name: decorateChannelName(channel, lists.length, config.gm),
                group: config.gm === "bucket"
                    ? cleanGroupName(bucketGroup)
                    : getDisplayGroupName(config, channel.group)
            }));

        let epgData = getCachedEPG(epgUrl);
        if (epgUrl && !epgData && epgPromise) epgData = await waitForFirstEPG(epgUrl, epgPromise);

        const { channels, matched } = attachEPGToChannels(rawChannels, epgData);
        state.channels.set(configKey, channels);
        state.channelIndex.set(configKey, buildChannelIndex(channels));
        state.epgMatchStats.set(configKey, {
            matched,
            total: channels.length,
            feedChannels: epgData?.channelCount || 0
        });
        state.lastCatalogUpdate.set(configKey, Date.now());

        if (epgUrl) console.log(`[EPG MATCH] matched=${matched}/${channels.length} feedChannels=${epgData?.channelCount || 0}`);
        console.log(`[CATALOG OK] channels=${channels.length} lists=${lists.length} force=${options.force ? 1 : 0}`);
        return channels;
    })();

    state.channelInflight.set(configKey, promise);
    try {
        return await promise;
    } finally {
        state.channelInflight.delete(configKey);
        state.updatingCatalog.delete(configKey);
    }
}

async function getChannelsFromCache(configKey, config) {
    const cached = state.channels.get(configKey);
    if (!cached) return fetchAndProcessChannels(configKey, config);

    if (!state.channelIndex.has(configKey)) state.channelIndex.set(configKey, buildChannelIndex(cached));
    const lastUpdate = state.lastCatalogUpdate.get(configKey) || 0;
    if (Date.now() - lastUpdate > settings.CATALOG_TTL) {
        fetchAndProcessChannels(configKey, config, { force: true })
            .catch(err => console.error("[CATALOG REFRESH]", err.message));
    }
    return cached;
}

async function getChannelById(configKey, config, id) {
    let channel = state.channelIndex.get(configKey)?.get(id);
    if (channel) return channel;

    const hadCache = state.channels.has(configKey);
    const channels = await getChannelsFromCache(configKey, config);
    channel = channels.find(item => item.id === id);
    if (channel) return channel;
    if (!hadCache) return null;

    await fetchAndProcessChannels(configKey, config, { force: true });
    return state.channelIndex.get(configKey)?.get(id) || null;
}

function findCachedChannelById(id) {
    for (const index of state.channelIndex.values()) {
        const channel = index?.get(id);
        if (channel) return channel;
    }
    for (const channels of state.channels.values()) {
        const channel = Array.isArray(channels) ? channels.find(item => item.id === id) : null;
        if (channel) return channel;
    }
    return null;
}

function startCatalogStartupPreload() {
    const preload = getStartupPreloadConfigs();
    if (!preload.configs.length) return;
    console.log(`[CATALOG PRELOAD] source=${preload.source} configs=${preload.configs.length}`);
    preload.configs.forEach(configKey => {
        try {
            const config = decodeConfig(configKey);
            fetchAndProcessChannels(configKey, config)
                .then(channels => console.log(`[CATALOG PRELOAD OK] channels=${channels.length}`))
                .catch(err => console.error(`[CATALOG PRELOAD ERROR] ${err.message}`));
        } catch (err) {
            console.error(`[CATALOG PRELOAD ERROR] invalid config: ${err.message}`);
        }
    });
}

function startCatalogPeriodicRefresh() {
    if (settings.CATALOG_REFRESH_INTERVAL_MS <= 0) return;
    console.log(`[CATALOG REFRESH SCHEDULED] interval=${settings.CATALOG_REFRESH_INTERVAL_MS / 1000}s`);
    const timer = setInterval(() => {
        const preload = getStartupPreloadConfigs();
        preload.configs.forEach(configKey => {
            try {
                const config = decodeConfig(configKey);
                fetchAndProcessChannels(configKey, config, { force: true })
                    .then(channels => console.log(`[CATALOG REFRESH OK] channels=${channels.length}`))
                    .catch(err => console.error(`[CATALOG REFRESH] ${err.message}`));
            } catch (err) {
                console.error(`[CATALOG REFRESH] invalid config: ${err.message}`);
            }
        });
    }, settings.CATALOG_REFRESH_INTERVAL_MS);
    timer.unref?.();
}

module.exports = {
    buildChannelIndex,
    fetchAndProcessChannels,
    findCachedChannelById,
    getChannelById,
    getChannelsFromCache,
    startCatalogPeriodicRefresh,
    startCatalogStartupPreload
};
