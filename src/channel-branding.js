const fs = require("fs");
const path = require("path");
const settings = require("./settings");
const { hashKey } = require("./utils");

const LOGO_ROOT = path.join(__dirname, "..", "public", "channel-logos", "italy");
const LOGO_PUBLIC_ROOT = "/channel-logos/italy";
const QUALITY_SUFFIX = /\s+(?:8K|4K|UHD|FHD|FULL\s*HD|HD|HEVC|SD|RAW|1080P|720P|576P|480P|HDR|HLG)$/i;

const FALSE_SKY_PREFIX = new RegExp(
    "^SKY\\s+(?=" + [
        "ACI SPORT(?: TV)?",
        "BOOMERANG",
        "CARTOON NETWORK",
        "CIELO",
        "COMEDY CENTRAL",
        "CRIME \\+ INVESTIGATION",
        "DEAKIDS",
        "DEA ?JUNIOR",
        "DISCOVERY(?: CHANNEL| TURBO)?",
        "DMAX",
        "EUROSPORT",
        "FOOD NETWORK",
        "GAMBERO ROSSO",
        "HISTORY(?: CHANNEL)?",
        "HORSE TV",
        "INTER(?: TV)?",
        "LA F",
        "LAZIO(?: STYLE CHANNEL)?",
        "MILAN(?: TV)?",
        "MTV",
        "NICK(?:ELODEON| JR|JR)",
        "REAL ?TIME",
        "BABY",
        "CACCIA",
        "DAZN",
        "LAF",
        "PESCA",
        "SUPER ?TENNIS"
    ].join("|") + ")",
    "i"
);

const NAME_REPLACEMENTS = [
    [/^NO EVENT STREAMING NOW.*?DAZN PPV\s+(\d+).*$/i, "DAZN EXCLUSIVE $1 8K"],
    [/^NEXT\s*\|.*?DAZN PPV\s+(\d+).*$/i, "DAZN EXCLUSIVE $1 8K"],
    [/^PRIM(?:E)?\s*1\s+(4K|HD)\s*\[LIVE-EVENT\]$/i, "PRIME VIDEO EVENTO 1 $1"],
    [/^PRIME:\s*INTER TV(?=\s|$)/i, "INTER TV"],
    [/^PRIME:\s*MILAN TV(?=\s|$)/i, "MILAN TV"],
    [/^PRIME:\s*MEZZO(?=\s|$)/i, "MEZZO"],
    [/^SKY DONNA TV SPORT(?=\s|$)/i, "SMART VISION TV"],
    [/^TOP CALCIO 24(?=\s|$)/i, "TELELOMBARDIA"],
    [/^SKY RETE 7(?=\s|$)/i, "CANALE 7"],
    [/^SKY EXPLORER TV(?=\s|$)/i, "EXPLORER HD CHANNEL"],
    [/^ELIVE TV BRESCIA(?=\s|$)/i, "ÈLIVE BRESCIA"],
    [/^SKY SPORT MOTO GP(?=\s|$)/i, "SKY SPORT MOTOGP"],
    [/^MEDIASET 1(?=\s|$)/i, "MEDIASET ITALIA"],
    [/^MEDIASET 2(?=\s|$)/i, "MEDIASET ITALIA 2"],
    [/^F1 CAR RUSSEL(?=\s|$)/i, "F1 CAR RUSSELL"],
    [/^TV\s*2000(?=\s|$)/i, "TV2000"],
    [/^NASA\s*TV(?=\s|$)/i, "NASA TV"],
    [/^SPORT\s+ITALIA(?=\s|$)/i, "SPORTITALIA"],
    [/^SAN MARINO RTV(?=\s|$)/i, "RTV SAN MARINO"],
    [/^HSE\s*24(?=\s|$)/i, "GM24"],
    [/^CLASS HORSE TV(?=\s|$)/i, "HORSE TV"],
    [/^CHAMPIONS LEAGUE INFINITY\s+(\d+)(?=\s|$)/i, "MEDIASET INFINITY CHAMPIONS $1"],
    [/^ALICE(?=\s|$)/i, "ALMA TV"],
    [/^EURONEWS(?:\s+(?:ITA|ITALIAN))?(?=\s|$)/i, "EURONEWS ITALIANO"],
    [/^MOTOGP ON BBOARD(?=\s|$)/i, "MOTOGP ON BOARD"],
    [/^20(?:TH)?\s*CENTURY\s*FOX(?=\s|$)/i, "20TH CENTURY STUDIOS"],
    [/^(.+?)\s+(8K|4K|UHD|FHD|HD|HEVC|SD|RAW)\s+24H$/i, "$1 24/7 $2"],
    [/^(ADRIANO CELENTANO|ALBERTO SORDI|ALDO GIOVANNI E GIACOMO|ALESSANDRO SIANI)\s+CHANNEL(?=\s|$)/i, "$1 24/7"],
    [/^AMERICAN DAD\s+24H(?=\s|$)/i, "AMERICAN DAD 24/7"],
    [/^SKY CINEMA DUE\s*24(?=\s|$)/i, "SKY CINEMA STORIES"],
    [/^SKY CINEMA DUE(?=\s|$)/i, "SKY CINEMA STORIES"],
    [/^SKY CINEMA UNO24(?=\s|$)/i, "SKY CINEMA UNO 24"],
    [/^SKY CRIME INVEST(?:IGATION)?(?=\s|$)/i, "SKY CRIME"],
    [/^CRIME INVESTIGATION(?=\s|$)/i, "CRIME + INVESTIGATION"],
    [/^DISCOVERY CHANNEL(?=\s|$)/i, "DISCOVERY"],
    [/^MOTOR\s*TREND(?=\s|$)/i, "DISCOVERY TURBO"],
    [/^HOME GARDEN TV(?:\s+HGTV)?(?=\s|$)/i, "HGTV"],
    [/^REALTIME(?=\s|$)/i, "REAL TIME"],
    [/^SUPER\s*TENNIS(?=\s|$)/i, "SUPER TENNIS"],
    [/^NICKJR(?=\s|$)/i, "NICK JR"],
    [/^EUROSPORT1(?=\s|$)/i, "EUROSPORT 1"],
    [/^EUROSPORT2(?=\s|$)/i, "EUROSPORT 2"],
    [/^DEA\s*KIDS(?=\s|$)/i, "DEAKIDS"],
    [/^DEA\s*JUNIOR(?=\s|$)/i, "DEAJUNIOR"],
    [/^TV\s*8(?=\s|$)/i, "TV8"],
    [/^LA\s*5(?=\s|$)/i, "LA5"],
    [/^LA\s*7\s*D(?=\s|$)/i, "LA7D"],
    [/^LA\s*7(?=\s|$)/i, "LA7"],
    [/^CINE\s*34(?=\s|$)/i, "CINE34"],
    [/^TOPCRIME(?=\s|$)/i, "TOP CRIME"],
    [/^TG\s*COM\s*24(?=\s|$)/i, "TGCOM24"],
    [/^SKY TG\s*24(?=\s|$)/i, "SKY TG24"],
    [/^RAI NEWS\s*24(?=\s|$)/i, "RAI NEWS 24"],
    [/^SKY CALCIO(?=\s|$)/i, "SKY SPORT CALCIO"],
    [/^SKY FOOTBALL(?=\s|$)/i, "SKY SPORT FOOTBALL"],
    [/^SKY SPORT SERIE A(?=\s|$)/i, "SKY SPORT CALCIO"],
    [/^SKY SPORT ACTION(?=\s|$)/i, "SKY SPORT ARENA"],
    [/^SKY SPORT NBA(?=\s|$)/i, "SKY SPORT BASKET"],
    [/^SKY SPORTPLUS(?=\s|$)/i, "SKY SPORT PLUS"],
    [/^SKY DAZN 1(?=\s|$)/i, "ZONA DAZN"],
    [/^SKY DAZN BAR(?=\s|$)/i, "DAZN BAR"],
    [/^SKY BABY(?=\s|$)/i, "BABY TV"],
    [/^DAZN 1(?=\s|$)/i, "ZONA DAZN"],
    [/^BABY(?=\s+(?:4K|UHD|HD|HEVC|SD|RAW)$|$)/i, "BABY TV"],
    [/SKY PRIMAFILA4K(?=\s|$)/i, "SKY PRIMAFILA"],
    [/^PESCA E CACCIA(?=\s|$)/i, "CACCIA E PESCA"],
    [/^CLASSICA(?=\s|$)/i, "SKY CLASSICA"],
    [/^MEDIA EXTRA(?=\s|$)/i, "MEDIASET EXTRA"],
    [/^CANALE 20(?=\s|$)/i, "20"],
    [/^MEDIASET TWENTYSEVEN(?=\s|$)/i, "27"],
    [/^PADREPIO(?=\s|$)/i, "PADRE PIO TV"],
    [/^BOEING(?=\s|$)/i, "BOING"],
    [/^MEDIASET ITALIA DUE(?=\s|$)/i, "MEDIASET ITALIA 2"],
    [/^INTER(?=\s+(?:4K|UHD|HD|HEVC|SD|RAW)$|$)/i, "INTER TV"],
    [/^MILAN(?=\s+(?:4K|UHD|HD|HEVC|SD|RAW)$|$)/i, "MILAN TV"],
    [/^LAZIO(?=\s+(?:4K|UHD|HD|HEVC|SD|RAW)$|$)/i, "LAZIO STYLE CHANNEL"]
];

const LOGO_RULES = [
    [/^ALMA TV(?:\s|$)/, "alma-tv-it.png"],
    [/^20TH CENTURY STUDIOS(?:\s|$)/, "20th-century-studios-it.png"],
    [/^MEDIASET INFINITY CHAMPIONS(?:\s|$)/, "mediaset-infinity-it.png"],
    [/^PRIME VIDEO EVENTO(?:\s|$)/, "prime-video-it.png"],
    [/^MEZZO LIVE(?:\s|$)/, "mezzo-live-it.png"],
    [/^MEZZO(?:\s|$)/, "mezzo-it.png"],
    [/^MOTOGP(?:\s|$)/, "motogp-it.png"],
    [/^F1 (?:TV|CAR|DATA)(?:\s|$)/, "formula-1-it.png"],
    [/^RSI LA 1(?:\s|$)/, "rsi-la-1-it.png"],
    [/^RSI LA 2(?:\s|$)/, "rsi-la-2-it.png"],
    [/^TV2000(?:\s|$)/, "tv2000-it.png"],
    [/^RAKUTEN TV(?:\s|$)/, "rakuten-tv-it.png"],
    [/^NASA TV(?:\s|$)/, "nasa-tv-it.png"],
    [/^FIFA\+(?:\s|$)/, "fifa-plus-it.png"],
    [/^FASHION TV(?:\s|$)/, "fashion-tv-it.png"],
    [/^QVC(?: ITALIA)?(?:\s|$)/, "qvc-it.png"],
    [/^SPORTITALIA(?:\s|$)/, "sportitalia-it.png"],
    [/^EXPLORER HD CHANNEL(?:\s|$)/, "explorer-it.png"],
    [/^RTL 102\.5(?:\s|$)/, "rtl-1025-it.png"],
    [/^VIDEOLINA(?:\s|$)/, "videolina-it.png"],
    [/^RTV SAN MARINO(?:\s|$)/, "rtv-san-marino-it.png"],
    [/^EURONEWS ITALIANO(?:\s|$)/, "euronews-it.png"],
    [/^ADRIANO CELENTANO(?:\s|$)/, "adriano-celentano-it.png"],
    [/^ALBERTO SORDI(?:\s|$)/, "alberto-sordi-it.png"],
    [/^ALDO GIOVANNI E GIACOMO(?:\s|$)/, "aldo-giovanni-giacomo-it.png"],
    [/^ALESSANDRO SIANI(?:\s|$)/, "alessandro-siani-it.png"],
    [/^AMERICAN DAD(?:\s|$)/, "american-dad-it.png"],
    [/^SMART VISION TV(?:\s|$)/, "smartvision-tv-it.png"],
    [/^TELELOMBARDIA(?:\s|$)/, "telelombardia-it.png"],
    [/^THE PET COLLECTIVE(?:\s|$)/, "the-pet-collective-it.png"],
    [/^FAILARMY(?:\s|$)/, "failarmy-it.png"],
    [/^PEOPLE ARE AWESOME(?:\s|$)/, "people-are-awesome-it.png"],
    [/^ÈLIVE BRESCIA(?:\s|$)/, "elive-brescia-it.png"],
    [/^EURO TV(?:\s|$)/, "euro-tv-it.png"],
    [/^CANALE 7(?:\s|$)/, "canale-7-it.png"],
    [/^MEDIASET ITALIA 2(?:\s|$)/, "italia2-it.png"],
    [/^MEDIASET ITALIA(?:\s|$)/, "mediaset-italia-it.png"],
    [/^RAI 1(?:\s|$)/, "rai-1-it.png"],
    [/^RAI 2(?:\s|$)/, "rai-2-it.png"],
    [/^RAI 3(?:\s|$)/, "rai-3-it.png"],
    [/^RAI 4(?:\s|$)/, "rai-4-it.png"],
    [/^RAI 5(?:\s|$)/, "rai-5-it.png"],
    [/^RAI NEWS 24(?:\s|$)/, "rai-news-24-it.png"],
    [/^RAI SPORT(?:\s|$)/, "rai-sport-it.png"],
    [/^RAI MOVIE(?:\s|$)/, "rai-movie-it.png"],
    [/^RAI PREMIUM(?:\s|$)/, "rai-premium-it.png"],
    [/^RAI STORIA(?:\s|$)/, "rai-storia-it.png"],
    [/^RAI SCUOLA(?:\s|$)/, "rai-scuola-it.png"],
    [/^RAI GULP(?:\s|$)/, "rai-gulp-it.png"],
    [/^RAI YOYO(?:\s|$)/, "rai-yoyo-it.png"],
    [/^RETE ?4(?:\s|$)/, "rete4-it.png"],
    [/^CANALE ?5(?:\s|$)/, "canale5-it.png"],
    [/^ITALIA ?1(?:\s|$)/, "italia1-it.png"],
    [/^(?:ITALIA|MEDIASET ITALIA) ?2(?:\s|$)/, "italia2-it.png"],
    [/^(?:MEDIASET )?20(?:\s|$)/, "20-it.png"],
    [/^(?:TWENTY SEVEN|27)(?:\s|$)/, "twenty-seven-it.png"],
    [/^LA5(?:\s|$)/, "la5-it.png"],
    [/^CINE34(?:\s|$)/, "cine34-it.png"],
    [/^TOP CRIME(?:\s|$)/, "top-crime-it.png"],
    [/^IRIS(?:\s|$)/, "iris-it.png"],
    [/^MEDIASET EXTRA(?:\s|$)/, "mediaset-extra-it.png"],
    [/^TGCOM24(?:\s|$)/, "tgcom24-it.png"],
    [/^TV8(?:\s|$)/, "tv8-it.png"],
    [/^CIELO(?:\s|$)/, "cielo-it.png"],
    [/^LA7D(?:\s|$)/, "la7d-it.png"],
    [/^LA7(?:\s|$)/, "la7-it.png"],
    [/^NOVE(?:\s|$)/, "nove-it.png"],
    [/^DMAX(?:\s|$)/, "dmax-it.svg"],
    [/^FOCUS(?:\s|$)/, "focus-it.png"],
    [/^GIALLO(?:\s|$)/, "giallo-it.png"],
    [/^REAL TIME(?:\s|$)/, "real-time-it.png"],
    [/^FOOD NETWORK(?:\s|$)/, "food-network-it.svg"],
    [/^HGTV(?:\s|$)/, "hgtv-it.svg"],
    [/^DISCOVERY TURBO(?:\s|$)/, "discovery-turbo-it.png"],
    [/^DISCOVERY(?:\s|$)/, "discovery-it.svg"],
    [/^CRIME \+ INVESTIGATION(?:\s|$)/, "crime-and-investigation-it.png"],
    [/^HISTORY(?:\s|$)/, "history-channel-it.png"],
    [/^K2(?:\s|$)/, "k2-it.png"],
    [/^FRISBEE(?:\s|$)/, "frisbee-it.png"],
    [/^BOING(?:\s|$)/, "boing-it.png"],
    [/^CARTOONITO(?:\s|$)/, "cartoonito-it.png"],
    [/^CARTOON NETWORK(?:\s|$)/, "cartoon-network-it.png"],
    [/^BOOMERANG(?:\s|$)/, "boomerang-it.png"],
    [/^NICKELODEON(?:\s|$)/, "nickelodeon-it.png"],
    [/^NICK JR(?:\s|$)/, "nick-jr-it.png"],
    [/^DEAKIDS(?:\s|$)/, "dea-kids-it.png"],
    [/^DEAJUNIOR(?:\s|$)/, "dea-junior-it.png"],
    [/^SKY CINEMA STORIES(?:\s|$)/, "sky-cinema-stories-it.png"],
    [/^SKY CINEMA UNO 24(?:\s|$)/, "sky-cinema-uno-plus24-it.png"],
    [/^SKY CINEMA UNO24(?:\s|$)/, "sky-cinema-uno-plus24-it.png"],
    [/^SKY CINEMA UNO(?:\s|$)/, "sky-cinema-uno-it.png"],
    [/^SKY CINEMA ACTION(?:\s|$)/, "sky-cinema-action-it.png"],
    [/^SKY CINEMA COLLECTION(?:\s|$)/, "sky-cinema-collection-it.png"],
    [/^SKY CINEMA COMEDY(?:\s|$)/, "sky-cinema-comedy-it.png"],
    [/^SKY CINEMA DRAMA(?:\s|$)/, "sky-cinema-drama-it.png"],
    [/^SKY CINEMA FAMILY(?:\s|$)/, "sky-cinema-family-it.png"],
    [/^SKY CINEMA ROMANCE(?:\s|$)/, "sky-cinema-romance-it.png"],
    [/^SKY CINEMA SUSPENSE(?:\s|$)/, "sky-cinema-suspense-it.png"],
    [/^SKY CINEMA(?:\s|$)/, "sky-cinema-it.png"],
    [/^(?:VETRINA )?SKY PRIMAFILA(?:\s|$)/, "sky-primafila-it.png"],
    [/^SKY ATLANTIC(?:\s|$)/, "sky-atlantic-it.png"],
    [/^SKY SERIE(?:\s|$)/, "sky-serie-it.png"],
    [/^SKY INVESTIGATION(?:\s|$)/, "sky-investigation-it.png"],
    [/^SKY COLLECTION(?:\s|$)/, "sky-collection-it.png"],
    [/^SKY CRIME(?:\s|$)/, "sky-crime-it.png"],
    [/^SKY DOCUMENTARIES(?:\s|$)/, "sky-documentaries-it.png"],
    [/^SKY ADVENTURE(?:\s|$)/, "sky-adventure-it.png"],
    [/^SKY NATURE(?:\s|$)/, "sky-nature-it.png"],
    [/^SKY ARTE(?:\s|$)/, "sky-arte-it.png"],
    [/^SKY CLASSICA(?:\s|$)/, "sky-classica-it.png"],
    [/^SKY UNO(?:\s|$)/, "sky-uno-it.png"],
    [/^SKY TG24(?:\s|$)/, "sky-tg24-it.png"],
    [/^SKY SPORT 24(?:\s|$)/, "sky-sport-24-it.png"],
    [/^SKY SPORT UNO(?:\s|$)/, "sky-sport-uno-it.png"],
    [/^SKY SPORT CALCIO(?:\s|$)/, "sky-sport-calcio-it.png"],
    [/^SKY SPORT TENNIS(?:\s|$)/, "sky-sport-tennis-it.png"],
    [/^SKY SPORT BASKET(?:\s|$)/, "sky-sport-basket-it.svg"],
    [/^SKY SPORT ARENA(?:\s|$)/, "sky-sport-arena-it.png"],
    [/^SKY SPORT MAX(?:\s|$)/, "sky-sport-max-it.png"],
    [/^SKY SPORT F1(?:\s|$)/, "sky-sport-f1-it.png"],
    [/^SKY SPORT MOTOGP(?:\s|$)/, "sky-sport-motogp-it.png"],
    [/^SKY SPORT GOLF(?:\s|$)/, "sky-sport-golf-it.png"],
    [/^SKY SPORT LEGEND(?:\s|$)/, "sky-sport-legend-it.png"],
    [/^SKY SPORT MIX(?:\s|$)/, "sky-sport-mix-it.png"],
    [/^SKY SPORT FOOTBALL(?:\s|$)/, "sky-sport-football-it.png"],
    [/^SKY SPORT(?:\s|$)/, "sky-sport-it.png"],
    [/^EUROSPORT 1(?:\s|$)/, "eurosport-1-it.png"],
    [/^EUROSPORT 2(?:\s|$)/, "eurosport-2-it.png"],
    [/^SUPER TENNIS(?:\s|$)/, "super-tennis-it.png"],
    [/^EUROSPORT PLAYER(?:\s|$)/, "eurosport-1-it.png"],
    [/^DAZN(?:\s|$)/, "dazn-it.svg"],
    [/^ZONA DAZN(?:\s|$)/, "zona-dazn-it.png"],
    [/^COMEDY CENTRAL(?:\s|$)/, "comedy-central-it.png"],
    [/^CACCIA E PESCA(?:\s|$)/, "caccia-pesca-it.png"],
    [/^CACCIA(?:\s|$)/, "caccia-it.png"],
    [/^PESCA(?:\s|$)/, "pesca-it.png"],
    [/^BABY TV(?:\s|$)/, "baby-tv-it.png"],
    [/^PADRE PIO TV(?:\s|$)/, "padre-pio-tv-it.png"],
    [/^RDS TV(?:\s|$)/, "rds-it.png"],
    [/^INTER TV(?:\s|$)/, "inter-tv-it.png"],
    [/^MILAN TV(?:\s|$)/, "milan-tv-it.png"],
    [/^LAZIO STYLE CHANNEL(?:\s|$)/, "lazio-style-channel-it.png"],
    [/^ACI SPORT(?: TV)?(?:\s|$)/, "aci-sport-tv-it.png"],
    [/^HORSE TV(?:\s|$)/, "horse-tv-it.png"],
    [/^GAMBERO ROSSO(?:\s|$)/, "gambero-rosso-it.png"],
    [/^GM24(?:\s|$)/, "gm24-it.png"],
    [/^MTV MUSIC(?:\s|$)/, "mtv-music-it.png"],
    [/^MTV(?:\s|$)/, "mtv-it.png"]
];

function brandingEnabled(configKey) {
    if (!settings.CHANNEL_BRANDING_CONFIGS.size) return false;
    const candidates = [String(configKey || ""), hashKey(configKey, 12), hashKey(configKey)];
    return candidates.some(value => settings.CHANNEL_BRANDING_CONFIGS.has(value));
}

function normalizeChannelName(value) {
    let name = String(value || "CANALE TV")
        .normalize("NFKC")
        .replace(/^\s*IT\s*(?:\||:)\s*/i, "")
        .replace(/[◉●•]+/g, " ")
        .replace(/[_]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLocaleUpperCase("it-IT");

    name = name.replace(FALSE_SKY_PREFIX, "");
    for (const [pattern, replacement] of NAME_REPLACEMENTS) {
        name = name.replace(pattern, replacement);
    }
    return name.replace(/\s+/g, " ").trim() || "CANALE TV";
}

function withoutQualitySuffix(value) {
    let result = String(value || "").trim();
    let previous = "";
    while (result !== previous) {
        previous = result;
        result = result.replace(QUALITY_SUFFIX, "").trim();
    }
    return result;
}

function fileExists(filename) {
    return filename && fs.existsSync(path.join(LOGO_ROOT, filename));
}

function localLogo(filename) {
    return fileExists(filename) ? `${LOGO_PUBLIC_ROOT}/${filename}` : "";
}

function directLogoFilename(name) {
    const slug = withoutQualitySuffix(name)
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/&/g, "and")
        .replace(/\+/g, "plus")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");
    return slug ? `${slug}-it.png` : "";
}

function logoForName(name) {
    const normalized = normalizeChannelName(name);
    return ruleLogoForName(normalized) || localLogo(directLogoFilename(normalized));
}

function ruleLogoForName(normalized) {
    for (const [pattern, filename] of LOGO_RULES) {
        if (pattern.test(normalized)) return localLogo(filename);
    }
    return "";
}

const EXCLUDED_CHANNELS = [
    /^A3SERIES(?:\s|$)/,
    /^AMUSE ANIMATION(?:\s|$)/,
    /^BABY TV(?:\s|$)/,
    /^BIKE SMART MOBILITY(?:\s|$)/,
    /^CANALE 10(?:\s|$)/,
    /^DISNEY PLUS NATURE(?:\s|$)/,
    /^DONNA SHOPPING(?:\s|$)/,
    /^LAF(?:\s|$)/,
    /^SKY SPORT SOLO (?:CALCIO|MOTORI)(?:\s|$)/,
    /^DIRETTA PRO ELEVEN SPORTS(?:\s|$)/,
    /^ELEVEN SPORT(?:\s|$)/,
    /^HELBIZ SERIE B LIVE(?:\s|$)/,
    /^HIGHLIGHTS SERIE A(?:\s|$)/,
    /^(?:PARAMOUNT(?: CHANNEL)?|SPIKE|VH1|BLAZE)(?:\s|$)/,
    /^SPOTLIGHT(?:\s|$)/,
    /^F1 CAR (?:VETTEL|RAIKKONEN|RICCIARDO|GIOVINAZZI|MAGNUSSEN|MAZEPIN|SCHUMACHER|TSUNODA)(?:\s|$)/,
    /\sGIRONE [ABC](?:\s|$)/,
    /SUPERLEGA SERIE A MASCHILE/
];

function shouldExcludeChannel(configKey, channel) {
    if (!brandingEnabled(configKey)) return false;
    const name = normalizeChannelName(channel?.name);
    return EXCLUDED_CHANNELS.some(pattern => pattern.test(name));
}

function applyChannelBranding(configKey, channel) {
    if (!brandingEnabled(configKey)) return channel;
    let name = normalizeChannelName(channel?.name);
    const group = String(channel?.group || "").toLocaleUpperCase("it-IT");
    if (/^CINEMA(?:\s|$)/.test(group)) {
        name = name.replace(/\s+CHANNEL(?=\s|$)/, " 24/7").replace(/\s+24H(?=\s|$)/, " 24/7");
    }
    const ruleLogo = ruleLogoForName(name);
    const groupLogo = /^DAZN(?:\s|$)/.test(group) ? localLogo("dazn-it.svg") : "";
    return {
        ...channel,
        name,
        logo: ruleLogo || groupLogo || logoForName(name) || channel.logo
    };
}

module.exports = {
    LOGO_PUBLIC_ROOT,
    applyChannelBranding,
    brandingEnabled,
    logoForName,
    normalizeChannelName,
    shouldExcludeChannel,
    withoutQualitySuffix
};
