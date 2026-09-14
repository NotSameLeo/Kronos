const test = require("node:test");
const assert = require("node:assert/strict");
const { adaptiveMasterManifest } = require("../src/transcode");
const { ffmpegArgs, delayedPlaylist } = require("../src/transcode");

test("broken source clock is repaired without reencoding HEVC video", () => {
    const args = ffmpegArgs("https://upstream.test/live.m3u8", {dir:"/tmp/test",repairClock:true,variants:[{name:"source",source:true}]});
    assert.ok(args.includes("-copyts"));
    assert.equal(args[args.indexOf("-c:v")+1], "copy");
    assert.equal(args[args.indexOf("-bsf:v")+1], "setts=ts=TS-STARTPTS");
    assert.match(args[args.indexOf("-af")+1], /asetpts=PTS-STARTPTS/);
    assert.equal(args[args.indexOf("-c:a")+1], "aac");
    assert.equal(args.includes("-filter_complex"), false);
});

test("healthy source clock retains its original relative AV timing", () => {
    const args = ffmpegArgs("https://upstream.test/live.m3u8", {dir:"/tmp/test",repairClock:false,variants:[{name:"source",source:true}]});
    assert.equal(args.includes("-copyts"), false);
    assert.equal(args.includes("-bsf:v"), false);
    assert.doesNotMatch(args[args.indexOf("-af")+1], /asetpts/);
});

test("HEVC source and scaled outputs share a single FFmpeg input", () => {
    const variants=[{name:"source",source:true},...[360,480,720].map(height=>({name:`${height}p`,height,videoK:900,audioK:96}))];
    const args=ffmpegArgs("https://upstream.test/live.m3u8",{dir:"/tmp/test",repairClock:true,variants});
    assert.equal(args.filter(x=>x==="-i").length,1);
    assert.match(args[args.indexOf("-filter_complex")+1], /split=3/);
    assert.ok(args.includes("[v0out]"));
    assert.ok(args.includes("[v2out]"));
    assert.equal(args.includes("[v3out]"),false);
    assert.equal(args.filter(x=>x==="libx264").length,3);
    assert.equal(args.filter(x=>x==="copy").length,1);
});

test("scaled broken-clock variants rebase video and audio together", () => {
    const variants=[360,480,720].map(height=>({name:`${height}p`,height,videoK:900,audioK:96}));
    const args=ffmpegArgs("https://upstream.test/live.m3u8",{dir:"/tmp/test",repairClock:true,variants});
    assert.match(args[args.indexOf("-filter_complex")+1], /setpts=PTS-STARTPTS,split=3/);
    assert.equal(args.filter(v=>v==="asetpts=PTS-STARTPTS,aresample=async=1000:first_pts=0").length,3);
});

test("delayed HLS keeps actual GOP durations instead of fabricating four-second segments", () => {
    const text="#EXTM3U\n#EXT-X-TARGETDURATION:11\n#EXT-X-MEDIA-SEQUENCE:4\n" + [10,10.08,9.92,10,10].map((d,i)=>`#EXTINF:${d},\nsource_seg_${String(i+4).padStart(6,"0")}.ts\n`).join("");
    const result=delayedPlaylist(text);
    assert.match(result, /#EXT-X-TARGETDURATION:11/);
    assert.match(result, /#EXT-X-MEDIA-SEQUENCE:4/);
    assert.match(result, /#EXTINF:10.08,/);
    assert.match(result, /#EXTINF:9.92,/);
    assert.doesNotMatch(result, /source_seg_000008/);
});

test("adaptiveMasterManifest advertises only scaled transcode variants", () => {
    const text = adaptiveMasterManifest("http://kronos.test", "abc", "http://upstream.example/live.m3u8", {
        blockOfflinePlaceholders: true,
        liveEdgeDelaySeconds: 60,
        startOffsetSeconds: 30,
        holdBackSeconds: 30
    });
    const lines = text.trim().split("\n");
    const urls = lines.filter(line => line.startsWith("http://")).map(line => new URL(line));

    assert.match(text, /Kronos 360p/);
    assert.match(text, /Kronos 480p/);
    assert.match(text, /Kronos 720p/);
    assert.doesNotMatch(text, /Kronos Source/);
    assert.doesNotMatch(text, /Kronos 240p/);
    assert.doesNotMatch(text, /\/proxy\/live\.m3u8/);
    assert.equal(urls[0].pathname, "/abc/proxy/transcode.m3u8");
    assert.equal(urls[0].searchParams.get("v"), "360p");
    assert.equal(urls[1].searchParams.get("v"), "480p");
    assert.equal(urls[2].searchParams.get("v"), "720p");
    assert.equal(urls[0].searchParams.get("d"), "60");
    assert.equal(urls[0].searchParams.get("st"), "30");
    assert.equal(urls.at(-1).searchParams.get("hb"), "30");
});
