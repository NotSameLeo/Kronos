const test = require("node:test");
const assert = require("node:assert/strict");
const { TsSegmentDiagnostics, compareTsSegments, detectKeyframe, readPts90k } = require("../src/ts-diagnostics");

test("readPts90k decodes a real 33-bit PES timestamp", () => {
    const value = 123456789;
    const encoded = encodePts(value);
    assert.equal(readPts90k(encoded, 0), value);
});

test("detectKeyframe recognizes H264 IDR and HEVC IRAP NAL units", () => {
    assert.equal(detectKeyframe(Buffer.from([0, 0, 1, 0x65, 0x88]), "h264"), true);
    assert.equal(detectKeyframe(Buffer.from([0, 0, 1, 0x41, 0x88]), "h264"), false);
    assert.equal(detectKeyframe(Buffer.from([0, 0, 1, 19 << 1, 0x01]), "hevc"), true);
});

test("TS diagnostics reads PAT, PMT, PCR, PTS, continuity and initial keyframe", () => {
    const analyzer = new TsSegmentDiagnostics();
    const packets = [
        makePsiPacket(0, makePatSection(100), 0),
        makePsiPacket(100, makePmtSection(256, 256, 0x1b), 0),
        makeVideoPacket(256, 0, 90000, 90000 * 300, true),
        makeVideoPacket(256, 1, 180000, 180000 * 300, false)
    ];
    const stream = Buffer.concat(packets);
    analyzer.push(stream.subarray(0, 301));
    analyzer.push(stream.subarray(301));
    const result = analyzer.finish();

    assert.equal(result.codec, "h264");
    assert.equal(result.videoPid, 256);
    assert.equal(result.pcrPid, 256);
    assert.equal(result.firstPts90k, 90000);
    assert.equal(result.lastPts90k, 180000);
    assert.equal(result.ptsSpanMs, 1000);
    assert.equal(result.pcrSpanMs, 1000);
    assert.equal(result.keyframeAtStart, true);
    assert.equal(result.continuityErrors, 0);
    assert.equal(result.transportErrors, 0);
});

test("TS diagnostics reports genuine continuity errors and segment overlap", () => {
    const analyzer = new TsSegmentDiagnostics();
    analyzer.push(Buffer.concat([
        makePsiPacket(0, makePatSection(100), 0),
        makePsiPacket(100, makePmtSection(256, 256, 0x1b), 0),
        makeVideoPacket(256, 0, 90000, 90000 * 300, true),
        makeVideoPacket(256, 3, 99000, 99000 * 300, false)
    ]));
    const current = analyzer.finish();
    const compared = compareTsSegments({
        lastPcr27m: 100000 * 300,
        lastPts90k: 100000
    }, current, 10);

    assert.equal(current.continuityErrors, 1);
    assert.equal(compared.overlap, true);
    assert.ok(compared.ptsGapMs < 0);
    assert.equal(compared.declaredMs, 10000);
});

function makePatSection(pmtPid) {
    return Buffer.from([
        0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00,
        0x00, 0x01, 0xe0 | ((pmtPid >> 8) & 0x1f), pmtPid & 0xff,
        0x00, 0x00, 0x00, 0x00
    ]);
}

function makePmtSection(pcrPid, videoPid, streamType) {
    return Buffer.from([
        0x02, 0xb0, 0x12, 0x00, 0x01, 0xc1, 0x00, 0x00,
        0xe0 | ((pcrPid >> 8) & 0x1f), pcrPid & 0xff,
        0xf0, 0x00,
        streamType, 0xe0 | ((videoPid >> 8) & 0x1f), videoPid & 0xff, 0xf0, 0x00,
        0x00, 0x00, 0x00, 0x00
    ]);
}

function makePsiPacket(pid, section, counter) {
    const packet = Buffer.alloc(188, 0xff);
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) & 0x1f);
    packet[2] = pid & 0xff;
    packet[3] = 0x10 | counter;
    packet[4] = 0;
    section.copy(packet, 5);
    return packet;
}

function makeVideoPacket(pid, counter, pts, pcr27m, keyframe) {
    const packet = Buffer.alloc(188, 0xff);
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) & 0x1f);
    packet[2] = pid & 0xff;
    packet[3] = 0x30 | counter;
    packet[4] = 7;
    packet[5] = 0x10;
    writePcr(packet, 6, pcr27m);
    const payload = Buffer.concat([
        Buffer.from([0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 0x05]),
        encodePts(pts),
        Buffer.from([0, 0, 1, keyframe ? 0x65 : 0x41, 0x88, 0x84])
    ]);
    payload.copy(packet, 12);
    return packet;
}

function encodePts(value) {
    const high = Math.floor(value / 2 ** 30) & 0x07;
    return Buffer.from([
        0x20 | (high << 1) | 1,
        Math.floor(value / 2 ** 22) & 0xff,
        ((Math.floor(value / 2 ** 15) & 0x7f) << 1) | 1,
        Math.floor(value / 2 ** 7) & 0xff,
        ((value & 0x7f) << 1) | 1
    ]);
}

function writePcr(packet, offset, value) {
    const base = Math.floor(value / 300);
    const extension = value % 300;
    packet[offset] = Math.floor(base / 2 ** 25) & 0xff;
    packet[offset + 1] = Math.floor(base / 2 ** 17) & 0xff;
    packet[offset + 2] = Math.floor(base / 2 ** 9) & 0xff;
    packet[offset + 3] = Math.floor(base / 2) & 0xff;
    packet[offset + 4] = ((base & 1) << 7) | 0x7e | ((extension >> 8) & 1);
    packet[offset + 5] = extension & 0xff;
}
