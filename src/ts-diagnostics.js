const TS_PACKET_SIZE = 188;
const PTS_WRAP = 2 ** 33;
const PCR_WRAP = PTS_WRAP * 300;

const VIDEO_STREAM_TYPES = new Map([
    [0x02, "mpeg2"],
    [0x10, "mpeg4"],
    [0x1b, "h264"],
    [0x24, "hevc"]
]);

class TsSegmentDiagnostics {
    constructor() {
        this.pending = Buffer.alloc(0);
        this.psi = new Map();
        this.continuity = new Map();
        this.pmtPid = null;
        this.videoPid = null;
        this.pcrPid = null;
        this.codec = null;
        this.packetCount = 0;
        this.syncLossBytes = 0;
        this.transportErrors = 0;
        this.continuityErrors = 0;
        this.pcrByPid = new Map();
        this.firstPts90k = null;
        this.lastPts90k = null;
        this.ptsRegressions = 0;
        this.firstVideoPes = null;
        this.firstVideoPesComplete = false;
        this.keyframeAtStart = null;
    }

    push(chunk) {
        if (!chunk?.length) return;
        let input = this.pending.length
            ? Buffer.concat([this.pending, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)])
            : (Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        let offset = 0;

        while (input.length - offset >= TS_PACKET_SIZE) {
            if (input[offset] !== 0x47) {
                const next = findSyncOffset(input, offset + 1);
                if (next < 0) break;
                this.syncLossBytes += next - offset;
                offset = next;
            }
            if (input.length - offset < TS_PACKET_SIZE) break;
            this.processPacket(input.subarray(offset, offset + TS_PACKET_SIZE));
            offset += TS_PACKET_SIZE;
        }

        this.pending = input.subarray(offset);
        if (this.pending.length > TS_PACKET_SIZE * 2) {
            this.syncLossBytes += this.pending.length - (TS_PACKET_SIZE - 1);
            this.pending = this.pending.subarray(-(TS_PACKET_SIZE - 1));
        }
    }

    processPacket(packet) {
        if (packet[0] !== 0x47) return;
        this.packetCount++;

        const transportError = (packet[1] & 0x80) !== 0;
        const payloadStart = (packet[1] & 0x40) !== 0;
        const pid = ((packet[1] & 0x1f) << 8) | packet[2];
        const adaptationControl = (packet[3] >> 4) & 0x03;
        const counter = packet[3] & 0x0f;
        const hasAdaptation = adaptationControl === 2 || adaptationControl === 3;
        const hasPayload = adaptationControl === 1 || adaptationControl === 3;
        if (transportError) this.transportErrors++;

        let payloadOffset = 4;
        let discontinuity = false;
        if (hasAdaptation) {
            const adaptationLength = packet[4];
            if (adaptationLength > 0 && payloadOffset + adaptationLength < TS_PACKET_SIZE) {
                const flags = packet[5];
                discontinuity = (flags & 0x80) !== 0;
                if ((flags & 0x10) !== 0 && adaptationLength >= 7) {
                    const pcr = readPcr27m(packet, 6);
                    if (pcr !== null) this.rememberPcr(pid, pcr);
                }
            }
            payloadOffset += 1 + adaptationLength;
        }

        if (pid !== 0x1fff && hasPayload && payloadOffset < TS_PACKET_SIZE) {
            const previousCounter = this.continuity.get(pid);
            if (!discontinuity && previousCounter !== undefined && counter !== ((previousCounter + 1) & 0x0f)) {
                this.continuityErrors++;
            }
            this.continuity.set(pid, counter);
        } else if (discontinuity) {
            this.continuity.delete(pid);
        }

        if (!hasPayload || payloadOffset >= TS_PACKET_SIZE || transportError) return;
        const payload = packet.subarray(payloadOffset);
        if (pid === 0 || pid === this.pmtPid) this.processPsi(pid, payload, payloadStart);
        if (pid === this.videoPid) this.processVideoPayload(payload, payloadStart);
    }

    processPsi(pid, payload, payloadStart) {
        let data = payload;
        if (payloadStart) {
            const pointer = payload[0] || 0;
            data = payload.subarray(Math.min(payload.length, pointer + 1));
            this.psi.set(pid, Buffer.alloc(0));
        }
        if (!data.length) return;

        const buffered = Buffer.concat([this.psi.get(pid) || Buffer.alloc(0), data]);
        if (buffered.length < 3) {
            this.psi.set(pid, buffered);
            return;
        }
        const sectionLength = ((buffered[1] & 0x0f) << 8) | buffered[2];
        const totalLength = 3 + sectionLength;
        if (sectionLength < 4 || totalLength > 4096) {
            this.psi.delete(pid);
            return;
        }
        if (buffered.length < totalLength) {
            this.psi.set(pid, buffered);
            return;
        }

        const section = buffered.subarray(0, totalLength);
        this.psi.delete(pid);
        if (pid === 0 && section[0] === 0x00) this.parsePat(section);
        if (pid === this.pmtPid && section[0] === 0x02) this.parsePmt(section);
    }

    parsePat(section) {
        const end = section.length - 4;
        for (let offset = 8; offset + 4 <= end; offset += 4) {
            const program = (section[offset] << 8) | section[offset + 1];
            if (program === 0) continue;
            this.pmtPid = ((section[offset + 2] & 0x1f) << 8) | section[offset + 3];
            return;
        }
    }

    parsePmt(section) {
        this.pcrPid = ((section[8] & 0x1f) << 8) | section[9];
        const programInfoLength = ((section[10] & 0x0f) << 8) | section[11];
        const end = section.length - 4;
        for (let offset = 12 + programInfoLength; offset + 5 <= end;) {
            const streamType = section[offset];
            const elementaryPid = ((section[offset + 1] & 0x1f) << 8) | section[offset + 2];
            const infoLength = ((section[offset + 3] & 0x0f) << 8) | section[offset + 4];
            if (this.videoPid === null && VIDEO_STREAM_TYPES.has(streamType)) {
                this.videoPid = elementaryPid;
                this.codec = VIDEO_STREAM_TYPES.get(streamType);
            }
            offset += 5 + infoLength;
        }
    }

    processVideoPayload(payload, payloadStart) {
        if (payloadStart) {
            if (this.firstVideoPes && !this.firstVideoPesComplete) {
                this.finishFirstVideoPes();
            }
            const pes = parsePes(payload);
            if (pes.pts90k !== null) this.rememberPts(pes.pts90k);
            if (!this.firstVideoPesComplete && !this.firstVideoPes) {
                this.firstVideoPes = Buffer.from(pes.elementary);
                this.updateInitialKeyframe();
            }
            return;
        }

        if (this.firstVideoPes && !this.firstVideoPesComplete && this.firstVideoPes.length < 256 * 1024) {
            this.firstVideoPes = Buffer.concat([this.firstVideoPes, payload]);
            this.updateInitialKeyframe();
        }
    }

    updateInitialKeyframe() {
        if (this.keyframeAtStart === true || !this.firstVideoPes) return;
        const detected = detectKeyframe(this.firstVideoPes, this.codec);
        if (detected === true) this.keyframeAtStart = true;
    }

    finishFirstVideoPes() {
        this.updateInitialKeyframe();
        if (this.keyframeAtStart === null && supportsKeyframeDetection(this.codec)) {
            this.keyframeAtStart = false;
        }
        this.firstVideoPesComplete = true;
        this.firstVideoPes = null;
    }

    rememberPcr(pid, value) {
        const current = this.pcrByPid.get(pid) || {
            first: null,
            last: null,
            backwards: 0
        };
        if (current.first === null) current.first = value;
        if (current.last !== null && signedWrapDelta(value, current.last, PCR_WRAP) < 0) {
            current.backwards++;
        }
        current.last = value;
        this.pcrByPid.set(pid, current);
    }

    rememberPts(value) {
        if (this.firstPts90k === null) this.firstPts90k = value;
        if (this.lastPts90k !== null && signedWrapDelta(value, this.lastPts90k, PTS_WRAP) < 0) {
            this.ptsRegressions++;
        }
        this.lastPts90k = value;
    }

    finish() {
        if (this.firstVideoPes && !this.firstVideoPesComplete) this.finishFirstVideoPes();
        const selectedPcrPid = selectPcrPid(this.pcrByPid, this.pcrPid, this.videoPid);
        const pcr = selectedPcrPid === null ? null : this.pcrByPid.get(selectedPcrPid);
        return {
            packetCount: this.packetCount,
            syncLossBytes: this.syncLossBytes,
            transportErrors: this.transportErrors,
            continuityErrors: this.continuityErrors,
            codec: this.codec,
            videoPid: this.videoPid,
            pcrPid: selectedPcrPid,
            firstPcr27m: pcr?.first ?? null,
            lastPcr27m: pcr?.last ?? null,
            pcrSpanMs: ticksToMs(pcr?.first, pcr?.last, PCR_WRAP, 27000),
            pcrBackwards: pcr?.backwards || 0,
            firstPts90k: this.firstPts90k,
            lastPts90k: this.lastPts90k,
            ptsSpanMs: ticksToMs(this.firstPts90k, this.lastPts90k, PTS_WRAP, 90),
            ptsRegressions: this.ptsRegressions,
            keyframeAtStart: this.keyframeAtStart
        };
    }
}

function compareTsSegments(previous, current, declaredDuration) {
    const pcrGapMs = gapMs(previous?.lastPcr27m, current.firstPcr27m, PCR_WRAP, 27000);
    const ptsGapMs = gapMs(previous?.lastPts90k, current.firstPts90k, PTS_WRAP, 90);
    const observedSpanMs = Number.isFinite(current.ptsSpanMs)
        ? current.ptsSpanMs
        : current.pcrSpanMs;
    const declaredMs = Number.isFinite(declaredDuration) ? Math.round(declaredDuration * 1000) : null;
    const pcrOverlap = Number.isFinite(pcrGapMs) && pcrGapMs < 0;
    const ptsOverlap = Number.isFinite(ptsGapMs) && ptsGapMs < 0;
    return {
        ...current,
        pcrGapMs,
        ptsGapMs,
        pcrOverlap,
        ptsOverlap,
        overlap: Number.isFinite(pcrGapMs) ? pcrOverlap : ptsOverlap,
        declaredMs,
        observedSpanMs,
        declaredDeltaMs: Number.isFinite(declaredMs) && Number.isFinite(observedSpanMs)
            ? declaredMs - observedSpanMs
            : null
    };
}

function selectPcrPid(pcrByPid, declaredPcrPid, videoPid) {
    if (pcrByPid.has(declaredPcrPid)) return declaredPcrPid;
    if (pcrByPid.has(videoPid)) return videoPid;
    if (pcrByPid.size === 1) return pcrByPid.keys().next().value;
    return null;
}

function parsePes(payload) {
    if (payload.length < 9 || payload[0] !== 0x00 || payload[1] !== 0x00 || payload[2] !== 0x01) {
        return { pts90k: null, elementary: payload };
    }
    const ptsDtsFlags = (payload[7] >> 6) & 0x03;
    const headerLength = payload[8];
    const pts90k = (ptsDtsFlags === 2 || ptsDtsFlags === 3) && payload.length >= 14
        ? readPts90k(payload, 9)
        : null;
    return {
        pts90k,
        elementary: payload.subarray(Math.min(payload.length, 9 + headerLength))
    };
}

function readPts90k(buffer, offset) {
    if (offset + 5 > buffer.length) return null;
    return ((buffer[offset] & 0x0e) * 536870912)
        + (buffer[offset + 1] * 4194304)
        + ((buffer[offset + 2] & 0xfe) * 16384)
        + (buffer[offset + 3] * 128)
        + ((buffer[offset + 4] & 0xfe) / 2);
}

function readPcr27m(packet, offset) {
    if (offset + 6 > packet.length) return null;
    const base = (packet[offset] * 2 ** 25)
        + (packet[offset + 1] * 2 ** 17)
        + (packet[offset + 2] * 2 ** 9)
        + (packet[offset + 3] * 2)
        + ((packet[offset + 4] & 0x80) >> 7);
    const extension = ((packet[offset + 4] & 0x01) << 8) | packet[offset + 5];
    return base * 300 + extension;
}

function detectKeyframe(buffer, codec) {
    if (!supportsKeyframeDetection(codec)) return null;
    for (let index = 0; index + 4 < buffer.length; index++) {
        let nalOffset = -1;
        if (buffer[index] === 0 && buffer[index + 1] === 0 && buffer[index + 2] === 1) nalOffset = index + 3;
        if (buffer[index] === 0 && buffer[index + 1] === 0 && buffer[index + 2] === 0 && buffer[index + 3] === 1) nalOffset = index + 4;
        if (nalOffset < 0 || nalOffset >= buffer.length) continue;
        if (codec === "h264" && (buffer[nalOffset] & 0x1f) === 5) return true;
        if (codec === "hevc") {
            const nalType = (buffer[nalOffset] >> 1) & 0x3f;
            if (nalType === 19 || nalType === 20 || nalType === 21) return true;
        }
        if (codec === "mpeg2" && buffer[nalOffset] === 0x00 && nalOffset + 2 < buffer.length) {
            const pictureCodingType = (buffer[nalOffset + 2] >> 3) & 0x07;
            if (pictureCodingType === 1) return true;
        }
    }
    return false;
}

function supportsKeyframeDetection(codec) {
    return codec === "h264" || codec === "hevc" || codec === "mpeg2";
}

function findSyncOffset(buffer, start) {
    for (let index = start; index < buffer.length; index++) {
        if (buffer[index] !== 0x47) continue;
        if (index + TS_PACKET_SIZE >= buffer.length || buffer[index + TS_PACKET_SIZE] === 0x47) return index;
    }
    return -1;
}

function signedWrapDelta(next, previous, wrap) {
    if (!Number.isFinite(next) || !Number.isFinite(previous)) return null;
    let delta = next - previous;
    if (delta > wrap / 2) delta -= wrap;
    if (delta < -wrap / 2) delta += wrap;
    return delta;
}

function ticksToMs(first, last, wrap, ticksPerMs) {
    const delta = signedWrapDelta(last, first, wrap);
    return Number.isFinite(delta) ? Math.round((delta / ticksPerMs) * 1000) / 1000 : null;
}

function gapMs(previous, next, wrap, ticksPerMs) {
    const delta = signedWrapDelta(next, previous, wrap);
    return Number.isFinite(delta) ? Math.round((delta / ticksPerMs) * 1000) / 1000 : null;
}

module.exports = {
    TsSegmentDiagnostics,
    compareTsSegments,
    detectKeyframe,
    readPcr27m,
    readPts90k,
    signedWrapDelta
};
