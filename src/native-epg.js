'use strict';
function schedule(meta, programmes, date) {
  const start = date ? Date.parse(date + 'T00:00:00.000Z') : Date.now() - 86400000;
  const end = date ? start + 86400000 : Date.now() + 7 * 86400000;
  const seen = new Set();
  const videos = (programmes || []).flatMap(p => {
    const a = new Date(p.start).getTime(), b = new Date(p.stop).getTime();
    if (!Number.isFinite(a) || !Number.isFinite(b) || b <= a || b <= start || a >= end || seen.has(a)) return [];
    seen.add(a);
    const startTime = new Date(a).toISOString();
    return [{ id: meta.id + ':epg:' + a, title: p.title || 'Programma',
      released: startTime, startTime, endTime: new Date(b).toISOString(),
      overview: p.desc || '' }];
  }).sort((a,b) => a.startTime.localeCompare(b.startTime));
  const behaviorHints = { ...meta.behaviorHints, isLive: true, hasScheduledVideos: videos.length > 0 };
  if (videos.length) delete behaviorHints.defaultVideoId;
  return { ...meta, behaviorHints, ...(videos.length ? { videos } : {}) };
}
// EPG entries select the channel's live stream; they are not catch-up assets.
function channelIdFromVideoId(id) {
  return typeof id === 'string' ? id.replace(/:epg:\d+$/, '') : id;
}
module.exports = { schedule, channelIdFromVideoId };
