const test = require('node:test');
const assert = require('node:assert/strict');
const { channelIdFromVideoId } = require('../src/native-epg');
const { toMeta, buildStreams } = require('../src/stremio');
const { getChannelById } = require('../src/catalog');
const state = require('../src/state');

test('guide selections resolve to the identical live channel and stream menu', async () => {
  const now = Date.now();
  const channel = { id:'channel_epg_test',name:'TEST',url:'https://example.test/live.m3u8',
    description:'In Onda: obsolete description',epgProgrammes:[{start:now,stop:now+3600000,title:'Show',desc:'Synopsis'}] };
  state.channelIndex.set('epg-test',new Map([[channel.id,channel]]));
  try {
    const meta=toMeta(channel,'https://addon.test');
    assert.equal(Object.hasOwn(meta,'description'),false);
    assert.equal(meta.behaviorHints.hasScheduledVideos,true);
    assert.equal(meta.videos[0].overview,'Synopsis');
    const selected=await getChannelById('epg-test',{},meta.videos[0].id);
    assert.equal(selected,channel);
    assert.deepEqual(buildStreams(selected,'https://addon.test'),buildStreams(channel,'https://addon.test'));
  } finally { state.channelIndex.delete('epg-test'); }
});
test('only a terminal numeric EPG suffix is removed',()=>{
  for(const id of ['channel_x','vavoo_name|group:123','channel_:epg:bad','channel_:epg:123:other']) assert.equal(channelIdFromVideoId(id),id);
  assert.equal(channelIdFromVideoId('channel_x:epg:123'),'channel_x');
});
