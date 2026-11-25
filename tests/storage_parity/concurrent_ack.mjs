// One private Core live stack, two independent SDK sessions on the same WebSocket gateway.
import assert from 'node:assert/strict';
import {holdArchive} from './archive_receipts.mjs';
import {withBindings} from './clients.mjs';
import {ackLossRelay, brokerAccepted} from './fault_wire.mjs';
import {sendSample} from './session.mjs';
import {connect, eventually, getOne, offline, online, prepare, raw, runId, serverRows} from './live_stack.mjs';

async function verifyConversations({native, web}, sharedSender) {
  const sides = [];
  let release, retry;
  try {
    for (const [label, open] of [['c', native], ['wasm', web]]) {
      const user = 'ack-' + (sharedSender ? 'shared' : label) + '-' + runId;
      const deviceId = user + '-' + label;
      const frames = [], sends = [];
      const wire = await ackLossRelay(ack => {
        // Query as the frame arrives, before forwarding it to the SDK. No polling for a missing row.
        const rowsAtAck = ack.persisted ? serverRows(ack.cid).filter(r => r.id === ack.clientId) : [];
        frames.push({...ack, rowsAtAck});
      }, frame => {
        const repeated = sends.some(s => s.cid === frame.cid && s.clientId === frame.clientId);
        sends.push(frame);
        if (retry && repeated) return retry.promise;
      });
      sides.push({label, user, wire, frames, sends});
      const side = sides.at(-1);
      side.device = await open(user);
      await prepare(side.device, user, wire.url, deviceId, 2);
      await connect(side.device, user, deviceId);
      side.cid = (await getOne(side.device, user + '-' + label + '-peer')).conversationId;
    }
    assert.notEqual(sides[0].cid, sides[1].cid);
    const seed = {...online, clientMsgId: 'ack-offset-' + runId, text: 'sequence offset'};
    await sendSample(sides[1].device.invoke, sides[1].cid, seed);
    await eventually('offset committed', () => sides[1].frames,
      frames => frames.some(f => f.clientId === seed.clientMsgId && f.persisted));
    for (const [i, sample] of [online, offline].entries()) {
      if (i === 0) {
        release = await holdArchive();
        retry = Promise.withResolvers();
      }
      await Promise.all(sides.map(s => sendSample(s.device.invoke, s.cid, sample)));
      if (release) {
        await eventually('both gateway responses while archival commit is held', () => sides.map(s => s.frames.some(f => f.clientId === sample.clientMsgId)),
          values => values.every(Boolean));
        // Wait for both bindings to process a response before sampling their public state.
        await eventually('both SDK send ACK events', () => sides.map(s =>
          s.device.acks.some(a => a.clientMsgId === sample.clientMsgId)), values => values.every(Boolean));
        const before = sides.map(s => ({
          raw: s.frames.filter(f => f.clientId === sample.clientMsgId).map(f => f.durability),
          sdkAccepted: s.device.acks.some(a => a.clientMsgId === sample.clientMsgId && a.stage === 'accepted'),
          sdkPersisted: s.device.acks.some(a => a.clientMsgId === sample.clientMsgId && a.stage === 'persisted'),
          rows: serverRows(s.cid).filter(r => r.id === sample.clientMsgId).length,
        }));
        console.log('Before archive commit', before);
        // Sync may complete the SDK before its retry after unlock. Capture each
        // SDK's actual retransmission while the writer is held, then forward it.
        if (before.every(s => !s.sdkPersisted)) {
          await eventually('both SDKs retried the admitted send', () => sides.map(s =>
            s.sends.filter(f => f.cid === s.cid && f.clientId === sample.clientMsgId).length),
            counts => counts.every(n => n >= 2));
        }
        await release();
        release = undefined;
        for (const s of sides) {
          await eventually('archive visible before forwarding the queued retry', () =>
            serverRows(s.cid).filter(r => r.id === sample.clientMsgId), rows => rows.length === 1);
        }
        retry.resolve();
        retry = undefined;
        if (before.some(s => s.sdkPersisted) || sides.some(s =>
            s.frames.some(f => f.clientId === sample.clientMsgId && f.persisted && f.rowsAtAck.length === 0))) {
          for (const s of sides) {
            const [row] = await eventually('post-release forensic row', () =>
              serverRows(s.cid).filter(r => r.id === sample.clientMsgId), rows => rows.length === 1);
            const ack = s.frames.find(f => f.clientId === sample.clientMsgId);
            console.log(s.label, 'after release', {cid: s.cid, serverId: row.server_id, seq: row.seq,
              matchesOriginalAck: ack.cid === s.cid && ack.serverId === row.server_id && ack.seq === row.seq});
          }
        }
        assert.ok(before.every(s => s.rows === 0 && s.sdkAccepted && !s.sdkPersisted));
        assert.ok(before.every(s => s.raw.length && s.raw.every(stage => stage === brokerAccepted)),
          'the broker may accept a send while the archive transaction is blocked');
        assert.ok(sides.every(s => s.frames.filter(f => f.clientId === sample.clientMsgId && f.persisted)
          .every(f => f.rowsAtAck.length === 1)), 'premature gateway persisted ACK');
      }
      await eventually('gateway and SDK persisted ACKs', () => sides.map(s =>
        s.frames.some(f => f.clientId === sample.clientMsgId && f.persisted) &&
        s.device.acks.some(a => a.clientMsgId === sample.clientMsgId && a.stage === 'persisted')), values => values.every(Boolean));
      const ids = [];
      for (const [offset, s] of sides.entries()) {
        const rows = serverRows(s.cid).filter(r => r.id === sample.clientMsgId);
        assert.equal(rows.length, 1);
        const [row] = rows;
        assert.equal(row.sender_id, s.user);
        assert.equal(row.seq, i + 1 + offset);
        const expected = [s.cid, row.server_id, row.seq];
        for (const ack of s.frames.filter(f => f.clientId === sample.clientMsgId && f.persisted)) {
          assert.deepEqual([ack.cid, ack.serverId, ack.seq], expected);
          assert.deepEqual(ack.rowsAtAck, rows, 'committed row must already exist when the raw ACK arrives');
        }
        for (const ack of s.device.acks.filter(a => a.clientMsgId === sample.clientMsgId && a.stage === 'persisted')) {
          assert.equal(ack.success, true);
          assert.deepEqual([ack.conversationId, ack.serverId, ack.seq], expected);
        }
        const local = await raw(s.device, row.server_id);
        assert.deepEqual([local?.conversationId, local?.serverId, local?.conversationSeq], expected,
          'the sender must be able to read its own acknowledged message');
        assert.deepEqual(await s.device.outbox(), []);
        ids.push(row.server_id);
        console.log(s.label, sample.clientMsgId, ...expected);
      }
      assert.notEqual(ids[0], ids[1]);
    }
    console.log(sharedSender ? 'Shared sender, two devices:' : 'Independent senders:',
      'all four C / production WASM persisted ACKs matched committed rows at receipt.');
  } finally {
    if (release) await release();
    retry?.resolve();
    await Promise.all(sides.map(s => s.wire.close()));
  }
}

for (const sharedSender of [false, true]) {
  await withBindings(process.argv[2], process.argv[3], devices => verifyConversations(devices, sharedSender));
}
