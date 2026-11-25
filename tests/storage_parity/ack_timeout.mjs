import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';
import {holdArchive, receiptMetrics} from './archive_receipts.mjs';
import {ackLossRelay, brokerAccepted, persisted} from './fault_wire.mjs';
import {connect, eventually, getOne, offline, online, prepare, queryServer, raw, runId, serverRows} from './live_stack.mjs';
import {sendSample} from './session.mjs';

async function verifyTimeout({native, web}, reopen) {
  let release, queuedWriterLock, forwardRetry;
  const mode = reopen ? 'reopen' : 'online';
  const sides = [];
  try {
    for (const [label, open] of [['C', native], ['WASM', web]]) {
      const user = 'timeout-' + mode + '-' + label.toLowerCase() + '-' + runId;
      const acks = [], sends = [];
      const wire = await ackLossRelay(ack => {
        // An early Persisted while the lookup is locked must fail without blocking the relay.
        const rows = ack.persisted && !release ? serverRows(ack.cid).filter(r => r.id === ack.clientId) : [];
        acks.push({...ack, rows});
      }, send => {
        const repeated = forwardRetry && sends.some(f => f.clientId === send.clientId && f.cid === send.cid);
        sends.push(send);
        if (repeated) return forwardRetry.promise;
      });
      sides.push({wire, acks, sends, label, open});
      const side = sides.at(-1);
      side.client = await open(user);
      await prepare(side.client, user, wire.url, user + '-device', reopen ? undefined : 2);
      await connect(side.client, user);
      side.cid = (await getOne(side.client, user + '-peer')).conversationId;
      side.user = user;
    }
    assert.notEqual(sides[0].cid, sides[1].cid);
    // The first sequence allocation for a new conversation reads its archived floor.
    // Warm it before locking messages so the fault reaches confirm_archive itself.
    for (const [index, s] of sides.entries()) {
      s.offset = index + 1;
      for (let n = 0; n < s.offset; n++) {
        const seed = {...online, clientMsgId: 'timeout-seed-' + index + '-' + n};
        await sendSample(s.client.invoke, s.cid, seed);
        await eventually('allocated sequence floor', () => s.client.acks,
          acks => acks.some(a => a.clientMsgId === seed.clientMsgId && a.stage === 'persisted'));
      }
    }
    for (const sample of [online, offline]) {
      const id = sample.clientMsgId;
      const before = await receiptMetrics();
      release = await holdArchive(true);
      if (!reopen) {
        // Queue a writer-only barrier before the first SEND. Once reads resume, the
        // connected clients can sync, but cannot learn a commit before their retry.
        queuedWriterLock = holdArchive().then(unlock => ({unlock}), error => ({error}));
        await eventually('writer barrier queued before SEND', () => queryServer('', `SELECT EXISTS (
          SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING(pid)
          WHERE l.relation='messages'::regclass AND l.mode='ShareLock' AND NOT l.granted
          AND a.application_name='flare-ack-archive-lock')::text`), Boolean);
        forwardRetry = Promise.withResolvers();
      }
      await Promise.all(sides.map(s => sendSample(s.client.invoke, s.cid, sample)));
      await eventually('both bindings saw admission', () => sides.map(s =>
        s.client.acks.some(a => a.clientMsgId === id && a.stage === 'accepted')), flags => flags.every(Boolean));
      const blocked = await receiptMetrics();
      assert.equal((blocked.outcomes.timeout ?? 0) - (before.outcomes.timeout ?? 0), 2,
        'both archive_receipt deadlines must actually expire');
      for (const s of sides) {
        const first = s.sends.filter(f => f.clientId === id);
        const admission = s.acks.filter(f => f.clientId === id);
        assert.equal(first.length, 1);
        assert.equal(admission.length, 1);
        assert.equal(admission[0].durability, brokerAccepted);
        assert.ok(admission[0].at - first[0].at >= 350);
        assert.equal(s.client.acks.some(a => a.clientMsgId === id && a.stage === 'persisted'), false);
        assert.deepEqual(await s.client.outbox(), [[id, s.cid]]);
        // The writer commits its idempotency ledger with its message in the same transaction.
        assert.equal(queryServer(s.cid, "SELECT count(*) FROM message_idempotency_ledger WHERE tenant_id=:'tenant' AND conversation_id=:'cid' AND client_msg_id=:'client_id'", id), 0);
      }
      if (reopen) {
        // A live sync can settle a send before retransmission. Reopen from the actual
        // durable outbox after a link loss. The driver only sends the original sample once.
        for (const s of sides) s.wire.cut();
        await Promise.all(sides.map(s => s.client.close()));
        await release();
        release = undefined;
        for (const s of sides) {
          await eventually('archive commit while sender offline', () => serverRows(s.cid).filter(r => r.id === id),
            rows => rows.length === 1);
          s.client = await s.open(s.user);
          await prepare(s.client, s.user, s.wire.url);
          assert.deepEqual(await s.client.outbox(), [[id, s.cid]]);
          s.wire.restore();
          await connect(s.client, s.user);
        }
      } else {
        await release();
        release = undefined;
        const {unlock, error} = await queuedWriterLock;
        queuedWriterLock = undefined;
        if (error) throw error;
        release = unlock;
        await eventually('automatic retries on the original sockets', () =>
          sides.map(s => s.sends.filter(f => f.clientId === id).length), counts => counts.every(n => n >= 2));
        for (const s of sides) {
          assert.equal(serverRows(s.cid).some(row => row.id === id), false);
          assert.equal(s.client.acks.some(ack => ack.clientMsgId === id && ack.stage === 'persisted'), false);
        }
        await release();
        release = undefined;
        forwardRetry.resolve();
        forwardRetry = undefined;
      }
      await eventually('retried wire and public persisted ACKs', () => sides.map(s =>
        ({sends: s.sends.filter(f => f.clientId === id), frames: s.acks.filter(a => a.clientId === id).map(a => a.durability),
          sdk: s.client.acks.filter(a => a.clientMsgId === id).map(a => a.stage)})),
        states => states.every(s => s.frames.includes(persisted) && s.sdk.includes('persisted')));
      for (const s of sides) {
        const attempts = s.sends.filter(f => f.clientId === id);
        const frames = s.acks.filter(f => f.clientId === id);
        const receipts = frames.filter(f => f.persisted);
        assert.ok(attempts.length >= 2 && attempts.every(f => f.cid === s.cid));
        assert.equal(attempts[0].connection !== attempts[1].connection, reopen);
        const forwardedAt = reopen ? attempts[1].at : attempts[1].forwardedAt;
        assert.ok(frames[0].at < attempts[1].at && attempts[1].at <= forwardedAt && forwardedAt <= receipts[0].at);
        const rows = serverRows(s.cid).filter(r => r.id === id);
        assert.equal(rows.length, 1);
        assert.equal(rows[0].sender_id, s.user);
        // Read receipts inserted on reconnect share the conversation sequence.
        assert.ok(rows[0].seq > (s.lastSampleSeq ?? 0));
        s.lastSampleSeq = rows[0].seq;
        const expected = [s.cid, rows[0].server_id, rows[0].seq];
        assert.deepEqual([frames[0].cid, frames[0].serverId, frames[0].seq], expected);
        for (const ack of receipts) {
          assert.deepEqual([ack.cid, ack.serverId, ack.seq], expected);
          assert.deepEqual(ack.rows, rows);
        }
        for (const ack of s.client.acks.filter(a => a.clientMsgId === id && a.stage === 'persisted')) {
          assert.ok(ack.success);
          assert.deepEqual([ack.conversationId, ack.serverId, ack.seq], expected);
        }
        const local = await raw(s.client, rows[0].server_id);
        assert.deepEqual([local?.conversationId, local?.serverId, local?.conversationSeq], expected);
        assert.deepEqual(await s.client.outbox(), []);
        console.log(JSON.stringify({mode, binding: s.label, clientId: id, cid: s.cid, serverId: rows[0].server_id,
          seq: rows[0].seq, attempts: attempts.length, admissionMs: Math.round(frames[0].at - attempts[0].at),
          retryMs: Math.round(attempts[1].at - attempts[0].at), retryForwardMs: Math.round(forwardedAt - attempts[0].at),
          durableMs: Math.round(receipts[0].at - attempts[0].at)}));
      }
    }
  } finally {
    try {
      if (release) await release();
      if (queuedWriterLock) await (await queuedWriterLock).unlock?.();
    } finally {
      forwardRetry?.resolve();
      await Promise.all(sides.map(s => s.wire.close()));
    }
  }
}

for (const reopen of [false, true]) {
  await withBindings(process.argv[2], process.argv[3], devices => verifyTimeout(devices, reopen));
}
