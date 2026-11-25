// The IndexedDB extension-record backend in a real browser: installed-schema CAS, stale
// schema writers rejected inside the transaction, scan order equal to SQLite BINARY
// (UTF-8 bytes), index lookups and namespace purge. Run: node tests/storage_parity/extension_records.mjs
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {webRequire} from './clients.mjs';

const {chromium} = webRequire('playwright');
const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)),
  '../../storage/indexeddb/src/extension_records.mjs');
const server = createServer(async (req, res) => {
  if (req.url === '/records.mjs') {
    res.setHeader('Content-Type', 'text/javascript');
    return res.end(await readFile(source));
  }
  res.end('<!doctype html><title>Extension records</title>');
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_PATH || undefined});
try {
  const page = await browser.newPage();
  await page.goto('http://127.0.0.1:' + server.address().port);
  const ids = ['b', 'a', 'é', '😀', 'Z'];
  const observed = await page.evaluate(async ids => {
    const {extensionRecords} = await import('/records.mjs');
    const scope = {tenant: 't', user: 'u', namespace: 'example.ext', partition: 'room'};
    const other = {...scope, namespace: 'example.extended'};
    const call = async (s, request) => {
      try { return JSON.parse(await extensionRecords(JSON.stringify({...request, scope: s}))); }
      catch (error) { return {error: String(error.message)}; }
    };
    const record = (id, schemaVersion) => ({id, revision: 1, schema_version: schemaVersion, fields: {label: id}});
    const commit = (s, id, schemaVersion, expected = 0) => call(s, {op: 'commit', id, expected, schemaVersion,
      recordJson: JSON.stringify(record(id, schemaVersion)), indexes: {label: JSON.stringify(id)}, checkpoint: null});
    const v1 = {schema_version: 1, migrating_after: null};
    const v2 = {schema_version: 2, migrating_after: ''};
    const out = {};
    out.install = await call(scope, {op: 'install', expected: null, next: v1});
    out.installAgain = await call(scope, {op: 'install', expected: null, next: v1});
    out.installed = await call(scope, {op: 'installed'});
    out.commits = [];
    for (const id of ids) out.commits.push(await commit(scope, id, 1));
    out.duplicate = await commit(scope, 'a', 1);
    out.scan = await call(scope, {op: 'scan', after: '', limit: 10});
    out.find = await call(scope, {op: 'find', index: 'label', value: JSON.stringify('é'), after: '', limit: 10});
    await call(other, {op: 'install', expected: null, next: v1});
    await commit(other, 'kept', 1);
    out.upgrade = await call(scope, {op: 'install', expected: v1, next: v2});
    out.staleWriter = await commit(scope, 'late', 1);
    out.currentWriter = await commit(scope, 'late', 2);
    out.purge = await call(scope, {op: 'purge'});
    out.afterPurge = [await call(scope, {op: 'get', id: 'a'}), await call(scope, {op: 'installed'})];
    out.otherNamespace = await call(other, {op: 'scan', after: '', limit: 10});
    return out;
  }, ids);

  assert.equal(observed.install, true);
  assert.equal(observed.installAgain, false, 'install is a compare-and-set');
  assert.deepEqual(observed.installed, {schema_version: 1, migrating_after: null});
  assert.deepEqual(observed.commits, ids.map(() => true));
  assert.equal(observed.duplicate, false, 'a second insert at revision 0 conflicts');
  const byUtf8 = [...ids].sort((x, y) => Buffer.compare(Buffer.from(x), Buffer.from(y)));
  assert.deepEqual(observed.scan, byUtf8, 'scan order matches SQLite BINARY');
  assert.deepEqual(observed.find, ['é']);
  assert.equal(observed.upgrade, true);
  assert.match(observed.staleWriter.error ?? '', /installed schema/, 'a v1 writer is rejected after the upgrade');
  assert.equal(observed.currentWriter, true);
  assert.equal(observed.purge, null);
  assert.deepEqual(observed.afterPurge, [null, null]);
  assert.deepEqual(observed.otherNamespace, ['kept'], 'purge leaves a namespace that merely shares a prefix');
  console.log('IndexedDB extension records matched:', JSON.stringify(observed.scan));
} finally {
  await browser.close();
  server.close();
}
