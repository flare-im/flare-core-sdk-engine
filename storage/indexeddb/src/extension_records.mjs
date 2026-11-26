// One connection per operation. A commit compares every row, installed schema and
// checkpoint it names before touching any of them, in one transaction across all three
// stores. Closing the tab cannot split that commit.
const STORES = ["records", "checkpoints", "partitions"];

function openDb() {
  return new Promise((resolve, reject) => {
    const open = indexedDB.open("flare-extension-records", 2);
    open.onupgradeneeded = event => {
      const db = open.result;
      if (event.oldVersion < 1) {
        const rows = db.createObjectStore("records");
        rows.createIndex("fields", "indexes", {multiEntry: true});
        db.createObjectStore("checkpoints");
      }
      if (event.oldVersion < 2) db.createObjectStore("partitions");
    };
    // Another tab still holds version 1 open; it closes on its versionchange below.
    open.onblocked = () => reject(new Error("extension storage upgrade blocked by another tab"));
    open.onerror = () => reject(open.error);
    open.onsuccess = () => {
      const db = open.result;
      db.onversionchange = () => db.close();
      resolve(db);
    };
  });
}

function storageError(error) {
  if (error?.name === "QuotaExceededError") return new Error("extension storage quota exceeded");
  return error ?? new Error("extension record transaction aborted");
}

export async function extensionRecords(encoded) {
  const p = JSON.parse(encoded);
  // SQLite BINARY compares UTF-8. Match it even for supplementary characters.
  const sortKey = id => Array.from(new TextEncoder().encode(id), byte => byte.toString(16).padStart(2, "0")).join("");
  const db = await openDb();
  const {tenant, user, namespace, partition} = p.scope;
  const scope = [tenant, user, namespace, partition];
  const writes = p.op === "commit" || p.op === "install" || p.op === "purge";
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORES, writes ? "readwrite" : "readonly");
      const rows = tx.objectStore("records");
      const cursors = tx.objectStore("checkpoints");
      const partitions = tx.objectStore("partitions");
      let result = null;
      let failure = null;
      tx.oncomplete = () => failure ? reject(failure) : resolve(p.op === "get" ? result ?? "null" : JSON.stringify(result));
      tx.onabort = () => reject(failure ?? storageError(tx.error));
      if (p.op === "find" || p.op === "scan") {
        const prefix = p.op === "find" ? [...scope, p.index, p.value] : scope;
        const range = IDBKeyRange.bound([...prefix, sortKey(p.after)], [...prefix, []], true, true);
        const cursor = (p.op === "find" ? rows.index("fields") : rows).openCursor(range);
        result = [];
        cursor.onsuccess = () => {
          const row = cursor.result;
          if (row) {
            result.push(row.value.id);
            if (result.length < p.limit) row.continue();
          }
        };
      } else if (p.op === "checkpoint") {
        const read = cursors.get(scope);
        read.onsuccess = () => { result = read.result ?? null; };
      } else if (p.op === "installed") {
        const read = partitions.get(scope);
        read.onsuccess = () => { result = read.result ?? null; };
      } else if (p.op === "install") {
        const read = partitions.get(scope);
        result = false;
        read.onsuccess = () => {
          if (JSON.stringify(read.result ?? null) !== JSON.stringify(p.expected ?? null)) return;
          partitions.put(p.next, scope);
          result = true;
        };
      } else if (p.op === "purge") {
        // Every key of this account's namespace starts with [tenant, user, namespace].
        const range = IDBKeyRange.bound([tenant, user, namespace], [tenant, user, namespace, []]);
        for (const store of [rows, cursors, partitions]) store.delete(range);
      } else if (p.op === "commit") {
        // Read every row, installed schema and checkpoint first; write only if all match.
        const scopeOf = s => [s.tenant, s.user, s.namespace, s.partition];
        let waiting = 0;
        const read = (store, key) => {
          const slot = {};
          waiting++;
          const request = store.get(key);
          request.onsuccess = () => { slot.value = request.result; if (--waiting === 0) decide(); };
          return slot;
        };
        const puts = p.rows.map(w => {
          const key = [...scopeOf(w.scope), sortKey(w.id)];
          return {w, key, old: read(rows, key), state: read(partitions, scopeOf(w.scope))};
        });
        const marks = p.checkpoints.map(c => ({c, current: read(cursors, scopeOf(c.scope))}));
        result = false;
        const decide = () => {
          if (puts.some(({w, state}) => state.value && state.value.schema_version !== w.schemaVersion)) {
            failure = new Error("extension handle schema is not the installed schema");
            tx.abort();
            return;
          }
          if (puts.some(({w, old}) => (old.value?.revision ?? 0) !== w.expected)) return;
          if (marks.some(({c, current}) => (current.value ?? null) !== (c.expected ?? null))) return;
          for (const {w, key} of puts) {
            const scope = scopeOf(w.scope);
            const indexes = Object.entries(w.indexes).map(([name, value]) => [...scope, name, value, sortKey(w.id)]);
            rows.put({id: w.id, recordJson: w.recordJson, revision: w.expected + 1, indexes}, key);
          }
          for (const {c} of marks) cursors.put(c.next, scopeOf(c.scope));
          result = true;
        };
        if (waiting === 0) decide();
      } else {
        const read = rows.get([...scope, sortKey(p.id)]);
        read.onsuccess = () => { result = read.result?.recordJson ?? null; };
      }
    });
  } finally { db.close(); }
}
