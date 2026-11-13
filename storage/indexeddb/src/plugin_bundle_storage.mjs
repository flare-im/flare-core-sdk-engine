// What plugin web bundles keep through the bridge (`storage.*`), one row per
// (tenant, user, plugin, key). Values arrive as JSON text with their cost (key
// and encoded value bytes); the quota check and the write share one
// readwrite transaction, so two writes never both slip under the quota.
export async function pluginBundleStorage(encoded) {
  const p = JSON.parse(encoded);
  const db = await new Promise((resolve, reject) => {
    const request = indexedDB.open("flare-plugin-bundle-storage-v1", 1);
    request.onupgradeneeded = () => {
      const store = request.result.createObjectStore("values", {keyPath: "key"});
      store.createIndex("owner", "owner");
      store.createIndex("user", "user");
    };
    request.onerror = () => reject(new Error("bundle storage unavailable"));
    request.onblocked = () => reject(new Error("bundle storage blocked"));
    request.onsuccess = () => resolve(request.result);
  });
  return new Promise((resolve, reject) => {
    const reads = p.operation === "get" || p.operation === "keys";
    let tx;
    try { tx = db.transaction("values", reads ? "readonly" : "readwrite"); }
    catch (e) { db.close(); reject(e); return; }
    const store = tx.objectStore("values");
    const owner = [p.tenantId, p.userId, p.pluginId];
    let result = {};
    tx.oncomplete = () => { db.close(); resolve(JSON.stringify(result)); };
    tx.onerror = tx.onabort = () => { db.close(); reject(new Error("bundle storage transaction failed")); };
    if (p.operation === "get") {
      const request = store.get([...owner, p.key]);
      request.onsuccess = () => { result = request.result ? {value: request.result.value} : {}; };
      return;
    }
    if (p.operation === "remove") {
      const id = [...owner, p.key];
      const request = store.get(id);
      request.onsuccess = () => {
        result = {removed: Boolean(request.result)};
        if (request.result) store.delete(id);
      };
      return;
    }
    if (p.operation === "clear") {
      const request = store.index("user").getAllKeys([p.tenantId, p.userId]);
      request.onsuccess = () => {
        for (const key of request.result) store.delete(key);
        result = {cleared: request.result.length};
      };
      return;
    }
    const request = store.index("owner").getAll(owner);
    request.onsuccess = () => {
      const rows = request.result;
      if (p.operation === "keys") {
        result = {keys: rows.map(row => row.name)
          .filter(name => p.prefix == null || name.startsWith(p.prefix))
          .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))};
        return;
      }
      if (p.operation === "set") {
        let used = 0, replaced = 0;
        for (const row of rows) {
          used += row.cost;
          if (row.name === p.key) replaced = row.cost;
        }
        if (used - replaced + p.cost > p.max) { result = {error: "quota_exceeded"}; return; }
        store.put({key: [...owner, p.key], owner, user: [p.tenantId, p.userId], name: p.key, value: p.value, cost: p.cost});
        result = {};
        return;
      }
      result = {error: "invalid_request"};
    };
  });
}
