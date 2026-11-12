// Plugin capability consents of the signed-in user, one row per
// (tenant, user, plugin, capability). Only grants are stored.
export async function pluginConsentStore(encoded) {
  const p = JSON.parse(encoded);
  const db = await new Promise((resolve, reject) => {
    const request = indexedDB.open("flare-plugin-consents-v1", 1);
    request.onupgradeneeded = () => {
      const store = request.result.createObjectStore("consents", {keyPath: "key"});
      store.createIndex("owner", "owner");
    };
    request.onerror = () => reject(new Error("consent storage unavailable"));
    request.onblocked = () => reject(new Error("consent storage blocked"));
    request.onsuccess = () => resolve(request.result);
  });
  return new Promise((resolve, reject) => {
    let tx;
    try { tx = db.transaction("consents", p.operation === "get" || p.operation === "list" ? "readonly" : "readwrite"); }
    catch (e) { db.close(); reject(e); return; }
    const store = tx.objectStore("consents");
    const owner = [p.tenantId, p.userId];
    const consent = row => ({pluginId: row.pluginId, capability: row.capability, manifestMajor: row.manifestMajor,
      level: row.level, grantedAtMs: row.grantedAtMs});
    let result = null;
    tx.oncomplete = () => { db.close(); resolve(JSON.stringify(result)); };
    tx.onerror = tx.onabort = () => { db.close(); reject(new Error("consent transaction failed")); };
    if (p.operation === "get") {
      const request = store.get([p.tenantId, p.userId, p.pluginId, p.capability]);
      request.onsuccess = () => { result = request.result ? consent(request.result) : null; };
      return;
    }
    if (p.operation === "grant") {
      const c = p.consent;
      store.put({key: [p.tenantId, p.userId, c.pluginId, c.capability], owner, pluginId: c.pluginId,
        capability: c.capability, manifestMajor: c.manifestMajor, level: c.level, grantedAtMs: c.grantedAtMs});
      result = {};
      return;
    }
    const request = store.index("owner").getAll(owner);
    request.onsuccess = () => {
      const rows = request.result
        .filter(row => p.pluginId == null || row.pluginId === p.pluginId)
        .filter(row => p.capability == null || row.capability === p.capability)
        .sort((a, b) => a.pluginId < b.pluginId ? -1 : a.pluginId > b.pluginId ? 1
          : a.capability < b.capability ? -1 : a.capability > b.capability ? 1 : 0);
      if (p.operation === "list") { result = rows.map(consent); return; }
      if (p.operation === "revoke") {
        for (const row of rows) store.delete(row.key);
        result = {revoked: rows.length};
        return;
      }
      result = {error: "invalid_consent"};
    };
  });
}
