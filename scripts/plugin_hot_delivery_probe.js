// Isolation probe for case 7 of plugin_hot_delivery_acceptance.py.
//
// Runs inside an open plugin web bundle page (the sandboxed frame on Web and Tauri, the
// bundle's WebView on Android, iOS and Flutter) and reports what the page could reach. It
// never decides pass or fail: the acceptance script does, from what this returns.
//
// The Web case evaluates it in the frame through the test browser. On a device, paste the
// whole file into the remote inspector console of the bundle page (Android: chrome://inspect
// with a debuggable build; iOS: Safari Web Inspector with the host's `inspectable` on), then
// run `await flareIsolationProbe({...})` with the options the script prints, and save the
// JSON result as the case evidence.
async function flareIsolationProbe(options) {
  const result = { href: String(location.href), origin: String(self.origin) };
  const violations = [];
  document.addEventListener("securitypolicyviolation", (event) => {
    violations.push({ directive: event.effectiveDirective || event.violatedDirective, blocked: event.blockedURI });
  });

  // 1. Any network request from the page: connect-src 'none'.
  try {
    await fetch(options.fetchUrl, { cache: "no-store", credentials: "include" });
    result.fetch = "resolved";
  } catch (error) {
    result.fetch = "rejected:" + (error && error.name);
  }
  await new Promise((resolve) => setTimeout(resolve, 300));
  result.violations = violations.slice();

  // 2. Cookies: an opaque or private origin has none, and keeps none.
  try {
    result.cookie = document.cookie;
    document.cookie = "flare_probe=1; path=/";
    result.cookieAfterWrite = document.cookie;
  } catch (error) {
    result.cookieError = error && error.name;
  }

  // 3. Navigating the top-level document away.
  result.topIsSelf = window.top === window;
  try {
    window.top.location.href = options.topUrl;
    result.topNavigation = "no-throw";
  } catch (error) {
    result.topNavigation = "threw:" + (error && error.name);
  }

  // 4. The bridge, spoken directly (as a hostile page would, without the SDK).
  const transport = (() => {
    const ios = window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.flareBridge;
    if (ios) return { kind: "ios", ios };
    if (window.flareBridge && typeof window.flareBridge.postMessage === "function") {
      return { kind: "android", android: window.flareBridge };
    }
    if (window.parent !== window) return { kind: "frame" };
    return null;
  })();
  result.transport = transport ? transport.kind : null;
  const pending = new Map();
  const deliver = (data) => {
    let envelope = data;
    if (typeof envelope === "string") {
      try {
        envelope = JSON.parse(envelope);
      } catch {
        return;
      }
    }
    if (envelope && envelope.type === "response" && pending.has(envelope.id)) {
      pending.get(envelope.id)(envelope);
      pending.delete(envelope.id);
    }
  };
  if (transport && transport.kind === "frame") {
    window.addEventListener("message", (event) => {
      if (event.source === window.parent) deliver(event.data);
    });
  }
  if (transport && transport.kind === "android") {
    transport.android.addEventListener("message", (event) => deliver(event.data));
  }
  const call = (id, method, params) =>
    new Promise((resolve) => {
      const envelope = { bridge: 1, id, type: "request", method, params };
      pending.set(id, resolve);
      setTimeout(() => {
        if (pending.has(id)) {
          pending.delete(id);
          resolve({ id, timeout: true });
        }
      }, options.timeoutMs || 8000);
      try {
        if (transport.kind === "ios") {
          transport.ios.postMessage(envelope).then(deliver, () => resolve({ id, transportError: true }));
        } else if (transport.kind === "android") {
          transport.android.postMessage(JSON.stringify(envelope));
        } else {
          window.parent.postMessage(envelope, "*");
        }
      } catch (error) {
        pending.delete(id);
        resolve({ id, transportError: String(error && error.name) });
      }
    });
  const outcome = (answer) =>
    answer.timeout ? "timeout" : answer.transportError ? "transport_error" : answer.error ? answer.error.code : "ok";
  if (!transport) {
    result.bridge = "no transport";
    return result;
  }
  const tag = Math.random().toString(36).slice(2, 10);
  result.declared = outcome(
    await call(`acc-${tag}-declared`, "plugin.query", { operation: options.declaredQuery, input: options.declaredInput || {} }),
  );
  result.undeclared = {};
  for (const [index, operation] of (options.undeclaredCommands || []).entries()) {
    result.undeclared[operation] = outcome(
      await call(`acc-${tag}-undeclared-${index}`, "plugin.command", { operation, input: {} }),
    );
  }
  // 5. A burst above the bridge rate (20 requests per second).
  const burst = await Promise.all(
    Array.from({ length: options.burst || 40 }, (_, index) => call(`acc-${tag}-burst-${index}`, "storage.keys", {})),
  );
  result.burst = burst.map(outcome);
  return result;
}
