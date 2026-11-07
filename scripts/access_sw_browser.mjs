#!/usr/bin/env node
// A test Chromium for the `web-sw` case of access_acceptance.py: one JSON command per stdin line,
// one JSON answer per stdout line. Its own profile directory; nothing of the system's trust
// settings changes, the private CA is only accepted by this one browser process.
import { createRequire } from "node:module";
import readline from "node:readline";

const require = createRequire(process.env.FLARE_WEB_APP_PACKAGE);
const { chromium } = require("@playwright/test");

const CLOCK_KEY = "lab-clock-offset-ms";
const site = process.env.FLARE_WEB_SITE;
const context = await chromium.launchPersistentContext(process.env.FLARE_BROWSER_PROFILE, {
  headless: true,
  executablePath: process.env.FLARE_PLAYWRIGHT_CHROMIUM || undefined,
  ignoreHTTPSErrors: true,
  serviceWorkers: "allow",
  args: ["--ignore-certificate-errors"],
});
// The update check runs at most every six hours; moving the page's clock is how one browser
// profile gets a second check in a test.
await context.addInitScript((key) => {
  const offset = Number(localStorage.getItem(key) || 0);
  const now = Date.now.bind(Date);
  if (offset) Date.now = () => now() + offset;
}, CLOCK_KEY);

const pages = [];
const consoleLines = [];
const mirrorResponses = [];

function watch(page, index) {
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning" || message.text().includes("[update]")) {
      consoleLines.push({ tab: index, type: message.type(), text: message.text().slice(0, 400) });
    }
  });
  page.on("pageerror", (error) => consoleLines.push({ tab: index, type: "pageerror", text: String(error).slice(0, 400) }));
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/flare/v1/")) mirrorResponses.push({ tab: index, status: response.status(), url });
  });
  page.on("requestfailed", (request) => {
    const url = request.url();
    if (url.includes("/flare/v1/")) mirrorResponses.push({ tab: index, status: 0, url, failure: request.failure()?.errorText });
  });
}

async function snapshot(page) {
  try {
    return await page.evaluate(async () => {
      const registration = await navigator.serviceWorker.getRegistration("/");
      const url = (worker) => worker?.scriptURL ?? null;
      return {
        href: location.href,
        timeOrigin: performance.timeOrigin,
        controller: navigator.serviceWorker.controller?.scriptURL ?? null,
        installing: url(registration?.installing),
        waiting: url(registration?.waiting),
        active: url(registration?.active),
        activeState: registration?.active?.state ?? null,
        caches: await caches.keys(),
        scripts: [...document.scripts].map((script) => script.src).filter(Boolean),
        prompt: document.querySelector(".flare-update-prompt")?.innerText ?? null,
        actions: [...document.querySelectorAll("[data-flare-update-action]")].map((button) => button.dataset.flareUpdateAction),
        bodyText: document.body.innerText.slice(0, 200),
      };
    });
  } catch (error) {
    return { error: String(error).slice(0, 200) };
  }
}

const commands = {
  async open({ count }) {
    for (let index = 0; index < count; index += 1) {
      const page = await context.newPage();
      watch(page, pages.length);
      pages.push(page);
      await page.goto(site, { waitUntil: "load" });
    }
    return { tabs: pages.length };
  },
  async snapshot() {
    return { tabs: await Promise.all(pages.map(snapshot)) };
  },
  async clock({ offsetMs }) {
    await pages[0].evaluate(([key, value]) => localStorage.setItem(key, String(value)), [CLOCK_KEY, offsetMs]);
    return {};
  },
  async reload({ tab }) {
    await pages[tab].reload({ waitUntil: "load" });
    return {};
  },
  async click({ tab, action }) {
    await pages[tab].locator(`[data-flare-update-action="${action}"]`).first().click({ timeout: 5000 });
    return {};
  },
  // The pre-login store as the page sees it: every key with its size, and the text of short values.
  async idb({ tab }) {
    const entries = await pages[tab].evaluate(async () => {
      const db = await new Promise((resolve, reject) => {
        const open = indexedDB.open("flare-access");
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
      });
      if (!db.objectStoreNames.contains("entries")) return [];
      const store = db.transaction("entries", "readonly").objectStore("entries");
      const read = (request) => new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      const [keys, values] = await Promise.all([read(store.getAllKeys()), read(store.getAll())]);
      db.close();
      const digest = async (bytes) => [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))]
        .map((byte) => byte.toString(16).padStart(2, "0")).join("");
      return Promise.all(keys.map(async (key, index) => {
        const value = values[index];
        const bytes = value instanceof Uint8Array ? value : new Uint8Array(value?.buffer ?? value ?? []);
        return { key: String(key), size: bytes.length, sha256: await digest(bytes),
                 text: bytes.length <= 4096 ? new TextDecoder().decode(bytes) : null };
      }));
    });
    return { entries };
  },
  async logs() {
    return { console: consoleLines.slice(-60), mirrorResponses: mirrorResponses.slice(-40), mirrorResponseCount: mirrorResponses.length };
  },
  async screenshot({ tab, path }) {
    await pages[tab].screenshot({ path });
    return {};
  },
  async quit() {
    await context.close();
    return {};
  },
};

const lines = readline.createInterface({ input: process.stdin });
for await (const line of lines) {
  const command = JSON.parse(line);
  let answer;
  try {
    answer = { ok: true, ...(await commands[command.op](command)) };
  } catch (error) {
    answer = { ok: false, error: String(error).slice(0, 400) };
  }
  process.stdout.write(`${JSON.stringify(answer)}\n`);
  if (command.op === "quit") break;
}
process.exit(0);
