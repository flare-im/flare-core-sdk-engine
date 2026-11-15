#!/usr/bin/env node
// A test Chromium for the Web cases of plugin_hot_delivery_acceptance.py: one JSON command per
// stdin line, one JSON answer per stdout line. It drives the real Social web app (built against
// the lab) as a lab account; nothing it sees is turned into a verdict here.
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import readline from "node:readline";

const require = createRequire(process.env.FLARE_WEB_APP_PACKAGE);
const { chromium } = require("@playwright/test");

const CATALOG = "flare.plugin.ui.catalog";
const PAGE_TITLE = process.env.FLARE_DASHBOARD_TITLE || "任务看板";
// Kit copy of an upgrade placeholder (pluginUpgrade.title / description / notAvailable, the last
// shown when the update channel offers no newer release; plugin.unavailable).
const UPGRADE_TEXTS = ["需要更新应用", "此内容需要更新到新版本后才能使用", "当前版本暂不支持，新版本发布后可用", "更新应用后查看"];
const DECLARATIVE_SETTLE_MS = 4000;
const probeSource = readFileSync(new URL("./plugin_hot_delivery_probe.js", import.meta.url), "utf8");

const browser = await chromium.launch({
  headless: process.env.FLARE_BROWSER_HEADED !== "1",
  executablePath: process.env.FLARE_PLAYWRIGHT_CHROMIUM || undefined,
});
// The lab's sandbox origin carries a lab certificate: only this test context accepts it.
const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1100, height: 850 } });
const page = await context.newPage();
const consoleLines = [];
page.on("console", (message) => {
  if (message.type() === "error" || message.type() === "warning") {
    consoleLines.push({ type: message.type(), text: message.text().slice(0, 400) });
  }
});
page.on("pageerror", (error) => consoleLines.push({ type: "pageerror", text: String(error).slice(0, 400) }));
let origin = null;
let patch = null;

// Rewrites the catalog the engine reads (case 3): one block is added to the dashboard page.
await page.route("**/api/v1/capabilities/dispatch", async (route) => {
  let body = null;
  try {
    body = route.request().postDataJSON();
  } catch {
    body = null;
  }
  if (!patch || body?.capabilityId !== CATALOG) return route.continue();
  const response = await route.fetch();
  const value = await response.json();
  const catalog = value?.data?.data;
  const plugin = catalog?.plugins?.find((item) => item.pluginId === patch.pluginId);
  const contribution = plugin?.extensions?.contributions?.find((item) => item.id === patch.contributionId);
  patch.seen = { catalog: Boolean(catalog), plugin: Boolean(plugin), contribution: Boolean(contribution) };
  if (contribution) {
    patch.seen.variant = contribution.variant ?? null;
    patch.seen.hadBundle = Boolean(contribution.bundle);
    contribution.schema = contribution.schema || {};
    contribution.schema.blocks = [patch.block, ...(contribution.schema.blocks || [])];
  }
  await route.fulfill({ response, json: value });
});

async function login({ account, passwordFile }) {
  const password = readFileSync(passwordFile, "utf8").trim();
  await page.goto(origin);
  await page.getByPlaceholder("请输入账号").fill(account);
  await page.getByPlaceholder("密码", { exact: true }).fill(password);
  const answer = page.waitForResponse((response) => new URL(response.url()).pathname === "/api/v1/auth/login");
  await page.getByRole("button", { name: "登录", exact: true }).click();
  const status = (await answer).status();
  if (status !== 200) throw new Error(`login answered ${status}`);
  await page.waitForURL(`${origin}/app`, { timeout: 45_000 });
  return { status };
}

function bundleFrameLocator() {
  return page.locator("iframe[sandbox]");
}

async function openDashboard({ timeoutMs = 30_000 } = {}) {
  await page.getByRole("button", { name: "我", exact: true }).click();
  await page.getByRole("button", { name: new RegExp(`^${PAGE_TITLE}`) }).first().click();
  const frame = bundleFrameLocator();
  const heading = page.getByRole("heading", { name: PAGE_TITLE, exact: true });
  const deadline = Date.now() + timeoutMs;
  // The host page draws its heading at once and mounts the bundle frame after the catalog read:
  // the page counts as declarative only once its heading stood that long without a frame.
  let headingSince = null;
  while (Date.now() < deadline) {
    if (await frame.count()) {
      const element = await frame.first().elementHandle();
      const content = await element.contentFrame();
      // The bundle drew something once its app root has children.
      for (let i = 0; i < 40 && content; i++) {
        const drawn = await content.evaluate(() => document.querySelector("#app")?.children.length ?? 0).catch(() => 0);
        if (drawn > 0) break;
        await page.waitForTimeout(250);
      }
      return {
        mode: "bundle",
        sandbox: await element.getAttribute("sandbox"),
        referrerpolicy: await element.getAttribute("referrerpolicy"),
        allow: await element.getAttribute("allow"),
        frameUrl: content ? content.url() : null,
        drawn: content ? await content.evaluate(() => document.querySelector("#app")?.children.length ?? 0).catch(() => 0) : 0,
      };
    }
    if (await heading.count()) {
      headingSince ??= Date.now();
      if (Date.now() - headingSince >= DECLARATIVE_SETTLE_MS) {
        const upgrade = await upgradeVisible();
        return { mode: "declarative", upgrade };
      }
    } else {
      headingSince = null;
    }
    await page.waitForTimeout(250);
  }
  return { mode: "none" };
}

async function upgradeVisible() {
  // The kit's FlarePluginUpgradeNotice marks itself (entry, page, card).
  const notice = page.locator("[data-upgrade-notice]");
  if (await notice.count()) return (await notice.first().innerText()).trim() || "upgrade notice";
  for (const text of UPGRADE_TEXTS) {
    if (await page.getByText(text).count()) return text;
  }
  return null;
}

async function lenient({ pluginId, contributionId, block, marker }) {
  patch = { pluginId, contributionId, block, seen: null };
  try {
    // A fresh catalog read: reload the signed-in app, then open the page.
    await page.goto(`${origin}/app`);
    await page.waitForLoadState("networkidle");
    const opened = await openDashboard();
    await page.waitForTimeout(1500);
    return {
      opened,
      seen: patch.seen,
      upgrade: await upgradeVisible(),
      markerVisible: (await page.getByText(marker).count()) > 0,
      headingVisible: (await page.getByRole("heading", { name: PAGE_TITLE, exact: true }).count()) > 0,
    };
  } finally {
    patch = null;
  }
}

async function bundleFrame() {
  const frame = bundleFrameLocator();
  if (!(await frame.count())) throw new Error("no open bundle frame");
  const content = await (await frame.first().elementHandle()).contentFrame();
  if (!content) throw new Error("bundle frame has no document");
  return content;
}

async function probe(options) {
  const content = await bundleFrame();
  const topBefore = page.url();
  const result = await content.evaluate(`(() => {\n${probeSource}\nreturn flareIsolationProbe(${JSON.stringify(options)});\n})()`);
  await page.waitForTimeout(1500);
  return { result, topBefore, topAfter: page.url(), frameStillOpen: (await bundleFrameLocator().count()) > 0 };
}

async function waitFallback({ timeoutMs = 60_000 }) {
  const started = Date.now();
  const heading = page.getByRole("heading", { name: PAGE_TITLE, exact: true });
  while (Date.now() - started < timeoutMs) {
    if (!(await bundleFrameLocator().count()) && (await heading.count())) {
      return { closedAfterMs: Date.now() - started };
    }
    await page.waitForTimeout(200);
  }
  return { closedAfterMs: null, frameOpen: (await bundleFrameLocator().count()) > 0 };
}

const commands = {
  start: async (input) => {
    origin = input.origin.replace(/\/$/, "");
    return login(input);
  },
  open_dashboard: openDashboard,
  lenient,
  probe,
  wait_fallback: waitFallback,
  screenshot: async ({ path }) => {
    await page.screenshot({ path, fullPage: false });
    return { path };
  },
  console: async () => ({ lines: consoleLines.splice(0) }),
  quit: async () => {
    await browser.close();
    return {};
  },
};

const lines = readline.createInterface({ input: process.stdin });
for await (const line of lines) {
  let command;
  try {
    command = JSON.parse(line);
    const run = commands[command.op];
    if (!run) throw new Error(`unknown op ${command.op}`);
    const answer = await run(command);
    process.stdout.write(JSON.stringify({ ok: true, ...answer }) + "\n");
    if (command.op === "quit") break;
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, error: String(error?.stack || error).slice(0, 1200) }) + "\n");
  }
}
process.exit(0);
