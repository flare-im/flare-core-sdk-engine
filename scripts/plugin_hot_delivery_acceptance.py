#!/usr/bin/env python3
"""Acceptance of plugin client hot delivery (variants, upgrade placeholders, lenient reading,
host capabilities, sandboxed web bundles) on the plugin labs of the workspace.

Every automated case drives the real path: Core's catalog selection through the API gateway
with a lab account's IM token, the platform's web bundle review API, the Core registry, the
sandbox file server, and the Social web app in a test Chromium. Each case writes its raw
samples to <lab>/evidence/hot-delivery/<case>.json and fails on the first unmet check; nothing
is adjusted here to make a case pass. A case that needs a device (Tauri, Android, iOS, Flutter)
lists its steps (`manual <case>`), records device facts and screenshots (`capture`) and stays
pending until an operator records a verdict (`verdict`). `report` shows every case.

Stacks (scripts/ of the workspace root):
  online           plugin-online-lab.py up      Core, Social, platform, poll/schedule/announcement/
                                                solitaire/task/call; the task dashboard is the bundle
  wallet-sandbox   wallet-sandbox-lab.py up     wallet and red packet (minClient floors, payment)
  isolated         plugin_hot_delivery_lab.py up (this directory) a lab of its own: own containers, ports,
                                                databases and directory, task + wallet + red packet, the
                                                access control plane with two https mirrors, and the
                                                plugin sandbox server block; every case runs on it
                                                (--profile isolated --lab-dir DIR --binaries DIR)

Order (online unless marked):
  python3 scripts/plugin_hot_delivery_acceptance.py preflight [--web-origin URL]
  python3 scripts/plugin_hot_delivery_acceptance.py variants          # 1  N vs N+1 vocabulary
  python3 scripts/plugin_hot_delivery_acceptance.py --profile wallet-sandbox upgrade   # 2
  python3 scripts/plugin_hot_delivery_acceptance.py lenient --web-origin URL           # 3  before bundle-publish
  python3 scripts/plugin_hot_delivery_acceptance.py realname --artifact web=DIR ...    # 4
  python3 scripts/plugin_hot_delivery_acceptance.py --profile wallet-sandbox payment   # 5
  python3 scripts/plugin_hot_delivery_acceptance.py bundle-publish    # 6  upload, review, apply, enable
  python3 scripts/plugin_hot_delivery_acceptance.py bundle-open --web-origin URL       # 6  Web; devices manual
  python3 scripts/plugin_hot_delivery_acceptance.py isolation --web-origin URL         # 7
  python3 scripts/plugin_hot_delivery_acceptance.py switch-off        # 8  restarts Core and platform, restores
  python3 scripts/plugin_hot_delivery_acceptance.py bundle-withdraw --web-origin URL --yes   # 6  final
  python3 scripts/plugin_hot_delivery_acceptance.py catalog-hygiene   # 9
  python3 scripts/plugin_hot_delivery_acceptance.py report

Preconditions of the bundle cases: the platform has an artifact mirror whose credentials allow the
`platform/artifacts/` prefix (primary uploads) and the bootstrap prefixes (copies); the bootstrap of
the lab app names a web bundle file base served by the flare-deploy plugin sandbox server block
(https, its own origin, frame-ancestors listing --web-origin); the task bundle is built and packed
(flare-plugins-commercial/plugins/task/web: `npm run build && npm run pack`, contract export,
catalog_sync), so the template declares the real digest. The web app runs against the lab:
`VITE_TENANT_ID=<lab app id> npm run dev` in flare-social-examples/apps/flare-social-web-app.
Withdrawing a bundle is final for that plugin version: rerunning the bundle cases needs a new
plugin version.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
LAB_SCRIPT = ROOT / "scripts" / "plugin-online-lab.py"
WALLET_SCRIPT = ROOT / "scripts" / "wallet-sandbox-lab.py"
KIT_VOCABULARY = ROOT / "flare-im-design" / "spec" / "plugin-ui-vocabulary.json"
WEB_APP = ROOT / "flare-social-examples" / "apps" / "flare-social-web-app"
COMMERCIAL = ROOT / "flare-plugins-commercial"
BROWSER = HERE / "plugin_hot_delivery_browser.mjs"
PROBE = HERE / "plugin_hot_delivery_probe.js"

CATALOG_OPERATION = "flare.plugin.ui.catalog"
TASK = "flare.task"
DASHBOARD_PAGE = "flare.task.page.dashboard"
DASHBOARD_BUNDLE = "dashboard"
REDPACKET = "flare.redpacket"
REDPACKET_COMPOSE = "flare.redpacket.page.compose"
WALLET = "flare.wallet"
WALLET_CONFIRM_OPERATION = "flare.wallet.payments.confirm"
WALLET_ORDER_OPERATION = "flare.wallet.payments.order"
WALLET_CONFIRM_PAGE = "flare.wallet.page.confirm"
CAPABILITY_METRICS_PORT = 19189
SANDBOX_POLICY = ("connect-src 'none'", "frame-src 'none'", "worker-src 'none'", "object-src 'none'",
                  "form-action 'none'", "base-uri 'none'")
FRAME_SANDBOX = "allow-scripts allow-forms"
# Page keys and screens of the realname flow the apps used to compile in (case 4).
REALNAME_NEEDLES = [b"flare.realname.page.", b"flare.realname.account", b"RealnameScreen", b"realname_screen",
                    b"@flare-im/realname", b"realnameModule"]
APP_PLATFORMS = ("web", "tauri", "android", "ios", "flutter-android")
# The vocabulary 1 every client knows (flare-plugin-protocol `base_vocabulary()`).
BASE_VOCABULARY = {
    "version": 1,
    "blocks": ["text", "fields", "choices", "entries", "people", "status", "progress", "time", "divider", "note",
               "rows", "protected-media"],
    "inputs": ["text", "password", "textarea", "number", "toggle", "select", "datetime", "options", "members",
               "file"],
    "formats": ["money", "enum", "person", "reference", "time"],
    "actions": ["operation", "page", "native"],
    "native": ["selectImage", "selectFile", "camera", "microphone", "clipboardWrite"],
    "renderers": [],
    "kinds": ["message-content", "message-action", "conversation-menu", "composer-action", "page", "account-entry"],
}
SETS = ("blocks", "inputs", "formats", "actions", "native", "renderers", "kinds")
WEB_RUNTIME = {"bridge": 1, "engine": "chromium", "engineMajor": 124}
ANDROID_RUNTIME = {"bridge": 1, "engine": "webview", "engineMajor": 124}


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


class CaseFailed(RuntimeError):
    pass


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ------------------------------------------------------------------ stack --

class Stack:
    """One running lab: its module (ports, helpers), keys, state, accounts and evidence."""

    def __init__(self, profile, wallet_directory, isolated=None):
        self.profile = profile
        if profile == "isolated":
            # One lab of its own (plugin_hot_delivery_lab.py) with task, wallet and red packet:
            # every case runs on it.
            isolated_lab = load_module("plugin_hot_delivery_lab", HERE / "plugin_hot_delivery_lab.py")
            isolated_lab.configure(isolated.lab_dir, isolated.binaries, offset=isolated.port_offset,
                                   block=isolated.block, lab_script=isolated.lab_script)
            self.lab = isolated_lab.lab
            self.metrics_port = isolated_lab.settings.metrics(CAPABILITY_METRICS_PORT)
            global LAB_CA
            LAB_CA = isolated_lab.ca_file()
        elif profile == "wallet-sandbox":
            wallet = load_module("wallet_sandbox_lab", WALLET_SCRIPT)
            wallet.configure(Path(wallet_directory).expanduser())
            self.lab = wallet.lab
            self.metrics_port = wallet.PORTS.get(CAPABILITY_METRICS_PORT, CAPABILITY_METRICS_PORT)
        else:
            self.lab = load_module("plugin_online_lab", LAB_SCRIPT)
            self.metrics_port = CAPABILITY_METRICS_PORT
        lab = self.lab
        state_file = lab.lab_path("state.json")
        if not state_file.exists():
            raise SystemExit(f"no lab state at {state_file}: bring the {profile} lab up first")
        self.keys = lab.Secrets()
        self.state = lab.load_state()
        self.tenant = self.state.get("app_id")
        if not self.tenant:
            raise SystemExit(f"the {profile} lab has no app yet (state without app_id)")
        self._accounts = None
        self.evidence_dir = lab.lab_path("evidence", "hot-delivery")
        self.evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.evidence_dir / "manual").mkdir(exist_ok=True, mode=0o700)

    @property
    def accounts(self):
        """alice, bob, carol: the lab's own accounts, signed in on the lab CLI devices."""
        if self._accounts is None:
            lab = self.lab
            self._accounts = [lab.ensure_account(self.state, self.keys, self.tenant, *lab.ACCOUNTS[i], f"lab-cli-{n}")
                              for i, n in enumerate(("alice", "bob", "carol"))]
        return self._accounts

    def core_url(self, path):
        return f"http://127.0.0.1:{self.lab.CORE_PORTS['api-gateway']}{path}"

    def catalog(self, account, descriptor):
        """Core's catalog for one client descriptor, as the engine reads it: (status, data)."""
        body = {"capabilityId": CATALOG_OPERATION, "requestId": f"phd-{uuid.uuid4().hex}",
                "expectedTenantId": self.tenant, "expectedUserId": account["user_id"],
                "payload": {"descriptor": descriptor}}
        status, value, _ = self.lab.http("POST", self.core_url("/api/v1/capabilities/dispatch"), body,
                                         {"Authorization": "Bearer " + account["im_token"]})
        data = value.get("data") if isinstance(value, dict) else None
        if status != 200 or not isinstance(data, dict) or not data.get("success"):
            return status, value
        return status, data.get("data")

    def metrics(self):
        status, value, _ = self.lab.http("GET", f"http://127.0.0.1:{self.metrics_port}/metrics")
        if status != 200 or not isinstance(value, str):
            raise CaseFailed(f"capability metrics on :{self.metrics_port} answered {status}")
        selection, descriptors = {}, {}
        for line in value.splitlines():
            match = re.match(r'^plugin_catalog_selection_total\{outcome="([a-z_]+)"\}\s+([0-9.eE+]+)$', line)
            if match:
                selection[match[1]] = float(match[2])
            match = re.match(r'^plugin_catalog_descriptor_total\{descriptor_hash="([a-z0-9]+)"\}\s+([0-9.eE+]+)$', line)
            if match:
                descriptors[match[1]] = float(match[2])
        return {"selection": selection, "descriptors": descriptors}

    def admin(self):
        return self.lab.platform_admin(self.keys)

    def staff(self, username):
        """A second platform staff member (platform_admin role), for four-eyes review."""
        lab, admin = self.lab, self.admin()
        initial = lab.random_secret(f"staff-{username}-initial", lambda: "Lab-" + uuid.uuid4().hex[:20])
        password = lab.random_secret(f"staff-{username}-password", lambda: "Lab-" + uuid.uuid4().hex[:20])
        status, listing = admin.call("GET", f"/rbac/employees?keyword={username}&limit=20&offset=0", expected=None)
        if status != 200:
            raise CaseFailed(f"list employees: {status} {listing}")
        found = [row for row in listing["data"] if row["username"] == username]
        if not found:
            _, created = admin.call("POST", "/rbac/employees", {"username": username, "display_name": username,
                                                                "password": initial}, reason="bundle reviewer")
            employee = created["data"]["id"]
            _, roles = admin.call("GET", "/rbac/roles")
            role = next(r for r in roles["data"] if r.get("builtin_key") == "platform_admin")
            admin.call("POST", f"/rbac/employees/{employee}/roles", {"role_id": role["id"]}, reason="bundle reviewer")
        return lab.PlatformApi(username, initial, password, lab.lab_path("secrets", f"{username}.pat"),
                               token_name="hot-delivery")

    def evidence(self, name, value):
        file = self.evidence_dir / f"{name}.json"
        self.lab.write_private(file, json.dumps(value, indent=1, ensure_ascii=False, sort_keys=True, default=str))
        return file


# ------------------------------------------------------------- descriptors --

def kit_vocabulary(kit):
    spec = json.loads(KIT_VOCABULARY.read_text())
    vocabulary = {key: list(value) if isinstance(value, list) else value for key, value in spec["vocabulary"].items()}
    omit = spec.get("kits", {}).get(kit, {}).get("omit", {})
    for key in SETS:
        vocabulary[key] = sorted(set(vocabulary.get(key, [])) - set(omit.get(key, [])))
    return vocabulary


def descriptor(vocabulary, platform="web", build=1, distribution="web", bundle=None):
    value = {"vocabulary": vocabulary["version"], "platform": platform, "appBuild": build,
             "distribution": distribution}
    for key in SETS:
        value[key] = sorted(vocabulary.get(key, []))
    if bundle:
        value["bundle"] = bundle
    return value


def plugin_of(catalog, plugin_id):
    return next((p for p in (catalog or {}).get("plugins", []) if p.get("pluginId") == plugin_id), None)


def contribution_of(catalog, plugin_id, contribution_id):
    plugin = plugin_of(catalog, plugin_id) or {}
    return next((c for c in (plugin.get("extensions") or {}).get("contributions", []) if c.get("id") == contribution_id),
                None)


def contribution_summary(contribution):
    if contribution is None:
        return None
    schema = contribution.get("schema") or {}
    return {"id": contribution.get("id"), "kind": contribution.get("kind"), "variant": contribution.get("variant"),
            "upgrade": contribution.get("upgrade"), "bundle": contribution.get("bundle"),
            "blocks": [b.get("kind") for b in schema.get("blocks") or []],
            "actions": [a.get("id") for a in schema.get("actions") or []]}


def web_bundles_of(catalog, plugin_id):
    return ((plugin_of(catalog, plugin_id) or {}).get("extensions") or {}).get("webBundles") or []


# ------------------------------------------------------------------- cases --

class Case:
    """Steps and checks of one case, written to evidence/hot-delivery/<name>.json whatever happens;
    cleanups run in reverse order in `finally`."""

    def __init__(self, stack, name):
        self.stack, self.name = stack, name
        self.record = {"case": name, "profile": stack.profile, "tenant": stack.tenant, "started": time.time(),
                       "steps": [], "checks": []}
        self.cleanups = []

    def step(self, label, detail):
        self.record["steps"].append({"step": label, "at": time.time(), "detail": detail})
        log(f"{self.name}: {label}")

    def check(self, label, condition, detail=None):
        self.record["checks"].append({"check": label, "ok": bool(condition), "detail": detail})
        shown = json.dumps(detail, ensure_ascii=False, default=str)[:600]
        if not condition:
            raise CaseFailed(f"{label}: {shown}")
        log(f"PASS {self.name}: {label}")

    def run(self, body):
        ok = False
        try:
            body(self)
            ok = True
        except CaseFailed as error:
            self.record["error"] = str(error)
            log(f"FAIL {self.name}: {error}")
        except Exception as error:  # A broken step is a failed case, with its trace as evidence.
            self.record["error"] = f"{type(error).__name__}: {error}"
            self.record["trace"] = traceback.format_exc()[-4000:]
            log(f"FAIL {self.name}: {type(error).__name__}: {error}")
        finally:
            for cleanup in reversed(self.cleanups):
                try:
                    cleanup()
                except Exception as error:  # Reported, never hidden: a lab left changed is a finding.
                    ok = False
                    self.record.setdefault("cleanup_errors", []).append(f"{type(error).__name__}: {error}")
                    log(f"cleanup of {self.name} failed: {error}")
            self.record["ok"] = ok
            self.record["finished"] = time.time()
            file = self.stack.evidence(self.name, self.record)
            log(f"{'PASS' if ok else 'FAIL'} {self.name} -> {file}")
        return ok


class Browser:
    """The test Chromium of plugin_hot_delivery_browser.mjs, signed in as alice."""

    def __init__(self, stack, origin):
        if not origin:
            raise CaseFailed("this case drives the Social web app: pass --web-origin (the app built against the lab)")
        package = WEB_APP / "package.json"
        env = {**os.environ, "FLARE_WEB_APP_PACKAGE": str(package),
               "NO_PROXY": "localhost,127.0.0.1,::1", "no_proxy": "localhost,127.0.0.1,::1"}
        self.process = subprocess.Popen(["node", str(BROWSER)], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        text=True, bufsize=1)
        self.stack = stack
        self.ask("start", origin=origin, account=stack.lab.ACCOUNTS[0][0],
                 passwordFile=str(stack.lab.lab_path("secrets", "account-password")))

    def ask(self, op, **arguments):
        self.process.stdin.write(json.dumps({"op": op, **arguments}) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise CaseFailed(f"browser exited during {op}")
        answer = json.loads(line)
        if not answer.get("ok"):
            raise CaseFailed(f"browser {op}: {answer.get('error')}")
        return answer

    def screenshot(self, name):
        return self.ask("screenshot", path=str(self.stack.evidence_dir / f"{name}.png"))["path"]

    def quit(self):
        try:
            self.ask("quit")
            self.process.wait(timeout=20)
        except (OSError, ValueError, CaseFailed, subprocess.TimeoutExpired):
            self.process.kill()


def open_browser(case, origin):
    browser = Browser(case.stack, origin)
    case.cleanups.append(browser.quit)
    return browser


def require_profile(stack, profile):
    if stack.profile != profile and stack.profile != "isolated":
        raise SystemExit(f"this case runs on the {profile} lab: pass --profile {profile}")


# Case 1 -------------------------------------------------------------------

def case_variants(case):
    stack = case.stack
    require_profile(stack, "online")
    alice = stack.accounts[0]
    before = stack.metrics()
    old = descriptor(BASE_VOCABULARY, build=1)
    new = descriptor(kit_vocabulary("vue"), build=2)
    answers = {}
    for label, value in (("vocabulary-1", old), ("vocabulary-2", new)):
        status, catalog = stack.catalog(alice, value)
        case.check(f"{label}: Core answers the catalog", status == 200 and isinstance(catalog, dict),
                   {"status": status, "answer": catalog if not isinstance(catalog, dict) else None})
        answers[label] = catalog
        case.step(f"{label} catalog", {"descriptor": value,
                                        "dashboard": contribution_summary(contribution_of(catalog, TASK, DASHBOARD_PAGE))})
    first = contribution_of(answers["vocabulary-1"], TASK, DASHBOARD_PAGE)
    second = contribution_of(answers["vocabulary-2"], TASK, DASHBOARD_PAGE)
    case.check("vocabulary 1 gets the rows variant", first and first.get("variant") == "basic"
               and not first.get("upgrade") and not first.get("bundle"), contribution_summary(first))
    case.check("vocabulary 2 gets the charts variant", second and second.get("variant") == "charts"
               and not second.get("upgrade") and not second.get("bundle"), contribution_summary(second))
    case.check("the charts variant carries the new blocks, the rows variant none of them",
               {"section", "stats", "chart"} & set(contribution_summary(second)["blocks"] + nested_blocks(second))
               and not {"section", "stats", "chart"} & set(contribution_summary(first)["blocks"]),
               {"rows": contribution_summary(first), "charts": contribution_summary(second)})
    cards = {label: [c for c in (plugin_of(catalog, TASK) or {}).get("extensions", {}).get("contributions", [])
                     if c.get("kind") == "message-content"] for label, catalog in answers.items()}
    case.check("both clients get the task card, neither as an upgrade placeholder",
               all(items and not any(c.get("upgrade") for c in items) for items in cards.values()),
               {label: [contribution_summary(c) for c in items] for label, items in cards.items()})
    after = stack.metrics()
    variant = after["selection"].get("variant", 0) - before["selection"].get("variant", 0)
    hashes = [h for h, v in after["descriptors"].items() if v > before["descriptors"].get(h, 0)]
    case.step("selection metrics", {"before": before, "after": after})
    case.check("Core counted the variant selections", variant >= 2, {"variant_delta": variant})
    case.check("Core counted two descriptor hashes", len(hashes) >= 2, {"increased": hashes})

    # The card both clients draw is one stored payload: Core posts it once, whatever the reader.
    lab = stack.lab
    proof = lab.Proof(stack.keys, stack.tenant, lab.SPECS["task"], stack.accounts, stack.state["group"]["conversation_id"],
                      stack.state["conversation_id"], stack.state, name="hot-delivery-variants")
    created = proof.step("alice creates a task for bob and carol", "alice", "create",
                         {"title": f"热交付验收 {proof.run_id}", "assigneeIds": [stack.accounts[1]["user_id"],
                                                                                stack.accounts[2]["user_id"]],
                          "commandId": f"{proof.run_id}-create"}, proof.group)
    card = proof.posted_card(created, created["task"]["taskId"], proof.group)
    payloads = {}
    for index, name in ((1, "bob"), (2, "carol")):
        message = lab.read_message(stack.keys, stack.tenant, stack.accounts[index], proof.group, card["seq"])
        payloads[name] = lab.card_of(message)
    case.step("card payload per reader", {"server_msg_id": card["server_msg_id"], "payloads": payloads})
    case.check("the card payload is the same for every reader and names a card contribution of both catalogs",
               payloads["bob"] is not None and payloads["bob"] == payloads["carol"]
               and payloads["bob"].get("contributionId") in {c["id"] for c in cards["vocabulary-1"]}
               and payloads["bob"].get("contributionId") in {c["id"] for c in cards["vocabulary-2"]},
               {"contribution": (payloads["bob"] or {}).get("contributionId")})


def nested_blocks(contribution):
    """Kinds of the blocks inside sections."""
    found = []
    for block in ((contribution or {}).get("schema") or {}).get("blocks") or []:
        found += [child.get("kind") for child in block.get("blocks") or []]
    return found


# Case 2 -------------------------------------------------------------------

def case_upgrade(case):
    stack = case.stack
    require_profile(stack, "wallet-sandbox")
    account = stack.accounts[0]
    before = stack.metrics()
    vocabulary = kit_vocabulary("vue")
    _, old = stack.catalog(account, descriptor(vocabulary, build=0))
    _, new = stack.catalog(account, descriptor(vocabulary, build=1))
    case.check("both builds get a catalog", isinstance(old, dict) and isinstance(new, dict), {"old": old, "new": new})
    placeholder = contribution_of(old, REDPACKET, REDPACKET_COMPOSE)
    served = contribution_of(new, REDPACKET, REDPACKET_COMPOSE)
    case.step("compose page per build", {"build-0": placeholder, "build-1": contribution_summary(served)})
    upgrade = (placeholder or {}).get("upgrade") or {}
    case.check("build 0 gets an upgrade placeholder naming the floor (minClient web 1)",
               upgrade.get("minBuild") == 1 and not upgrade.get("unsupported"), upgrade)
    schema = (placeholder or {}).get("schema") or {}
    case.check("the placeholder carries the title only: no blocks, actions or bundle",
               schema.get("title") and not schema.get("blocks") and not schema.get("actions")
               and not placeholder.get("bundle"), schema)
    case.check("build 1 gets the page itself", served and not served.get("upgrade")
               and (served.get("schema") or {}).get("blocks"), contribution_summary(served))
    after = stack.metrics()
    delta = after["selection"].get("upgrade", 0) - before["selection"].get("upgrade", 0)
    case.check("Core counted the upgrade selection", delta >= 1, {"before": before["selection"],
                                                                  "after": after["selection"]})
    case.step("client steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual upgrade"})


# Case 3 -------------------------------------------------------------------

def case_lenient(case, args):
    stack = case.stack
    require_profile(stack, "online")
    browser = open_browser(case, args.web_origin)
    marker = f"PHD-LENIENT-{uuid.uuid4().hex[:6]}"
    block = {"kind": "acceptance-future-block", "text": marker}
    optional = browser.ask("lenient", pluginId=TASK, contributionId=DASHBOARD_PAGE,
                           block={**block, "required": False}, marker=marker)
    case.step("unknown optional block injected", optional)
    browser.screenshot("lenient-optional")
    seen = optional.get("seen") or {}
    case.check("the catalog read carried the dashboard page and was rewritten", seen.get("contribution"), seen)
    case.check("the dashboard was declarative when injected (run before bundle-publish or after bundle-withdraw)",
               not seen.get("hadBundle"), seen)
    case.check("an unknown optional block is skipped: the page shows, without the block and without an upgrade",
               optional["opened"]["mode"] == "declarative" and optional["headingVisible"]
               and not optional["markerVisible"] and not optional["upgrade"], optional)
    required = browser.ask("lenient", pluginId=TASK, contributionId=DASHBOARD_PAGE,
                           block={**block, "required": True}, marker=marker)
    case.step("unknown required block injected", required)
    browser.screenshot("lenient-required")
    case.check("an unknown required block turns the page into an upgrade notice",
               required["upgrade"] and not required["markerVisible"], required)


# Case 4 -------------------------------------------------------------------

def scan_artifact(path):
    """Needle hits in a directory, a file, or the entries of an .apk/.ipa/.zip/.aab."""
    hits, scanned = [], 0
    path = Path(path)

    def scan_bytes(label, data):
        nonlocal scanned
        scanned += 1
        for needle in REALNAME_NEEDLES:
            if needle in data:
                hits.append({"file": label, "needle": needle.decode()})

    files = [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]
    for file in files:
        data = file.read_bytes()
        if file.suffix in {".apk", ".ipa", ".zip", ".aab", ".jar"} and zipfile.is_zipfile(file):
            with zipfile.ZipFile(file) as archive:
                for entry in archive.infolist():
                    if not entry.is_dir():
                        scan_bytes(f"{file}!{entry.filename}", archive.read(entry))
        else:
            scan_bytes(str(file), data)
    return hits, scanned


def run_tests(case, label, command, cwd):
    env = {**os.environ, "RUSTC_WRAPPER": ""}
    started = time.time()
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=3600)
    output = (result.stdout + result.stderr)[-6000:]
    ran = sum(int(n) for n in re.findall(r"^running (\d+) tests?$", result.stdout, re.M))
    case.step(label, {"command": command, "cwd": str(cwd), "exit": result.returncode, "ran": ran,
                      "seconds": round(time.time() - started), "output": output})
    case.check(f"{label}: tests ran and passed", result.returncode == 0 and ran >= 1,
               {"exit": result.returncode, "ran": ran})


def case_realname(case, args):
    artifacts = dict(item.split("=", 1) for item in args.artifact or [])
    missing = [p for p in APP_PLATFORMS if p not in artifacts]
    case.check("an app artifact is given for every client", not missing, {"missing": missing,
                                                                          "expected": list(APP_PLATFORMS)})
    for platform, path in artifacts.items():
        case.check(f"{platform}: artifact exists", Path(path).exists(), path)
        hits, scanned = scan_artifact(path)
        case.step(f"{platform} scan", {"path": path, "files": scanned, "hits": hits})
        case.check(f"{platform}: no realname page key or screen in the build", scanned and not hits,
                   {"files": scanned, "hits": hits[:20]})
    contract = COMMERCIAL / "metaservices" / "realname" / "contract"
    run_tests(case, "realname pages serve the four states declaratively to a vocabulary 1 client",
              ["cargo", "test", "--test", "user_pages"], contract)
    case.step("device steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual realname"})


# Case 5 -------------------------------------------------------------------

def case_payment(case):
    stack = case.stack
    require_profile(stack, "wallet-sandbox")
    account = stack.accounts[0]
    _, catalog = stack.catalog(account, descriptor(kit_vocabulary("vue"), build=1, bundle=WEB_RUNTIME))
    wallet = plugin_of(catalog, WALLET)
    case.check("the wallet is in the catalog", wallet, {"plugins": [p.get("pluginId") for p in (catalog or {})
                                                                     .get("plugins", [])]})
    transient = set(wallet.get("transientOperations") or [])
    case.step("wallet client plugin", {"transientOperations": sorted(transient),
                                       "operations": sorted(wallet.get("operations") or []),
                                       "webBundles": web_bundles_of(catalog, WALLET)})
    case.check("payment confirmation is a transient operation the client is told about",
               WALLET_CONFIRM_OPERATION in transient or "payments.confirm" in transient, sorted(transient))
    case.check("the wallet declares the host's paymentConfirmation",
               "paymentConfirmation" in (wallet["extensions"].get("nativeCapabilities") or []),
               wallet["extensions"].get("nativeCapabilities"))
    confirm = contribution_of(catalog, WALLET, WALLET_CONFIRM_PAGE)
    schema = (confirm or {}).get("schema") or {}
    pay = next((a for a in schema.get("actions") or [] if a.get("id") == "pay"), None)
    case.step("wallet confirm page", {"pay": pay, "inputs": [i.get("kind") for i in schema.get("inputs") or []]})
    case.check("paying asks for the order only: the engine takes it to the host's confirmation, whose sheet draws "
               "the amount and takes the password", pay and (pay.get("target") or {}).get("operation")
               == WALLET_ORDER_OPERATION, pay)
    case.check("the page itself has no password input", not any(i.get("kind") == "password"
                                                                 for i in schema.get("inputs") or []),
               schema.get("inputs"))
    pages = [c for c in wallet["extensions"].get("contributions", []) if c.get("kind") == "page"]
    case.check("no wallet contribution is served as a web bundle", not any(c.get("bundle") for c in pages)
               and not web_bundles_of(catalog, WALLET), [contribution_summary(c) for c in pages])
    protocol = ROOT / "flare-plugin-devkit" / "flare-plugin-protocol"
    run_tests(case, "a bundle.json naming a transient operation is refused and the grant never includes one",
              ["cargo", "test", "--test", "web_bundles", "--",
               "bundle_json_asks_for_manifest_operations_and_declared_capabilities",
               "the_grant_is_the_intersection_and_every_overreach_is_denied"], protocol)
    run_tests(case, "the engine broker refuses a transient operation sent through plugin.command",
              ["cargo", "test", "--lib", "the_bridge_privilege_table"], ROOT / "flare-core-sdk-engine")
    case.step("device steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual payment"})


# Case 6 -------------------------------------------------------------------

def task_release(stack):
    lab = stack.lab
    spec = lab.SPECS["task"]
    version = lab.template_version(spec)
    return spec, version, lab.template_manifest(spec)


def declared_bundle(manifest):
    return next((b for b in (manifest.get("clientExtensions") or {}).get("webBundles") or []
                 if b.get("id") == DASHBOARD_BUNDLE), None)


def flare_plugin_cli(args):
    for candidate in (args.cli, shutil.which("flare-plugin"), ROOT / "target" / "debug" / "flare-plugin",
                      getattr(args, "binaries", None) and Path(args.binaries) / "flare-plugin",
                      ROOT / "flare-plugin-devkit" / "target" / "debug" / "flare-plugin"):
        if candidate and Path(candidate).exists():
            return str(candidate)
    return None


def platform_ok(case, label, outcome):
    status, value = outcome
    case.check(label, status == 200 and isinstance(value, dict) and "data" in value, {"status": status,
                                                                                      "answer": value})
    return value["data"]


def bundle_rows(admin, plugin, version):
    status, value = admin.call("GET", f"/plugins/catalog/{plugin}/versions/{version}/web-bundles", expected=None)
    return status, (value or {}).get("data") if isinstance(value, dict) else value


def bundle_view(admin, bundle_id):
    status, value = admin.call("GET", f"/plugin-web-bundles/{bundle_id}", expected=None)
    data = (value or {}).get("data") if isinstance(value, dict) else None
    return status, (data.get("bundle", data) if isinstance(data, dict) else data), data


def core_registry_row(stack, plugin, version):
    lab = stack.lab
    raw = lab.psql("SELECT coalesce(json_agg(json_build_object('bundleId',bundle_id,'sha256',sha256,'size',size,"
                   "'approvedAt',approved_at,'withdrawnAt',withdrawn_at)),'[]'::json) FROM plugin_web_bundles "
                   f"WHERE plugin_id={lab.literal(plugin)} AND plugin_version={lab.literal(version)}",
                   lab.DATABASES["core"], check=False)
    rows = json.loads(raw or "[]")
    return next((r for r in rows if r["bundleId"] == DASHBOARD_BUNDLE), None)


# The private CA of a lab whose mirrors present certificates of their own (the isolated lab).
LAB_CA = None


def loopback(host):
    return host in ("127.0.0.1", "localhost", "::1") or (host or "").endswith(".localhost")


def put_presigned(request, body):
    """The uploader's step, exactly as the platform handed it out. Only a loopback lab mirror
    may present the lab's own certificate: verified against the lab's CA when it has one."""
    url = urllib.parse.urlsplit(request["url"])
    context = ssl.create_default_context()
    if loopback(url.hostname):
        if LAB_CA:
            context = ssl.create_default_context(cafile=str(LAB_CA))
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
    item = urllib.request.Request(request["url"], data=body, method=request.get("method") or "PUT",
                                  headers=request.get("headers") or {})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
    try:
        with opener.open(item, timeout=180) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code
    except OSError as error:
        return f"{type(error).__name__}: {error}"


def fetch(url, method="GET", dest=None):
    """A sandbox or mirror read with the lab certificate accepted (loopback test only). `dest` is
    the Sec-Fetch-Dest a browser sends: the sandbox serves HTML to a frame load only."""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
    headers = {"Origin": "null", **({"Sec-Fetch-Dest": dest} if dest else {})}
    item = urllib.request.Request(url, method=method, headers=headers)
    try:
        with opener.open(item, timeout=30) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()
    except OSError as error:
        return 0, {}, str(error).encode()


def case_bundle_publish(case, args):
    stack = case.stack
    require_profile(stack, "online")
    lab = stack.lab
    spec, version, manifest = task_release(stack)
    declared = declared_bundle(manifest)
    case.check("the task template declares the dashboard bundle with a real digest",
               declared and declared.get("sha256") != "0" * 64 and declared.get("size", 0) > 1, declared)
    archive = Path(args.bundle or COMMERCIAL / "plugins" / "task" / "web" / f"{TASK}-{version}-{DASHBOARD_BUNDLE}.fwb")
    case.check("the packed bundle exists", archive.exists(), str(archive))
    body = archive.read_bytes()
    case.check("the bundle bytes are the ones the manifest declares",
               hashlib.sha256(body).hexdigest() == declared["sha256"] and len(body) == declared["size"],
               {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body), "declared": declared})
    cli = flare_plugin_cli(args)
    case.check("the flare-plugin command is available (--cli)", cli, None)
    with tempfile.TemporaryDirectory(prefix="phd-plugin-") as directory:
        Path(directory, "manifest.json").write_text(json.dumps(manifest))
        verified = subprocess.run([cli, "bundle", "verify", str(archive), "--plugin", directory], capture_output=True,
                                  text=True, timeout=120)
    case.step("flare-plugin bundle verify", {"exit": verified.returncode, "output": (verified.stdout + verified.stderr)[-3000:]})
    case.check("the platform rules accept the archive against the manifest", verified.returncode == 0,
               verified.returncode)

    admin = stack.admin()
    release = lab.signed_release(stack.state, stack.keys, version, spec)
    lab.register_release(admin, release)
    status, rows = bundle_rows(admin, TASK, version)
    case.check("the platform lists the version's declared bundle", status == 200 and isinstance(rows, list),
               {"status": status, "rows": rows})
    row = next((r for r in rows if r.get("bundle_id") == DASHBOARD_BUNDLE), None)
    case.step("bundle row before", row)
    case.check("the bundle is not withdrawn (withdrawal is final for a version)",
               (row or {}).get("status") not in ("withdrawn", "rejected"), row)
    reviewer = stack.staff("phd_bundle_reviewer")
    if (row or {}).get("status") != "approved":
        if (row or {}).get("status") not in ("verified", "in_review"):
            initiated = platform_ok(case, "the platform opens an upload of the declared bundle", admin.call(
                "POST", "/plugin-web-bundles/initiate", {"plugin_id": TASK, "plugin_version": version,
                                                          "bundle_id": DASHBOARD_BUNDLE},
                expected=None, reason="hot delivery acceptance upload"))
            bundle_id = initiated["bundle"]["id"]
            upload = initiated["upload"]
            sent = put_presigned(upload["uploadRequest"], body)
            case.step("presigned upload to the mirror", {"status": sent, "url": urllib.parse.urlsplit(
                upload["uploadRequest"]["url"]).path})
            case.check("the mirror accepts the upload (credentials allow platform/artifacts/)", sent == 200, sent)
            platform_ok(case, "the platform takes the completed upload", admin.call(
                "POST", f"/plugin-web-bundles/{bundle_id}/complete", {"upload_id": upload["upload"]["id"]},
                expected=None, reason="hot delivery acceptance upload"))
        else:
            bundle_id = row["id"]
        deadline = time.time() + 300
        while time.time() < deadline:
            _, current, review = bundle_view(admin, bundle_id)
            if (current or {}).get("status") in ("verified", "in_review", "approved") or (current or {}).get("failure"):
                break
            time.sleep(2)
        case.step("read-back and check", review)
        case.check("the read-back verified the archive", (current or {}).get("status") in ("verified", "in_review")
                   and not current.get("failure"), current)
        case.check("the reviewer sees the bridge permissions and operations of bundle.json",
                   set(review.get("bridge_permissions") or []) == {"plugin.query", "plugin.command", "people.resolve",
                                                                   "storage"}
                   and "flare.task.dashboard.get" in (review.get("operations") or []), review)
        if current["status"] == "verified":
            current = platform_ok(case, "the uploader submits it for review", admin.call(
                "POST", f"/plugin-web-bundles/{bundle_id}/submit-review", {"revision": current["revision"]},
                expected=None, reason="hot delivery acceptance review"))
        status, refused = admin.call("POST", f"/plugin-web-bundles/{bundle_id}/approve",
                                     {"revision": current["revision"], "comment": "self review"}, expected=None,
                                     reason="hot delivery acceptance review")
        case.check("the uploader cannot approve their own bundle", status == 409, {"status": status,
                                                                                  "answer": refused})
        current = platform_ok(case, "a second reviewer approves it", reviewer.call(
            "POST", f"/plugin-web-bundles/{bundle_id}/approve",
            {"revision": current["revision"], "comment": "acceptance: bundle.json, warnings and preview checked"},
            expected=None, reason="hot delivery acceptance review"))
        case.check("the bundle is approved", current.get("status") == "approved", current)
    registry = core_registry_row(stack, TASK, version)
    case.check("Core's registry holds the approved digest", registry and registry["sha256"] == declared["sha256"]
               and registry["approvedAt"] and not registry["withdrawnAt"], registry)

    # Request, approval and enable of the version for the lab app; the approval carries the web code.
    lab.deploy_plugin(stack.state, stack.keys, stack.tenant, spec)
    alice = stack.accounts[0]
    web = descriptor(kit_vocabulary("vue"), build=2, bundle=WEB_RUNTIME)
    android = descriptor(kit_vocabulary("android"), platform="android", build=2, distribution="play",
                         bundle=ANDROID_RUNTIME)

    def served():
        _, catalog = stack.catalog(alice, web)
        page = contribution_of(catalog, TASK, DASHBOARD_PAGE)
        return catalog if page and page.get("variant") == "bundle" and web_bundles_of(catalog, TASK) else None

    catalog = lab.wait_for(served, 300, "the dashboard bundle in the web catalog (copy, projection)", 3)
    entry = next(b for b in web_bundles_of(catalog, TASK) if b["id"] == DASHBOARD_BUNDLE)
    case.step("web catalog entry", {"entry": entry,
                                    "page": contribution_summary(contribution_of(catalog, TASK, DASHBOARD_PAGE))})
    case.check("the web entry carries the digest, https mirrors, the https file base and approved permissions",
               entry["sha256"] == declared["sha256"] and 1 <= len(entry.get("urls") or []) <= 8
               and all(u.startswith("https://") for u in entry["urls"])
               and str(entry.get("files", "")).startswith("https://") and entry["files"].endswith("/")
               and set(entry.get("permissions") or []) <= {"plugin.query", "plugin.command", "people.resolve",
                                                           "storage"}, entry)
    _, native = stack.catalog(alice, android)
    native_entry = next((b for b in web_bundles_of(native, TASK) if b["id"] == DASHBOARD_BUNDLE), None)
    case.check("the Android entry has mirrors and no file base", native_entry and native_entry.get("urls")
               and not native_entry.get("files"), native_entry)
    _, plain = stack.catalog(alice, descriptor(kit_vocabulary("vue"), build=2))
    page = contribution_of(plain, TASK, DASHBOARD_PAGE)
    case.check("a client without a bundle runtime still gets charts and no bundle entry",
               page and page.get("variant") == "charts" and not web_bundles_of(plain, TASK), contribution_summary(page))

    status, _, mirrored = fetch(entry["urls"][0])
    case.check("the first mirror serves the declared bytes", status == 200
               and hashlib.sha256(mirrored).hexdigest() == declared["sha256"], {"status": status})
    status, headers, _ = fetch(entry["files"] + "index.html", dest="iframe")
    policy = headers.get("Content-Security-Policy", "")
    case.step("sandbox entry response", {"status": status, "headers": headers})
    case.check("the sandbox serves the entry with the bundle policy, CORS for an opaque origin and no cookie",
               status == 200 and all(rule in policy for rule in SANDBOX_POLICY)
               and headers.get("Access-Control-Allow-Origin") == "*" and "Set-Cookie" not in headers
               and headers.get("X-Content-Type-Options") == "nosniff", headers)
    if args.web_origin:
        ancestors = re.search(r"frame-ancestors ([^;]+)", policy)
        case.check("frame-ancestors names the web app", ancestors and args.web_origin.rstrip("/")
                   in ancestors[1].split(), policy)
    case.check("the bundle policy carries the CSP sandbox directive", policy.startswith("sandbox allow-scripts allow-forms"),
               policy)
    for dest in ("document", None):
        status, _, _ = fetch(entry["files"] + "index.html", dest=dest)
        case.check(f"the entry is not served as a top-level page (Sec-Fetch-Dest {dest or 'absent'})", status == 404,
                   status)
    status, _, _ = fetch(entry["files"] + "bundle.json.map", dest="iframe")
    case.check("an unlisted type is refused by the sandbox", status == 404, status)
    status, _, _ = fetch(entry["files"] + "index.html", method="POST", dest="iframe")
    case.check("only GET reaches the sandbox", status in (403, 405), status)


def case_bundle_open(case, args):
    stack = case.stack
    require_profile(stack, "online")
    _, catalog = stack.catalog(stack.accounts[0], descriptor(kit_vocabulary("vue"), build=2, bundle=WEB_RUNTIME))
    entry = next((b for b in web_bundles_of(catalog, TASK) if b["id"] == DASHBOARD_BUNDLE), None)
    case.check("the dashboard bundle is served (run bundle-publish first)", entry, None)
    browser = open_browser(case, args.web_origin)
    opened = browser.ask("open_dashboard")
    browser.screenshot("bundle-open-web")
    case.step("web opens the dashboard", opened)
    case.check("Web opens the page as a sandboxed frame from the sandbox file base",
               opened["mode"] == "bundle" and opened.get("sandbox") == FRAME_SANDBOX
               and str(opened.get("frameUrl", "")).startswith(entry["files"])
               and opened.get("referrerpolicy") == "no-referrer", opened)
    case.check("the bundle drew its page (hello answered, data read through the bridge)", opened.get("drawn", 0) > 0,
               opened)
    case.step("device steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual bundle-open"})


def case_bundle_withdraw(case, args):
    stack = case.stack
    require_profile(stack, "online")
    if not args.yes:
        raise SystemExit("withdrawing is final for this plugin version in this lab: pass --yes")
    _, version, _ = task_release(stack)
    reviewer = stack.staff("phd_bundle_reviewer")
    status, rows = bundle_rows(reviewer, TASK, version)
    row = next((r for r in rows or [] if r.get("bundle_id") == DASHBOARD_BUNDLE), None)
    case.check("the bundle is approved before the withdrawal", (row or {}).get("status") == "approved", row)
    browser = open_browser(case, args.web_origin)
    opened = browser.ask("open_dashboard")
    case.check("the bundle page is open in the web app", opened["mode"] == "bundle", opened)
    started = time.time()
    withdrawn = platform_ok(case, "the platform withdraws the bundle", reviewer.call(
        "POST", f"/plugin-web-bundles/{row['id']}/withdraw",
        {"revision": row["revision"], "reason": "acceptance: withdrawal closes open pages"}, expected=None,
        reason="hot delivery acceptance withdrawal"))
    case.check("the bundle is withdrawn", withdrawn.get("status") == "withdrawn", withdrawn)
    closed = browser.ask("wait_fallback", timeoutMs=int(args.withdraw_timeout * 1000))
    browser.screenshot("bundle-withdrawn-web")
    case.step("open page after the withdrawal", {**closed, "since_withdraw_ms": round((time.time() - started) * 1000)})
    case.check("the open page closed and the declarative page took its place", closed.get("closedAfterMs") is not None,
               closed)
    registry = core_registry_row(stack, TASK, version)
    case.check("Core's registry marks it withdrawn", registry and registry["withdrawnAt"], registry)
    _, catalog = stack.catalog(stack.accounts[0], descriptor(kit_vocabulary("vue"), build=2, bundle=WEB_RUNTIME))
    page = contribution_of(catalog, TASK, DASHBOARD_PAGE)
    case.check("the catalog falls back to the declarative variant", page and page.get("variant") == "charts"
               and not web_bundles_of(catalog, TASK), contribution_summary(page))
    case.step("device steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual bundle-withdraw"})


# Case 7 -------------------------------------------------------------------

def probe_options():
    return {"fetchUrl": "https://example.com/flare-probe", "topUrl": "https://example.com/flare-probe-top",
            "declaredQuery": "flare.task.dashboard.get", "declaredInput": {"scope": "all"},
            "undeclaredCommands": ["flare.task.create", "flare.task.admin.cancel"], "burst": 40}


def judge_probe(case, label, outcome, device=False):
    """The isolation rules, applied to one probe result (Web here; devices through `verdict`, where
    the top-level document is the bundle's own WebView and its staying put is shown by the
    screenshot taken after the probe)."""
    result = outcome["result"]
    case.check(f"{label}: the bridge answers a declared query (control)", result.get("declared") == "ok", result)
    case.check(f"{label}: fetch is refused by connect-src", result.get("fetch", "").startswith("rejected")
               and any(v.get("directive") == "connect-src" for v in result.get("violations") or []), result)
    case.check(f"{label}: document.cookie is empty and stays empty",
               (result.get("cookie") == "" and result.get("cookieAfterWrite") == "")
               or result.get("cookieError") == "SecurityError", result)
    if not device:
        case.check(f"{label}: the top-level document did not navigate", outcome["topBefore"] == outcome["topAfter"]
                   and outcome["frameStillOpen"], {k: outcome[k] for k in ("topBefore", "topAfter", "frameStillOpen")})
    case.check(f"{label}: operations outside bundle.json answer permission_denied",
               result.get("undeclared") and all(code == "permission_denied" for code in result["undeclared"].values()),
               result.get("undeclared"))
    burst = result.get("burst") or []
    case.check(f"{label}: a burst above 20 requests per second hits the rate limit",
               burst.count("rate_limited") >= 1 and burst.count("ok") >= 1,
               {code: burst.count(code) for code in set(burst)})


def case_isolation(case, args):
    stack = case.stack
    require_profile(stack, "online")
    browser = open_browser(case, args.web_origin)
    opened = browser.ask("open_dashboard")
    case.check("the bundle page is open (run bundle-publish first)", opened["mode"] == "bundle", opened)
    outcome = browser.ask("probe", **probe_options())
    case.step("web probe", outcome)
    browser.screenshot("isolation-web")
    judge_probe(case, "web", outcome)
    case.step("device steps", {"manual": "python3 scripts/plugin_hot_delivery_acceptance.py manual isolation"})


# Case 8 -------------------------------------------------------------------

def case_switch_off(case):
    stack = case.stack
    require_profile(stack, "online")
    lab = stack.lab
    alice = stack.accounts[0]
    web = descriptor(kit_vocabulary("vue"), build=2, bundle=WEB_RUNTIME)
    _, before = stack.catalog(alice, web)
    page = contribution_of(before, TASK, DASHBOARD_PAGE)
    case.check("with the switch on the dashboard is served as a bundle (run bundle-publish first)",
               page and page.get("variant") == "bundle", contribution_summary(page))
    core_env, platform_env = lab.core_env, lab.platform_env

    def restore():
        lab.core_env, lab.platform_env = core_env, platform_env
        lab.start_core(stack.state, stack.keys)
        lab.start_platform(stack.state, stack.keys)
        restored = lab.wait_for(lambda: (contribution_of(stack.catalog(alice, web)[1], TASK, DASHBOARD_PAGE) or {})
                                .get("variant") == "bundle", 180, "the bundle variant after switching L3 back on", 3)
        case.step("switch restored", {"bundle_variant": restored})

    case.cleanups.append(restore)
    lab.core_env = lambda keys, state: {**core_env(keys, state), "FLARE_PLUGIN_WEB_BUNDLES_ENABLED": "false"}
    lab.platform_env = lambda keys: {**platform_env(keys), "FLARE_PLUGIN_WEB_BUNDLES_ENABLED": "false"}
    lab.start_core(stack.state, stack.keys)
    lab.start_platform(stack.state, stack.keys)
    _, after = lab.wait_for(lambda: (lambda answer: answer if isinstance(answer[1], dict) else None)(
        stack.catalog(alice, web)), 120, "a catalog from the restarted Core", 2)
    contributions = [c for p in after.get("plugins", []) for c in (p.get("extensions") or {}).get("contributions", [])]
    case.step("catalog with L3 off", {"dashboard": contribution_summary(contribution_of(after, TASK, DASHBOARD_PAGE)),
                                      "bundles": {p["pluginId"]: (p.get("extensions") or {}).get("webBundles")
                                                  for p in after.get("plugins", [])}})
    case.check("no contribution of any plugin carries a bundle", not any(c.get("bundle") for c in contributions),
               [c["id"] for c in contributions if c.get("bundle")])
    case.check("no plugin lists web bundles", not any((p.get("extensions") or {}).get("webBundles")
                                                      for p in after.get("plugins", [])), None)
    dashboard = contribution_of(after, TASK, DASHBOARD_PAGE)
    case.check("the dashboard falls to its declarative variant for a bundle-capable client",
               dashboard and dashboard.get("variant") == "charts", contribution_summary(dashboard))
    _, version, _ = task_release(stack)
    status, answer = stack.admin().call("POST", "/plugin-web-bundles/initiate",
                                        {"plugin_id": TASK, "plugin_version": version, "bundle_id": DASHBOARD_BUNDLE},
                                        expected=None, reason="hot delivery acceptance switch off")
    case.check("the platform refuses bundle uploads with L3 off", status != 200, {"status": status, "answer": answer})


# Case 9 -------------------------------------------------------------------

def case_catalog_hygiene(case):
    script = COMMERCIAL / "scripts" / "catalog_sync.py"
    keep = [] if (ROOT / "flare-plugin-call" / "sdk" / "plugin.json").exists() else ["--keep-missing"]
    clean = subprocess.run([sys.executable, str(script), "--check", "--workspace", str(ROOT), *keep],
                           capture_output=True, text=True, timeout=120)
    case.step("both catalogs as committed", {"exit": clean.returncode, "stderr": clean.stderr[-2000:]})
    case.check("the committed catalogs agree with the manifests", clean.returncode == 0, clean.stderr[-1000:])
    public = ROOT / "flare-plugin-catalog" / "plugins.json"
    catalog = json.loads(public.read_text())
    entries = catalog.get("plugins") or []
    case.check("the public catalog has entries", entries, list(catalog))
    # An entry this checkout generates (a private sibling's entry may be kept as written).
    changed = next((e for e in entries if e.get("id") == "sdk.plugin.task"), entries[-1])
    changed["version"] = changed["version"] + "-drift"
    with tempfile.TemporaryDirectory(prefix="phd-catalog-") as directory:
        drifted = Path(directory, "plugins.json")
        drifted.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")
        result = subprocess.run([sys.executable, str(script), "--check", "--workspace", str(ROOT), "--public",
                                 str(drifted), *keep], capture_output=True, text=True, timeout=120)
    case.step("public catalog drifted from the private one", {"plugin": changed.get("id"), "exit": result.returncode,
                                                              "stderr": result.stderr[-2000:]})
    case.check("the CI check fails and names the difference", result.returncode == 1
               and changed.get("id", "") in result.stderr, result.stderr[-1000:])


# ----------------------------------------------------------------- devices --

LOGIN = ("Sign in with the lab account labalice@lab.flare.test; its password is in "
         "<lab>/secrets/account-password. Build the app against the lab (online: the plugin-online-lab "
         "`up` summary prints the emulator and simulator settings).")
MANUAL = {
    "upgrade": {
        platform: [
            f"Run the {platform} build reporting build 0 against the wallet-sandbox lab (web: VITE_FLARE_APP_BUILD=0). "
            + LOGIN,
            "Open a chat, open +: the 红包 entry shows the 需更新 tag.",
            "Tap it: the upgrade notice shows 需要更新应用. With no newer release on the channel it shows only "
            "当前版本暂不支持，新版本发布后可用 and no button. capture <platform> label upgrade-<platform>-no-release.",
            "Publish a release with build >= 1 to the app's update channel (access lab `release` case or the console "
            "update center), reopen the notice: 去更新 shows; tap it: the update flow opens (prompt sheet). capture "
            "label upgrade-<platform>-release.",
        ] for platform in APP_PLATFORMS
    },
    "realname": {
        platform: [
            "Deploy the realname metaservice with the lab app (flare-plugins-commercial/metaservices/realname/docs/"
            "deployment.md) and enable Realname 0.4.0. " + LOGIN,
            "Me -> 实名认证: state 未提交 (notSubmitted) with the four steps; capture label realname-<platform>-1.",
            "Start, fill the application, add the three document sides, submit: state 审核中 (pendingReview); "
            "capture label realname-<platform>-2.",
            "Reject it in the console: state 未通过 with the reason; capture label realname-<platform>-3.",
            "Apply again and approve it in the console: state 已认证 (approved) with the masked name; capture "
            "label realname-<platform>-4. Every screen is the kit plugin page (no app screen).",
        ] for platform in APP_PLATFORMS
    },
    "payment": {
        platform: [
            "On the wallet-sandbox lab, fund the account (wallet guide) and set a payment password. " + LOGIN,
            "Send a red packet of 1.23 in a chat: the confirmation sheet is the kit FlarePaymentConfirmation and its "
            "amount reads 1.23 as the host draws it; capture label payment-<platform>.",
            "Enter the password: the packet is posted. A wrong password is refused by the wallet server.",
        ] for platform in APP_PLATFORMS
    },
    "bundle-open": {
        platform: [
            "After bundle-publish, on a device at the platform floor (Android System WebView >= 111, iOS >= 16; "
            "Tauri: macOS WKWebView or Windows WebView2). " + LOGIN,
            f"`capture --platform {platform} --device <id> --label floor-{platform}` records the engine version.",
            "Me -> 任务看板: the page opens as the web bundle (stat cards, burndown, deadline board), not the "
            "declarative rows; capture label bundle-open-<platform>.",
            "Drag a task from 今天 to 本周: the deadline moves (card in the chat shows it).",
            "Put the app in the background for over 30 s and return: the page reopens where it was.",
        ] for platform in ("tauri", "android", "ios", "flutter-android")
    },
    "isolation": {
        platform: [
            "With the bundle page open, attach the remote inspector to it (Android: chrome://inspect on a debuggable "
            "build; iOS and Tauri macOS: Safari Web Inspector).",
            f"Paste {PROBE} into its console and run "
            f"`JSON.stringify(await flareIsolationProbe({json.dumps(probe_options())}))`; save the output as "
            f"probe-{platform}.json.",
            "Take a screenshot showing the bundle page still in place, then `verdict isolation <platform> --probe "
            "probe-<platform>.json --evidence <screenshot>`: the probe is judged with the same rules as Web.",
        ] for platform in ("tauri", "android", "ios", "flutter-android")
    },
    "bundle-withdraw": {
        platform: [
            "Before bundle-withdraw --yes, keep 任务看板 open on the device.",
            "After the withdrawal the page closes within a catalog read (notice or reopen) and the declarative "
            "charts page shows; capture label withdrawn-<platform>.",
        ] for platform in ("tauri", "android", "ios", "flutter-android")
    },
    "rollback": {
        platform: [
            "Needs two plugin versions that each carry an approved dashboard bundle (the second with other bytes).",
            "Enable the newer version, open 任务看板 on the device (its bundle is now cached), then switch the app "
            "back to the older version and back again to the newer one.",
            "Reopen 任务看板: it opens with no download from the mirrors (mirror access log shows no GET of that "
            "digest; the engine cache directory plugin_bundles/<sha256> was reused); capture label rollback-<platform>.",
        ] for platform in ("android", "ios", "flutter-android", "tauri")
    },
}


def manual_file(stack, case_name, platform):
    return stack.evidence_dir / "manual" / f"{case_name}-{platform}.json"


def command_manual(stack, args):
    cases = [args.case] if args.case else list(MANUAL)
    for name in cases:
        print(f"\n== {name}")
        for platform, steps in MANUAL[name].items():
            file = manual_file(stack, name, platform)
            record = json.loads(file.read_text()) if file.exists() else {"verdict": None}
            print(f"-- {platform} (verdict: {record.get('verdict') or 'pending'})")
            for index, text in enumerate(steps, 1):
                print(f"  {index}. {text.replace('<platform>', platform)}")


def adb():
    for candidate in (os.environ.get("ADB"), shutil.which("adb"),
                      str(Path.home() / "Library/Android/sdk/platform-tools/adb")):
        if candidate and Path(candidate).exists():
            return candidate
    raise SystemExit("adb not found (set ADB)")


def command_capture(stack, args):
    """Device facts (engine version against the platform floor) and a screenshot, into the evidence."""
    facts = {"platform": args.platform, "device": args.device, "label": args.label, "at": time.time()}
    shot = stack.evidence_dir / "manual" / f"{args.label}.png"
    if args.platform in ("android", "flutter-android"):
        tool = adb()
        dump = subprocess.run([tool, "-s", args.device, "shell", "dumpsys", "webviewupdate"], capture_output=True,
                              text=True, timeout=60).stdout
        match = re.search(r"Current WebView package \(name, version\): \(([^,]+), ([0-9]+)", dump)
        facts["webview"] = {"package": match[1], "major": int(match[2])} if match else None
        facts["meets_floor"] = bool(match) and int(match[2]) >= 111
        with shot.open("wb") as handle:
            subprocess.run([tool, "-s", args.device, "exec-out", "screencap", "-p"], stdout=handle, timeout=60,
                           check=True)
    elif args.platform in ("ios", "flutter-ios"):
        listing = json.loads(subprocess.run(["xcrun", "simctl", "list", "devices", "-j"], capture_output=True,
                                            text=True, timeout=60, check=True).stdout)
        runtime = next((name for name, devices in listing["devices"].items()
                        for device in devices if args.device in (device["udid"], device["name"])), None)
        version = re.search(r"iOS-(\d+)", runtime or "")
        facts["runtime"] = runtime
        facts["meets_floor"] = bool(version) and int(version[1]) >= 16
        subprocess.run(["xcrun", "simctl", "io", args.device, "screenshot", str(shot)], check=True, timeout=60,
                       capture_output=True)
    else:
        raise SystemExit("capture drives Android emulators and iOS simulators; take Tauri and Web screenshots by hand")
    facts["screenshot"] = str(shot)
    file = stack.evidence_dir / "manual" / f"{args.label}.facts.json"
    stack.lab.write_private(file, json.dumps(facts, indent=1))
    print(json.dumps(facts, indent=1))


def command_verdict(stack, args):
    if args.case not in MANUAL or args.platform not in MANUAL[args.case]:
        raise SystemExit(f"no manual steps for {args.case} on {args.platform}")
    record = {"case": args.case, "platform": args.platform, "at": time.time(), "note": args.note, "evidence": []}
    for path in args.evidence or []:
        if not Path(path).exists():
            raise SystemExit(f"evidence file {path} does not exist")
        target = stack.evidence_dir / "manual" / f"{args.case}-{args.platform}-{Path(path).name}"
        shutil.copyfile(path, target)
        record["evidence"].append(str(target))
    if args.probe:
        # Device isolation is judged by the same rules as Web, never by the operator's word; the
        # screenshot after the probe shows the bundle page still in place (no top navigation).
        if not record["evidence"]:
            raise SystemExit("isolation needs --evidence: a screenshot of the bundle page taken after the probe")
        result = json.loads(Path(args.probe).read_text())
        case = Case(stack, f"isolation-{args.platform}")
        case.run(lambda c: judge_probe(c, args.platform, {"result": result}, device=True))
        record["verdict"] = "pass" if case.record["ok"] else "fail"
        record["probe"] = case.record
    else:
        if not args.result:
            raise SystemExit("pass --result pass|fail (or --probe for isolation)")
        if args.result == "pass" and not record["evidence"]:
            raise SystemExit("a passing manual verdict needs at least one --evidence file")
        record["verdict"] = args.result
    stack.lab.write_private(manual_file(stack, args.case, args.platform), json.dumps(record, indent=1,
                                                                                     ensure_ascii=False))
    print(f"{args.case} {args.platform}: {record['verdict']}")


AUTOMATED = ["variants", "upgrade", "lenient", "realname", "payment", "bundle-publish", "bundle-open", "isolation",
             "switch-off", "bundle-withdraw", "catalog-hygiene"]


def command_report(stack, _args):
    rows = []
    for name in AUTOMATED:
        file = stack.evidence_dir / f"{name}.json"
        record = json.loads(file.read_text()) if file.exists() else None
        rows.append((name, "auto", "not run" if record is None else "pass" if record.get("ok") else "fail",
                     (record or {}).get("error", "")))
    for name, platforms in MANUAL.items():
        for platform in platforms:
            file = manual_file(stack, name, platform)
            record = json.loads(file.read_text()) if file.exists() else {}
            rows.append((name, platform, record.get("verdict") or "pending", record.get("note") or ""))
    width = max(len(r[0]) for r in rows)
    for name, where, state, note in rows:
        print(f"{name:<{width}}  {where:<16} {state:<8} {note[:100]}")
    summary = {"profile": stack.profile, "rows": [dict(zip(("case", "where", "state", "note"), r)) for r in rows]}
    stack.evidence("report", summary)
    if any(r[2] in ("fail",) for r in rows):
        return 1
    return 0


# ---------------------------------------------------------------- preflight --

def command_preflight(stack, args):
    lab, problems, facts = stack.lab, [], {}
    for name, port in lab.CORE_PORTS.items():
        if not lab.port_open(port):
            problems.append(f"Core {name} is not listening on {port}")
    facts["platform_ready"] = lab.http("GET", f"http://{lab.PLATFORM_ADDR}/ready")[0]
    if facts["platform_ready"] != 200:
        problems.append("platform /ready is not 200")
    try:
        facts["metrics"] = stack.metrics()
    except CaseFailed as error:
        problems.append(str(error))
    status, catalog = stack.catalog(stack.accounts[0], descriptor(kit_vocabulary("vue"), build=2))
    facts["catalog_status"] = status
    facts["plugins"] = sorted(p["pluginId"] for p in (catalog or {}).get("plugins", [])) if isinstance(catalog, dict) else catalog
    if stack.profile in ("online", "isolated"):
        _, version, manifest = task_release(stack)
        declared = declared_bundle(manifest)
        facts["task"] = {"version": version, "declared": declared}
        if not declared or declared.get("sha256") == "0" * 64:
            problems.append("the task template still declares the placeholder digest: build and pack the bundle")
        if not flare_plugin_cli(args):
            problems.append("flare-plugin command not found (--cli)")
        status, rows = bundle_rows(stack.admin(), TASK, version)
        facts["platform_bundles"] = {"status": status, "rows": rows}
    if args.web_origin:
        facts["web_origin"] = lab.http("GET", args.web_origin)[0]
        if facts["web_origin"] != 200:
            problems.append(f"the web app does not answer at {args.web_origin}")
        if not (WEB_APP / "node_modules" / "@playwright" / "test").exists():
            problems.append("@playwright/test is not installed in the web app")
    print(json.dumps({"facts": facts, "problems": problems}, indent=1, ensure_ascii=False, default=str))
    stack.evidence("preflight", {"facts": facts, "problems": problems})
    return 1 if problems else 0


# --------------------------------------------------------------------- main --

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", choices=["online", "wallet-sandbox", "isolated"], default="online")
    parser.add_argument("--wallet-directory", default="~/.flare-wallet-sandbox-lab")
    isolated = parser.add_argument_group("isolated profile (plugin_hot_delivery_lab.py)")
    isolated.add_argument("--lab-dir", help="the isolated lab's directory")
    isolated.add_argument("--binaries", help="the isolated lab's binaries (flare-plugin is looked up here too)")
    isolated.add_argument("--lab-script", help="scripts/plugin-online-lab.py of the workspace root")
    isolated.add_argument("--port-offset", type=int, default=-36000)
    isolated.add_argument("--block", type=int, default=47000)
    commands = parser.add_subparsers(dest="command", required=True)

    def with_web(sub):
        sub.add_argument("--web-origin", help="the Social web app built against the lab, e.g. http://127.0.0.1:5173")
        return sub

    with_web(commands.add_parser("preflight", help="check the stack and tools")).add_argument("--cli")
    commands.add_parser("variants", help="1: vocabulary N and N+1 get different variants; one card payload")
    commands.add_parser("upgrade", help="2: a floor without an alternative gives an upgrade placeholder")
    with_web(commands.add_parser("lenient", help="3: unknown optional block skipped, required one asks to update"))
    realname = commands.add_parser("realname", help="4: no realname page keys in the five app builds")
    realname.add_argument("--artifact", action="append", help="platform=path, platform in " + ", ".join(APP_PLATFORMS))
    commands.add_parser("payment", help="5: payment confirmation stays with the host")
    publish = with_web(commands.add_parser("bundle-publish", help="6: upload, review, apply, enable"))
    publish.add_argument("--bundle", help="the packed .fwb (default: the task web project's)")
    publish.add_argument("--cli", help="the flare-plugin command")
    with_web(commands.add_parser("bundle-open", help="6: Web opens the bundle; devices are manual"))
    with_web(commands.add_parser("isolation", help="7: CSP, cookies, top navigation, permissions, rate"))
    commands.add_parser("switch-off", help="8: with L3 off the catalog has declarative variants only")
    withdraw = with_web(commands.add_parser("bundle-withdraw", help="6: withdrawal closes open pages (final)"))
    withdraw.add_argument("--yes", action="store_true")
    withdraw.add_argument("--withdraw-timeout", type=float, default=60.0)
    commands.add_parser("catalog-hygiene", help="9: the catalog CI check fails on drift")
    manual = commands.add_parser("manual", help="print the device steps")
    manual.add_argument("case", nargs="?", choices=list(MANUAL))
    capture = commands.add_parser("capture", help="device facts and a screenshot into the evidence")
    capture.add_argument("--platform", required=True, choices=["android", "flutter-android", "ios", "flutter-ios"])
    capture.add_argument("--device", required=True)
    capture.add_argument("--label", required=True)
    verdict = commands.add_parser("verdict", help="record a device verdict")
    verdict.add_argument("case", choices=list(MANUAL))
    verdict.add_argument("platform")
    verdict.add_argument("--result", choices=["pass", "fail"])
    verdict.add_argument("--evidence", action="append")
    verdict.add_argument("--probe", help="isolation: the probe JSON saved from the device")
    verdict.add_argument("--note", default="")
    commands.add_parser("report", help="every case and device verdict")
    args = parser.parse_args()

    if args.profile == "isolated" and not args.lab_dir:
        parser.error("--profile isolated needs --lab-dir")
    stack = Stack(args.profile, args.wallet_directory, args)
    tools = {"preflight": command_preflight, "manual": command_manual, "capture": command_capture,
             "verdict": command_verdict, "report": command_report}
    if args.command in tools:
        return tools[args.command](stack, args) or 0
    cases = {
        "variants": lambda c: case_variants(c),
        "upgrade": lambda c: case_upgrade(c),
        "lenient": lambda c: case_lenient(c, args),
        "realname": lambda c: case_realname(c, args),
        "payment": lambda c: case_payment(c),
        "bundle-publish": lambda c: case_bundle_publish(c, args),
        "bundle-open": lambda c: case_bundle_open(c, args),
        "isolation": lambda c: case_isolation(c, args),
        "switch-off": lambda c: case_switch_off(c),
        "bundle-withdraw": lambda c: case_bundle_withdraw(c, args),
        "catalog-hygiene": lambda c: case_catalog_hygiene(c),
    }
    return 0 if Case(stack, args.command).run(cases[args.command]) else 1


if __name__ == "__main__":
    sys.exit(main())
