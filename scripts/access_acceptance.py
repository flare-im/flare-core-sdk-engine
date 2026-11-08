#!/usr/bin/env python3
"""Acceptance of the bootstrap plane and the access console on the stack of access_stack.py.

Every case drives the real management API (through the gateway admin partition) and real engine
clients (examples/access_lab_client.rs, one process each). Each case writes its raw samples to
~/.flare-access-lab/evidence/<case>.json; thresholds are the ones of the design, never adjusted here.

  python3 scripts/access_acceptance.py seed            # staff, apps, keys, topology, first publication
  python3 scripts/access_acceptance.py publish-p95     # >= 20 publications, approve -> published
  python3 scripts/access_acceptance.py adoption        # >= 20 online clients, time to the new serial
  python3 scripts/access_acceptance.py negative        # forged documents on the mirrors
  python3 scripts/access_acceptance.py outage          # control plane down for 60 minutes
  python3 scripts/access_acceptance.py migrate         # move every address; online and frozen clients
  python3 scripts/access_acceptance.py mirror-down     # primary mirror closed, then black-holed
  python3 scripts/access_acceptance.py lint            # one reverse case per publication rule
  python3 scripts/access_acceptance.py release         # hand-signed release manifest and packs on the mirrors
  python3 scripts/access_acceptance.py recovery        # a signed recovery link brings a stranded client back
  python3 scripts/access_acceptance.py total-outage    # every access point down; new nodes, emergency publish
  python3 scripts/access_acceptance.py break-glass     # access points and control plane down; offline-signed nodes
  python3 scripts/access_acceptance.py serial-continuity # reserved recovery serial; an unannounced serial is a rollback
  python3 scripts/access_acceptance.py doh-fallback    # every listed mirror closed; the DoH pointer finds a rescue mirror
  python3 scripts/access_acceptance.py extras          # storage, projection, prober, admin, Edge facts
  python3 scripts/access_acceptance.py web-sw          # the web bundle update in a real browser
  python3 scripts/access_acceptance.py upgrade-required # a stand-in gateway refuses this version
"""
import argparse
import base64
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import access_stack as st  # noqa: E402

ls = st.ls
CLIENT = ls.TARGET / "examples" / "access_lab_client"
ADMIN = f"http://127.0.0.1:{ls.SOCIAL_API}/admin/v1"
PLATFORM = f"http://127.0.0.1:{st.PLATFORM_API}/api/v1/admin"
ORIGIN = f"https://127.0.0.1:{st.PLATFORM_API}"
ACCESS_PAYLOAD_TYPE = "application/vnd.flare.access.v1+json"
RELEASE_PAYLOAD_TYPE = "application/vnd.flare.release.v1+json"
DAY_MS = 86_400_000
ACCESS_FILE, KEYS_FILE = "access.dsse.json", "keys.dsse.json"
# Only what these clients reach: REST is fixed on port 443, which this stack has no listener on,
# and a refused REST call would count against the access point's health.
SERVICES = {"imWs": {"path": "/ws"}, "ping": {"path": "/.well-known/flare/ping"}}


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def now_ms():
    return int(time.time() * 1000)


def evidence(name, value):
    directory = st.lab_path("evidence")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    file = directory / f"{name}.json"
    file.write_text(json.dumps(value, indent=1, ensure_ascii=False, sort_keys=True))
    log(f"evidence -> {file}")


def load_facts():
    file = st.lab_path("acceptance.json")
    return json.loads(file.read_text()) if file.exists() else {}


def save_facts(facts):
    ls.write_private(st.lab_path("acceptance.json"), json.dumps(facts, indent=1))


def percentile(samples, fraction):
    """Nearest rank: the value at or below which `fraction` of the samples lie."""
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def request(method, url, body=None, headers=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    item = urllib.request.Request(url, data=data, method=method,
                                  headers={"content-type": "application/json", **(headers or {})})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(item, timeout=timeout) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers
    except OSError as error:
        return 0, str(error).encode(), None


def decoded(raw):
    try:
        return json.loads(raw or b"null")
    except ValueError:
        return raw.decode(errors="replace")


class Staff:
    """A staff member with a personal access token. Access management goes through the gateway
    admin partition; only what the partition does not carry (login, tokens, directory) goes to
    the platform directly."""

    def __init__(self, username, initial, password):
        self.username = username
        self.token_file = st.lab_path("secrets", f"pat-{username}")
        self.token = self.token_file.read_text().strip() if self.token_file.exists() else ""
        if not self.token or self.direct("GET", "/me")[0] != 200:
            self.token = self.login(initial, password)
            ls.write_private(self.token_file, self.token)

    def session(self, password):
        headers = {"Origin": ORIGIN}
        status, raw, response = request("POST", f"{PLATFORM}/auth/session",
                                        {"username": self.username, "password": password}, headers)
        if status != 200:
            return None
        jar = {}
        for line in response.get_all("Set-Cookie") or []:
            name, _, rest = line.partition("=")
            jar[name] = rest.split(";")[0]
        data = decoded(raw)["data"]
        return {**headers, "Cookie": "; ".join(f"{k}={v}" for k, v in jar.items() if v),
                "X-CSRF-Token": data["csrf_token"]}, data

    def login(self, initial, password):
        result = self.session(password)
        if result is None:
            result = self.session(initial)
            ls.require(result is not None, f"staff login failed for {self.username}")
            cookies, data = result
            if data["me"].get("must_change_password"):
                status, raw, _ = request("PUT", f"{PLATFORM}/me", {
                    "display_name": data["me"].get("display_name") or self.username,
                    "password": password, "current_password": initial}, cookies)
                ls.require(status == 200, f"password change for {self.username}: {status} {raw[:200]!r}")
            result = self.session(password)
            ls.require(result is not None, f"login with the new password failed for {self.username}")
        cookies, _ = result
        status, raw, _ = request("POST", f"{PLATFORM}/me/tokens", {"name": f"accesslab-{int(time.time())}"}, cookies)
        ls.require(status == 200, f"personal token: {status} {raw[:200]!r}")
        return decoded(raw)["data"]["token"]

    def headers(self, reason, key):
        return {"Authorization": "Bearer " + self.token, "Origin": ORIGIN,
                "X-Audit-Reason": urllib.parse.quote(reason), "Idempotency-Key": key or str(uuid.uuid4())}

    def direct(self, method, path, body=None, key=None, reason="m2 acceptance"):
        status, raw, _ = request(method, PLATFORM + path, body, self.headers(reason, key))
        return status, decoded(raw)

    def call(self, method, path, body=None, reason="m2 acceptance", key=None, timeout=20):
        """Through the admin partition. (status, body)."""
        status, raw, _ = request(method, ADMIN + path, body, self.headers(reason, key), timeout)
        return status, decoded(raw)

    def ok(self, method, path, body=None, reason="m2 acceptance"):
        status, value = self.call(method, path, body, reason)
        ls.require(status == 200, f"{method} {path}: {status} {str(value)[:600]}")
        return value["data"] if isinstance(value, dict) and "data" in value else value


def staff_secret(name):
    return st.lab_secret(f"staff-{name}", lambda: "Lab-" + secrets.token_urlsafe(18))


def ensure_employee(admin, username, role_key):
    initial, password = staff_secret(f"{username}-initial"), staff_secret(f"{username}-password")
    status, listing = admin.direct("GET", f"/rbac/employees?keyword={username}&limit=20&offset=0")
    ls.require(status == 200, f"list employees: {status} {listing}")
    found = [e for e in listing["data"] if e["username"] == username]
    if found:
        employee = found[0]["id"]
    else:
        status, created = admin.direct("POST", "/rbac/employees", {
            "username": username, "display_name": username, "password": initial})
        ls.require(status == 200, f"create employee: {status} {created}")
        employee = created["data"]["id"]
        _, roles = admin.direct("GET", "/rbac/roles")
        role = next(r for r in roles["data"] if r.get("builtin_key") == role_key)
        status, value = admin.direct("POST", f"/rbac/employees/{employee}/roles", {"role_id": role["id"]})
        ls.require(status == 200, f"assign {role_key}: {status} {value}")
    return Staff(username, initial, password)


def people():
    lab = st.Lab()
    admin = Staff("accesslab_admin", lab.admin_initial, lab.admin_password)
    return admin, ensure_employee(admin, "accesslab_approver", "platform_admin"), \
        ensure_employee(admin, "accesslab_auditor", "platform_auditor")


def ensure_app(admin, facts, key):
    """An organization and its application through the real API, projected to Core and Social."""
    record = facts.setdefault("apps", {}).setdefault(key, {})
    if not record.get("orgId"):
        status, body = admin.direct("POST", "/orgs", {
            "code": f"access-{key}", "name": f"Access lab {key}",
            "owner": {"username": f"accesslab_owner_{key}", "display_name": f"Owner {key}",
                      "password": staff_secret(f"owner-{key}")},
            "application": {"name": f"Access lab {key}", "mode": "test"}},
            key=record.setdefault("key", str(uuid.uuid4())))
        ls.require(status == 200, f"create organization: {status} {body}")
        record["orgId"] = body["data"]["org_id"]
        save_facts(facts)
    if not record.get("appId"):
        status, body = admin.direct("GET", f"/orgs/{record['orgId']}/apps")
        items = body["data"] if isinstance(body["data"], list) else body["data"].get("items", [])
        ls.require(status == 200 and len(items) == 1, f"applications: {status} {body}")
        record["appId"] = items[0]["app_id"]
        save_facts(facts)
    keys = st.Lab().core
    app = record["appId"]
    ls.wait_for(lambda: ls.psql(keys, f"SELECT status FROM tenants WHERE tenant_id='{app}'",
                                ls.DATABASES["core"]) == "active", 90, f"Core tenant {app}", 2)
    ls.wait_for(lambda: ls.psql(keys, f"SELECT 1 FROM flare_tenants WHERE tenant_id='{app}'",
                                ls.DATABASES["social"]) == "1", 90, f"Social tenant {app}", 2)
    log(f"application {key}: {app}")
    return record


# ------------------------------------------------------------------ publish --
def wait_change_set(staff, change_set, timeout=300, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = staff.call("GET", f"/platform/access/change-sets/{change_set}")
        if status == 200 and body["data"]["status"] in ("published", "failed"):
            return body["data"]
        time.sleep(interval)
    raise SystemExit(f"change set {change_set} not finished within {timeout}s")


def publish(author, approver, reason="m2 acceptance publication"):
    """The whole flow; timing starts at approval, as the acceptance states."""
    detail = author.ok("POST", "/platform/access/change-sets", reason=reason)
    change_set = detail["id"]
    detail = author.ok("POST", f"/platform/access/change-sets/{change_set}:submit", reason=reason)
    ls.require(detail["status"] == "pending_approval",
               f"submit left {detail['status']}: {json.dumps(detail.get('lint'))[:800]}")
    started = time.time()
    approver.ok("POST", f"/platform/access/change-sets/{change_set}:approve", reason=reason)
    final = wait_change_set(author, change_set)
    elapsed = time.time() - started
    ls.require(final["status"] == "published", f"change set failed: {final.get('failure')}")
    return {"changeSet": change_set, "approveToPublishedSec": round(elapsed, 3),
            "observedAtMs": now_ms(), "decidedAt": final.get("decidedAt"), "publishedAt": final.get("publishedAt"),
            "lint": final.get("lint")}


def serials(admin, change_set):
    return admin.ok("GET", f"/platform/access/publications?changeSetId={change_set}&pageSize=50")["items"]


def bump_weight(admin, ap_id):
    """A real topology edit, so the publication has something to change."""
    view = admin.ok("GET", f"/platform/access/access-points?pageSize=100")["items"]
    point = next(p for p in view if p["id"] == ap_id)
    weight = 100 if point["weight"] != 100 else 101
    admin.ok("PATCH", f"/platform/access/access-points/{ap_id}",
             {"expectedRevision": point["revision"], "weight": weight})


# ------------------------------------------------------------------- seeding --
def cert_file(name):
    return st.lab_path("trust", "certs", name)


def certify(key_id, public_key, name, not_before=None, not_after=None, bootstrap="*"):
    st.lab_path("trust", "certs").mkdir(parents=True, exist_ok=True, mode=0o700)
    file = cert_file(name)
    if file.exists():
        return file.read_text().strip()
    extra = []
    if not_before:
        extra += ["--not-before", not_before]
    if not_after:
        extra += ["--not-after", not_after]
    st.flarectl("trust", "certify", "--roots", st.lab_path("trust", "trust-roots.json"),
                "--root-key", st.lab_path("trust", "root-a.key.pem"), "--key-id", key_id, "--public-key", public_key,
                "--usage", "access", "--usage", "release", "--bootstrap-id", bootstrap, *extra, "--output", file)
    return file.read_text().strip()


def seed_signing(admin, approver, facts):
    if facts.get("onlineKey"):
        return
    keys = admin.ok("GET", "/platform/signing/keys")["keys"]
    key = next((k for k in keys if k["status"] == "pending_cert"), None) \
        or admin.ok("POST", "/platform/signing/keys:rotate", reason="first online key")
    # `flarectl trust certify` writes the value that goes into `document` as it is.
    document = certify(key["keyId"], key["publicKey"], "online-1.cert")
    # The upload is the second person's in production.
    status, body = admin.call("POST", f"/platform/signing/keys/{key['keyId']}/cert",
                              {"document": document}, "same person uploads")
    facts["secondPersonRefusal"] = {"status": status, "reason": body.get("reason") if isinstance(body, dict) else body}
    ls.require(status == 403, f"same person must be refused: {status} {body}")
    approver.ok("POST", f"/platform/signing/keys/{key['keyId']}/cert", {"document": document},
                "second person uploads")
    facts["onlineKey"] = key["keyId"]
    save_facts(facts)


def seed_keystate(admin, facts):
    if facts.get("keystateSerial"):
        return
    file = st.lab_path("trust", "keystate-1.dsse.json")
    st.flarectl("trust", "keystate", "--roots", st.lab_path("trust", "trust-roots.json"),
                "--root-key", st.lab_path("trust", "root-a.key.pem"), "--serial", 1,
                "--certificate", cert_file("online-1.cert"), "--output", file)
    document = base64.urlsafe_b64encode(file.read_bytes()).decode().rstrip("=")
    admin.ok("POST", "/platform/signing/keystate", {"document": document}, "first key state")
    facts["keystateSerial"] = 1
    save_facts(facts)


def seed_topology(admin, lab, sites):
    admin.ok("POST", "/platform/access/regions", {"id": st.REGION, "displayName": {"zh-CN": "实验区", "en-US": "H lab"}})
    admin.ok("POST", "/platform/access/cells", {
        "id": st.CELL, "regionId": st.REGION, "status": "enabled",
        "coreAdminUrl": f"http://127.0.0.1:{ls.port(41051)}"})
    for site in sites:
        admin.ok("POST", "/platform/access/access-points", {
            "id": site.ap_id, "cellId": st.CELL, "host": site.host, "services": SERVICES, "status": "active"})
    for key, site in st.MIRRORS.items():
        access, secret_key = lab.rustfs[key]
        admin.ok("POST", "/platform/access/mirrors", {
            "id": site.id, "kind": "s3", "baseUrl": site.base_url, "provider": site.provider, "tld": site.tld,
            "enabled": True, "credentials": {
                "endpoint": site.s3_endpoint, "region": "us-east-1", "bucket": site.bucket, "prefix": "flare/v1",
                "accessKeyId": access, "secretAccessKey": secret_key}})


def command_seed(_args):
    facts = load_facts()
    admin, approver, auditor = people()
    lab = st.Lab()
    main_app = ensure_app(admin, facts, "main")
    forge_app = ensure_app(admin, facts, "forge")
    seed_signing(admin, approver, facts)
    if not facts.get("topology"):
        seed_topology(admin, lab, [st.SITES["ap-a1"], st.SITES["ap-a2"]])
        facts["topology"] = True
        save_facts(facts)
    for record in (main_app, forge_app):
        if not record.get("bootstrapId"):
            placement = admin.ok("PUT", f"/apps/{record['appId']}/access/placement", {
                "expectedRevision": 0, "homeCellId": st.CELL, "allowedRegions": [st.REGION], "clientPolicy": {}})
            record["bootstrapId"] = placement["bootstrapId"]
            save_facts(facts)
    seed_keystate(admin, facts)
    if not facts.get("firstPublication"):
        result = publish(admin, approver, "first publication")
        facts["firstPublication"] = {k: result[k] for k in ("changeSet", "approveToPublishedSec")}
        save_facts(facts)
        evidence("seed-first-publication", result)
    log(json.dumps({k: v.get("bootstrapId") for k, v in facts["apps"].items()}))


# ------------------------------------------------------------------- clients --
def issue_token(user, tenant):
    """The IM token, from the real api-gateway; the client's own renewal would need REST on 443."""
    status, raw, _ = request("POST", f"http://127.0.0.1:{ls.API}/api/v1/auth/tokens",
                             {"userId": user, "tenantId": tenant, "ttlSecs": st.TOKEN_TTL_SECONDS})
    ls.require(status == 200, f"issue token: {status} {raw[:300]!r}")
    return decoded(raw)["data"]["token"]


class Client:
    """One engine client process. Everything it reports stays in `events`."""

    def __init__(self, name, app, *, data_dir=None, seed=None, no_login=False, tick=5, user=None, app_info=None,
                 single_runtime=False):
        self.name = name
        self.root = Path(data_dir) if data_dir else st.lab_path("clients", name)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        roots = json.loads(st.lab_path("trust", "trust-roots.json").read_text())
        config = {"dataDir": str(self.root / "data"), "user": user or f"h-{name}", "tenant": app["appId"],
                  "bootstrapId": app["bootstrapId"], "trustRoots": roots,
                  "caFile": str(st.ca_file()), "tickSeconds": tick, "noLogin": no_login, "singleRuntime": single_runtime}
        if seed:
            config["seedFile"] = str(seed)
        if app_info:
            config["app"] = app_info
        if not no_login:
            config["token"] = issue_token(config["user"], app["appId"])
            config["route"] = {"hostSuffix": st.HOST_SUFFIX, "port": st.SNI_PORT}
        self.config_file = self.root / "client.json"
        ls.write_private(self.config_file, json.dumps(config))
        self.events = []
        self.write_errors = 0
        self.last_write_error = None
        self.lock = threading.Condition()
        self.stderr = open(self.root / "client.log", "ab")
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [str(CLIENT), "run", "--config", str(self.config_file)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self.stderr, text=True, bufsize=1, start_new_session=True,
            env={"PATH": os.environ["PATH"], "HOME": os.environ["HOME"], "RUST_LOG": "info",
                 "NO_PROXY": ls.NO_PROXY, "no_proxy": ls.NO_PROXY})
        self.started_at = now_ms()
        threading.Thread(target=self._read, daemon=True).start()
        return self

    def _read(self):
        with open(self.root / "events.ndjson", "a") as file:
            for line in self.process.stdout:
                try:
                    file.write(line)
                    file.flush()
                except OSError as error:
                    # A full disk costs the raw copy, not the run: `events` is what the case reads,
                    # and a reader that stops would block the client on its stdout.
                    self.write_errors += 1
                    self.last_write_error = str(error)
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                with self.lock:
                    self.events.append(event)
                    self.lock.notify_all()

    def send(self, **command):
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()

    def wait(self, predicate, timeout):
        """The first event, past or future, for which `predicate` holds; None on timeout."""
        deadline = time.time() + timeout
        with self.lock:
            while True:
                for event in self.events:
                    if predicate(event):
                        return event
                left = deadline - time.time()
                if left <= 0:
                    return None
                self.lock.wait(min(left, 1))

    def ask(self, op, timeout=30, **arguments):
        identity = uuid.uuid4().hex
        self.send(op=op, id=identity, **arguments)
        return self.wait(lambda e: isinstance(e.get("data"), dict) and e["data"].get("id") == identity, timeout)

    def refresh(self, timeout=40):
        return self.ask("refresh", timeout)

    def status(self, timeout=15):
        event = self.ask("status", timeout)
        return event["data"] if event else None

    def stop(self):
        if self.process and self.process.poll() is None:
            try:
                self.send(op="quit")
                self.process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired, ValueError):
                self.process.terminate()
                self.process.wait(timeout=10)


def online_on(status, access_points=None):
    connection = (status or {}).get("connection") or {}
    return connection.get("phase") == "online" and (
        access_points is None or connection.get("accessPointId") in access_points)


def start_fleet(app, count, prefix, seed):
    fleet = [Client(f"{prefix}-{index:02d}", app, seed=seed) for index in range(count)]
    for client in fleet:
        client.start()
        time.sleep(0.2)
    return fleet


def wait_fleet_online(fleet, timeout=120):
    deadline = time.time() + timeout
    waiting = list(fleet)
    while waiting and time.time() < deadline:
        waiting = [c for c in waiting if not online_on(c.status())]
        if waiting:
            time.sleep(1)
    ls.require(not waiting, f"not online: {[c.name for c in waiting]}")


def seed_file(app):
    """The current signed manifest as a build would embed it: read from a real mirror."""
    site = st.MIRRORS["b"]
    status, body, _ = https_get(f"{site.base_url}/{app['bootstrapId']}/{ACCESS_FILE}")
    ls.require(status == 200, f"read seed: {status}")
    file = st.lab_path("seeds", f"{app['bootstrapId']}-{now_ms()}.json")
    file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    file.write_bytes(body)
    return file


def https_get(url, headers=None, timeout=10):
    import ssl
    context = ssl.create_default_context(cafile=str(st.ca_file()))
    item = urllib.request.Request(url, headers=headers or {})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                         urllib.request.HTTPSHandler(context=context))
    try:
        with opener.open(item, timeout=timeout) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers
    except OSError as error:
        return 0, str(error).encode(), None


# ----------------------------------------------------------- 1. publication --
def command_publish_p95(args):
    facts = load_facts()
    admin, approver, _ = people()
    samples = []
    for index in range(args.count):
        bump_weight(admin, "ap-a1")
        result = publish(admin, approver, f"p95 sample {index}")
        rows = serials(admin, result["changeSet"])
        result["publications"] = [{"app": r["appId"], "serial": r["serial"], "status": r["status"],
                                   "mirrors": [{"id": m["mirrorId"], "status": m["status"]} for m in r["mirrors"]]}
                                  for r in rows]
        result.pop("lint", None)
        samples.append(result)
        log(f"sample {index}: {result['approveToPublishedSec']}s")
    values = [s["approveToPublishedSec"] for s in samples]
    summary = {"count": len(values), "p50": percentile(values, .5), "p95": percentile(values, .95),
               "max": max(values), "min": min(values), "mean": round(statistics.mean(values), 3),
               "limitSec": 120, "pass": percentile(values, .95) <= 120}
    evidence("1-publish-p95", {"summary": summary, "samples": samples})
    log(json.dumps(summary))


def command_adoption(args):
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    fleet = start_fleet(app, args.clients, "adopt", seed)
    rounds = []
    try:
        wait_fleet_online(fleet)
        for round_index in range(args.rounds):
            bump_weight(admin, "ap-a2")
            result = publish(admin, approver, f"adoption round {round_index}")
            published = result["observedAtMs"]
            serial = max(r["serial"] for r in serials(admin, result["changeSet"]) if r["appId"] == app["appId"])
            adopted = {}
            deadline = time.time() + 30 * 60
            while len(adopted) < len(fleet) and time.time() < deadline:
                for client in fleet:
                    if client.name in adopted:
                        continue
                    event = client.wait(lambda e: e["kind"] == "changed" and e["data"]["serial"] >= serial, 0)
                    if event:
                        adopted[client.name] = {"delayMs": event["ts"] - published, "source": event["data"]["source"]}
                time.sleep(0.5)
            delays = [v["delayMs"] for v in adopted.values()]
            rounds.append({
                "serial": serial, "clients": len(fleet), "adopted": len(adopted),
                "rate": len(adopted) / len(fleet), "maxDelayMs": max(delays, default=None),
                "p50DelayMs": percentile(delays, .5) if delays else None, "samples": adopted,
                "pass": len(adopted) / len(fleet) >= 0.9 and max(delays, default=1 << 60) <= 30 * 60_000})
            log(f"round {round_index}: {len(adopted)}/{len(fleet)} max {max(delays, default=None)} ms")
    finally:
        for client in fleet:
            client.stop()
    evidence("1-adoption", {"rounds": rounds})


# ------------------------------------------------------------- 2. negatives --
def mirror_objects(app, name=ACCESS_FILE):
    return [(site, f"/{site.bucket}/flare/v1/{app['bootstrapId']}/{name}") for site in st.MIRRORS.values()]


def put_everywhere(lab, app, body, name=ACCESS_FILE, headers=None):
    for site, path in mirror_objects(app, name):
        access, secret_key = lab.rustfs[site.key]
        status, raw, _ = st.s3_request("PUT", site.s3_endpoint, access, secret_key, path, body=body,
                                       headers={"content-type": "application/json", **(headers or {})})
        ls.require(status == 200, f"put {path}: {status} {raw[:200]!r}")


def delete_everywhere(lab, app, name):
    for site, path in mirror_objects(app, name):
        access, secret_key = lab.rustfs[site.key]
        st.s3_request("DELETE", site.s3_endpoint, access, secret_key, path)


def read_document(app, name=ACCESS_FILE):
    status, body, _ = https_get(f"{st.MIRRORS['b'].base_url}/{app['bootstrapId']}/{name}")
    return body if status == 200 else None


def payload_of(envelope):
    document = json.loads(envelope)
    raw = document["payload"]
    return json.loads(base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_"))


def tool(*arguments, stdin=None):
    return subprocess.run([str(CLIENT), *arguments], input=stdin, capture_output=True, text=True, check=True).stdout


def lab_key(name):
    file = st.lab_path("secrets", f"forge-{name}.json")
    if not file.exists():
        ls.write_private(file, tool("keygen"))
    return json.loads(file.read_text())


def forge(key_id, key, payload, certificates, payload_type=ACCESS_PAYLOAD_TYPE):
    job = {"seedHex": key["seedHex"], "keyId": key_id, "payloadType": payload_type,
           "payload": payload if isinstance(payload, str) else json.dumps(payload), "keyCerts": certificates}
    return tool("sign", stdin=json.dumps(job)).encode()


def iso(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def negative_cases(lab, facts):
    """Forged documents go to the `forge` application's objects on every mirror, so a client has
    nothing valid to fall back on and clients of the main application see none of this."""
    app = facts["apps"]["forge"]
    other = facts["apps"]["main"]
    genuine = read_document(app)
    ls.require(genuine, "the forge application has no published manifest")
    keys_doc = read_document(app, KEYS_FILE)
    seed = st.lab_path("seeds", f"forge-{now_ms()}.json")
    seed.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    seed.write_bytes(genuine)
    payload = payload_of(genuine)
    serial = payload["serial"]
    good_key = lab_key("good")
    good_cert = certify("ok-forge-good", good_key["publicKey"], "forge-good.cert")
    expired_key = lab_key("expired")
    expired_cert = certify("ok-forge-expired", expired_key["publicKey"], "forge-expired.cert",
                           not_before=iso(now_ms() - 3 * DAY_MS), not_after=iso(now_ms() - DAY_MS))
    revoked_key = lab_key("revoked")
    revoked_cert = certify("ok-forge-revoked", revoked_key["publicKey"], "forge-revoked.cert")
    next_manifest = {**payload, "serial": serial + 1}
    valid = forge("ok-forge-good", good_key, next_manifest, [good_cert])

    tampered = json.loads(genuine)
    raw = bytearray(base64.b64decode(tampered["payload"] + "=" * (-len(tampered["payload"]) % 4), altchars=b"-_"))
    raw[len(raw) // 2] ^= 0x01
    tampered["payload"] = base64.urlsafe_b64encode(bytes(raw)).decode().rstrip("=")

    other_payload = payload_of(read_document(other))
    padded = {**next_manifest, "padding": "x" * (70 * 1024)}
    oversize = forge("ok-forge-good", good_key, padded, [good_cert])
    revocation = st.lab_path("trust", f"keystate-revoke-{now_ms()}.dsse.json")
    st.flarectl("trust", "keystate", "--roots", st.lab_path("trust", "trust-roots.json"),
                "--root-key", st.lab_path("trust", "root-a.key.pem"), "--serial", facts.get("keystateSerial", 1) + 100,
                "--certificate", cert_file("online-1.cert"), "--revoke", "ok-forge-revoked", "--output", revocation)
    cases = [
        ("control-valid-forgery", valid, {}, None, "adopted", None),
        ("tampered-payload", json.dumps(tampered).encode(), {}, None, "rejected", "badSignature"),
        ("expired-online-cert", forge("ok-forge-expired", expired_key, next_manifest, [expired_cert]), {}, None,
         "rejected", "keyCert.expired"),
        ("revoked-keyid", forge("ok-forge-revoked", revoked_key, next_manifest, [revoked_cert]), {},
         revocation.read_bytes(), "rejected", "revoked"),
        ("wrong-payload-type", forge("ok-forge-good", good_key, next_manifest, [good_cert], RELEASE_PAYLOAD_TYPE), {},
         None, "rejected", "wrongPayloadType"),
        ("wrong-bootstrap-id", forge("ok-forge-good", good_key, {**next_manifest,
                                                                 "bootstrapId": other_payload["bootstrapId"],
                                                                 "appId": other_payload["appId"]}, [good_cert]), {},
         None, "rejected", "wrongBootstrapId"),
        ("over-64-kib", oversize, {}, None, "rejected", "tooLarge"),
        ("over-64-kib-gzip", gzip.compress(oversize), {"content-encoding": "gzip"}, None, "rejected", "tooLarge"),
        ("serial-rollback", forge("ok-forge-good", good_key, {**payload, "serial": serial - 1}, [good_cert]), {},
         None, "rejected", "rollback"),
    ]
    results = []
    for name, document, headers, keystate, expected_result, expected_code in cases:
        put_everywhere(lab, app, document, headers=headers)
        if keystate:
            put_everywhere(lab, app, keystate, KEYS_FILE)
        client = Client(f"neg-{name}-{now_ms()}", app, seed=seed, no_login=True, tick=60).start()
        try:
            before = client.status()
            event = client.refresh()
            after = client.status()
        finally:
            client.stop()
            put_everywhere(lab, app, genuine)
            if keys_doc:
                put_everywhere(lab, app, keys_doc, KEYS_FILE)
            elif keystate:
                delete_everywhere(lab, app, KEYS_FILE)
        report = (event or {}).get("data", {}).get("report", {})
        attempts = [a for a in report.get("attempts", []) if a["document"] == "access"]
        results_seen = {a["result"] for a in attempts}
        codes = {a.get("code") for a in attempts}
        # `before` can land ahead of the directory assembly; the seed's serial is the reference.
        held = (after or {}).get("access", {}).get("serial")
        if expected_result == "adopted":
            passed = report.get("outcome") == "adopted" and held == serial + 1
        else:
            passed = (held == serial and report.get("outcome") in ("failed", "upToDate")
                      and "adopted" not in results_seen and expected_code in codes)
        results.append({"case": name, "expected": expected_code or expected_result, "outcome": report.get("outcome"),
                        "seedSerial": serial, "serialBefore": (before or {}).get("access", {}).get("serial"),
                        "serialAfter": held,
                        "attempts": attempts, "documentBytes": len(document), "pass": passed})
        log(f"{name}: outcome={report.get('outcome')} codes={sorted(c for c in codes if c)} pass={passed}")
    return results


def command_negative(_args):
    results = negative_cases(st.Lab(), load_facts())
    evidence("2-negative", {"cases": results, "pass": all(r["pass"] for r in results)})


# ------------------------------------------------------- 3. control plane down --
def command_outage(args):
    facts = load_facts()
    app = facts["apps"]["main"]
    seed = seed_file(app)
    state = ls.load_state()
    fleet = start_fleet(app, args.clients, "outage", seed)
    samples, disconnects, negative = [], [], {}
    try:
        wait_fleet_online(fleet)
        marks = {c.name: len(c.events) for c in fleet}
        for role in ("api", "worker", "prober"):
            ls.signal_process(state, f"platform-{role}", 15, wait=30)
        started = time.time()
        log(f"control plane stopped; holding for {args.minutes} minutes")
        worker = threading.Thread(target=lambda: negative.update(cases=negative_cases(st.Lab(), facts)), daemon=True)
        worker.start()
        probe = 0
        killed = False
        while time.time() - started < args.minutes * 60:
            minute_started = time.time()
            for client in fleet:
                event = client.refresh()
                status = client.status()
                report = (event or {}).get("data", {}).get("report", {})
                samples.append({"atSec": round(time.time() - started, 1), "client": client.name,
                                "outcome": report.get("outcome"),
                                "tiers": sorted({a.get("tier") for a in report.get("attempts", [])}),
                                "elapsedMs": (event or {}).get("data", {}).get("elapsedMs"),
                                "serial": report.get("serial"), "online": online_on(status),
                                "ap": ((status or {}).get("connection") or {}).get("accessPointId")})
            probe += 1
            if not killed and time.time() - started > args.minutes * 30:
                # Half way: the gateway most clients sit on dies; reconnection must not need the
                # control plane.
                busiest = statistics.mode(s["ap"] for s in samples[-len(fleet):] if s["ap"])
                gateway = st.SITES[busiest].gateway
                ls.signal_process(state, gateway, 9)
                time.sleep(20)
                st.command_ctl(argparse.Namespace(action="start", process=gateway))
                killed = gateway
            time.sleep(max(0, 60 - (time.time() - minute_started)))
        worker.join(timeout=600)
        elapsed = time.time() - started
        for client in fleet:
            disconnects.append({"client": client.name, "connectionEvents": [
                e for e in client.events[marks[client.name]:] if e["kind"] in ("connection", "switched")]})
    finally:
        for client in fleet:
            client.stop()
        for role in ("api", "worker", "prober"):
            if not ls.live(ls.load_state(), f"platform-{role}"):
                st.platform_process(ls.load_state(), st.Lab(), role)
    good = [s for s in samples if s["outcome"] in ("upToDate", "adopted") and s["online"]]
    summary = {"minutes": round(elapsed / 60, 2), "samples": len(samples), "good": len(good), "gatewayKilled": killed,
               "eventWriteErrors": sum(c.write_errors for c in fleet),
               "bad": [s for s in samples if s not in good][:50],
               "maxRefreshMs": max((s["elapsedMs"] or 0) for s in samples),
               "pass": len(good) == len(samples) and elapsed >= args.minutes * 60}
    evidence("3-outage", {"summary": summary, "samples": samples, "connectionEvents": disconnects})
    evidence("2-negative-during-outage", negative)
    log(json.dumps({k: v for k, v in summary.items() if k != "bad"}))


# ----------------------------------------------------------- 4. address move --
def rewind_refresh_clock(data_dir, bootstrap_id, by_ms):
    """Move the persisted refresh and success times back: what a device sees after that many
    days switched off. The signed LKG itself is aged separately by `aged_seed`."""
    database = sqlite3.connect(Path(data_dir) / "access" / "access.db")
    key = f"bootstrap/sources/{bootstrap_id}"
    row = database.execute("SELECT value FROM access_kv WHERE key = ?", (key,)).fetchone()
    # A client switched off before its first refresh has no book yet.
    book = json.loads(bytes(row[0])) if row else {"sources": {}, "lastRefreshMs": now_ms()}
    if book.get("lastRefreshMs"):
        book["lastRefreshMs"] -= by_ms
    for record in book.get("sources", {}).values():
        if record.get("lastSuccessMs"):
            record["lastSuccessMs"] -= by_ms
    database.execute("INSERT OR REPLACE INTO access_kv (key, value) VALUES (?, ?)", (key, json.dumps(book).encode()))
    database.commit()
    database.close()
    return book


def aged_seed(app):
    """The live manifest with issuedAt seven days back, signed by a certified lab key: the
    manifest a device would hold had it last refreshed a week ago."""
    genuine = read_document(app)
    payload = payload_of(genuine)
    key = lab_key("aged")
    cert = certify("ok-forge-aged", key["publicKey"], "forge-aged.cert", not_before=iso(now_ms() - 8 * DAY_MS))
    payload["issuedAt"] = iso(now_ms() - 7 * DAY_MS)
    payload["expiresAt"] = iso(now_ms() + 23 * DAY_MS)
    file = st.lab_path("seeds", f"aged-{now_ms()}.json")
    file.write_bytes(forge("ok-forge-aged", key, payload, [cert]))
    return file, payload["serial"]


def command_migrate(args):
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    # Frozen client: online on the old addresses, then switched off with its data directory.
    old_seed, old_serial = aged_seed(app)
    frozen = Client(f"frozen-{now_ms()}", app, seed=old_seed).start()
    ls.require(online_wait(frozen, {"ap-a1", "ap-a2"}, 60), "frozen client did not come online")
    frozen_status = frozen.status()
    frozen.stop()
    fleet = start_fleet(app, args.clients, "migrate", seed)
    result = {"frozen": {"serial": old_serial, "statusBeforeFreeze": frozen_status}}
    try:
        wait_fleet_online(fleet)
        for ap_id in ("ap-b1", "ap-b2"):
            site = st.SITES[ap_id]
            admin.ok("POST", "/platform/access/access-points", {
                "id": ap_id, "cellId": st.CELL, "host": site.host, "services": SERVICES, "status": "active"})
        points = {p["id"]: p for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"]}
        for ap_id in ("ap-a1", "ap-a2"):
            admin.ok("POST", f"/platform/access/access-points/{ap_id}:drain", {
                "expectedRevision": points[ap_id]["revision"], "drainUntil": now_ms() + 300_000})
        publication = publish(admin, approver, "address migration")
        published = publication["observedAtMs"]
        moved = {}
        deadline = time.time() + 300
        while len(moved) < len(fleet) and time.time() < deadline:
            for client in fleet:
                if client.name not in moved:
                    status = client.status()
                    if online_on(status, {"ap-b1", "ap-b2"}):
                        moved[client.name] = {"delayMs": now_ms() - published,
                                              "ap": status["connection"]["accessPointId"]}
            time.sleep(0.5)
        delays = [v["delayMs"] for v in moved.values()]
        result["online"] = {"clients": len(fleet), "moved": len(moved), "maxDelayMs": max(delays, default=None),
                            "samples": moved, "pass": len(moved) == len(fleet) and max(delays) <= 120_000}
        log(f"online clients: {len(moved)}/{len(fleet)}, max {max(delays, default=None)} ms")
        # Old addresses go away for good.
        points = {p["id"]: p for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"]}
        for ap_id in ("ap-a1", "ap-a2"):
            admin.ok("PATCH", f"/platform/access/access-points/{ap_id}",
                     {"expectedRevision": points[ap_id]["revision"], "status": "disabled"})
        publish(admin, approver, "old addresses disabled")
        state = ls.load_state()
        for ap_id in ("ap-a1", "ap-a2"):
            ls.signal_process(state, st.SITES[ap_id].edge, 15, wait=10)
            ls.signal_process(state, st.SITES[ap_id].gateway, 15, wait=30)
        evidence("4-migrate", result)
        result["frozen"].update(restart_frozen(app, frozen.name, frozen.root, old_seed))
    finally:
        for client in fleet:
            client.stop()
    evidence("4-migrate", result)


def restart_frozen(app, name, root, seed):
    """The device comes back after a week: its clock of the last refresh is seven days old, its
    manifest names addresses that are gone."""
    book = rewind_refresh_clock(root / "data", app["bootstrapId"], 7 * DAY_MS)
    restarted = Client(name, app, data_dir=root, seed=seed).start()
    ok = online_wait(restarted, {"ap-b1", "ap-b2"}, 120)
    delay = now_ms() - restarted.started_at
    status = restarted.status()
    events = list(restarted.events)
    restarted.stop()
    log(f"frozen client: {delay} ms to online on {((status or {}).get('connection') or {}).get('accessPointId')}")
    return {"rewoundBook": {"lastRefreshMs": book.get("lastRefreshMs")}, "startToOnlineMs": delay, "statusAfter": status,
            "events": [e for e in events if e["kind"] in ("changed", "switched", "login")],
            "phases": [(e["ts"] - restarted.started_at, e["data"].get("phase"), e["data"].get("accessPointId"))
                       for e in events if e["kind"] == "connection"],
            "pass": bool(ok) and delay <= 60_000}


def command_migrate_frozen(_args):
    """The second half of `migrate` over an existing frozen directory, with the online samples read
    back from what each client recorded."""
    facts = load_facts()
    app = facts["apps"]["main"]
    root = max(st.lab_path("clients").glob("frozen-*"), key=lambda path: path.stat().st_mtime)
    seed = max(st.lab_path("seeds").glob("aged-*.json"), key=lambda path: path.stat().st_mtime)
    payload = payload_of(seed.read_bytes())
    change = platform_psql("SELECT id, (extract(epoch FROM published_at) * 1000)::bigint FROM platform_access.change_sets "
                           "WHERE reason LIKE 'address migration%' AND published_at IS NOT NULL ORDER BY published_at DESC LIMIT 1")
    published = int(change.split("|")[1]) if change else None
    moved = {}
    for file in sorted(st.lab_path("clients").glob("migrate-*/events.ndjson")):
        for line in file.read_text(errors="replace").splitlines():
            event = json.loads(line)
            data = event.get("data") or {}
            if event["kind"] == "connection" and data.get("phase") == "online" and data.get("accessPointId") in ("ap-b1", "ap-b2"):
                moved[file.parent.name] = {"delayMs": event["ts"] - published, "ap": data["accessPointId"]}
                break
    delays = [value["delayMs"] for value in moved.values()]
    result = {"online": {"reconstructedFrom": "each client's events.ndjson and change_sets.published_at",
                         "clients": 20, "moved": len(moved), "maxDelayMs": max(delays, default=None),
                         "p50DelayMs": percentile(delays, .5) if delays else None, "samples": moved,
                         "pass": len(moved) == 20 and max(delays, default=1 << 60) <= 120_000},
              "frozen": {"serial": payload["serial"], "issuedAt": payload["issuedAt"], "directory": root.name,
                         **restart_frozen(app, root.name, root, seed)}}
    evidence("4-migrate", result)
    log(json.dumps({"online": {k: result["online"][k] for k in ("moved", "p50DelayMs", "maxDelayMs", "pass")},
                    "frozen": {k: result["frozen"][k] for k in ("serial", "startToOnlineMs", "pass")}}))


def online_wait(client, access_points, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if online_on(client.status(), access_points):
            return True
        time.sleep(0.25)
    return False


# ------------------------------------------------------------- 5. mirror down --
class BlackHole:
    """Accepts TCP on a mirror's port and never answers: the failure a closed port is not."""

    def __init__(self, port):
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", port))
        self.server.listen(128)
        self.held = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                self.held.append(self.server.accept()[0])
            except OSError:
                return

    def close(self):
        self.server.close()
        for connection in self.held:
            connection.close()


def command_mirror_down(args):
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    primary = st.MIRRORS["a"]
    fleet = start_fleet(app, args.clients, "mirror", seed)
    result = {}

    def refresh_all(label):
        rows = []
        for client in fleet:
            event = client.refresh()
            report = (event or {}).get("data", {}).get("report", {})
            rows.append({"client": client.name, "elapsedMs": (event or {}).get("data", {}).get("elapsedMs"),
                         "outcome": report.get("outcome"), "source": report.get("source"),
                         "attempts": [{k: a.get(k) for k in ("source", "result", "code")}
                                      for a in report.get("attempts", [])],
                         "online": online_on(client.status())})
        values = [r["elapsedMs"] or 0 for r in rows]
        result[label] = {"p50Ms": percentile(values, .5), "maxMs": max(values), "rows": rows,
                         "allFine": all(r["outcome"] in ("upToDate", "adopted") and r["online"] for r in rows)}
        log(f"{label}: p50 {result[label]['p50Ms']} ms, max {result[label]['maxMs']} ms, "
            f"fine={result[label]['allFine']}")

    try:
        wait_fleet_online(fleet)
        refresh_all("baseline")
        state = ls.load_state()
        st.mirror_stop(state, primary)
        refresh_all("primary-closed")
        bump_weight(admin, "ap-b1")
        publication = publish(admin, approver, "publication with the primary mirror closed")
        serial = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        adopted = {}
        deadline = time.time() + 180
        while len(adopted) < len(fleet) and time.time() < deadline:
            for client in fleet:
                event = client.wait(lambda e: e["kind"] == "changed" and e["data"]["serial"] >= serial, 0)
                if event:
                    adopted[client.name] = {"delayMs": event["ts"] - publication["observedAtMs"],
                                            "source": event["data"]["source"]}
            time.sleep(0.5)
        result["publication-while-closed"] = {"approveToPublishedSec": publication["approveToPublishedSec"],
                                              "adopted": len(adopted), "clients": len(fleet), "samples": adopted}
        hole = BlackHole(primary.tls_port)
        refresh_all("primary-black-holed")
        hole.close()
    finally:
        state = ls.load_state()
        if not ls.live(state, primary.front):
            st.mirror_start(state, primary)
        for client in fleet:
            client.stop()
    evidence("5-mirror-down", result)


def command_mirror_hole(args):
    """Clients with no history ask the primary first, so here the black hole really is in the way:
    a port that takes the connection and never answers, unlike a closed one."""
    facts = load_facts()
    app = facts["apps"]["main"]
    seed = seed_file(app)
    primary = st.MIRRORS["a"]
    state = ls.load_state()
    st.mirror_stop(state, primary)
    hole = BlackHole(primary.tls_port)
    rows = []
    try:
        for index in range(args.clients):
            client = Client(f"hole-{now_ms()}-{index}", app, seed=seed, no_login=True, tick=60).start()
            try:
                ls.require(client.wait(lambda e: e["kind"] == "started", 30), "client did not start")
                event = client.refresh()
            finally:
                client.stop()
            data = (event or {}).get("data", {})
            report = data.get("report", {})
            rows.append({"elapsedMs": data.get("elapsedMs"), "outcome": report.get("outcome"),
                         "attempts": [{k: a.get(k) for k in ("source", "result", "code")}
                                      for a in report.get("attempts", [])]})
    finally:
        hole.close()
        state = ls.load_state()
        st.mirror_start(state, primary)
    values = [r["elapsedMs"] or 0 for r in rows]
    summary = {"clients": len(rows), "p50Ms": percentile(values, .5), "maxMs": max(values),
               "heldConnections": len(hole.held),
               "allFine": all(r["outcome"] in ("upToDate", "adopted") for r in rows)}
    evidence("5-mirror-hole", {"summary": summary, "rows": rows})
    log(json.dumps(summary))


# --------------------------------------------------------------------- 6. lint --
def lint_case(admin, rule, expected_code, prepare, restore):
    """Refused by the rule on :submit: still 200, still draft, with the finding in `lint`."""
    try:
        prepare()
        detail = admin.ok("POST", "/platform/access/change-sets", reason=f"lint reverse case {rule}")
        submitted = admin.ok("POST", f"/platform/access/change-sets/{detail['id']}:submit", reason=rule)
        result = next(r for r in submitted["lint"]["rules"] if r["rule"] == rule)
        codes = [f["code"] for f in result["findings"]]
        return {"rule": rule, "status": submitted["status"], "ruleStatus": result["status"], "codes": codes,
                "findings": result["findings"], "note": result.get("note"), "lintPassed": submitted["lint"]["passed"],
                "pass": submitted["status"] == "draft" and result["status"] == "fail" and expected_code in codes}
    finally:
        restore()


def access_point(admin, ap_id):
    return next(p for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"] if p["id"] == ap_id)


def set_status(admin, ap_id, status):
    point = access_point(admin, ap_id)
    admin.ok("PATCH", f"/platform/access/access-points/{ap_id}", {"expectedRevision": point["revision"], "status": status})


def set_mirror(admin, mirror_id, **fields):
    mirror = next(m for m in admin.ok("GET", "/platform/access/mirrors") if m["id"] == mirror_id)
    admin.ok("PATCH", f"/platform/access/mirrors/{mirror_id}", {"expectedRevision": mirror["revision"], **fields})


def set_policy(admin, app, policy):
    placement = admin.ok("GET", f"/apps/{app['appId']}/access/placement")
    admin.ok("PUT", f"/apps/{app['appId']}/access/placement",
             {"expectedRevision": placement["revision"], "clientPolicy": policy})


def active_points(admin):
    return [p["id"] for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"]
            if p["status"] == "active"]


def short_certificate(admin, ap_id):
    """A certificate expiring within thirty days on a live Edge; the prober reports it."""
    site = st.SITES[ap_id]
    state = ls.load_state()
    certificate, key = st.tls_dir() / f"edge-{ap_id}.crt", st.tls_dir() / f"edge-{ap_id}.key"
    saved = certificate.read_bytes(), key.read_bytes()
    certificate.unlink()
    key.unlink()
    st.issue_certificate(f"edge-{ap_id}", [site.host], days=10)
    ls.signal_process(state, site.edge, 15, wait=10)
    st.command_ctl(argparse.Namespace(action="start", process=site.edge))
    return saved


def command_lint(_args):
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, _, _ = people()
    keys = st.Lab().core
    results = []
    active = active_points(admin)
    ls.require(len(active) >= 2, f"need two active access points, have {active}")
    keep, other = active[0], active[1]

    results.append(lint_case(admin, "homeCellCapacity", "homeCellTooFewActive",
                             lambda: set_status(admin, other, "disabled"),
                             lambda: set_status(admin, other, "active")))
    results.append(lint_case(admin, "mirrorDiversity", "mirrorsNotDiverse",
                             lambda: [set_mirror(admin, m, enabled=False) for m in ("m-b", "m-c")],
                             lambda: [set_mirror(admin, m, enabled=True) for m in ("m-b", "m-c")]))
    # The placement API refuses such a policy first, so what reaches the rule is a stored row the
    # table has since moved past: written straight to the database here.
    placement = admin.ok("GET", f"/apps/{app['appId']}/access/placement")
    refused = admin.call("PUT", f"/apps/{app['appId']}/access/placement",
                         {"expectedRevision": placement["revision"], "clientPolicy": {"raceStaggerMs": 1}})
    row = lint_case(admin, "clientPolicyBounds", "clientPolicyOutOfBounds",
                    lambda: platform_psql("UPDATE platform_access.app_placements SET client_policy='{\"raceStaggerMs\":1}' "
                                          f"WHERE app_id='{app['appId']}'"),
                    lambda: platform_psql("UPDATE platform_access.app_placements SET client_policy='{}' "
                                          f"WHERE app_id='{app['appId']}'"))
    row["apiRefusal"] = {"status": refused[0], "field": ((refused[1].get("error") or {}).get("params") or {}).get("field")
                         if isinstance(refused[1], dict) else None}
    row["pass"] = row["pass"] and refused[0] == 400
    results.append(row)
    results.append(lint_case(admin, "noPlugPull", "onlineUsersCutOff",
                             lambda: [set_status(admin, ap, "disabled") for ap in active],
                             lambda: [set_status(admin, ap, "active") for ap in active]))

    created = []

    def many_access_points():
        tags = [f"pad-{index:02d}-" + "x" * 40 for index in range(16)]
        known = {p["id"] for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"]}
        for index in range(70):
            ap_id = f"ap-pad-{index:03d}"
            if ap_id in known:
                set_status(admin, ap_id, "active")
            else:
                admin.ok("POST", "/platform/access/access-points", {
                    "id": ap_id, "cellId": st.CELL, "host": f"{ap_id}.{st.HOST_SUFFIX}", "services": SERVICES,
                    "status": "active", "tags": tags})
            created.append(ap_id)

    results.append(lint_case(admin, "manifestSize", "manifestTooLarge", many_access_points,
                             lambda: [set_status(admin, ap, "disabled") for ap in created]))

    target = next(ap for ap in active)
    saved = {}

    def expire():
        saved["files"] = short_certificate(admin, target)
        host = st.SITES[target].host
        deadline = time.time() + 180
        limit = now_ms() + 30 * DAY_MS
        while time.time() < deadline:
            row = platform_psql("SELECT (extract(epoch FROM not_after) * 1000)::bigint "
                                f"FROM platform_access.ap_certificates WHERE host='{host}'")
            if row and row.isdigit() and int(row) < limit:
                return
            time.sleep(3)
        raise SystemExit("the prober did not report the short certificate")

    def renew():
        certificate, key = st.tls_dir() / f"edge-{target}.crt", st.tls_dir() / f"edge-{target}.key"
        if "files" not in saved:
            return
        certificate.write_bytes(saved["files"][0])
        key.write_bytes(saved["files"][1])
        state = ls.load_state()
        ls.signal_process(state, st.SITES[target].edge, 15, wait=10)
        st.command_ctl(argparse.Namespace(action="start", process=st.SITES[target].edge))

    results.append(lint_case(admin, "certificateExpiry", "certificateExpiring", expire, renew))
    for row in results:
        log(f"{row['rule']}: {row['status']} {row['ruleStatus']} {row['codes']} pass={row['pass']}")
    evidence("6-lint", {"cases": results, "pass": all(r["pass"] for r in results) and len(results) == 6})


# --------------------------------------------- secondary: release manifest --
RELEASE_FILE = "release/tauri/stable.dsse.json"
RELEASE_APP = {"version": "1.0.0", "build": 100, "distribution": "direct", "channel": "stable",
               "target": "darwin-aarch64", "locale": "zh-CN"}


def object_url(site, app, name):
    return f"{site.base_url}/{app['bootstrapId']}/{name}"


def release_entry(app, packs, *, build=200, halted=False, package=None):
    artifact = {"kind": "apk", "urls": [object_url(s, app, package["name"]) for s in st.MIRRORS.values()],
                "sizeBytes": package["size"], "sha256": package["sha256"], "signingCertSha256": "ab" * 32}
    return {"releaseId": f"rel-{build}", "version": f"2.{build}", "build": build, "publishedAt": iso(now_ms()),
            "rollout": {"basisPoints": 10000, "salt": f"rel-{build}", "halted": halted},
            "notes": {"zh-CN": [f"第 {build} 版实验台说明"], "en": [f"Lab notes for {build}"]},
            "artifacts": {"direct": artifact}, "resourcePacks": packs}


def release_document(app, key, cert, serial, releases, policy=None):
    payload = {"schema": "flare.release.v1", "bootstrapId": app["bootstrapId"], "platform": "tauri",
               "channel": "stable", "serial": serial, "issuedAt": iso(now_ms() - 60_000),
               "expiresAt": iso(now_ms() + 14 * DAY_MS), "policy": policy or {}, "releases": releases}
    return forge("ok-forge-good", key, payload, [cert], RELEASE_PAYLOAD_TYPE)


def update_status(client, op="update_state", **arguments):
    event = client.ask(op, 20, **arguments)
    return (event or {}).get("data", {}).get("status")


def settled_update(client, timeout=30):
    """`update_check` only starts the round, and a state past `checking` can be the old one: the
    round is over when the check time moves."""
    before = (update_status(client) or {}).get("lastCheckedAt")
    time.sleep(0.01)
    status = update_status(client, "update_check", manual=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = update_status(client)
        if status and status["state"] != "checking" and status.get("lastCheckedAt") != before:
            return status
        time.sleep(0.2)
    return status


def stored_packs(client, app):
    database = sqlite3.connect(client.root / "data" / "access" / "access.db")
    try:
        rows = database.execute("SELECT key FROM access_kv WHERE key LIKE ?",
                                (f"resource/{app['bootstrapId']}/%",)).fetchall()
    finally:
        database.close()
    return sorted(row[0].rsplit("/", 1)[1] for row in rows)


def wait_packs(client, app, expected, timeout=20):
    deadline = time.time() + timeout
    held = stored_packs(client, app)
    while held != expected and time.time() < deadline:
        time.sleep(0.2)
        held = stored_packs(client, app)
    return held


def command_release(_args):
    """Everything a control plane would publish, signed by hand with a certified lab key: the
    release service does not exist yet. The mirrors, TLS, the fetch path and the client are real."""
    facts = load_facts()
    app = facts["apps"]["forge"]
    lab = st.Lab()
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    seed = seed_file(app)
    steps = {}
    pack_body = json.dumps({"schema": "flare.resource.v1", "kind": "strings",
                            "locales": {"zh-CN": {"composerReadOnlyUpdate": "实验台词条"}}}, ensure_ascii=False).encode()
    pack_name = "res/strings-1.json"
    blob = secrets.token_bytes(1 << 20)
    package = {"name": "pkg/app-200.apk", "size": len(blob), "sha256": hashlib.sha256(blob).hexdigest()}
    put_everywhere(lab, app, pack_body, pack_name)
    put_everywhere(lab, app, blob, package["name"], {"content-type": "application/octet-stream"})
    primary = st.MIRRORS["a"]
    pack = {"id": "strings-main", "kind": "strings", "packVersion": 1, "minBuild": 0,
            # The first address answers 404: the client has to fail over to the next mirror.
            "url": [object_url(primary, app, "res/missing.json"), object_url(st.MIRRORS["b"], app, pack_name)],
            "sha256": hashlib.sha256(pack_body).hexdigest(), "sizeBytes": len(pack_body)}
    client_name = f"release-{now_ms()}"

    def client(**extra):
        return Client(client_name, app, seed=seed, no_login=True, tick=60, app_info=RELEASE_APP, **extra).start()

    def wait_ready(item):
        ls.require(item.wait(lambda e: e["kind"] == "started", 30), "release client did not start")

    def summary(status):
        status = status or {}
        return {k: status.get(k) for k in ("state", "prompt", "blockUi", "readOnly", "error", "blockReason")} | \
            {"releaseBuild": (status.get("release") or {}).get("build")}

    document_1 = release_document(app, key, cert, 1, [release_entry(app, [pack], package=package)])
    put_everywhere(lab, app, document_1, RELEASE_FILE)
    first = client()
    try:
        wait_ready(first)
        checked = settled_update(first)
        steps["check"] = summary(checked)
        steps["resourcesFirstRun"] = (first.ask("update_resources", 20) or {}).get("data", {}).get("resources")
        steps["packsStored"] = wait_packs(first, app, ["strings-main"])
        started = time.time()
        downloaded = (first.ask("update_download", 60) or {}).get("data", {}).get("status")
        steps["download"] = {**summary(downloaded), "seconds": round(time.time() - started, 3)}
        steps["install"] = (first.ask("update_install", 20) or {}).get("data", {})
    finally:
        first.stop()
    second = client()
    try:
        wait_ready(second)
        resources = (second.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["resourcesAfterRestart"] = resources
        steps["contentMatches"] = [p.get("contentJson") == pack_body.decode() for p in resources.get("packs", [])]
        # Withdrawn: the release is halted and carries no pack any more.
        document_2 = release_document(app, key, cert, 2, [release_entry(app, [], halted=True, package=package)])
        put_everywhere(lab, app, document_2, RELEASE_FILE)
        steps["afterWithdrawal"] = summary(settled_update(second))
        steps["packsAfterWithdrawal"] = wait_packs(second, app, [])
        run_snapshot = (second.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["runSnapshotKept"] = [p["id"] for p in run_snapshot.get("packs", [])]
        # A mirror serving the older document changes nothing.
        put_everywhere(lab, app, document_1, RELEASE_FILE)
        steps["afterReplayOfSerial1"] = summary(settled_update(second))
        put_everywhere(lab, app, document_2, RELEASE_FILE)
    finally:
        second.stop()
    third = client()
    try:
        wait_ready(third)
        resources = (third.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["resourcesAfterWithdrawalRestart"] = [p["id"] for p in resources.get("packs", [])]
        # Forced update from the manifest alone.
        document_3 = release_document(app, key, cert, 3, [release_entry(app, [], build=201, package=package)],
                                      {"minBuild": 150, "blockUi": True})
        put_everywhere(lab, app, document_3, RELEASE_FILE)
        steps["required"] = summary(settled_update(third))
    finally:
        third.stop()
        delete_everywhere(lab, app, RELEASE_FILE)
    checks = {
        "checkFindsRelease": steps["check"]["state"] == "available" and steps["check"]["releaseBuild"] == 200,
        "firstRunHasNoPacks": (steps["resourcesFirstRun"] or {}).get("packs") == [],
        "packStoredViaSecondUrl": steps["packsStored"] == ["strings-main"],
        "downloadVerified": steps["download"]["state"] == "readyToInstall",
        "packLoadedOnRestart": steps["contentMatches"] == [True],
        "withdrawalClearsRelease": steps["afterWithdrawal"]["state"] == "upToDate",
        "withdrawalRemovesPack": steps["packsAfterWithdrawal"] == [],
        "runSnapshotUnchanged": steps["runSnapshotKept"] == ["strings-main"],
        "replayIgnored": steps["afterReplayOfSerial1"]["state"] == "upToDate",
        "goneAfterRestart": steps["resourcesAfterWithdrawalRestart"] == [],
        "requiredBlocks": steps["required"]["state"] == "required" and steps["required"]["blockUi"] is True,
    }
    evidence("s1-release", {"steps": steps, "checks": checks, "pass": all(checks.values())})
    log(json.dumps(checks))


# -------------------------------------------- secondary: recovery link import --
def recovery_link(app, serial, access_points, sources, name, expires_ms=None):
    output = st.lab_path("trust", f"recovery-{name}-{now_ms()}.txt")
    arguments = ["trust", "recovery", "--roots", st.lab_path("trust", "trust-roots.json"),
                 "--root-key", st.lab_path("trust", "root-a.key.pem"), "--bootstrap-id", app["bootstrapId"],
                 "--app-id", app["appId"], "--serial", serial, "--region-id", st.REGION, "--cell-id", st.CELL]
    for ap_id in access_points:
        arguments += ["--access-point", f"{ap_id}={st.SITES[ap_id].host}"]
    for source in sources:
        arguments += ["--source", source]
    if expires_ms:
        arguments += ["--issued-at", iso(expires_ms - DAY_MS), "--expires-at", iso(expires_ms)]
    st.flarectl(*arguments, "--output", output)
    return output.read_text().strip()


def only_dead_mirror(payload, app):
    """The manifest's bootstrap sources replaced by one mirror nobody answers on."""
    payload["bootstrap"]["sources"] = [
        {"kind": "https", "url": f"https://dead-mirror.{st.HOST_SUFFIX}:47999/flare/v1/{app['bootstrapId']}/"}]


def stranded_seed(app, serial=None):
    """A manifest whose every address is dead: access points gone, mirrors closed. One serial above
    the published manifest unless `serial` is given."""
    payload = payload_of(read_document(app))
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    payload["serial"] = serial if serial is not None else payload["serial"] + 1
    only_dead_mirror(payload, app)
    for region in payload["regions"]:
        for cell in region["cells"]:
            for point in cell["accessPoints"]:
                point["host"] = f"dead-{point['id']}.{st.HOST_SUFFIX}"
    file = st.lab_path("seeds", f"stranded-{now_ms()}.json")
    file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    file.write_bytes(forge("ok-forge-good", key, payload, [cert]))
    return file, payload


def command_recovery(_args):
    facts = load_facts()
    app = facts["apps"]["main"]
    other = facts["apps"]["forge"]
    seed, payload = stranded_seed(app)
    serial = payload["serial"]
    alive = [ap for ap in ("ap-b1", "ap-b2", "ap-a1", "ap-a2") if ls.port_open(st.SITES[ap].tls_port)][:2]
    sources = [f"{site.base_url}/{app['bootstrapId']}" for site in st.MIRRORS.values()]
    client = Client(f"recovery-{now_ms()}", app, seed=seed).start()
    steps = {"aliveAccessPoints": alive, "heldSerial": serial}
    try:
        deadline = time.time() + 120
        status = client.status()
        while time.time() < deadline and not ((status or {}).get("access") or {}).get("recovering"):
            time.sleep(1)
            status = client.status()
        steps["beforeImport"] = {"recovering": ((status or {}).get("access") or {}).get("recovering"),
                                 "connection": (status or {}).get("connection"),
                                 "secondsToRecovering": round(120 - (deadline - time.time()), 1)}
        cases = [
            ("tampered", recovery_link(app, serial + 10, alive, sources, "tampered")[:-6] + "AAAAAA"),
            ("wrong-bootstrap", recovery_link(other, serial + 10, alive, sources, "wrong-app")),
            ("rollback", recovery_link(app, serial - 1, alive, sources, "rollback")),
        ]
        steps["rejected"] = {}
        for name, link in cases:
            event = client.ask("import", 20, link=link)
            steps["rejected"][name] = (event or {}).get("data")
        link = recovery_link(app, serial + 10, alive, sources, "good")
        steps["linkBytes"] = len(link)
        started = now_ms()
        event = client.ask("import", 30, link=link)
        steps["imported"] = (event or {}).get("data")
        ok = online_wait(client, set(alive), 90)
        steps["importToOnlineMs"] = now_ms() - started
        steps["after"] = client.status()
    finally:
        client.stop()
    restarted = Client(client.name, app, data_dir=client.root, seed=seed).start()
    try:
        steps["onlineAfterRestart"] = online_wait(restarted, set(alive), 60)
        steps["afterRestart"] = restarted.status()
    finally:
        restarted.stop()
    result = (steps["imported"] or {}).get("result") or {}
    refused = all(not ((v or {}).get("result") or {}).get("outcome") == "adopted" for v in steps["rejected"].values())
    evidence("s2-recovery", {"steps": steps, "online": ok, "refused": refused,
                             "pass": bool(ok) and refused and result.get("outcome") == "adopted"
                             and steps["onlineAfterRestart"] and bool(steps["beforeImport"]["recovering"])})
    log(json.dumps({"online": ok, "refused": refused, "outcome": result.get("outcome"),
                    "importToOnlineMs": steps["importToOnlineMs"], "restart": steps["onlineAfterRestart"]}))


# ------------------------------------------- combined failures: everything down --
# Every stranded client is back within this long after the publication that brings it back has
# completed (upload verified on the mirrors, or the manual upload done).
RECONNECT_AFTER_PUBLISH_MS = 10 * 60_000
# The runbook's margin over the highest serial anyone may hold when signing outside the platform.
OFFLINE_SERIAL_MARGIN = 100_000
SERIAL_FLOOR_MAX_JUMP = 1_000_000
RESCUE_BASE = f"https://{st.RESCUE_MIRROR_HOST}:{st.RESCUE_MIRROR_PORT}/flare/v1"
DNS_HEADER, DNS_TXT, DNS_IN = 12, 16, 1


def offline_serial(mirror_serials, recovery_serials=()):
    """The runbook's rule for a manifest signed while the control plane is down: above every serial
    a reachable mirror holds and every recovery serial reserved or handed out, plus the margin.

    >>> offline_serial([1040, 1046, None], [1100])
    101100
    >>> offline_serial([7])
    100007
    """
    known = [serial for serial in (*mirror_serials, *recovery_serials) if serial]
    ls.require(known, "no serial to start from: no mirror answered")
    return max(known) + OFFLINE_SERIAL_MARGIN


def dns_query(name, record_type=DNS_TXT):
    """A query as the client builds it (id 0, recursion desired, one question).

    >>> dns_query("a.b").hex()
    '000001000001000000000000016101620000100001'
    """
    message = bytearray([0, 0, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0])
    for label in name.rstrip(".").split("."):
        message += bytes([len(label)]) + label.encode()
    return bytes(message + b"\x00" + record_type.to_bytes(2, "big") + DNS_IN.to_bytes(2, "big"))


def dns_question(message):
    """(name, type, end of the question) of a one-question query; None when it is not one.

    >>> dns_question(dns_query("_flare.ab12.boot.example"))
    ('_flare.ab12.boot.example', 16, 42)
    >>> dns_question(b"\\x00" * 5) is None
    True
    """
    if len(message) < DNS_HEADER or int.from_bytes(message[4:6], "big") != 1:
        return None
    labels, at = [], DNS_HEADER
    while at < len(message) and message[at]:
        length = message[at]
        if length & 0xC0 or at + 1 + length > len(message):
            return None
        labels.append(message[at + 1:at + 1 + length].decode(errors="replace"))
        at += 1 + length
    if at + 5 > len(message):
        return None
    return ".".join(labels).lower(), int.from_bytes(message[at + 1:at + 3], "big"), at + 5


def dns_answer(query, texts, exists=True):
    """The answer to `query` with these TXT strings, laid out as the client's own test resolver
    does (flare_access::bootstrap::doh::encode_txt_response): answers point back at the question,
    text is split into 255-byte strings. A name that does not exist gets NXDOMAIN.

    >>> query = dns_query("_flare.ab12.boot.example")
    >>> dns_texts(dns_answer(query, ["v=flare1;u=https://m.example/flare/v1/ab12/"]))
    ['v=flare1;u=https://m.example/flare/v1/ab12/']
    >>> dns_texts(dns_answer(query, ["x" * 600])) == ["x" * 600]
    True
    >>> answer = dns_answer(query, [], exists=False)
    >>> answer[3] & 0x0F, dns_texts(answer)
    (3, None)
    >>> dns_texts(dns_answer(query, []))
    []
    """
    _, _, end = dns_question(query)
    header = bytearray(query[:DNS_HEADER])
    header[2], header[3] = 0x81, 0x80 if exists else 0x83
    header[4:12] = (1).to_bytes(2, "big") + len(texts).to_bytes(2, "big") + bytes(4)
    message = bytes(header) + query[DNS_HEADER:end]
    for text in texts:
        raw = text.encode()
        chunks = [raw[index:index + 255] for index in range(0, len(raw), 255)] or [b""]
        rdata = b"".join(bytes([len(chunk)]) + chunk for chunk in chunks)
        message += (b"\xc0\x0c" + DNS_TXT.to_bytes(2, "big") + DNS_IN.to_bytes(2, "big") + (60).to_bytes(4, "big")
                    + len(rdata).to_bytes(2, "big") + rdata)
    return message


def dns_texts(message):
    """What the client reads from an answer: every TXT record's strings joined; None for an error
    or a malformed message (flare_access::bootstrap::doh::parse_txt_response)."""
    if len(message) < DNS_HEADER or message[3] & 0x0F:
        return None

    def skip(at):
        while True:
            length = message[at]
            if length == 0:
                return at + 1
            if length & 0xC0 == 0xC0:
                return at + 2
            at += 1 + length

    try:
        at = DNS_HEADER
        for _ in range(int.from_bytes(message[4:6], "big")):
            at = skip(at) + 4
        texts = []
        for _ in range(int.from_bytes(message[6:8], "big")):
            at = skip(at)
            kind = int.from_bytes(message[at:at + 2], "big")
            length = int.from_bytes(message[at + 8:at + 10], "big")
            rdata, at = message[at + 10:at + 10 + length], at + 10 + length
            if kind != DNS_TXT:
                continue
            text, offset = b"", 0
            while offset < len(rdata):
                text += rdata[offset + 1:offset + 1 + rdata[offset]]
                offset += 1 + rdata[offset]
            texts.append(text.decode(errors="replace"))
        return texts
    except IndexError:
        return None


class LabHttps:
    """A small HTTPS server on the lab's private CA, for what the stack itself does not run: a
    public DoH resolver and a rescue mirror on a fresh host. `respond(path, query, headers)` gives
    (status, headers, body)."""

    def __init__(self, name, host, port, respond):
        import http.server
        import ssl
        certificate, key = st.issue_certificate(name, [host])
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):  # noqa: N802
                parsed = urllib.parse.urlsplit(self.path)
                status, headers, body = respond(parsed.path, urllib.parse.parse_qs(parsed.query), self.headers)
                with outer.lock:
                    outer.served.append({"atMs": now_ms(), "path": parsed.path, "status": status})
                self.send_response(status)
                for header, value in {**headers, "Content-Length": str(len(body))}.items():
                    self.send_header(header, value)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        self.lock = threading.Lock()
        self.served = []
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(certificate), str(key))
        context.set_alpn_protocols(["http/1.1"])
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class FakeDoh(LabHttps):
    """A public DNS-over-HTTPS resolver (RFC 8484 GET): TXT records from `records`, NXDOMAIN for
    names it has none for, every question kept in `asked`. Clients and the platform's audit both
    reach it as the manifest's `dohTxt` resolver."""

    def __init__(self):
        self.records = {}
        self.asked = []
        super().__init__("doh-resolver", st.DOH_HOST, st.DOH_PORT, self._respond)

    def serve(self, name, *texts):
        with self.lock:
            self.records[name.lower()] = list(texts)

    def _respond(self, path, query, _headers):
        raw = (query.get("dns") or [""])[0]
        try:
            message = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        except ValueError:
            message = b""
        question = dns_question(message) if path == "/dns-query" else None
        if not question:
            return 400, {}, b""
        name, record_type, _ = question
        with self.lock:
            texts = self.records.get(name)
            self.asked.append({"atMs": now_ms(), "name": name, "type": record_type})
        answer = dns_answer(message, (texts or []) if record_type == DNS_TXT else [], exists=texts is not None)
        return 200, {"Content-Type": "application/dns-message"}, answer


class RescueMirror(LabHttps):
    """A mirror on a host no manifest names, filled by hand during an incident: what the TXT
    pointer sends clients to. Serves objects with the platform's cache headers and ETags."""

    def __init__(self):
        self.objects = {}
        super().__init__("rescue-mirror", st.RESCUE_MIRROR_HOST, st.RESCUE_MIRROR_PORT, self._respond)

    def put(self, app, name, body):
        with self.lock:
            self.objects[f"/flare/v1/{app['bootstrapId']}/{name}"] = body

    def _respond(self, path, _query, headers):
        with self.lock:
            body = self.objects.get(path)
        if body is None:
            return 404, {}, b""
        etag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
        common = {"ETag": etag, "Cache-Control": "public, max-age=60, stale-while-revalidate=300, stale-if-error=604800",
                  "Access-Control-Allow-Origin": "*"}
        if headers.get("If-None-Match") == etag:
            return 304, common, b""
        return 200, {**common, "Content-Type": "application/json"}, body


def manifest_points(app):
    """Access point ids of the manifest the mirrors serve now: what a client knows about."""
    payload = payload_of(read_document(app))
    return [point["id"] for region in payload["regions"] for cell in region["cells"] for point in cell["accessPoints"]]


def spare_sites(app):
    """Lab sites no published manifest names: the 'new nodes' of an incident."""
    known = set(manifest_points(app))
    return [ap_id for ap_id in st.SITES if ap_id not in known]


def activate(admin, ap_ids):
    """New access points in the topology, or old rows brought back to active."""
    known = {p["id"]: p for p in admin.ok("GET", "/platform/access/access-points?pageSize=100")["items"]}
    for ap_id in ap_ids:
        if ap_id not in known:
            site = st.SITES[ap_id]
            admin.ok("POST", "/platform/access/access-points", {
                "id": ap_id, "cellId": st.CELL, "host": site.host, "services": SERVICES, "status": "active"})
        elif known[ap_id]["status"] != "active":
            set_status(admin, ap_id, "active")


def retire(admin, approver, ap_ids, reason):
    """After an incident: the dead access points leave the manifest through an ordinary two-person
    publication, once their users sit elsewhere. Clean-up, so it is recorded, never fatal."""
    try:
        for ap_id in ap_ids:
            if access_point(admin, ap_id)["status"] != "disabled":
                set_status(admin, ap_id, "disabled")
        result = publish(admin, approver, reason)
        return {"accessPoints": ap_ids, "changeSet": result["changeSet"], "retired": True}
    except (SystemExit, StopIteration) as error:
        return {"accessPoints": ap_ids, "retired": False, "error": str(error)[:800]}


def stop_control_plane():
    state = ls.load_state()
    for role in ("api", "worker", "prober"):
        ls.signal_process(state, f"platform-{role}", 15, wait=30)


def start_control_plane():
    for role in ("api", "worker", "prober"):
        if not ls.live(ls.load_state(), f"platform-{role}"):
            st.platform_process(ls.load_state(), st.Lab(), role)


def emergency_publish(author, other, reason):
    """Author alone, stated reason, passing lint. The second person is refused first: the
    emergency path is the author's and only the author's."""
    detail = author.ok("POST", "/platform/access/change-sets", reason=reason)
    change_set = detail["id"]
    path = f"/platform/access/change-sets/{change_set}:emergency-publish"
    refused_status, refused = other.call("POST", path, reason="not the author")
    started = time.time()
    status, body = author.call("POST", path, reason=reason)
    ls.require(status == 200, f"emergency publish: {status} {str(body)[:800]}")
    final = wait_change_set(author, change_set)
    ls.require(final["status"] == "published", f"emergency change set failed: {final.get('failure')}")
    return {"changeSet": change_set, "callToPublishedSec": round(time.time() - started, 3), "observedAtMs": now_ms(),
            "emergency": final.get("emergency"), "publishedAt": final.get("publishedAt"),
            "notAuthor": {"status": refused_status,
                          "body": refused if not isinstance(refused, (bytes, str)) else str(refused)[:300]},
            "lintPassed": (final.get("lint") or {}).get("passed")}


def first_online(client, access_points, mark=0):
    for event in list(client.events[mark:]):
        data = event.get("data") or {}
        if event["kind"] == "connection" and data.get("phase") == "online" and data.get("accessPointId") in access_points:
            return event
    return None


def wait_reconnected(fleet, access_points, published_ms, marks, limit_ms=RECONNECT_AFTER_PUBLISH_MS):
    """Each client's first `online` on one of `access_points` after its mark, measured from the
    publication. Waits a minute past the limit so a late client is measured, not just failed."""
    back = {}
    deadline = published_ms / 1000 + (limit_ms + 60_000) / 1000
    while len(back) < len(fleet) and time.time() < deadline:
        for client in fleet:
            if client.name not in back:
                event = first_online(client, access_points, marks[client.name])
                if event:
                    back[client.name] = {"delayMs": event["ts"] - published_ms, "ap": event["data"]["accessPointId"]}
        time.sleep(0.5)
    delays = [value["delayMs"] for value in back.values()]
    return {"clients": len(fleet), "back": len(back), "maxDelayMs": max(delays, default=None),
            "p50DelayMs": percentile(delays, .5) if delays else None, "limitMs": limit_ms, "samples": back,
            "missing": [c.name for c in fleet if c.name not in back],
            "pass": len(back) == len(fleet) and max(delays, default=1 << 60) <= limit_ms}


def wait_offline(fleet, timeout=180):
    """How long each client takes to notice that its access point is gone."""
    started, gone = time.time(), {}
    while len(gone) < len(fleet) and time.time() - started < timeout:
        for client in fleet:
            if client.name not in gone and not online_on(client.status(timeout=5)):
                gone[client.name] = round(time.time() - started, 1)
        time.sleep(1)
    return gone


def send_round(fleet, label, timeout=60):
    """Each client sends one text to the next one. Dispatched to all first, then collected, so
    one client waiting for its acknowledgement holds up nobody else."""
    pending = []
    for index, client in enumerate(fleet):
        peer = fleet[(index + 1) % len(fleet)]
        identity = uuid.uuid4().hex
        client.send(op="send", id=identity, to=json.loads(peer.config_file.read_text())["user"],
                    text=f"{label} {client.name} {now_ms()}")
        pending.append((client, identity))
    rows = []
    for client, identity in pending:
        event = client.wait(lambda e: isinstance(e.get("data"), dict) and e["data"].get("id") == identity, timeout)
        data = (event or {}).get("data") or {}
        rows.append({"client": client.name, "label": label, "clientMsgId": data.get("clientMsgId"),
                     "error": data.get("message") if event and event["kind"] == "error" else (None if event else "timeout")})
    return rows


def persisted(app, sent, timeout=180):
    """Which accepted messages Core stored: an accepted message must not be lost to the outage."""
    ids = [row["clientMsgId"] for row in sent if row["clientMsgId"]]
    ls.require(all(re.fullmatch(r"[A-Za-z0-9_.:-]+", i) for i in ids), "unexpected client message id")
    keys = st.Lab().core
    deadline, found = time.time() + timeout, set()
    while ids and time.time() < deadline:
        listed = ",".join(f"'{i}'" for i in ids)
        rows = ls.psql(keys, f"SELECT client_msg_id FROM messages WHERE tenant_id='{app['appId']}' "
                             f"AND client_msg_id IN ({listed})", ls.DATABASES["core"], check=False)
        found = set(rows.split()) if rows else set()
        if found >= set(ids):
            break
        time.sleep(2)
    return {"accepted": len(ids), "stored": len(found & set(ids)), "lost": sorted(set(ids) - found),
            "refused": [row for row in sent if not row["clientMsgId"]]}


def stop_access_points(ap_ids):
    state = ls.load_state()
    for ap_id in ap_ids:
        st.access_point_stop(state, ap_id)


def start_access_points(ap_ids):
    state = ls.load_state()
    for ap_id in ap_ids:
        st.access_point_start(state, ap_id)


def run_job(admin, handler, attempts=30):
    """Asks the scheduler to run a platform job now instead of at its next tick; a run already in
    progress holds the lease, so it is asked again until accepted."""
    status, body = admin.direct("GET", "/system/jobs?page=1&page_size=100")
    ls.require(status == 200, f"list jobs: {status} {str(body)[:300]}")
    job = next((j for j in body["data"]["items"] if j.get("handler") == handler), None)
    ls.require(job is not None, f"no job runs {handler}")
    for _ in range(attempts):
        status, body = admin.direct("POST", f"/system/jobs/{job['id']}/run", reason=f"acceptance runs {handler}")
        if status == 200:
            return job["id"]
        time.sleep(2)
    raise SystemExit(f"job {handler} was not accepted for a run: {status} {str(body)[:300]}")


def mirror_bases(app, directory):
    """Runbook step 2: the current document from every mirror that still answers, kept as files."""
    bases = {}
    for key, site in st.MIRRORS.items():
        status, body, _ = https_get(f"{site.base_url}/{app['bootstrapId']}/{ACCESS_FILE}")
        if status == 200:
            file = directory / f"base-{key}.{ACCESS_FILE}"
            file.write_bytes(body)
            bases[key] = {"file": file, "serial": payload_of(body)["serial"]}
    return bases


def offline_manifest(app, name, access_points, serial=None, recovery_serials=()):
    """The runbook for publishing new nodes while the control plane is down: a fresh incident key
    certified for 14 days, the mirrors' copies as bases, the serial by the runbook's rule unless
    given, the new access points added. Nothing here talks to the control plane."""
    directory = st.lab_path("trust", "incident", f"{name}-{now_ms()}")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    key_id = f"ok-incident-{time.strftime('%Y%m%d')}-{secrets.token_hex(3)}"
    key_file = directory / f"{key_id}.key.pem"
    public_key = st.flarectl("trust", "keygen", "--output", key_file, "--unencrypted").stdout.strip()
    cert = directory / f"{key_id}.cert"
    st.flarectl("trust", "certify", "--roots", st.lab_path("trust", "trust-roots.json"),
                "--root-key", st.lab_path("trust", "root-a.key.pem"), "--key-id", key_id, "--public-key", public_key,
                "--usage", "access", "--bootstrap-id", app["bootstrapId"], "--not-after", iso(now_ms() + 14 * DAY_MS),
                "--output", cert)
    bases = mirror_bases(app, directory)
    ls.require(bases, "no mirror answered; the runbook needs at least one base")
    serial = serial or offline_serial([b["serial"] for b in bases.values()], recovery_serials)
    output = directory / ACCESS_FILE
    arguments = ["trust", "manifest", "--roots", st.lab_path("trust", "trust-roots.json"), "--online-key", key_file,
                 "--key-id", key_id, "--key-cert", cert]
    for base in bases.values():
        arguments += ["--base", base["file"]]
    arguments += ["--serial", serial, "--cell-id", st.CELL]
    for ap_id in access_points:
        arguments += ["--access-point", f"{ap_id}={st.SITES[ap_id].host}"]
    st.flarectl(*arguments, "--output", output)
    return {"file": output, "keyId": key_id, "serial": serial, "accessPoints": list(access_points),
            "baseSerials": {key: base["serial"] for key, base in bases.items()}}


def wait_adopted(fleet, serial, marks, timeout):
    """Each client's `changed` event to at least `serial` after its mark."""
    deadline, adopted = time.time() + timeout, {}
    while len(adopted) < len(fleet) and time.time() < deadline:
        for client in fleet:
            if client.name in adopted:
                continue
            for event in list(client.events[marks[client.name]:]):
                if event["kind"] == "changed" and event["data"].get("serial", 0) >= serial:
                    adopted[client.name] = {"ts": event["ts"], "serial": event["data"]["serial"],
                                            "source": event["data"].get("source")}
                    break
        time.sleep(0.5)
    return adopted


def recovery_serials(facts):
    """What the incident record knows: serials reserved for recovery links and signed offline."""
    return [*facts.get("recoverySerials", []), *facts.get("offlineSerials", [])]


def command_total_outage(args):
    """Clients online, then every access point they know goes away. New nodes come up and the
    author publishes them alone through the emergency path."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    old, spares = manifest_points(app), spare_sites(app)
    ls.require(spares, f"every lab site is in the manifest ({old}); retire some first")
    fleet = start_fleet(app, args.clients, "total", seed)
    result = {"oldAccessPoints": old, "newAccessPoints": spares}
    messages = []
    try:
        wait_fleet_online(fleet)
        messages += send_round(fleet, "before")
        marks = {c.name: len(c.events) for c in fleet}
        stop_access_points(old)
        result["outageAtMs"] = now_ms()
        result["secondsToOffline"] = wait_offline(fleet)
        messages += send_round(fleet, "during")
        start_access_points(spares)
        activate(admin, spares)
        publication = emergency_publish(admin, approver, "every access point down: new nodes")
        result["publication"] = publication
        result["reconnect"] = wait_reconnected(fleet, set(spares), publication["observedAtMs"], marks)
        log(f"back on new nodes: {result['reconnect']['back']}/{len(fleet)}, "
            f"max {result['reconnect']['maxDelayMs']} ms")
        messages += send_round(fleet, "after")
        result["messages"] = {"sent": messages, **persisted(app, messages)}
        result["retire"] = retire(admin, approver, old, "dead access points after the total outage")
    finally:
        for client in fleet:
            client.stop()
    stored = result.get("messages", {})
    notes = result.get("publication", {}).get("notAuthor", {})
    result["checks"] = {
        "reconnectedWithinLimit": result.get("reconnect", {}).get("pass", False),
        "noAcceptedMessageLost": bool(stored) and not stored["lost"] and stored["accepted"] > 0,
        "secondPersonRefused": notes.get("status") not in (None, 200),
        "publishedAsEmergency": bool(result.get("publication", {}).get("emergency")),
    }
    result["pass"] = all(result["checks"].values())
    evidence("r1-total-outage", result)
    log(json.dumps(result["checks"]))


def command_break_glass(args):
    """Every access point down and the control plane down too: the runbook signs the new nodes
    offline and uploads them by hand. When the control plane is back the floor goes past that
    serial and the platform publishes the same topology."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    old, spares = manifest_points(app), spare_sites(app)
    ls.require(spares, f"every lab site is in the manifest ({old}); retire some first")
    lab = st.Lab()
    fleet = start_fleet(app, args.clients, "glass", seed)
    result = {"oldAccessPoints": old, "newAccessPoints": spares}
    try:
        wait_fleet_online(fleet)
        marks = {c.name: len(c.events) for c in fleet}
        stop_access_points(old)
        result["secondsToOffline"] = wait_offline(fleet)
        start_access_points(spares)
        stop_control_plane()
        offline = offline_manifest(app, "break-glass", spares, recovery_serials=recovery_serials(facts))
        put_everywhere(lab, app, offline["file"].read_bytes(),
                       headers={"cache-control": "public, max-age=60, stale-while-revalidate=300, stale-if-error=604800"})
        uploaded = now_ms()
        facts.setdefault("offlineSerials", []).append(offline["serial"])
        save_facts(facts)
        result["offline"] = {k: (str(v) if isinstance(v, Path) else v) for k, v in offline.items()}
        result["reconnect"] = wait_reconnected(fleet, set(spares), uploaded, marks)
        adopted = wait_adopted(fleet, offline["serial"], marks, 0)
        result["adoptedOffline"] = {"clients": len(fleet), "adopted": len(adopted), "samples": adopted}
        log(f"offline manifest {offline['serial']}: {len(adopted)}/{len(fleet)} adopted, "
            f"{result['reconnect']['back']} back on new nodes")
        start_control_plane()
        floor = admin.ok("POST", "/platform/access/serial-floor", {"floor": offline["serial"]},
                         reason=f"offline manifest {offline['serial']} signed during the outage")
        status, body = admin.call("POST", "/platform/access/serial-floor",
                                  {"floor": floor["after"] + SERIAL_FLOOR_MAX_JUMP + 1}, reason="implausible floor")
        result["floor"] = {**floor, "tooHigh": {"status": status, "body": body if isinstance(body, dict) else str(body)}}
        activate(admin, spares)
        marks = {c.name: len(c.events) for c in fleet}
        bump_weight(admin, spares[0])
        publication = publish(admin, approver, "the offline topology, signed by the platform again")
        platform_serial = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        adopted = wait_adopted(fleet, platform_serial, marks, 10 * 60)
        result["platform"] = {"serial": platform_serial, "approveToPublishedSec": publication["approveToPublishedSec"],
                              "adopted": len(adopted), "samples": adopted,
                              "online": sum(online_on(c.status(), set(spares)) for c in fleet)}
        result["retire"] = retire(admin, approver, old, "dead access points after the break-glass publication")
    finally:
        start_control_plane()
        for client in fleet:
            client.stop()
    platform = result.get("platform", {})
    floor = result.get("floor", {})
    result["checks"] = {
        "offlineAdoptedByAll": result.get("adoptedOffline", {}).get("adopted") == len(fleet),
        "reconnectedWithinLimit": result.get("reconnect", {}).get("pass", False),
        "floorRaisedPastOffline": floor.get("after", 0) >= result.get("offline", {}).get("serial", 1 << 62),
        "floorTooHighRefused": floor.get("tooHigh", {}).get("status") not in (None, 200)
        and "floorTooHigh" in json.dumps(floor.get("tooHigh", {}).get("body")),
        "platformAboveOffline": platform.get("serial", 0) > result.get("offline", {}).get("serial", 1 << 62),
        "platformAdoptedByAll": platform.get("adopted") == len(fleet) and platform.get("online") == len(fleet),
    }
    result["pass"] = all(result["checks"].values())
    evidence("r2-break-glass", result)
    log(json.dumps(result["checks"]))


def command_serial_continuity(_args):
    """A recovery link signed with a reserved serial gives way to the next platform manifest. The
    reverse: an offline manifest whose serial the platform was never told about makes the next
    platform manifest a rollback for whoever holds it, and the mirror audit reports the copy ahead."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    lab = st.Lab()
    steps = {}

    # Reserved: the link's serial comes from the platform's own sequence.
    reserved = admin.ok("POST", "/platform/access/serial-reservations", reason="recovery link for a stranded client")
    serial = reserved["serial"]
    facts.setdefault("recoverySerials", []).append(serial)
    save_facts(facts)
    held = payload_of(read_document(app))["serial"]
    seed, _ = stranded_seed(app, serial=held)
    alive = [ap for ap in manifest_points(app) if ls.port_open(st.SITES[ap].tls_port)][:2]
    ls.require(alive, "no access point of the manifest is listening")
    sources = [f"{site.base_url}/{app['bootstrapId']}" for site in st.MIRRORS.values()]
    client = Client(f"continuity-{now_ms()}", app, seed=seed).start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline and not ((client.status() or {}).get("access") or {}).get("recovering"):
            time.sleep(1)
        event = client.ask("import", 30, link=recovery_link(app, serial, alive, sources, "reserved"))
        steps["reserved"] = {"serial": serial, "seedSerial": held, "import": (event or {}).get("data"),
                             "online": online_wait(client, set(alive), 90)}
        mark = len(client.events)
        bump_weight(admin, alive[0])
        publication = publish(admin, approver, "after the recovery link")
        platform_serial = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        adopted = wait_adopted([client], platform_serial, {client.name: mark}, 10 * 60)
        status = client.status()
        steps["reserved"].update({"platformSerial": platform_serial, "adopted": adopted.get(client.name),
                                  "after": (status or {}).get("access")})
    finally:
        client.stop()
    access = steps["reserved"].get("after") or {}
    import_result = ((steps["reserved"].get("import") or {}).get("result") or {})
    steps["reserved"]["pass"] = (import_result.get("outcome") == "adopted" and bool(steps["reserved"]["online"])
                                 and steps["reserved"]["platformSerial"] > serial
                                 and access.get("serial") == steps["reserved"]["platformSerial"]
                                 and access.get("source") != "recovery")

    # Reverse: signed outside the platform, floor never raised.
    client = Client(f"continuity-ahead-{now_ms()}", app, seed=seed_file(app)).start()
    try:
        ls.require(online_wait(client, set(manifest_points(app)), 90), "the reverse-case client did not come online")
        current = manifest_points(app)
        spares = spare_sites(app)
        ls.require(spares, "every lab site is in the manifest; the offline manifest needs a new access point id")
        offline = offline_manifest(app, "unannounced", spares[:1], recovery_serials=recovery_serials(facts))
        facts.setdefault("offlineSerials", []).append(offline["serial"])
        save_facts(facts)
        mark = len(client.events)
        put_everywhere(lab, app, offline["file"].read_bytes())
        client.refresh()
        took = wait_adopted([client], offline["serial"], {client.name: mark}, 120)
        triggered = now_ms()
        run_job(admin, "access_mirror_audit")
        ahead, deadline = {}, time.time() + 180
        while len(ahead) < len(st.MIRRORS) and time.time() < deadline:
            for mirror in admin.ok("GET", "/platform/access/mirrors"):
                check = mirror.get("lastCheck") or {}
                if mirror["id"] in {m.id for m in st.MIRRORS.values()} and (check.get("at") or 0) >= triggered \
                        and (check.get("error") or "").startswith("ahead:"):
                    ahead[mirror["id"]] = check
            time.sleep(2)
        bump_weight(admin, current[0])
        publication = publish(admin, approver, "next platform manifest, floor not raised")
        lower = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        event = client.refresh()
        report = (event or {}).get("data", {}).get("report", {})
        attempts = [a for a in report.get("attempts", []) if a.get("document") == "access"]
        steps["unannounced"] = {"offlineSerial": offline["serial"], "adopted": took.get(client.name),
                                "mirrorAudit": ahead, "platformSerial": lower, "refreshAttempts": attempts,
                                "held": ((client.status() or {}).get("access") or {}).get("serial")}
        # Put right: the floor past the offline serial, then the platform is ahead again.
        floor = admin.ok("POST", "/platform/access/serial-floor", {"floor": offline["serial"]},
                         reason=f"offline manifest {offline['serial']} found on the mirrors")
        mark = len(client.events)
        bump_weight(admin, current[0])
        publication = publish(admin, approver, "after raising the floor")
        repaired = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        steps["unannounced"]["repair"] = {"floor": floor, "platformSerial": repaired,
                                          "adopted": wait_adopted([client], repaired, {client.name: mark}, 10 * 60)
                                          .get(client.name)}
    finally:
        client.stop()
    reverse = steps["unannounced"]
    reverse["pass"] = (bool(reverse["adopted"]) and len(reverse["mirrorAudit"]) == len(st.MIRRORS)
                       and reverse["platformSerial"] < reverse["offlineSerial"]
                       and any(a.get("code") == "rollback" or a.get("result") == "olderSerial"
                               for a in reverse["refreshAttempts"])
                       and reverse["held"] == reverse["offlineSerial"] and bool(reverse["repair"]["adopted"]))
    evidence("r3-serial-continuity", {"steps": steps, "pass": steps["reserved"]["pass"] and reverse["pass"]})
    log(json.dumps({"reserved": steps["reserved"]["pass"], "unannounced": reverse["pass"]}))


def doh_audit(admin, app, timeout=180):
    """One audit run on demand: the record of `app` as the API reports it once checked again."""
    triggered = now_ms()
    run_job(admin, "access_doh_audit")
    deadline = time.time() + timeout
    while time.time() < deadline:
        view = admin.ok("GET", "/platform/access/doh-records")
        record = next(r for r in view["records"] if r["appId"] == app["appId"])
        if record.get("status") and (record.get("checkedAt") or 0) >= triggered:
            return record
        time.sleep(2)
    raise SystemExit(f"no DoH audit of {app['appId']} within {timeout}s")


def doh_seed(app, payload):
    """A manifest older than the published one, whose every access point is dead, that still
    lists the real mirrors and the DoH pointer: what a long-offline device holds."""
    stale = json.loads(json.dumps(payload))
    stale["serial"] = payload["serial"] - 1
    for region in stale["regions"]:
        for cell in region["cells"]:
            for point in cell["accessPoints"]:
                point["host"] = f"dead-{point['id']}.{st.HOST_SUFFIX}"
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    file = st.lab_path("seeds", f"doh-{now_ms()}.json")
    file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    file.write_bytes(forge("ok-forge-good", key, stale, [cert]))
    return file


def command_doh_fallback(_args):
    """Every mirror the manifest lists is closed and every access point the client knows is dead.
    The TXT record points at a rescue mirror on a fresh host; the client finds it through the
    resolver the manifest names. The audit reports the record as different while it points away
    and matching once it is put back."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    view = admin.ok("GET", "/platform/access/doh-records")
    ls.require(view["configured"] and st.DOH_RESOLVER in view["resolvers"],
               "the platform has no DoH pointer for the lab resolver: restart platform-api, platform-worker and "
               f"platform-prober with this access_stack.py (it sets FLARE_ACCESS_DOH_*); now {view}")
    doh, rescue = FakeDoh(), RescueMirror()
    result = {}
    state = ls.load_state()
    closed = []
    client = None
    try:
        bump_weight(admin, active_points(admin)[0])
        publication = publish(admin, approver, "manifest that lists the DoH pointer")
        published = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        ls.wait_for(lambda: payload_of(read_document(app) or b'{"payload":"e30"}').get("serial") == published,
                    120, f"serial {published} on mirror b")
        genuine = read_document(app)
        payload = payload_of(genuine)
        record = next(r for r in admin.ok("GET", "/platform/access/doh-records")["records"] if r["appId"] == app["appId"])
        listed = [s for s in payload["bootstrap"]["sources"] if s.get("kind") == "dohTxt"]
        result["manifest"] = {"serial": published, "dohSources": listed, "record": record}
        ls.require(any(s.get("name") == record["name"] and st.DOH_RESOLVER in s.get("resolvers", []) for s in listed),
                   f"the manifest does not list the pointer {record['name']}: {listed}")

        doh.serve(record["name"], record["value"])
        result["auditBefore"] = doh_audit(admin, app)

        rescue.put(app, ACCESS_FILE, genuine)
        keys_doc = read_document(app, KEYS_FILE)
        if keys_doc:
            rescue.put(app, KEYS_FILE, keys_doc)
        seed = doh_seed(app, payload)
        for site in st.MIRRORS.values():
            st.mirror_stop(state, site)
            closed.append(site)
        pointer = f"v=flare1;u={RESCUE_BASE}/{app['bootstrapId']}/"
        doh.serve(record["name"], pointer)
        live = {p["id"] for region in payload["regions"] for cell in region["cells"] for p in cell["accessPoints"]}
        started = now_ms()
        client = Client(f"doh-{now_ms()}", app, seed=seed).start()
        adopted = wait_adopted([client], published, {client.name: 0}, RECONNECT_AFTER_PUBLISH_MS / 1000)
        online = online_wait(client, live, 120)
        status = client.status()
        result["client"] = {"adopted": adopted.get(client.name), "online": online, "startToOnlineMs": now_ms() - started,
                            "status": status, "pointerQueries": [q for q in doh.asked if q["name"] == record["name"]
                                                                 and q["atMs"] >= started],
                            "rescueRequests": [r for r in rescue.served if r["atMs"] >= started]}
        log(f"client via DoH: adopted={bool(adopted)} online={online}")
        result["auditDuring"] = doh_audit(admin, app)
    finally:
        if client:
            client.stop()
        for site in closed:
            if not ls.live(ls.load_state(), site.front):
                st.mirror_start(ls.load_state(), site)
    try:
        doh.serve(record["name"], record["value"])
        result["auditAfter"] = doh_audit(admin, app)
    finally:
        doh.close()
        rescue.close()
    source = ((result.get("client") or {}).get("adopted") or {}).get("source") or ""
    result["checks"] = {
        "adoptedFromRescueMirror": st.RESCUE_MIRROR_HOST in source,
        "online": bool((result.get("client") or {}).get("online")),
        "withinLimit": ((result.get("client") or {}).get("startToOnlineMs") or 1 << 60) <= RECONNECT_AFTER_PUBLISH_MS,
        "auditMatchingBefore": (result.get("auditBefore") or {}).get("status") == "matching",
        "auditDiffersDuring": (result.get("auditDuring") or {}).get("status") == "differs",
        "auditMatchingAfter": (result.get("auditAfter") or {}).get("status") == "matching",
    }
    result["pass"] = all(result["checks"].values())
    evidence("r4-doh-fallback", result)
    log(json.dumps(result["checks"]))


# ---------------------------------------- stand-in: the server's version gate --
class RefusingGateway:
    """Takes a gateway's port behind the real Edge and refuses every CONNECT the way a gateway
    with the version gate would. The gate itself does not exist yet: this is the stand-in."""

    def __init__(self, port, frame):
        self.frame = frame
        self.served = 0
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", port))
        self.server.listen(16)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                connection, _ = self.server.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(connection,), daemon=True).start()

    def _serve(self, connection):
        try:
            connection.settimeout(5)
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = connection.recv(4096)
                if not chunk:
                    return
                request += chunk
            key = next(line.split(b":", 1)[1].strip() for line in request.split(b"\r\n")
                       if line.lower().startswith(b"sec-websocket-key:"))
            accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
            connection.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                               b"Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
            connection.recv(65536)  # the client's CONNECT
            size = len(self.frame)
            header = bytes([0x82, size]) if size < 126 else bytes([0x82, 126]) + size.to_bytes(2, "big")
            connection.sendall(header + self.frame)
            self.served += 1
            time.sleep(0.5)
        except (OSError, StopIteration):
            pass
        finally:
            connection.close()

    def close(self):
        self.server.close()


def command_upgrade_required(_args):
    """A client refused at CONNECT goes to `required` and read-only with nothing else telling it.
    Uses an access point whose gateway is gone (after `migrate`): the real SNI router and Edge in
    front, the stand-in on the gateway's port."""
    facts = load_facts()
    app = facts["apps"]["main"]
    site = st.SITES["ap-a1"]
    gateway_port = ls.GATEWAYS[site.gateway][0]
    ls.require(not ls.port_open(gateway_port), "the gateway of ap-a1 still runs; run migrate first")
    state = ls.load_state()
    if not ls.live(state, site.edge):
        st.command_ctl(argparse.Namespace(action="start", process=site.edge))
    payload = payload_of(read_document(app))
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    payload["serial"] += 1
    only_dead_mirror(payload, app)
    cell = payload["regions"][0]["cells"][0]
    cell["accessPoints"] = [{**cell["accessPoints"][0], "id": site.ap_id, "host": site.host}]
    file = st.lab_path("seeds", f"upgrade-{now_ms()}.json")
    file.write_bytes(forge("ok-forge-good", key, payload, [cert]))
    min_build = 30201
    frame = subprocess.run([str(CLIENT), "upgrade-frame", str(min_build)], capture_output=True, check=True).stdout
    gateway = RefusingGateway(gateway_port, frame)
    client = Client(f"upgrade-{now_ms()}", app, seed=file, app_info=RELEASE_APP, single_runtime=True)
    client.start()
    result = {"standIn": True, "minBuild": min_build}
    try:
        terminal = client.wait(lambda e: e["kind"] == "connection" and e["data"].get("phase") == "actionRequired", 60)
        result["connection"] = (terminal or {}).get("data")
        failed = client.wait(lambda e: e["kind"] == "loginFailed", 20)
        result["connectError"] = (failed or {}).get("data")
        event = client.ask("update_state", 20)
        result["update"] = (event or {}).get("data", {}).get("status")
        result["refusals"] = gateway.served
    finally:
        client.stop()
        gateway.close()
    status = result["update"] or {}
    result["pass"] = (bool(terminal) and status.get("state") == "required" and status.get("readOnly") is True
                      and gateway.served >= 1)
    evidence("s4-upgrade-required", result)
    log(json.dumps({k: result[k] for k in ("refusals", "pass")}) + " " + json.dumps(
        {k: status.get(k) for k in ("state", "readOnly", "blockUi", "prompt")}))


# ------------------------------------------- secondary: Service Worker, real browser --
WEB_SITE_PORT = ls.port(43620)
WEB_HOST = f"web.{st.HOST_SUFFIX}"
WEB_ORIGIN = f"https://{WEB_HOST}:{WEB_SITE_PORT}"
WEB_APP = ls.ROOT / "flare-social-examples/apps/flare-social-web-app"
WEB_RELEASE_FILE = "release/web/stable.dsse.json"
CHROMIUM = Path.home() / ("Library/Caches/ms-playwright/chromium-1248/chrome-mac-arm64/"
                          "Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing")
HOUR_MS = 3_600_000
WEB_BUILD_PREFIX = ["build-"]


def web_site_source(root):
    certificate, key = st.issue_certificate("web-site", [WEB_HOST])
    directory = ls.path("nginx", "web-site")
    return f"""worker_processes 1;
pid {directory}/nginx.pid;
error_log {directory}/error.log warn;
events {{ worker_connections 512; }}
http {{
    types {{
        text/html html; text/css css; application/javascript js mjs; application/json json;
        application/wasm wasm; image/svg+xml svg; image/png png; font/woff2 woff2;
    }}
    default_type application/octet-stream;
    log_format access '$msec $status $request_method $uri $body_bytes_sent';
    access_log {directory}/access.log access;
    client_body_temp_path {directory}/body;
    proxy_temp_path {directory}/proxy;
    fastcgi_temp_path {directory}/fastcgi;
    uwsgi_temp_path {directory}/uwsgi;
    scgi_temp_path {directory}/scgi;
    server {{
        listen 127.0.0.1:{WEB_SITE_PORT} ssl;
        server_name {WEB_HOST};
        ssl_certificate {certificate};
        ssl_certificate_key {key};
        root {root};
        location / {{
            add_header Cache-Control "no-cache";
            try_files $uri /index.html;
        }}
    }}
}}
"""


def web_site_up(root):
    state = ls.load_state()
    if ls.live(state, "web-site"):
        return
    ls.path("nginx", "web-site").mkdir(parents=True, exist_ok=True, mode=0o700)
    st.nginx_process(state, "web-site", web_site_source(root))
    ls.wait_for(lambda: ls.port_open(WEB_SITE_PORT), 20, "web site")


def web_build_dir(build):
    return st.lab_path("web", f"{WEB_BUILD_PREFIX[0]}{build}")


def web_deploy(root, build):
    """A deployment overwrites the site's entry files and adds this build's own; older hashed
    assets and builds/<id>/ stay, which is what a rollback fetches."""
    shutil.copytree(web_build_dir(build), root, dirs_exist_ok=True)


def web_manifest_sha(root, build):
    return hashlib.sha256((root / "builds" / build / "asset-manifest.json").read_bytes()).hexdigest()


def web_release(app, key, cert, serial, build, sha256, number, packs=()):
    entry = {"releaseId": f"web-{number}", "version": f"1.{number}", "build": number, "publishedAt": iso(now_ms()),
             "rollout": {"basisPoints": 10000, "salt": f"web-{number}", "halted": False},
             "notes": {"zh-CN": [f"构建 {build}"], "en": [f"Build {build}"]},
             "artifacts": {"web": {"kind": "web_bundle", "buildId": build, "sha256": sha256,
                                   "urls": [f"{WEB_ORIGIN}/builds/{build}/asset-manifest.json"]}},
             "resourcePacks": list(packs)}
    payload = {"schema": "flare.release.v1", "bootstrapId": app["bootstrapId"], "platform": "web", "channel": "stable",
               "serial": serial, "issuedAt": iso(now_ms() - 60_000), "expiresAt": iso(now_ms() + 14 * DAY_MS),
               "policy": {}, "releases": [entry]}
    return forge("ok-forge-good", key, payload, [cert], RELEASE_PAYLOAD_TYPE)


class Browser:
    def __init__(self, profile):
        env = {**os.environ, "FLARE_WEB_APP_PACKAGE": str(WEB_APP / "package.json"), "FLARE_WEB_SITE": WEB_ORIGIN + "/",
               "FLARE_BROWSER_PROFILE": str(profile), "FLARE_PLAYWRIGHT_CHROMIUM": str(CHROMIUM),
               "NO_PROXY": ls.NO_PROXY + ",.localhost", "no_proxy": ls.NO_PROXY + ",.localhost"}
        self.process = subprocess.Popen(["node", str(Path(__file__).with_name("access_sw_browser.mjs"))], env=env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)

    def ask(self, op, **arguments):
        self.process.stdin.write(json.dumps({"op": op, **arguments}) + "\n")
        self.process.stdin.flush()
        answer = json.loads(self.process.stdout.readline())
        ls.require(answer.get("ok"), f"browser {op}: {answer}")
        return answer

    def tabs(self):
        return self.ask("snapshot")["tabs"]

    def quit(self):
        try:
            self.ask("quit")
            self.process.wait(timeout=20)
        except (OSError, ValueError, SystemExit, subprocess.TimeoutExpired):
            self.process.kill()


def worker_build(url):
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url or "").query)
    return (query.get("build") or [None])[0]


def web_entries():
    """Which build a tab runs: the entry script no other build has."""
    assets = {}
    for build in ("lab-a", "lab-b"):
        manifest = json.loads((web_build_dir(build) / "asset-manifest.json").read_text())
        assets[build] = {a["url"] for a in manifest["assets"]}
    return {build: names - assets[other] for build, other, names in
            (("lab-a", "lab-b", assets["lab-a"]), ("lab-b", "lab-a", assets["lab-b"]))}


def tab_build(tab, entries):
    paths = {urllib.parse.urlsplit(src).path for src in tab.get("scripts", [])}
    found = [build for build, names in entries.items() if paths & names]
    return found[0] if len(found) == 1 else None


def tab_view(tab, entries):
    return {"running": tab_build(tab, entries), "controller": worker_build(tab.get("controller")),
            "active": worker_build(tab.get("active")), "waiting": worker_build(tab.get("waiting")),
            "installing": worker_build(tab.get("installing")), "caches": tab.get("caches"),
            "actions": tab.get("actions"), "prompt": (tab.get("prompt") or "")[:160], "error": tab.get("error")}


def wait_tabs(browser, entries, predicate, timeout, what):
    deadline = time.time() + timeout
    view = []
    while time.time() < deadline:
        view = [tab_view(tab, entries) for tab in browser.tabs()]
        if all(predicate(tab) for tab in view):
            return view
        time.sleep(0.5)
    log(f"{what}: timed out: {json.dumps(view, ensure_ascii=False)[:800]}")
    return None


def wait_prompt(browser, entries, timeout, what):
    """The tab showing the offer. An optional version pops up once: the first tab to evaluate it
    records that, so another tab shows a badge only."""
    deadline = time.time() + timeout
    view = []
    while time.time() < deadline:
        view = [tab_view(tab, entries) for tab in browser.tabs()]
        for index, tab in enumerate(view):
            if "update" in (tab["actions"] or []):
                return index, view
        time.sleep(0.5)
    log(f"{what}: timed out: {json.dumps(view, ensure_ascii=False)[:800]}")
    return None, view


def web_update_to(browser, entries, target, clock_ms):
    """Moves both tabs through the real flow: a due check, the prompt, install, confirm, reload."""
    browser.ask("clock", offsetMs=clock_ms)
    for tab in range(len(browser.tabs())):
        browser.ask("reload", tab=tab)
    index, prompted = wait_prompt(browser, entries, 30, "prompt")
    if index is None:
        return {"prompted": False, "promptView": prompted}
    started = time.time()
    browser.ask("click", tab=index, action="update")
    clicks = 1
    moved = None
    other = None
    while time.time() - started < 60:
        tabs = browser.tabs()
        view = [tab_view(tab, entries) for tab in tabs]
        if all(t["running"] == target and t["controller"] == target for t in view):
            moved = view
            break
        if other is None and view[index]["running"] == target and view[1 - index]["running"] != target:
            other = round(time.time() - started, 2)
        if "restart" in (view[index]["actions"] or []) and worker_build(tabs[index].get("waiting")) == target:
            try:
                browser.ask("click", tab=index, action="restart")
                clicks += 1
            except SystemExit:
                pass
        time.sleep(0.25)
    return {"prompted": True, "promptTab": index, "promptView": prompted, "clicks": clicks,
            "seconds": round(time.time() - started, 2), "firstTabMovedAloneAt": other,
            "after": moved or [tab_view(tab, entries) for tab in browser.tabs()], "moved": moved is not None}


def web_store(browser, tab=0):
    """The pre-login store of the page, key without its namespace."""
    entries = browser.ask("idb", tab=tab)["entries"]
    return {e["key"].removeprefix("flare-access/"): e for e in entries}


def wait_store(browser, predicate, timeout, what):
    deadline = time.time() + timeout
    store = {}
    while time.time() < deadline:
        store = web_store(browser)
        if predicate(store):
            return store
        time.sleep(0.5)
    log(f"{what}: timed out; keys {sorted(store)}")
    return store


def store_facts(store, bootstrap):
    """What has to survive a reload, reduced to something comparable."""
    def entry(key):
        item = store.get(key)
        return {"size": item["size"], "sha256": item["sha256"], "text": (item["text"] or "")[:200]} if item else None

    state = store.get(f"update/state/{bootstrap}")
    return {"lkg": entry(f"manifest/access/{bootstrap}/current"),
            "serial": entry(f"serial/access/{bootstrap}"),
            "keystate": [k for k in store if k.startswith("manifest/keys/")],
            "release": [k for k in store if k.startswith(f"manifest/release/{bootstrap}/")],
            "updateState": json.loads(state["text"]) if state and state["text"] else None,
            "packs": {k.rsplit("/", 1)[1]: store[k]["sha256"] for k in store if k.startswith(f"resource/{bootstrap}/")},
            "sources": (store.get(f"bootstrap/sources/{bootstrap}") or {}).get("text"), "keys": sorted(store)}


def command_web_sw(args):
    """The update path a browser takes: signed manifests from the mirrors through fetch and CORS,
    the worker installed from a new query string, every open tab reloaded, then the same way back;
    and what a reload has to keep. Runs on the `forge` application, whose documents nothing else
    reads, so it can also serve an older manifest on purpose."""
    WEB_BUILD_PREFIX[0] = "core-build-" if args.app == "core" else "build-"
    facts = load_facts()
    app = facts["apps"]["forge"]
    bootstrap = app["bootstrapId"]
    lab = st.Lab()
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    web = st.lab_path("web")
    root = web / "site"
    profile = web / f"profile-{now_ms()}"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, mode=0o700)
    web_deploy(root, "lab-a")
    web_site_up(root)
    sha_a = web_manifest_sha(root, "lab-a")
    entries = web_entries()
    steps = {"entries": {k: sorted(v) for k, v in entries.items()}}
    genuine = read_document(app)
    ls.require(genuine, "the forge application has no published manifest")
    payload = payload_of(genuine)
    serial = payload["serial"]
    newer = forge("ok-forge-good", key, {**payload, "serial": serial + 1}, [cert])
    pack_body = json.dumps({"schema": "flare.resource.v1", "kind": "strings",
                            "locales": {"zh-CN": {"composer.readOnlyUpdate": "实验台词条"}}}, ensure_ascii=False).encode()
    put_everywhere(lab, app, pack_body, "res/web-strings-1.json")
    pack = {"id": "web-strings", "kind": "strings", "packVersion": 1, "minBuild": 0,
            "url": [object_url(s, app, "res/web-strings-1.json") for s in st.MIRRORS.values()],
            "sha256": hashlib.sha256(pack_body).hexdigest(), "sizeBytes": len(pack_body)}
    put_everywhere(lab, app, web_release(app, key, cert, 1, "lab-a", sha_a, 1), WEB_RELEASE_FILE)
    browser = Browser(profile)
    try:
        browser.ask("open", count=2)
        first = wait_tabs(browser, entries, lambda t: t["active"] == "lab-a" and t["controller"] == "lab-a", 60,
                          "first install")
        steps["firstInstall"] = first or [tab_view(t, entries) for t in browser.tabs()]
        steps["storeFirstLoad"] = store_facts(wait_store(
            browser, lambda s: f"update/state/{bootstrap}" in s and f"serial/access/{bootstrap}" in s, 30,
            "first store"), bootstrap)
        web_deploy(root, "lab-b")
        sha_b = web_manifest_sha(root, "lab-b")
        browser.ask("reload", tab=0)
        steps["reloadAfterDeploy"] = tab_view(browser.tabs()[0], entries)
        put_everywhere(lab, app, newer)
        put_everywhere(lab, app, web_release(app, key, cert, 2, "lab-b", sha_b, 2, [pack]), WEB_RELEASE_FILE)
        steps["update"] = web_update_to(browser, entries, "lab-b", 7 * HOUR_MS)
        adopted = wait_store(browser, lambda s: f"resource/{bootstrap}/web-strings" in s and
                             s.get(f"manifest/access/{bootstrap}/current", {}).get("sha256") ==
                             hashlib.sha256(newer).hexdigest(), 30, "newer manifest and pack")
        steps["storeAfterAdoption"] = store_facts(adopted, bootstrap)
        # A plain reload keeps all of it.
        browser.ask("reload", tab=0)
        browser.ask("reload", tab=1)
        time.sleep(1)
        steps["storeAfterReload"] = store_facts(web_store(browser), bootstrap)
        # The mirrors fall back to the older, validly signed manifest. The serial on the page's own
        # disk is what refuses it; the seed in the bundle alone would take it.
        put_everywhere(lab, app, genuine)
        browser.ask("clock", offsetMs=14 * HOUR_MS)
        browser.ask("reload", tab=0)
        attempted = wait_store(browser, lambda s: re.search(r'"demotedUntilMs":\d', (s.get(f"bootstrap/sources/{bootstrap}") or {}).get("text") or ""),
                               15, "rollback attempt")
        steps["storeAfterRollbackAttempt"] = store_facts(attempted, bootstrap)
        put_everywhere(lab, app, newer)
        # The site is gone: the shell still comes from the worker.
        state = ls.load_state()
        ls.signal_process(state, "web-site", 15, wait=10)
        browser.ask("reload", tab=0)
        steps["reloadWithSiteDown"] = tab_view(browser.tabs()[0], entries)
        st.command_ctl(argparse.Namespace(action="start", process="web-site"))
        ls.wait_for(lambda: ls.port_open(WEB_SITE_PORT), 20, "web site")
        # Rollback is the same step with the older build in the manifest.
        put_everywhere(lab, app, web_release(app, key, cert, 3, "lab-a", sha_a, 3, [pack]), WEB_RELEASE_FILE)
        steps["rollback"] = web_update_to(browser, entries, "lab-a", 21 * HOUR_MS)
        # A wrong hash must not install.
        put_everywhere(lab, app, web_release(app, key, cert, 4, "lab-b", "0" * 64, 4), WEB_RELEASE_FILE)
        browser.ask("clock", offsetMs=28 * HOUR_MS)
        browser.ask("reload", tab=0)
        browser.ask("reload", tab=1)
        index, _ = wait_prompt(browser, entries, 30, "bad prompt")
        if index is not None:
            browser.ask("click", tab=index, action="update")
            time.sleep(6)
        steps["wrongHash"] = {"prompted": index is not None,
                              "after": [tab_view(t, entries) for t in browser.tabs()]}
        # Withdrawn from the manifest, the pack leaves the page's store.
        wait_store(browser, lambda s: f"resource/{bootstrap}/web-strings" not in s, 15, "pack withdrawal")
        steps["storeAfterWithdrawal"] = store_facts(web_store(browser), bootstrap)
        steps["logs"] = browser.ask("logs")
        browser.ask("screenshot", tab=0, path=str(web / "last.png"))
    finally:
        browser.quit()
        delete_everywhere(lab, app, WEB_RELEASE_FILE)
        delete_everywhere(lab, app, "res/web-strings-1.json")
        put_everywhere(lab, app, genuine)
    update, rollback, wrong = steps["update"], steps["rollback"], steps["wrongHash"]
    adoption, reload_facts = steps["storeAfterAdoption"], steps["storeAfterReload"]
    attempt = steps["storeAfterRollbackAttempt"]
    checks = {
        "firstInstallRegistersRunningBuild": bool(first),
        "reloadStaysOnOwnBuildAfterDeploy": steps["reloadAfterDeploy"]["running"] == "lab-a",
        "newQueryInstallsAndBothTabsMove": update.get("moved") is True,
        "oldShellCacheGone": all("flare-shell-lab-a" not in (t["caches"] or []) for t in update.get("after", [])),
        "lkgSurvivesReload": bool(adoption["lkg"]) and adoption["lkg"] == reload_facts["lkg"],
        "serialSurvivesReload": bool(adoption["serial"]) and adoption["serial"] == reload_facts["serial"],
        "updateStateSurvivesReload": bool((reload_facts["updateState"] or {}).get("lastCheckedAtMs")),
        "packSurvivesReload": bool(adoption["packs"]) and adoption["packs"] == reload_facts["packs"],
        "olderManifestRefusedAfterReload": bool(attempt["lkg"]) and attempt["lkg"] == adoption["lkg"]
        and attempt["serial"] == adoption["serial"],
        "shellServedWithSiteDown": steps["reloadWithSiteDown"]["running"] == "lab-b",
        "rollbackMovesBothTabs": rollback.get("moved") is True,
        "wrongHashStaysPut": all(t["active"] == "lab-a" and t["running"] == "lab-a" for t in wrong["after"]),
        "withdrawnPackLeavesStore": steps["storeAfterWithdrawal"]["packs"] == {},
        "mirrorsReachedByFetch": any(r["status"] == 200 for r in steps["logs"]["mirrorResponses"]),
    }
    evidence(f"s3-web-sw-{args.app}", {"steps": steps, "checks": checks, "pass": all(checks.values())})
    log(json.dumps(checks))


# ------------------------------------------------------ secondary: plain facts --
def command_cell_mirror(_args):
    """The Cell's own mirror: the worker writes RustFS directly, a client reads it through the
    Edge's well-known route of the production template. It does not count as one of the two clouds."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    lab = st.Lab()
    access, secret_key = lab.rustfs["cell"]
    site = st.SITES["ap-b1"]
    base = f"https://{site.host}:{st.SNI_PORT}/.well-known/flare/v1"
    known = {m["id"] for m in admin.ok("GET", "/platform/access/mirrors")}
    if "m-cell-1" not in known:
        admin.ok("POST", "/platform/access/mirrors", {
            "id": "m-cell-1", "kind": "cell_local", "baseUrl": base, "provider": "cell", "tld": "local",
            "enabled": True, "credentials": {
                "endpoint": f"http://127.0.0.1:{st.CELL_S3_PORT}", "region": "us-east-1", "bucket": st.CELL_BUCKET,
                "prefix": "flare/v1", "accessKeyId": access, "secretAccessKey": secret_key}})
    active = active_points(admin)
    bump_weight(admin, active[0])
    publication = publish(admin, approver, "with the Cell mirror")
    rows = serials(admin, publication["changeSet"])
    url = f"{base}/{app['bootstrapId']}/{ACCESS_FILE}"
    deadline = time.time() + 120
    status, body = 0, b""
    while time.time() < deadline:
        status, body, _ = https_get(url)
        if status == 200 and payload_of(body)["serial"] == max(r["serial"] for r in rows if r["appId"] == app["appId"]):
            break
        time.sleep(1)
    result = {"approveToPublishedSec": publication["approveToPublishedSec"], "wellKnown": status, "bytes": len(body),
              "serial": payload_of(body)["serial"] if status == 200 else None,
              "uploads": platform_psql("SELECT mirror_id, status, count(*) FROM platform_access.mirror_uploads "
                                       "WHERE mirror_id='m-cell-1' GROUP BY mirror_id, status"),
              "anonymousWrite": ls.http("PUT", f"http://127.0.0.1:{st.CELL_S3_PORT}/{st.CELL_BUCKET}/x.txt")[0],
              "traversal": https_get(f"https://{site.host}:{st.SNI_PORT}/.well-known/flare/v1/../../flare-media/x")[0],
              "pass": status == 200}
    evidence("extras-cell-mirror", result)
    log(json.dumps(result))


def command_extras(_args):
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, _, auditor = people()
    keys = st.Lab().core
    lab = st.Lab()
    report = {}
    mirrors = {}
    for key, site in st.MIRRORS.items():
        url = object_url(site, app, ACCESS_FILE)
        status, body, headers = https_get(url, {"Origin": "https://app.example.com"})
        access, secret_key = lab.rustfs[key]
        anonymous_put = ls.http("PUT", f"{site.s3_endpoint}/{site.bucket}/flare/v1/{app['bootstrapId']}/anon.txt")[0]
        anonymous_list = ls.http("GET", f"{site.s3_endpoint}/{site.bucket}?list-type=2")[0]
        mirrors[key] = {"get": status, "bytes": len(body), "cors": (headers or {}).get("Access-Control-Allow-Origin"),
                        "cacheControl": (headers or {}).get("Cache-Control"), "etag": (headers or {}).get("ETag"),
                        "anonymousPut": anonymous_put, "anonymousList": anonymous_list}
    report["mirrors"] = mirrors
    report["mirrorUploads"] = platform_psql(
        "SELECT status || ':' || count(*) FROM platform_access.mirror_uploads GROUP BY status")
    report["mirrorUploadErrors"] = platform_psql(
        "SELECT coalesce(last_error,'') || ':' || count(*) FROM platform_access.mirror_uploads GROUP BY last_error")
    report["probeResults"] = platform_psql(
        "SELECT kind || ':' || coalesce(error,'ok') || ':' || count(*) FROM platform_access.ap_probe_results "
        "GROUP BY kind, error ORDER BY 1")
    report["healthRollups"] = platform_psql("SELECT count(*) FROM platform_access.ap_health_rollups")
    report["certificates"] = platform_psql("SELECT host || ':' || coalesce(not_after::text,'null') "
                                           "FROM platform_access.ap_certificates")
    report["coreClientPolicy"] = ls.psql(keys, "SELECT tenant_id || ':' || access_serial || ':' || version "
                                               "FROM tenant_client_policy", ls.DATABASES["core"], check=False)
    report["socialClientPolicy"] = ls.psql(keys, "SELECT tenant_id || ':' || access_serial || ':' || version "
                                                 "FROM flare_tenant_client_policy", ls.DATABASES["social"], check=False)
    report["noticeOutboxPending"] = ls.psql(keys, "SELECT count(*) FROM tenant_notice_outbox", ls.DATABASES["core"],
                                            check=False)
    gate = {}
    gate["noToken"] = request("POST", f"{ADMIN}/platform/access/change-sets", {})[0]
    status, body = auditor.call("POST", "/platform/access/change-sets", {}, "auditor must not write")
    gate["auditorWrite"] = {"status": status, "reason": body.get("reason") if isinstance(body, dict) else str(body)[:200]}
    gate["auditorRead"] = auditor.call("GET", "/platform/access/mirrors")[0]
    gate["adminRead"] = admin.call("GET", "/platform/access/mirrors")[0]
    report["adminPartition"] = gate
    edges = {}
    for site in st.SITES.values():
        if not ls.port_open(site.tls_port):
            edges[site.ap_id] = "closed"
            continue
        base = f"https://{site.host}:{st.SNI_PORT}"
        status, body, headers = https_get(f"{base}/.well-known/flare/ping")
        well_known = https_get(f"{base}/.well-known/flare/v1/{app['bootstrapId']}/{ACCESS_FILE}")
        forbidden = https_get(f"{base}/.well-known/flare/v1/../../flare-media/x")
        edges[site.ap_id] = {"ping": status, "pingBody": body[:200].decode(errors="replace"),
                             "wellKnown": well_known[0], "wellKnownBytes": len(well_known[1]),
                             "traversal": forbidden[0]}
    report["edges"] = edges
    evidence("extras", report)
    print(json.dumps(report, indent=1, ensure_ascii=False)[:6000])


def platform_psql(sql):
    keys = st.Lab().core
    return ls.psql(keys, sql, st.PG_DATABASE_PLATFORM)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("seed").set_defaults(func=command_seed)
    p95 = sub.add_parser("publish-p95")
    p95.add_argument("--count", type=int, default=24)
    p95.set_defaults(func=command_publish_p95)
    adoption = sub.add_parser("adoption")
    adoption.add_argument("--clients", type=int, default=24)
    adoption.add_argument("--rounds", type=int, default=3)
    adoption.set_defaults(func=command_adoption)
    sub.add_parser("negative").set_defaults(func=command_negative)
    outage = sub.add_parser("outage")
    outage.add_argument("--clients", type=int, default=20)
    outage.add_argument("--minutes", type=int, default=60)
    outage.set_defaults(func=command_outage)
    migrate = sub.add_parser("migrate")
    migrate.add_argument("--clients", type=int, default=20)
    migrate.set_defaults(func=command_migrate)
    sub.add_parser("migrate-frozen").set_defaults(func=command_migrate_frozen)
    mirror = sub.add_parser("mirror-down")
    mirror.add_argument("--clients", type=int, default=20)
    mirror.set_defaults(func=command_mirror_down)
    hole = sub.add_parser("mirror-hole")
    hole.add_argument("--clients", type=int, default=10)
    hole.set_defaults(func=command_mirror_hole)
    sub.add_parser("lint").set_defaults(func=command_lint)
    sub.add_parser("release").set_defaults(func=command_release)
    sub.add_parser("recovery").set_defaults(func=command_recovery)
    total = sub.add_parser("total-outage", help="every access point down; new nodes through the emergency publish")
    total.add_argument("--clients", type=int, default=20)
    total.set_defaults(func=command_total_outage)
    glass = sub.add_parser("break-glass", help="access points and control plane down; the offline runbook")
    glass.add_argument("--clients", type=int, default=20)
    glass.set_defaults(func=command_break_glass)
    sub.add_parser("serial-continuity", help="reserved recovery serial; an unannounced serial is a rollback"
                   ).set_defaults(func=command_serial_continuity)
    sub.add_parser("doh-fallback", help="every listed mirror closed; the DoH pointer leads to a rescue mirror"
                   ).set_defaults(func=command_doh_fallback)
    sub.add_parser("extras").set_defaults(func=command_extras)
    sub.add_parser("cell-mirror").set_defaults(func=command_cell_mirror)
    web = sub.add_parser("web-sw")
    web.add_argument("--app", choices=["social", "core"], default="social")
    web.set_defaults(func=command_web_sw)
    sub.add_parser("upgrade-required").set_defaults(func=command_upgrade_required)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
