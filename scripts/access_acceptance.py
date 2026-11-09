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
  python3 scripts/access_acceptance.py release         # a release through the platform: upload, approve, publish, withdraw
  python3 scripts/access_acceptance.py recovery        # a signed recovery link brings a stranded client back
  python3 scripts/access_acceptance.py total-outage    # every access point down; new nodes, emergency publish
  python3 scripts/access_acceptance.py break-glass     # access points and control plane down; offline-signed nodes
  python3 scripts/access_acceptance.py serial-continuity # reserved recovery serial; an unannounced serial is not overwritten
  python3 scripts/access_acceptance.py doh-fallback    # every listed mirror closed; the DoH pointer finds a rescue mirror
  python3 scripts/access_acceptance.py extras          # storage, projection, prober, admin, Edge facts
  python3 scripts/access_acceptance.py web-sw          # the web bundle update in a real browser, releases from the platform
  python3 scripts/access_acceptance.py upgrade-required # the real Core gateway refuses a build below the policy floor
  python3 scripts/access_acceptance.py release-rollout # 10,000 installs against a 5% rollout, advance, halt
  python3 scripts/access_acceptance.py forced-update   # floor raised: CONNECT refused, REST 426, newer build, reconnect
  python3 scripts/access_acceptance.py floor-eviction  # floor raised under an online old build: evicted, terminal
  python3 scripts/access_acceptance.py apk-negative    # wrong signing cert, wrong hash, unsigned file, downgrade

total-outage, break-glass, serial-continuity and doh-fallback need a platform database initialized by
the flare-platform build under test. So do the release cases (release, release-rollout, forced-update,
apk-negative, upgrade-required, web-sw): they use the release schema, the two-person release and policy
flow, the artifact credentials on the mirrors (set by the cases, through the platform) and the mirrors'
HTTPS upload fronts, which `access_stack.py up` renders. A lab brought up before those existed needs
`access_stack.py up` again (it restarts a mirror front whose configuration changed) and, because the
platform schema changed, `access_stack.py down --destroy` then `up` and `seed`.
A lab initialized before the initial schema or seeds changed is
refused by flare-platform (`access_stack.py up` says so): re-initialize it with
`access_stack.py down --destroy` and `up`, or drop the platform database (see access_stack.SCHEMA_REMEDY).
"""
import argparse
import base64
import gzip
import hashlib
import io
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
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

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
    """A real topology edit, so the publication has something to change.

    Only an active access point is in the published manifests: editing a disabled one (the lab
    after `migrate` or an incident case retired the a-points) changes nothing, and the change set
    would be "published" with no publication at all. So a named point that is not active gives way
    to the first active one.

    >>> class Admin:
    ...     def __init__(self): self.sent = []
    ...     def ok(self, method, path, body=None):
    ...         if method == "GET":
    ...             return {"items": [{"id": "ap-a1", "status": "disabled", "weight": 100, "revision": 3},
    ...                               {"id": "ap-b1", "status": "active", "weight": 100, "revision": 7}]}
    ...         self.sent.append((path, body))
    >>> admin = Admin()
    >>> bump_weight(admin, "ap-a1")
    'ap-b1'
    >>> admin.sent
    [('/platform/access/access-points/ap-b1', {'expectedRevision': 7, 'weight': 101})]
    """
    view = admin.ok("GET", f"/platform/access/access-points?pageSize=100")["items"]
    point = next(p for p in view if p["id"] == ap_id)
    if point["status"] != "active":
        point = next(p for p in view if p["status"] == "active")
    weight = 100 if point["weight"] != 100 else 101
    admin.ok("PATCH", f"/platform/access/access-points/{point['id']}",
             {"expectedRevision": point["revision"], "weight": weight})
    return point["id"]


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
               "limitSec": 120, "everySamplePublished": all(s["publications"] for s in samples),
               "pass": percentile(values, .95) <= 120 and all(s["publications"] for s in samples)}
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
        # New rows on a fresh lab; rows an earlier incident case retired come back to active.
        activate(admin, ["ap-b1", "ap-b2"])
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


# ------------------------------------- the release platform, driven for real --
# Release, policy and package flows go through the admin API of flare-platform (client application,
# draft, upload, verification job, submit, approve, publish, advance, halt, policy change) with two
# staff members; manifests are signed by the platform's online key and reach the mirrors through its
# publisher. Nothing here signs or writes a release manifest by hand. A lab initialized before the
# release schema existed has to be re-initialized: `access_stack.py down --destroy`, then `up`
# (flare-platform refuses the old platform database, access_stack.SCHEMA_REMEDY), then `seed`.
RELEASE_APP = {"version": "1.0.0", "build": 100, "distribution": "tauri", "channel": "stable",
               "target": "darwin-aarch64", "locale": "zh-CN"}
TAURI_TARGET = "darwin-aarch64"
SOCIAL_API = f"http://127.0.0.1:{ls.SOCIAL_API}/api/v1"
COUNT_BUILD_FLOOR = 1000


def object_url(site, app, name):
    return f"{site.base_url}/{app['bootstrapId']}/{name}"


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


def release_file(platform, channel):
    return f"release/{platform}/{channel}.dsse.json"


def fault_of(body):
    """The fault key of an error answer, whatever the envelope calls the field."""
    return json.dumps(body, ensure_ascii=False) if not isinstance(body, str) else body


def put_presigned(request, body):
    """The uploader's step: the presigned PUT exactly as the platform handed it out."""
    import ssl
    context = ssl.create_default_context(cafile=str(st.ca_file()))
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


def lab_signer(name):
    """A throwaway APK signing identity in the lab directory: an RSA key and a self-signed
    certificate. (DER certificate, SHA-256 fingerprint as the client application registers it.)"""
    directory = st.lab_path("apk")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    key, pem, der = directory / f"{name}.key.pem", directory / f"{name}.cert.pem", directory / f"{name}.cert.der"
    if not der.exists():
        openssl = ls.openssl()
        ls.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3650", "-sha256",
                "-subj", f"/CN=Flare access lab APK signer {name}", "-keyout", str(key), "-out", str(pem)])
        os.chmod(key, 0o600)
        ls.run([openssl, "x509", "-in", str(pem), "-outform", "DER", "-out", str(der)])
    certificate = der.read_bytes()
    return certificate, hashlib.sha256(certificate).hexdigest()


def _prefixed(value):
    return struct.pack("<I", len(value)) + value


def lab_apk(certificate=None, size=1 << 20):
    """A ZIP with a real APK Signing Block (scheme v2) in front of the central directory, carrying
    `certificate`. The platform reads the signer certificate out of that block and does not check
    the signature value (the device does at install time), so the signature fields are empty.
    Without a certificate the file is a plain ZIP: no signing block."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("AndroidManifest.xml", b"flare access lab")
        archive.writestr("classes.dex", secrets.token_bytes(size))
    data = buffer.getvalue()
    if certificate is None:
        return data
    eocd = data.rindex(b"PK\x05\x06")
    directory_offset = struct.unpack("<I", data[eocd + 16:eocd + 20])[0]
    signed_data = _prefixed(b"") + _prefixed(_prefixed(certificate)) + _prefixed(b"")
    signer = _prefixed(signed_data) + _prefixed(b"") + _prefixed(b"")
    value = _prefixed(_prefixed(signer))
    pair = struct.pack("<Q", 4 + len(value)) + struct.pack("<I", 0x7109871A) + value
    size_field = struct.pack("<Q", len(pair) + 8 + 16)
    block = size_field + pair + size_field + b"APK Sig Block 42"
    return (data[:directory_offset] + block + data[directory_offset:eocd + 16]
            + struct.pack("<I", directory_offset + len(block)) + data[eocd + 20:])


MINISIGN_UNTRUSTED_KEY = "untrusted comment: minisign public key: "
MINISIGN_UNTRUSTED_SIGNATURE = "untrusted comment: signature from tauri secret key\n"


def minisign_public_key(key_id, public_key):
    r"""The registered value of a minisign Ed25519 key, as `tauri signer generate` prints it and
    `tauri.conf.json` carries it: the base64 of the minisign file text, whose second line is the
    base64 of `Ed` + the 8-byte key id + the 32-byte public key.

    >>> value = minisign_public_key(bytes(range(8)), bytes(32))
    >>> minisign_key_parts(value) == (bytes(range(8)), bytes(32))
    True
    >>> minisign_key_parts(base64.b64decode(value).decode().splitlines()[1]) == (bytes(range(8)), bytes(32))
    True
    >>> minisign_key_parts(base64.b64encode(b"\0" * 42).decode()) is None
    True
    """
    assert len(key_id) == 8 and len(public_key) == 32
    line = base64.b64encode(b"Ed" + key_id + public_key).decode()
    # minisign prints the id as the little-endian u64 it is.
    text = f"{MINISIGN_UNTRUSTED_KEY}{int.from_bytes(key_id, 'little'):016X}\n{line}\n"
    return base64.b64encode(text.encode()).decode()


def minisign_key_parts(value):
    """(key id, public key) of a registered minisign key, read the way the platform's
    `valid_minisign_key` / `minisign::PublicKey::parse` read it: 40..=512 base64 characters, either
    the base64 of the file text (second line) or that line alone, decoding to 42 bytes starting
    `Ed`. `None` when the platform would refuse it. (The platform also checks that the 32 bytes are
    a valid Ed25519 point; a key from `openssl genpkey` always is.)"""
    if not 40 <= len(value) <= 512 or not re.fullmatch(r"[A-Za-z0-9+/=]+", value):
        return None
    try:
        text = base64.b64decode(value, validate=True).decode()
        line = text.splitlines()[1].strip() if text.startswith("untrusted comment:") else value.strip()
        raw = base64.b64decode(line, validate=True)
    except (ValueError, IndexError, UnicodeDecodeError):
        try:
            raw = base64.b64decode(value.strip(), validate=True)
        except ValueError:
            return None
    if len(raw) != 42 or raw[:2] != b"Ed":
        return None
    return raw[2:10], raw[10:]


def minisign_signature(sign, key_id, data, file_name, timestamp):
    r"""A Tauri `.sig` value for `data`: what `tauri signer sign` writes, base64 of the minisign
    signature file. Tauri signs prehashed (algorithm `ED`): the file signature is Ed25519 over the
    BLAKE2b-512 of the bytes; the global signature, which the platform checks against the
    registered key, is Ed25519 over the file signature followed by the trusted comment. `sign` is
    Ed25519 over a message.

    >>> parts = minisign_signature_parts(minisign_signature(lambda m: bytes(64), b"k" * 8, b"x", "a.tar.gz", 7))
    >>> parts["algorithm"], parts["key_id"], parts["trusted"]
    (b'ED', b'kkkkkkkk', 'timestamp:7\tfile:a.tar.gz')
    """
    file_signature = sign(hashlib.blake2b(data, digest_size=64).digest())
    trusted = f"timestamp:{timestamp}\tfile:{file_name}"
    global_signature = sign(file_signature + trusted.encode())
    text = (MINISIGN_UNTRUSTED_SIGNATURE + base64.b64encode(b"ED" + key_id + file_signature).decode() + "\n"
            + "trusted comment: " + trusted + "\n" + base64.b64encode(global_signature).decode() + "\n")
    return base64.b64encode(text.encode()).decode()


def minisign_signature_parts(value):
    """The fields of a `.sig` value the platform's `minisign::PublicKey::signed` reads."""
    lines = base64.b64decode(value).decode().splitlines()
    raw = base64.b64decode(lines[1])
    assert len(raw) == 74 and lines[2].startswith("trusted comment: ")
    return {"algorithm": raw[:2], "key_id": raw[2:10], "file_signature": raw[10:],
            "trusted": lines[2][len("trusted comment: "):], "global": base64.b64decode(lines[3])}


class LabMinisign:
    """The lab's Tauri updater key: an Ed25519 key from OpenSSL kept in the lab's private secrets
    (the same key on every run, so the registered client application stays valid), with a key id
    derived from the public key."""

    def __init__(self):
        self.key = ls.Key("access-lab-minisign")
        der = self.key._out("-pubout", "-outform", "DER")
        self.public_key = der[-32:]
        self.key_id = hashlib.blake2b(self.public_key, digest_size=8).digest()

    def sign(self, message):
        # Ed25519 in OpenSSL signs a whole file, not a stream.
        scratch = ls.path("secrets", f"minisign-message-{uuid.uuid4().hex}")
        ls.write_private(scratch, message)
        try:
            signed = subprocess.run([ls.openssl(), "pkeyutl", "-sign", "-rawin", "-inkey", str(self.key.file),
                                     "-in", str(scratch)], check=True, capture_output=True).stdout
        finally:
            scratch.unlink(missing_ok=True)
        ls.require(len(signed) == 64, "openssl Ed25519 signature")
        return signed

    def public_value(self):
        value = minisign_public_key(self.key_id, self.public_key)
        ls.require(minisign_key_parts(value) == (self.key_id, self.public_key), "lab minisign key format")
        return value

    def signature(self, data, file_name):
        return minisign_signature(self.sign, self.key_id, data, file_name, int(time.time()))


_LAB_MINISIGN = []


def lab_minisign():
    if not _LAB_MINISIGN:
        _LAB_MINISIGN.append(LabMinisign())
    return _LAB_MINISIGN[0]


def lab_minisign_key():
    return lab_minisign().public_value()


def lab_updater_signature(data, file_name):
    return lab_minisign().signature(data, file_name)


def artifact_credentials(admin, lab, facts):
    """Each mirror's package credential, apart from the credential that writes manifests. The
    endpoint is the mirror's HTTPS upload front (the platform moves packages over HTTPS only).
    RustFS has one root key per instance, so the lab cannot scope it to {bootstrapId}/artifacts/ as
    production does."""
    wanted = {site.id: site.upload_endpoint for site in st.MIRRORS.values()}
    if facts.get("artifactCredentials") == wanted:
        return
    mirrors = {m["id"]: m for m in admin.ok("GET", "/platform/access/mirrors")}
    for key, site in st.MIRRORS.items():
        access, secret_key = lab.rustfs[key]
        admin.ok("PUT", f"/platform/access/mirrors/{site.id}/artifact-credential", {
            "expectedRevision": mirrors[site.id]["revision"],
            "credentials": {"endpoint": site.upload_endpoint, "region": "us-east-1", "bucket": site.bucket,
                            "prefix": "flare/v1", "accessKeyId": access, "secretAccessKey": secret_key}},
            "lab package credential")
    facts["artifactCredentials"] = wanted
    save_facts(facts)


def urls_of(artifact):
    if not artifact:
        return []
    if artifact.get("targets"):
        return next(iter(artifact["targets"].values())).get("urls") or []
    return artifact.get("urls") or []


class Releases:
    """The release API of one application, driven by two staff members: the author writes and
    submits, a second person approves (the platform refuses anything else in production mode)."""

    def __init__(self, app, author, approver):
        self.app, self.author, self.approver = app, author, approver
        self.base = f"/apps/{app['appId']}"

    def get(self, path):
        return self.author.ok("GET", self.base + path)

    # -- registration
    def client_app(self, platform, distributions, *, channels=("stable",), certs=(), minisign=None):
        found = next((c for c in self.get("/client-apps") if c["platform"] == platform), None)
        if found is None:
            body = {"platform": platform, "displayName": f"Access lab {platform}", "channels": list(channels),
                    "distributions": distributions}
            if certs:
                body["signingCertSha256"] = list(certs)
            if minisign:
                body["minisignPublicKey"] = minisign
            return self.author.ok("POST", self.base + "/client-apps", body)
        patch = {"revision": found["revision"]}
        if not set(channels) <= set(found["channels"]):
            patch["channels"] = sorted(set(channels) | set(found["channels"]))
        if not set(certs) <= set(found["signingCertSha256"]):
            patch["signingCertSha256"] = sorted(set(certs) | set(found["signingCertSha256"]))
        if not all(found["distributions"].get(k) == v for k, v in distributions.items()):
            # A desktop shell keeps only the distribution named after it (a lab registered before
            # that rule had `direct`).
            kept = {k: v for k, v in found["distributions"].items() if v["kind"] not in ("tauri", "electron")}
            patch["distributions"] = {**kept, **distributions}
        if len(patch) == 1:
            return found
        return self.author.ok("PATCH", f"{self.base}/client-apps/{found['id']}", patch)

    def tauri_app(self):
        # A desktop shell's one distribution is named after the shell.
        return self.client_app("tauri", {"tauri": {"kind": "tauri"}}, channels=("stable", "canary"),
                               minisign=lab_minisign_key())

    # -- releases
    def next_build(self, platform, channel):
        return max([COUNT_BUILD_FLOOR, *(r["build"] for r in self.releases(platform, channel))]) + 10

    def draft(self, platform, channel, build, *, packs=(), stages=None):
        body = {"platform": platform, "channel": channel, "version": f"2.{build}.0", "build": build,
                "notes": {"zh-CN": [f"第 {build} 版实验台说明"], "en": [f"Lab notes for {build}"]},
                "resourcePacks": list(packs)}
        if stages:
            body["rolloutStages"] = list(stages)
        return self.author.ok("POST", self.base + "/releases", body)

    def view(self, release_id):
        return self.get(f"/releases/{release_id}")

    def transition(self, release_id, verb, who=None, reason=None):
        return (who or self.author).ok("POST", f"{self.base}/releases/{release_id}:{verb}", None,
                                       reason or f"lab {verb}")

    def go_live(self, release_id):
        self.transition(release_id, "submit")
        self.transition(release_id, "approve", self.approver, "second person approves")
        return self.transition(release_id, "publish")

    def advance(self, release_id):
        return self.transition(release_id, "advance")

    def halt(self, release_id):
        return self.transition(release_id, "halt")

    def releases(self, platform, channel, page_size=100):
        """Every release of the channel, all pages."""
        items, page = [], 1
        while True:
            answer = self.get(f"/releases?platform={platform}&channel={channel}&page={page}&pageSize={page_size}")
            items += answer["items"]
            if len(answer["items"]) < page_size or len(items) >= answer.get("total", 0):
                return items
            page += 1

    def quiet(self, platform, channel):
        """No floor and nothing on offer: the state a case starts from, whatever earlier runs left.
        Published releases are halted and submitted or approved ones sent back to draft, on every
        page. The platform has no way to delete a draft; a leftover draft is inert (never in a
        manifest, and `next_build` steps past its build)."""
        self.set_policy(platform, channel)
        for item in self.releases(platform, channel):
            self.withdraw(item["id"], item["status"])

    def withdraw(self, release_id, status=None):
        """Takes one release out of play: halt it when published, send it back to draft when it
        waits for review. Cleanup must not fail the case, so a refusal is only logged."""
        try:
            status = status or self.view(release_id)["status"]
            if status == "published":
                self.halt(release_id)
            elif status in ("submitted", "approved"):
                self.transition(release_id, "reject", self.approver, "lab cleanup")
        except SystemExit as error:
            log(f"cleanup of {release_id} refused: {error}")

    # -- packages
    def upload(self, release, distribution, body, name, *, target=None, signature=None, declared_sha256=None,
               lab=None, outcome=None):
        """initiate -> presigned PUT -> complete; returns the upload after the worker's verdict.

        A declared SHA-256 other than the body's is signed into the presigned PUT, so the store
        refuses it. With `lab`, the body is then written behind the platform's back, as a store
        that ignores the checksum would keep it, to reach the worker's own read-back hash.
        `outcome` (a dict) receives the presigned PUT's status."""
        started = self.author.ok("POST", f"{self.base}/releases/{release['id']}/artifacts:initiate", {
            "distribution": distribution, "target": target, "name": name, "sizeBytes": len(body),
            "sha256": declared_sha256 or hashlib.sha256(body).hexdigest(), "signature": signature})
        status = put_presigned(started["uploadRequest"], body)
        if outcome is not None:
            outcome["presignedPut"] = status
        if status != 200 and lab is not None:
            target_url = urllib.parse.urlsplit(started["uploadRequest"]["url"])
            site = next(m for m in st.MIRRORS.values() if m.upload_port == target_url.port)
            access, secret_key = lab.rustfs[site.key]
            written, raw, _ = st.s3_request("PUT", site.s3_endpoint, access, secret_key,
                                            urllib.parse.unquote(target_url.path), body=body,
                                            headers={"content-type": "application/octet-stream"})
            ls.require(written == 200, f"direct write of {name}: {written} {raw[:200]!r}")
        else:
            ls.require(status == 200, f"presigned upload of {name}: {status}")
        upload_id = started["upload"]["id"]
        completed = self.author.call("POST", f"{self.base}/release-uploads/{upload_id}:complete", {})
        ls.require(completed[0] == 200, f"complete {name}: {completed}")
        return self.wait_upload(upload_id)

    def wait_upload(self, upload_id, timeout=300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            upload = self.get(f"/release-uploads/{upload_id}")
            if upload["status"] in ("verified", "failed"):
                return upload
            time.sleep(1)
        raise SystemExit(f"upload {upload_id} still not verified after {timeout}s")

    def wait_replicas(self, release_id, distribution, count, timeout=240):
        """The worker copies a verified package to the other independent mirrors; the artifact's
        addresses grow as each copy verifies."""
        deadline = time.time() + timeout
        urls = []
        while time.time() < deadline:
            urls = urls_of(self.view(release_id)["artifacts"].get(distribution))
            if len(urls) >= count:
                break
            time.sleep(2)
        return urls

    def package(self, release, *, size=1 << 20, name=None):
        """A tauri package for the release's one distribution, replicated everywhere."""
        blob = secrets.token_bytes(size)
        name = name or f"app-{release['build']}.tar.gz"
        upload = self.upload(release, "tauri", blob, name, target=TAURI_TARGET,
                             signature=lab_updater_signature(blob, name))
        ls.require(upload["status"] == "verified", f"package not verified: {upload.get('failure')}")
        urls = self.wait_replicas(release["id"], "tauri", len(st.MIRRORS))
        return {"sha256": hashlib.sha256(blob).hexdigest(), "size": len(blob), "urls": urls, "upload": upload}

    # -- policy
    def policy(self, platform, channel):
        return self.get(f"/release-policies/{platform}/{channel}")

    def set_policy(self, platform, channel, *, min_build=0, recommend_below=0, block_ui=False, blocked=()):
        """Propose and approve: two people, effective immediately. No-op when it already holds."""
        view = self.policy(platform, channel)
        if view.get("pending"):
            self.author.ok("POST", f"{self.base}/release-policy-changes/{view['pending']['id']}:reject", None,
                           "lab clears a leftover proposal")
            view = self.policy(platform, channel)
        current = view.get("effective")
        if (current is None and not (min_build or recommend_below or block_ui or blocked)) or (
                current and current["minBuild"] == min_build and current["recommendBelowBuild"] == recommend_below
                and current["blockUi"] == block_ui and not blocked and not current["blockedBuilds"]):
            return current
        proposal = {"revision": current["revision"] if current else None, "minBuild": min_build,
                    "recommendBelowBuild": recommend_below, "blockedBuilds": list(blocked), "blockUi": block_ui,
                    "effectiveFrom": now_ms() - 1000}
        change = self.author.ok("POST", f"{self.base}/release-policies/{platform}/{channel}/changes", proposal,
                                "lab policy change")
        self.approver.ok("POST", f"{self.base}/release-policy-changes/{change['id']}:approve", None,
                         "second person approves the policy")
        return self.policy(platform, channel)["effective"]


def release_lab(facts, app):
    admin, approver, _ = people()
    artifact_credentials(admin, st.Lab(), facts)
    return Releases(app, admin, approver)


def read_release(app, platform, channel):
    body = read_document(app, release_file(platform, channel))
    if not body:
        return None, None
    try:
        return body, payload_of(body)
    except (ValueError, KeyError):
        return None, None


def wait_release(app, platform, channel, predicate, timeout=240, what="release manifest"):
    """The signed manifest on the mirrors once `predicate(payload)` holds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        body, payload = read_release(app, platform, channel)
        if payload is not None and predicate(payload):
            return body, payload
        time.sleep(1)
    raise SystemExit(f"{what}: the mirrors did not carry the expected {platform}/{channel} manifest in {timeout}s")


def entry_of(payload, build):
    return next((r for r in payload["releases"] if r["build"] == build), None)


def wait_projection(app, platform, min_build, timeout=150):
    """The build gate reaches both data planes: Core's and Social's client policy rows."""
    keys = st.Lab().core

    def seen():
        for database, table in (("core", "tenant_client_policy"), ("social", "flare_tenant_client_policy")):
            value = ls.psql(keys, f"SELECT coalesce(build_policies->'{platform}'->>'min_build','0') FROM {table} "
                                  f"WHERE tenant_id='{app['appId']}'", ls.DATABASES[database], check=False)
            if (value or "0") != str(min_build):
                return False
        return True
    ls.wait_for(seen, timeout, f"{platform} min_build {min_build} projected to Core and Social", 2)


def release_summary(status):
    status = status or {}
    return {k: status.get(k) for k in ("state", "prompt", "blockUi", "readOnly", "error", "blockReason")} | \
        {"releaseBuild": (status.get("release") or {}).get("build")}


def command_release(_args):
    """A tauri release through the real platform: client application, draft with a resource pack,
    package upload, verification and copy by the worker, two-person approval, publish; the signed
    manifest and pack come from the mirrors, then the release is halted (withdrawn) and a policy
    raises the floor. The client side is the same engine client as ever; its assertions are the
    ones this case always made. Needs `seed` run on a lab initialized by this build."""
    facts = load_facts()
    app = facts["apps"]["forge"]
    lab = st.Lab()
    rel = release_lab(facts, app)
    rel.tauri_app()
    seed = seed_file(app)
    rel.quiet("tauri", "stable")
    steps = {}
    pack_body = json.dumps({"schema": "flare.resource.v1", "kind": "strings",
                            "locales": {"zh-CN": {"composerReadOnlyUpdate": "实验台词条"}}}, ensure_ascii=False).encode()
    pack_name = "res/strings-1.json"
    put_everywhere(lab, app, pack_body, pack_name)
    primary = st.MIRRORS["a"]
    pack = {"id": "strings-main", "kind": "strings", "packVersion": 1, "minBuild": 0,
            # The first address answers 404: the client has to fail over to the next mirror.
            "url": [object_url(primary, app, "res/missing.json"), object_url(st.MIRRORS["b"], app, pack_name)],
            "sha256": hashlib.sha256(pack_body).hexdigest(), "sizeBytes": len(pack_body)}
    build = rel.next_build("tauri", "stable")
    app_info = {**RELEASE_APP, "build": build - 100}
    client_name = f"release-{now_ms()}"
    draft = rel.draft("tauri", "stable", build, packs=[pack], stages=[10000])
    # Everything this case publishes is withdrawn again at the end, pass or fail.
    published = [draft["id"]]
    package = rel.package(draft)
    steps["package"] = {"status": package["upload"]["status"], "copies": len(package["urls"]),
                        "signingCert": package["upload"].get("signingCertSha256")}
    rel.go_live(draft["id"])
    document_live, payload_live = wait_release(app, "tauri", "stable", lambda p: (
        entry_of(p, build) is not None and not entry_of(p, build)["rollout"]["halted"]))
    steps["manifest"] = {"serial": payload_live["serial"], "builds": [r["build"] for r in payload_live["releases"]],
                         "artifact": entry_of(payload_live, build)["artifacts"]["tauri"]["kind"]}

    def client(**extra):
        return Client(client_name, app, seed=seed, no_login=True, tick=60, app_info=app_info, **extra).start()

    def wait_ready(item):
        ls.require(item.wait(lambda e: e["kind"] == "started", 30), "release client did not start")

    first = client()
    try:
        wait_ready(first)
        steps["check"] = release_summary(settled_update(first))
        steps["resourcesFirstRun"] = (first.ask("update_resources", 20) or {}).get("data", {}).get("resources")
        steps["packsStored"] = wait_packs(first, app, ["strings-main"])
        # A desktop release is not downloaded by the engine: the install plan hands the Tauri
        # updater the version and the package hash it must verify (the controller downloads
        # only `package` releases).
        steps["install"] = (first.ask("update_install", 20) or {}).get("data", {})
    finally:
        first.stop()
    second = client()
    try:
        wait_ready(second)
        resources = (second.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["resourcesAfterRestart"] = resources
        steps["contentMatches"] = [p.get("contentJson") == pack_body.decode() for p in resources.get("packs", [])]
        # Withdrawn: the release is halted, so clients stop being offered it and drop its pack.
        rel.halt(draft["id"])
        document_halted, _ = wait_release(app, "tauri", "stable", lambda p: (
            entry_of(p, build) is not None and entry_of(p, build)["rollout"]["halted"]))
        steps["afterWithdrawal"] = release_summary(settled_update(second))
        steps["packsAfterWithdrawal"] = wait_packs(second, app, [])
        run_snapshot = (second.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["runSnapshotKept"] = [p["id"] for p in run_snapshot.get("packs", [])]
        # A mirror serving the older (validly signed) document changes nothing.
        put_everywhere(lab, app, document_live, release_file("tauri", "stable"))
        steps["afterReplayOfOlderSerial"] = release_summary(settled_update(second))
        put_everywhere(lab, app, document_halted, release_file("tauri", "stable"))
    finally:
        second.stop()
    third = client()
    try:
        wait_ready(third)
        resources = (third.ask("update_resources", 20) or {}).get("data", {}).get("resources") or {}
        steps["resourcesAfterWithdrawalRestart"] = [p["id"] for p in resources.get("packs", [])]
        # Forced update from the signed manifest alone: a newer build is on offer and a policy
        # (proposed and approved by two people) raises the floor above this client's build.
        newer = rel.draft("tauri", "stable", rel.next_build("tauri", "stable"), stages=[10000])
        published.append(newer["id"])
        rel.package(newer)
        rel.go_live(newer["id"])
        rel.set_policy("tauri", "stable", min_build=app_info["build"] + 50, block_ui=True)
        wait_release(app, "tauri", "stable", lambda p: p["policy"].get("minBuild") == app_info["build"] + 50)
        steps["required"] = release_summary(settled_update(third))
    finally:
        third.stop()
        rel.set_policy("tauri", "stable")
        for release_id in published:
            rel.withdraw(release_id)
    checks = {
        "uploadVerifiedAndCopied": steps["package"]["status"] == "verified" and steps["package"]["copies"] == len(st.MIRRORS),
        "manifestSignedByPlatform": steps["manifest"]["artifact"] == "tauri",
        "checkFindsRelease": steps["check"]["state"] == "available" and steps["check"]["releaseBuild"] == build,
        "firstRunHasNoPacks": (steps["resourcesFirstRun"] or {}).get("packs") == [],
        "packStoredViaSecondUrl": steps["packsStored"] == ["strings-main"],
        "installPlanCarriesPackage": steps["install"].get("plan") == {
            "kind": "desktopUpdater", "sha256": package["sha256"], "target": TAURI_TARGET, "version": f"2.{build}.0"},
        "packLoadedOnRestart": steps["contentMatches"] == [True],
        "withdrawalClearsRelease": steps["afterWithdrawal"]["state"] == "upToDate",
        "withdrawalRemovesPack": steps["packsAfterWithdrawal"] == [],
        "runSnapshotUnchanged": steps["runSnapshotKept"] == ["strings-main"],
        "replayIgnored": steps["afterReplayOfOlderSerial"]["state"] == "upToDate",
        "goneAfterRestart": steps["resourcesAfterWithdrawalRestart"] == [],
        "requiredBlocks": steps["required"]["state"] == "required" and steps["required"]["blockUi"] is True,
    }
    evidence("s1-release", {"steps": steps, "checks": checks, "pass": all(checks.values())})
    log(json.dumps(checks))


# One (installId, salt) -> bucket pair the client's rule must reproduce. The same vector is
# asserted against `access/src/release/evaluate.rs::bucket` in a Rust unit test
# (`the_bucket_matches_the_acceptance_golden_vector`), so this helper and the client cannot drift.
BUCKET_GOLDEN = (("install-00042", "rel_01JC8WGOLDEN", 1274), ("7f3c1e9a-golden", "salt-0001", 8848))
ROLLOUT_INSTALLS = 100_000


def bucket_of(install_id, salt):
    """The client's rollout bucket (access/src/release/evaluate.rs): u32_be(SHA-256(id ':' salt)[0..4]) mod 10000.

    >>> [bucket_of(i, s) == expected for i, s, expected in BUCKET_GOLDEN]
    [True, True]
    """
    digest = hashlib.sha256(f"{install_id}:{salt}".encode()).digest()
    return int.from_bytes(digest[:4], "big") % 10_000


def offered(entry, install_id):
    """Whether the client offers this release to the installation: the release is not halted and
    the installation's bucket is inside the rollout (`usable` and `in_rollout` in evaluate.rs).

    >>> live = {"rollout": {"halted": False, "basisPoints": 10000, "salt": "s"}}
    >>> offered(live, "a"), offered({"rollout": {**live["rollout"], "halted": True}}, "a")
    (True, False)
    >>> offered({"rollout": {"halted": False, "basisPoints": 0, "salt": "s"}}, "a")
    False
    """
    rollout = entry["rollout"]
    return not rollout["halted"] and bucket_of(install_id, rollout["salt"]) < rollout["basisPoints"]


def command_release_rollout(_args):
    """The staged rollout as clients see it: a release published at 500 basis points reaches 5% of
    100,000 synthetic installations, 5% +- 0.5%, decided by the client's own rule with the salt in
    the published manifest; the advance to the last stage reaches all of them, the halt none (the
    halted manifest is evaluated like any other). One standard deviation of the hit rate is
    0.069%, so the tolerance is about 7 sigma: a correct rule practically never fails it. The
    helper is pinned to the client by a golden vector checked on both sides."""
    facts = load_facts()
    app = facts["apps"]["forge"]
    rel = release_lab(facts, app)
    rel.tauri_app()
    rel.quiet("tauri", "canary")
    build = rel.next_build("tauri", "canary")
    draft = rel.draft("tauri", "canary", build, stages=[500, 10000])
    installs = [f"install-{index:06d}" for index in range(ROLLOUT_INSTALLS)]

    def reach(payload):
        entry = entry_of(payload, build)
        return {"basisPoints": entry["rollout"]["basisPoints"], "halted": entry["rollout"]["halted"],
                "salt": entry["rollout"]["salt"], "hits": sum(offered(entry, i) for i in installs)}

    try:
        rel.package(draft)
        rel.go_live(draft["id"])
        _, payload = wait_release(app, "tauri", "canary", lambda p: entry_of(p, build) is not None)
        staged = reach(payload)
        rel.advance(draft["id"])
        _, payload = wait_release(app, "tauri", "canary",
                                  lambda p: entry_of(p, build)["rollout"]["basisPoints"] == 10000)
        full = reach(payload)
        rel.halt(draft["id"])
        _, payload = wait_release(app, "tauri", "canary", lambda p: entry_of(p, build)["rollout"]["halted"])
        halted = reach(payload)
    finally:
        rel.withdraw(draft["id"])
    rate = staged["hits"] / len(installs)
    checks = {"bucketMatchesClientGoldenVector": all(bucket_of(i, s) == b for i, s, b in BUCKET_GOLDEN),
              "publishedAtFivePercent": staged["basisPoints"] == 500 and not staged["halted"],
              "hitRateWithinHalfPoint": 0.045 <= rate <= 0.055,
              "advanceReachesEveryone": full["basisPoints"] == 10000 and full["hits"] == len(installs),
              "haltReachesNobody": halted["halted"] is True and halted["hits"] == 0}
    evidence("s1-release-rollout", {"installs": len(installs), "build": build, "staged": staged, "rate": rate,
                                    "full": full, "halted": halted, "checks": checks, "pass": all(checks.values())})
    log(json.dumps(checks) + f" rate={rate:.4f}")


def command_apk_negative(_args):
    """What the platform refuses to ship, with a real APK signing block: one signed by a
    certificate the client application did not register; a file whose bytes are not the declared
    SHA-256; a plain ZIP without any signing block; and a build that does not rise. The positive
    control is the registered certificate: verified, copied, and carried by the signed manifest
    with its real fingerprint."""
    facts = load_facts()
    app = facts["apps"]["forge"]
    rel = release_lab(facts, app)
    good_cert, good_fingerprint = lab_signer("good")
    bad_cert, bad_fingerprint = lab_signer("other")
    rel.client_app("android", {"direct": {"kind": "apk"}}, certs=(good_fingerprint,))
    checks, results = {}, {}

    def negative(label, build, body, *, declared=None):
        draft = rel.draft("android", "stable", build)
        outcome = {}
        upload = rel.upload(draft, "direct", body, f"{label}-{build}.apk", declared_sha256=declared,
                            lab=st.Lab() if declared else None, outcome=outcome)
        attached = rel.view(draft["id"])["artifacts"].get("direct")
        results[label] = {"status": upload["status"], "failure": upload.get("failure"),
                          "signingCertSha256": upload.get("signingCertSha256"), "attached": bool(attached), **outcome}
        return upload

    first = rel.next_build("android", "stable")
    mismatch = negative("cert-mismatch", first, lab_apk(bad_cert))
    checks["certMismatchFailed"] = mismatch["status"] == "failed" and "signingCertMismatch" in (mismatch.get("failure") or "")
    # The refusal must name the certificate it found: the field, or the failure detail. Neither
    # saying anything fails the check.
    checks["certMismatchNamesTheFoundFingerprint"] = mismatch.get("signingCertSha256") == bad_fingerprint \
        or bad_fingerprint in (mismatch.get("failure") or "")
    body = lab_apk(good_cert)
    wrong = negative("hash-mismatch", first + 10, body, declared=hashlib.sha256(lab_apk(good_cert)).hexdigest())
    # Two layers: the store refuses the presigned PUT (the declared hash is signed into it), and
    # the bytes written anyway are refused by the worker's read-back.
    checks["hashMismatchRefusedByStore"] = results["hash-mismatch"]["presignedPut"] == 400
    checks["hashMismatchFailed"] = wrong["status"] == "failed" and "digestMismatch" in (wrong.get("failure") or "")
    plain = negative("unsigned", first + 20, lab_apk(None))
    checks["unsignedFailed"] = plain["status"] == "failed" and "unsigned" in (plain.get("failure") or "")
    checks["nothingFailedIsAttached"] = not any(r["attached"] for r in results.values())
    # The registered certificate: verified, copied, published.
    live_build = first + 30
    draft = rel.draft("android", "stable", live_build, stages=[10000])
    upload = rel.upload(draft, "direct", lab_apk(good_cert), f"app-{live_build}.apk")
    urls = rel.wait_replicas(draft["id"], "direct", len(st.MIRRORS))
    results["registered"] = {"status": upload["status"], "failure": upload.get("failure"),
                             "signingCertSha256": upload.get("signingCertSha256"), "copies": len(urls)}
    checks["registeredCertVerified"] = upload["status"] == "verified" and upload.get("signingCertSha256") == good_fingerprint
    checks["registeredCopiedToEveryMirror"] = len(urls) == len(st.MIRRORS)
    try:
        rel.go_live(draft["id"])
        _, payload = wait_release(app, "android", "stable", lambda p: entry_of(p, live_build) is not None)
        artifact = entry_of(payload, live_build)["artifacts"]["direct"]
        checks["manifestCarriesRealFingerprint"] = artifact.get("kind") == "apk" and \
            artifact.get("signingCertSha256") == good_fingerprint
        # Downgrade: a build that is not above everything already published.
        low = rel.author.call("POST", f"{rel.base}/releases", {
            "platform": "android", "channel": "stable", "version": "0.0.1", "build": live_build - 1,
            "notes": {"en": ["downgrade"]}})
        same = rel.author.call("POST", f"{rel.base}/releases", {
            "platform": "android", "channel": "stable", "version": "0.0.2", "build": live_build,
            "notes": {"en": ["same build"]}})
    finally:
        # Withdrawn again, pass or fail. The negative drafts cannot be deleted (the platform has
        # no such operation); they are inert and `next_build` steps past them.
        rel.withdraw(draft["id"])
    results["downgrade"] = {"status": low[0], "body": low[1]}
    results["sameBuild"] = {"status": same[0], "body": same[1]}
    checks["downgradeRefused"] = low[0] == 409 and "buildNotMonotonic" in fault_of(low[1])
    checks["sameBuildRefused"] = same[0] == 409 and "buildTaken" in fault_of(same[1])
    evidence("s6-apk-negative", {"fingerprints": {"registered": good_fingerprint, "other": bad_fingerprint},
                                 "results": results, "checks": checks, "pass": all(checks.values())})
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
# What the platform answers a second person who tries the author's emergency path.
AUTHOR_ONLY = "error.access.changeSet.emergencyAuthorOnly"
CACHE_CONTROL = "public, max-age=60, stale-while-revalidate=300, stale-if-error=604800"


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
    publication, once their users sit elsewhere. Clean-up, so it is recorded, never fatal. The
    points it disabled are listed with their earlier status: when the publication then fails,
    `revert_retired` puts them back instead of leaving the lab's topology half retired."""
    disabled = []
    try:
        for ap_id in ap_ids:
            before = access_point(admin, ap_id)["status"]
            if before != "disabled":
                set_status(admin, ap_id, "disabled")
                disabled.append({"id": ap_id, "was": before})
        result = publish(admin, approver, reason)
        return {"accessPoints": ap_ids, "changeSet": result["changeSet"], "retired": True}
    except (SystemExit, StopIteration) as error:
        return {"accessPoints": ap_ids, "retired": False, "error": str(error)[:800], "disabled": disabled}


def revert_retired(admin, retired):
    """Clean-up after a retirement that failed part-way: the access points it had disabled get
    their earlier status back. Nothing to do when it succeeded or never started. A failure of the
    revert itself is raised, for `cleanup_step` to record and fail the case.

    >>> revert_retired(None, None), revert_retired(None, {"retired": True, "disabled": [{"id": "a", "was": "active"}]})
    ([], [])
    >>> revert_retired(None, {"retired": False, "disabled": []})
    []
    """
    if not retired or retired.get("retired"):
        return []
    reverted = []
    for entry in retired.get("disabled") or []:
        set_status(admin, entry["id"], entry["was"])
        reverted.append(entry["id"])
    return reverted


def refused_as_not_author(status, body):
    """The second person's emergency call is refused for being not the author, not for any other
    reason (a missing permission would prove nothing about the author-only rule).

    >>> refused_as_not_author(403, {"code": 403, "message": AUTHOR_ONLY})
    True
    >>> refused_as_not_author(403, {"code": 403, "message": "error.permission.denied"})
    False
    >>> refused_as_not_author(409, {"message": AUTHOR_ONLY})
    False
    """
    return status == 403 and AUTHOR_ONLY in json.dumps(body, ensure_ascii=False, default=str)


def points_to_restart(stopped, retired):
    """After a case, whatever access points it stopped run again, unless a publication has since
    taken them out of the manifest: then every point a client can still find is running.

    >>> points_to_restart(["ap-a1", "ap-a2"], None)
    ['ap-a1', 'ap-a2']
    >>> points_to_restart(["ap-a1", "ap-a2"], {"retired": False, "error": "lint"})
    ['ap-a1', 'ap-a2']
    >>> points_to_restart(["ap-a1", "ap-a2"], {"retired": True})
    []
    """
    return [] if (retired or {}).get("retired") else list(stopped)


def needs_supersede(offline_serial, platform_serial):
    """An offline manifest left on the mirrors must be overtaken by a platform manifest before the
    next case: otherwise the sequence stays below it and every later publication is a rollback.

    >>> needs_supersede(None, None)
    False
    >>> needs_supersede(201000, None), needs_supersede(201000, 201000), needs_supersede(201000, 201001)
    (True, True, False)
    """
    return offline_serial is not None and (platform_serial or 0) <= offline_serial


def supersede_offline(admin, approver, app, offline_serial, reason, before_publish=None):
    """The floor past the offline serial, then an ordinary publication above it. `before_publish`
    runs between the two: where the worker is still stopped, it starts only once the floor is up,
    as the runbook orders it."""
    floor = admin.ok("POST", "/platform/access/serial-floor", {"floor": offline_serial}, reason=reason)
    if before_publish:
        before_publish()
    bump_weight(admin, active_points(admin)[0])
    publication = publish(admin, approver, reason)
    serial = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
    return {"floor": floor, "platformSerial": serial}


def cleanup_step(cleanup, name, action):
    """One clean-up step: its failure is recorded and fails the case, and the next step still runs."""
    try:
        cleanup[name] = action()
    except (SystemExit, Exception) as error:  # noqa: BLE001 - clean-up must reach every step
        cleanup[name] = {"error": str(error)[:800]}


def cleanup_failed(cleanup):
    """
    >>> cleanup_failed({"accessPoints": [], "offline": {"error": "x"}})
    True
    >>> cleanup_failed({"accessPoints": ["ap-a1"], "offline": None})
    False
    """
    return any(isinstance(value, dict) and "error" in value for value in cleanup.values())


CONTROL_PLANE = ("api", "worker", "prober")


def stop_control_plane():
    state = ls.load_state()
    for role in CONTROL_PLANE:
        ls.signal_process(state, f"platform-{role}", 15, wait=30)


def platform_port(role):
    """The port a control plane role listens on once it is ready.

    >>> platform_port("api") == st.PLATFORM_API and platform_port("prober") == st.PLATFORM_PROBER
    True
    """
    return dict(api=st.PLATFORM_API, worker=st.PLATFORM_WORKER, prober=st.PLATFORM_PROBER)[role]


def start_control_plane(roles=CONTROL_PLANE):
    """Starts the roles that are not running, and returns once every named role listens: a role
    already started but still coming up (an earlier step began it) is waited for as well, so the
    next call to the API does not meet a process that is not ready. After an offline manifest,
    `("api",)` first: the worker publishes on its own (a leftover intent, a due re-signature) and
    must only start once the serial floor is above the offline serial."""
    for role in roles:
        if not ls.live(ls.load_state(), f"platform-{role}"):
            st.platform_process(ls.load_state(), st.Lab(), role)  # returns once it listens
        else:
            port = platform_port(role)
            ls.wait_for(lambda port=port: ls.port_open(port), 60, f"platform-{role} :{port}")


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
            "notAuthor": {"status": refused_status, "authorOnly": refused_as_not_author(refused_status, refused),
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


def dispatch_round(fleet, label):
    """Each client sends one text to the next one. Every message carries its own token in the
    text, so Core's copy is found even when the acknowledgement never came back to the client."""
    rows = []
    for index, client in enumerate(fleet):
        peer = fleet[(index + 1) % len(fleet)]
        identity, token = uuid.uuid4().hex, uuid.uuid4().hex
        client.send(op="send", id=identity, to=json.loads(peer.config_file.read_text())["user"],
                    text=f"{label} {client.name} {token}")
        rows.append({"client": client.name, "label": label, "id": identity, "token": token,
                     "dispatchedAtMs": now_ms(), "clientMsgId": None, "error": None})
    return rows


def ack_of(events, identity):
    """The client's answer to one `send`: `sent` with the client message id, or `error`."""
    for event in list(events):
        data = event.get("data")
        if event.get("kind") in ("sent", "error") and isinstance(data, dict) and data.get("id") == identity:
            return event
    return None


def collect_acks(fleet, rows, timeout):
    """Fills in the answers that have arrived, under one deadline for all rows: a client stuck on
    its acknowledgement costs the round `timeout` once, not once per client. Rows still open
    stay open and can be collected again later (after the reconnect).

    >>> class C: pass
    >>> c = C(); c.name = "a"; c.events = [{"kind": "sent", "data": {"id": "1", "clientMsgId": "m1"}},
    ...                                     {"kind": "error", "data": {"id": "2", "message": "offline"}}]
    >>> rows = [{"client": "a", "id": i, "clientMsgId": None, "error": None} for i in ("1", "2", "3")]
    >>> [(r["clientMsgId"], r["error"]) for r in collect_acks([c], rows, 0)]
    [('m1', None), (None, 'offline'), (None, None)]
    """
    clients = {client.name: client for client in fleet}
    deadline = time.time() + timeout
    while True:
        for row in rows:
            if row["clientMsgId"] or row["error"]:
                continue
            event = ack_of(clients[row["client"]].events, row["id"])
            if event and event["kind"] == "sent":
                row["clientMsgId"] = event["data"].get("clientMsgId")
            elif event:
                row["error"] = event["data"].get("message") or "error"
        if all(row["clientMsgId"] or row["error"] for row in rows) or time.time() >= deadline:
            return rows
        time.sleep(0.5)


def message_outcome(rows, stored_ids, stored_tokens):
    """Every dispatched message the SDK did not refuse outright must be in Core: acknowledged or
    not, found by its client message id or by its token.

    >>> rows = [{"clientMsgId": "m1", "token": "t1", "error": None},
    ...         {"clientMsgId": None, "token": "t2", "error": None},
    ...         {"clientMsgId": None, "token": "t3", "error": "not connected"}]
    >>> message_outcome(rows, {"m1"}, {"t2"})
    {'dispatched': 3, 'refused': 1, 'mustStore': 2, 'stored': 2, 'lost': []}
    >>> message_outcome(rows, {"m1"}, set())["lost"]
    ['t2']
    """
    must = [row for row in rows if not row["error"]]
    lost = [row["token"] for row in must
            if row["clientMsgId"] not in stored_ids and row["token"] not in stored_tokens]
    return {"dispatched": len(rows), "refused": len(rows) - len(must), "mustStore": len(must),
            "stored": len(must) - len(lost), "lost": lost}


def round_delivered(rows, label, planned):
    """A round sent while the client is online must deliver everything: as many rows as planned,
    each acknowledged, none refused. A refusal there is a defect of the SDK or the lab, not the
    excused refusal of a client that is offline.

    >>> ok = {"label": "before", "clientMsgId": "m", "error": None}
    >>> round_delivered([ok, ok], "before", 2), round_delivered([ok], "before", 2)
    (True, False)
    >>> round_delivered([ok, {**ok, "error": "not connected"}], "before", 2)
    False
    >>> round_delivered([ok, {**ok, "clientMsgId": None}], "before", 2)
    False
    """
    own = [row for row in rows if row["label"] == label]
    return len(own) == planned and all(row["clientMsgId"] and not row["error"] for row in own)


def rounds_sent(rows, planned):
    """Every round dispatched its planned count of messages (`planned` maps label to count).

    >>> rows = [{"label": "before"}, {"label": "before"}, {"label": "during"}]
    >>> rounds_sent(rows, {"before": 2, "during": 1}), rounds_sent(rows, {"before": 2, "during": 2})
    (True, False)
    >>> rounds_sent(rows, {"before": 2, "after": 1})
    False
    """
    return all(sum(row["label"] == label for row in rows) == count for label, count in planned.items())


def persisted(app, rows, since_ms, timeout=180):
    """What Core stored of these messages, polled until every one that must be there is."""
    for row in rows:
        ls.require(re.fullmatch(r"[0-9a-f]{32}", row["token"])
                   and (row["clientMsgId"] is None or re.fullmatch(r"[A-Za-z0-9_.:-]+", row["clientMsgId"])),
                   f"unexpected message id in {row}")
    keys = st.Lab().core
    deadline = time.time() + timeout
    while True:
        ids = [row["clientMsgId"] for row in rows if row["clientMsgId"]]
        tokens = [row["token"] for row in rows]
        scope = f"tenant_id='{app['appId']}' AND created_at >= to_timestamp({since_ms} / 1000.0)"
        found_ids = ls.psql(keys, f"SELECT client_msg_id FROM messages WHERE {scope} AND client_msg_id IN ("
                                  + ",".join(f"'{i}'" for i in ids) + ")", ls.DATABASES["core"], check=False) \
            if ids else ""
        found_tokens = ls.psql(keys, "SELECT t FROM unnest(ARRAY[" + ",".join(f"'{t}'" for t in tokens)
                                     + f"]) AS t WHERE EXISTS (SELECT 1 FROM messages WHERE {scope} "
                                     "AND position(convert_to(t, 'UTF8') in content) > 0)",
                               ls.DATABASES["core"], check=False)
        outcome = message_outcome(rows, set(found_ids.split()), set(found_tokens.split()))
        if not outcome["lost"] or time.time() >= deadline:
            return outcome
        time.sleep(2)


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
    """Each client's `changed` event to at least `serial` after its mark. Looks at least once, so
    a timeout of 0 reports what has already happened.

    >>> class C: pass
    >>> c = C(); c.name = "a"; c.events = [{"kind": "changed", "ts": 5, "data": {"serial": 9, "source": "https:x"}}]
    >>> wait_adopted([c], 9, {"a": 0}, 0)
    {'a': {'ts': 5, 'serial': 9, 'source': 'https:x'}}
    >>> wait_adopted([c], 10, {"a": 0}, 0), wait_adopted([c], 9, {"a": 1}, 0)
    ({}, {})
    """
    deadline, adopted = time.time() + timeout, {}
    while True:
        for client in fleet:
            if client.name in adopted:
                continue
            for event in list(client.events[marks[client.name]:]):
                if event["kind"] == "changed" and event["data"].get("serial", 0) >= serial:
                    adopted[client.name] = {"ts": event["ts"], "serial": event["data"]["serial"],
                                            "source": event["data"].get("source")}
                    break
        if len(adopted) == len(fleet) or time.time() >= deadline:
            return adopted
        time.sleep(0.5)


def recovery_serials(facts):
    """What the incident record knows: serials reserved for recovery links and signed offline."""
    return [*facts.get("recoverySerials", []), *facts.get("offlineSerials", [])]


def command_total_outage(args):
    """Clients online, then every access point they know goes away. New nodes come up and the
    author publishes them alone through the emergency path.

    Needs a platform database initialized by this flare-platform build (see the module docstring)."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    old, spares = manifest_points(app), spare_sites(app)
    ls.require(spares, f"every lab site is in the manifest ({old}); retire some first")
    fleet = start_fleet(app, args.clients, "total", seed)
    result = {"oldAccessPoints": old, "newAccessPoints": spares}
    messages, stopped, cleanup = [], [], {}
    since = now_ms()
    try:
        wait_fleet_online(fleet)
        messages += collect_acks(fleet, dispatch_round(fleet, "before"), 60)
        marks = {c.name: len(c.events) for c in fleet}
        stopped = list(old)
        stop_access_points(old)
        result["outageAtMs"] = now_ms()
        result["secondsToOffline"] = wait_offline(fleet)
        # Sent while nothing is reachable: answers may only come after the reconnect.
        during = dispatch_round(fleet, "during")
        collect_acks(fleet, during, 10)
        messages += during
        start_access_points(spares)
        activate(admin, spares)
        publication = emergency_publish(admin, approver, "every access point down: new nodes")
        result["publication"] = publication
        result["reconnect"] = wait_reconnected(fleet, set(spares), publication["observedAtMs"], marks)
        log(f"back on new nodes: {result['reconnect']['back']}/{len(fleet)}, "
            f"max {result['reconnect']['maxDelayMs']} ms")
        messages += dispatch_round(fleet, "after")
        collect_acks(fleet, messages, 120)
        result["messages"] = {"rows": messages, **persisted(app, messages, since)}
        result["retire"] = retire(admin, approver, old, "dead access points after the total outage")
    finally:
        for client in fleet:
            client.stop()
        cleanup_step(cleanup, "accessPoints",
                     lambda: start_access_points(points_to_restart(stopped, result.get("retire")))
                     or points_to_restart(stopped, result.get("retire")))
        cleanup_step(cleanup, "retireReverted", lambda: revert_retired(admin, result.get("retire")))
        result["cleanup"] = cleanup
    stored = result.get("messages", {})
    by_label = {label: [r for r in stored.get("rows", []) if r["label"] == label] for label in ("before", "during", "after")}
    result["checks"] = {
        "reconnectedWithinLimit": result.get("reconnect", {}).get("pass", False),
        "noDispatchedMessageLost": bool(stored) and not stored["lost"] and stored["mustStore"] > 0,
        "beforeRoundDelivered": round_delivered(stored.get("rows", []), "before", len(fleet)),
        "everyRoundSent": rounds_sent(stored.get("rows", []), {label: len(fleet) for label in by_label}),
        "secondPersonRefused": bool(result.get("publication", {}).get("notAuthor", {}).get("authorOnly")),
        "publishedAsEmergency": bool(result.get("publication", {}).get("emergency")),
        "deadPointsRetired": bool(result.get("retire", {}).get("retired")),
        "labRestored": not cleanup_failed(cleanup),
    }
    result["pass"] = all(result["checks"].values())
    evidence("r1-total-outage", result)
    log(json.dumps(result["checks"]))


def command_break_glass(args):
    """Every access point down and the control plane down too: the runbook signs the new nodes
    offline and uploads them by hand. When the control plane is back it follows the runbook's
    order: platform-api alone, the floor past that serial, then worker and prober, then the
    platform publishes the same topology.

    Needs a platform database initialized by this flare-platform build (see the module docstring)."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    seed = seed_file(app)
    old, spares = manifest_points(app), spare_sites(app)
    ls.require(spares, f"every lab site is in the manifest ({old}); retire some first")
    lab = st.Lab()
    fleet = start_fleet(app, args.clients, "glass", seed)
    result = {"oldAccessPoints": old, "newAccessPoints": spares}
    stopped, cleanup, uploaded_serial = [], {}, None
    try:
        wait_fleet_online(fleet)
        marks = {c.name: len(c.events) for c in fleet}
        stopped = list(old)
        stop_access_points(old)
        result["secondsToOffline"] = wait_offline(fleet)
        start_access_points(spares)
        stop_control_plane()
        offline = offline_manifest(app, "break-glass", spares, recovery_serials=recovery_serials(facts))
        facts.setdefault("offlineSerials", []).append(offline["serial"])
        save_facts(facts)
        uploaded_serial = offline["serial"]
        put_everywhere(lab, app, offline["file"].read_bytes(), headers={"cache-control": CACHE_CONTROL})
        uploaded = now_ms()
        result["offline"] = {k: (str(v) if isinstance(v, Path) else v) for k, v in offline.items()}
        result["reconnect"] = wait_reconnected(fleet, set(spares), uploaded, marks)
        left = (uploaded + RECONNECT_AFTER_PUBLISH_MS) / 1000 - time.time()
        adopted = wait_adopted(fleet, offline["serial"], marks, max(0, left))
        delays = [value["ts"] - uploaded for value in adopted.values()]
        result["adoptedOffline"] = {"clients": len(fleet), "adopted": len(adopted), "maxDelayMs": max(delays, default=None),
                                    "limitMs": RECONNECT_AFTER_PUBLISH_MS, "samples": adopted}
        log(f"offline manifest {offline['serial']}: {len(adopted)}/{len(fleet)} adopted, "
            f"{result['reconnect']['back']} back on new nodes")
        start_control_plane(("api",))
        floor = admin.ok("POST", "/platform/access/serial-floor", {"floor": offline["serial"]},
                         reason=f"offline manifest {offline['serial']} signed during the outage")
        start_control_plane()
        status, body = admin.call("POST", "/platform/access/serial-floor",
                                  {"floor": floor["after"] + SERIAL_FLOOR_MAX_JUMP + 1}, reason="implausible floor")
        result["floor"] = {**floor, "tooHigh": {"status": status, "body": body if isinstance(body, dict) else str(body)}}
        activate(admin, spares)
        marks = {c.name: len(c.events) for c in fleet}
        bump_weight(admin, spares[0])
        publication = publish(admin, approver, "the offline topology, signed by the platform again")
        platform_serial = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        result["platform"] = {"serial": platform_serial, "approveToPublishedSec": publication["approveToPublishedSec"]}
        adopted = wait_adopted(fleet, platform_serial, marks, 10 * 60)
        result["platform"].update({"adopted": len(adopted), "samples": adopted,
                                   "online": sum(online_on(c.status(), set(spares)) for c in fleet)})
        result["retire"] = retire(admin, approver, old, "dead access points after the break-glass publication")
    finally:
        for client in fleet:
            client.stop()
        # The same order as the main path: the worker waits for the floor if an offline copy is left.
        cleanup_step(cleanup, "controlPlaneApi", lambda: start_control_plane(("api",)) or "running")
        restart = points_to_restart(stopped, result.get("retire"))
        cleanup_step(cleanup, "accessPoints", lambda: start_access_points(restart) or restart)
        cleanup_step(cleanup, "retireReverted", lambda: revert_retired(admin, result.get("retire")))
        if needs_supersede(uploaded_serial, result.get("platform", {}).get("serial")):
            cleanup_step(cleanup, "offline", lambda: supersede_offline(
                admin, approver, app, uploaded_serial, f"offline manifest {uploaded_serial} left by an interrupted case",
                before_publish=start_control_plane))
        cleanup_step(cleanup, "controlPlane", lambda: start_control_plane() or "running")
        result["cleanup"] = cleanup
    platform = result.get("platform", {})
    floor = result.get("floor", {})
    adopted_offline = result.get("adoptedOffline", {})
    result["checks"] = {
        "offlineAdoptedByAll": adopted_offline.get("adopted") == len(fleet)
        and (adopted_offline.get("maxDelayMs") or 1 << 60) <= RECONNECT_AFTER_PUBLISH_MS,
        "reconnectedWithinLimit": result.get("reconnect", {}).get("pass", False),
        "floorRaisedPastOffline": floor.get("after", 0) >= result.get("offline", {}).get("serial", 1 << 62),
        "floorTooHighRefused": floor.get("tooHigh", {}).get("status") not in (None, 200)
        and "floorTooHigh" in json.dumps(floor.get("tooHigh", {}).get("body")),
        "platformAboveOffline": platform.get("serial", 0) > result.get("offline", {}).get("serial", 1 << 62),
        "platformAdoptedByAll": platform.get("adopted") == len(fleet) and platform.get("online") == len(fleet),
        "deadPointsRetired": bool(result.get("retire", {}).get("retired")),
        "labRestored": not cleanup_failed(cleanup),
    }
    result["pass"] = all(result["checks"].values())
    evidence("r2-break-glass", result)
    log(json.dumps(result["checks"]))


def mirrors_ahead(admin, timeout=180):
    """One mirror audit on demand: the lab mirrors whose check, once run again, reports a copy
    ahead of anything the platform signed."""
    triggered = now_ms()
    run_job(admin, "access_mirror_audit")
    lab_ids = {m.id for m in st.MIRRORS.values()}
    ahead, deadline = {}, time.time() + timeout
    while len(ahead) < len(lab_ids) and time.time() < deadline:
        for mirror in admin.ok("GET", "/platform/access/mirrors"):
            check = mirror.get("lastCheck") or {}
            if mirror["id"] in lab_ids and (check.get("at") or 0) >= triggered \
                    and (check.get("error") or "").startswith("ahead:"):
                ahead[mirror["id"]] = check
        time.sleep(2)
    return ahead


def mirror_serials(app):
    """The access serial each lab mirror serves right now, read anonymously as a client does."""
    found = {}
    for key, site in st.MIRRORS.items():
        status, body, _ = https_get(f"{site.base_url}/{app['bootstrapId']}/{ACCESS_FILE}")
        try:
            found[key] = payload_of(body)["serial"] if status == 200 else None
        except (ValueError, KeyError):
            found[key] = None
    return found


def kept_everywhere(serials_by_mirror, offline_serial):
    """
    >>> kept_everywhere({"a": 201000, "b": 201000}, 201000)
    True
    >>> kept_everywhere({"a": 201000, "b": 1500}, 201000)
    False
    >>> kept_everywhere({}, 201000), kept_everywhere(None, 201000)
    (False, False)
    """
    return bool(serials_by_mirror) and all(value == offline_serial for value in serials_by_mirror.values())


def publish_refused(author, approver, reason):
    """The two-person flow, expected to end failed: the change set as the API reports it."""
    detail = author.ok("POST", "/platform/access/change-sets", reason=reason)
    change_set = detail["id"]
    detail = author.ok("POST", f"/platform/access/change-sets/{change_set}:submit", reason=reason)
    ls.require(detail["status"] == "pending_approval",
               f"submit left {detail['status']}: {json.dumps(detail.get('lint'))[:800]}")
    approver.ok("POST", f"/platform/access/change-sets/{change_set}:approve", reason=reason)
    final = wait_change_set(author, change_set)
    return {"changeSet": change_set, "status": final["status"], "failure": final.get("failure")}


def refused_as_ahead(publication):
    """The platform's publication failed because a mirror holds a higher serial, and for no other
    reason: a lint or signing failure would prove nothing about the mirrors.

    >>> refused_as_ahead({"status": "failed", "failure": "mirrors.ahead:apps=1"})
    True
    >>> refused_as_ahead({"status": "failed", "failure": "mirrors.quorumNotReached:apps=1"})
    False
    >>> refused_as_ahead({"status": "published", "failure": None}), refused_as_ahead(None)
    (False, False)
    """
    publication = publication or {}
    return publication.get("status") == "failed" and (publication.get("failure") or "").startswith("mirrors.ahead")


def command_serial_continuity(_args):
    """A recovery link signed with a reserved serial gives way to the next platform manifest. The
    reverse: an offline manifest whose serial the platform was never told about stays on the
    mirrors. The mirror audit reports the copy ahead, and the next platform publication refuses to
    write over it: the change set fails with `mirrors.ahead`, every mirror still serves the offline
    serial and the client keeps it. Raising the floor past it puts the platform ahead again.

    Needs a platform database initialized by this flare-platform build (see the module docstring)."""
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
    uploaded_serial, repaired, cleanup = None, None, {}
    steps["unannounced"] = {}
    try:
        ls.require(online_wait(client, set(manifest_points(app)), 90), "the reverse-case client did not come online")
        current = manifest_points(app)
        spares = spare_sites(app)
        ls.require(spares, "every lab site is in the manifest; the offline manifest needs a new access point id")
        offline = offline_manifest(app, "unannounced", spares[:1], recovery_serials=recovery_serials(facts))
        facts.setdefault("offlineSerials", []).append(offline["serial"])
        save_facts(facts)
        mark = len(client.events)
        uploaded_serial = offline["serial"]
        put_everywhere(lab, app, offline["file"].read_bytes(), headers={"cache-control": CACHE_CONTROL})
        client.refresh()
        took = wait_adopted([client], offline["serial"], {client.name: mark}, 120)
        ahead = mirrors_ahead(admin)
        bump_weight(admin, current[0])
        refused = publish_refused(admin, approver, "next platform manifest, floor not raised")
        client.refresh()
        steps["unannounced"].update({"offlineSerial": offline["serial"], "adopted": took.get(client.name),
                                     "mirrorAudit": ahead, "refusedPublication": refused,
                                     "mirrorSerials": mirror_serials(app),
                                     "mirrorAuditAfter": mirrors_ahead(admin),
                                     "held": ((client.status() or {}).get("access") or {}).get("serial")})
        # Put right: the floor past the offline serial, then the platform is ahead again.
        floor = admin.ok("POST", "/platform/access/serial-floor", {"floor": offline["serial"]},
                         reason=f"offline manifest {offline['serial']} found on the mirrors")
        mark = len(client.events)
        bump_weight(admin, current[0])
        publication = publish(admin, approver, "after raising the floor")
        repaired = max(r["serial"] for r in serials(admin, publication["changeSet"]) if r["appId"] == app["appId"])
        steps["unannounced"]["repair"] = {"floor": floor, "platformSerial": repaired}
        steps["unannounced"]["repair"]["adopted"] = wait_adopted([client], repaired, {client.name: mark},
                                                                 10 * 60).get(client.name)
    finally:
        client.stop()
        if needs_supersede(uploaded_serial, repaired):
            cleanup_step(cleanup, "offline", lambda: supersede_offline(
                admin, approver, app, uploaded_serial, f"offline manifest {uploaded_serial} left by an interrupted case"))
        steps["unannounced"]["cleanup"] = cleanup
    reverse = steps["unannounced"]
    reverse["checks"] = {
        "offlineAdopted": bool(reverse.get("adopted")),
        "auditAheadOnEveryMirror": len(reverse.get("mirrorAudit", {})) == len(st.MIRRORS),
        "lowerPublicationRefused": refused_as_ahead(reverse.get("refusedPublication")),
        "everyMirrorKeptOffline": kept_everywhere(reverse.get("mirrorSerials"), reverse.get("offlineSerial")),
        "auditStillAhead": len(reverse.get("mirrorAuditAfter", {})) == len(st.MIRRORS),
        "clientKeptOffline": reverse.get("held") is not None and reverse.get("held") == reverse.get("offlineSerial"),
        "repairAdopted": bool((reverse.get("repair") or {}).get("adopted")),
        "labRestored": not cleanup_failed(cleanup),
    }
    reverse["pass"] = all(reverse["checks"].values())
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


def closable_mirrors(sources, mirrors):
    """The lab mirrors behind the manifest's https sources, and the sources no lab mirror serves.
    The case must close every listed mirror, so anything it cannot close stops it. `wellKnown`
    (the Cell's own mirror) is reached through the access points, which are dead in the seed.

    >>> class M:
    ...     def __init__(self, key, base): self.key, self.base_url = key, base
    >>> mirrors = {"a": M("a", "https://cfg-a.x:1/flare/v1"), "b": M("b", "https://cfg-b.x:2/flare/v1")}
    >>> sources = [{"kind": "https", "url": "https://cfg-a.x:1/flare/v1/0123/"}, {"kind": "wellKnown"},
    ...            {"kind": "dohTxt", "name": "_flare.0123.boot.x"}]
    >>> closable_mirrors(sources, mirrors)
    (['a'], [])
    >>> closable_mirrors(sources + [{"kind": "https", "url": "https://other.example/flare/v1/0123/"}], mirrors)
    (['a'], ['https://other.example/flare/v1/0123/'])
    """
    keys, unknown = [], []
    for source in sources:
        if source.get("kind") != "https":
            continue
        url = source.get("url") or ""
        key = next((m.key for m in mirrors.values() if url.startswith(m.base_url.rstrip("/") + "/")), None)
        if key is None:
            unknown.append(url)
        elif key not in keys:
            keys.append(key)
    return keys, unknown


def command_doh_fallback(_args):
    """Every mirror the manifest lists is closed and every access point the client knows is dead.
    The TXT record points at a rescue mirror on a fresh host; the client finds it through the
    resolver the manifest names. The audit reports the record as different while it points away
    and matching once it is put back.

    Needs a platform database initialized by this flare-platform build (see the module docstring)."""
    facts = load_facts()
    app = facts["apps"]["main"]
    admin, approver, _ = people()
    view = admin.ok("GET", "/platform/access/doh-records")
    ls.require(view["configured"] and st.DOH_RESOLVER in view["resolvers"],
               "the platform has no DoH pointer for the lab resolver: restart platform-api, platform-worker and "
               "platform-prober with this access_stack.py (it sets FLARE_ACCESS_DOH_*). Their platform database must "
               "be initialized by this flare-platform build, or they refuse to start: `access_stack.py down "
               f"--destroy` and `up`, or drop {st.PG_DATABASE_PLATFORM} (see access_stack.SCHEMA_REMEDY); now {view}")
    doh, rescue = FakeDoh(), RescueMirror()
    result = {}
    state = ls.load_state()
    closed = []
    client = None
    finished = False
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
        to_close, unknown = closable_mirrors(payload["bootstrap"]["sources"], st.MIRRORS)
        result["mirrorsClosed"] = to_close
        ls.require(not unknown, f"the manifest lists mirrors this case cannot close: {unknown}; "
                                "disable them in the console first")
        for key in to_close:
            site = st.MIRRORS[key]
            closed.append(site)
            st.mirror_stop(state, site)
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
        finished = True
    finally:
        if client:
            client.stop()
        for site in closed:
            if not ls.live(ls.load_state(), site.front):
                st.mirror_start(ls.load_state(), site)
        if not finished:
            # The case broke off: the fake resolver and the rescue mirror still go away, and the
            # error that stopped it is the one that propagates.
            doh.close()
            rescue.close()
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


# ------------------------------------- the server's version gate, for real --
def social_user(app):
    """A Social account of the application, through the real register route (the verification
    code is the one the lab's Social writes to its log, as in live_stack's prove-s5)."""
    run = secrets.token_hex(4)
    login_key, password, device = f"fu-{run}@forced.flare.test", f"Fu#{run}aA1", f"forced-device-{run}"
    headers = {"x-flare-platform": "web"}
    status, raw, _ = request("POST", SOCIAL_API + "/auth/verification-codes", {
        "tenant_id": app["appId"], "login_type": 1, "login_key": login_key, "purpose": 1}, headers)
    ls.require(status == 200, f"verification code: {status} {raw[:300]!r}")
    code_file = ls.path("logs", "verification_codes.log")

    def code():
        if not code_file.exists():
            return None
        for line in reversed(code_file.read_text().splitlines()):
            parts = line.split("\t")
            if len(parts) == 6 and parts[1] == app["appId"] and parts[3] == login_key:
                return parts[5]
        return None
    verification = ls.wait_for(code, 20, "verification code")
    status, raw, _ = request("POST", SOCIAL_API + "/auth/register", {
        "tenant_id": app["appId"], "login_type": 1, "login_key": login_key, "password": password,
        "device_id": device, "platform": "web", "display_name": "Forced update", "verification_code": verification},
        headers)
    ls.require(status == 200, f"register: {status} {raw[:300]!r}")
    data = decoded(raw)["data"]
    return (data.get("tokens") or data)["access_token"]


def social_probe(token, build, path="/users/me/profile"):
    """An authenticated Social write as the desktop shell sends it. (status, body, headers)."""
    headers = {"Authorization": "Bearer " + token, "x-flare-platform": "desktop", "x-flare-app-build": str(build),
               "x-flare-distribution": RELEASE_APP["distribution"]}
    if path == "/users/me/profile":
        headers["Idempotency-Key"] = str(uuid.uuid4())
        status, raw, response = request("PUT", SOCIAL_API + path, {"nickname": "forced update"}, headers)
    else:
        status, raw, response = request("POST", SOCIAL_API + path, {}, headers)
    return status, decoded(raw), response


def rejections_of(metrics_text, reason):
    """`handshake_rejected_total{reason=...}` summed over the series of one gateway's metrics.

    >>> text = "\\n".join(['handshake_rejected_total{reason="below_min_build"} 3',
    ...                    'handshake_rejected_total{reason="token_invalid"} 9'])
    >>> rejections_of(text, "below_min_build"), rejections_of(text, "blocked_build")
    (3, 0)
    """
    total = 0
    for line in metrics_text.splitlines():
        if line.startswith("handshake_rejected_total") and f'reason="{reason}"' in line:
            total += int(float(line.rsplit(" ", 1)[1]))
    return total


def refusals_counted(reason="below_min_build"):
    """Core gateways' handshake_rejected_total{reason}, summed over the gateways that answered."""
    total, reachable = 0, 0
    for _name, gateway in ls.GATEWAYS.items():
        status, raw, _ = request("GET", f"http://127.0.0.1:{gateway[2]}/metrics", None, {})
        if status != 200:
            continue
        reachable += 1
        total += rejections_of(raw.decode(errors="replace"), reason)
    return {"reachableGateways": reachable, "refusals": total}


def gateway_refusal(client):
    """What the gateway's CONNECT refusal carried, as the client recorded it (diagnostics export):
    `{"atMs", "minBuild"}`, or None when no gateway refused it. A manifest that raises the floor
    leaves this empty, so it tells the two ways of learning about an upgrade apart."""
    event = client.ask("diagnostics", 20) or {}
    return ((event.get("data") or {}).get("value") or {}).get("upgradeRefusal")


def upgrade_refused(event):
    """The terminal CONNECT refusal for an upgrade, as the connection status reports it.

    >>> upgrade_refused({"kind": "connection", "data": {"phase": "actionRequired", "reason": "upgradeRequired"}})
    True
    >>> upgrade_refused({"kind": "connection", "data": {"phase": "actionRequired", "reason": "expired"}})
    False
    """
    data = event.get("data") or {}
    return event.get("kind") == "connection" and data.get("phase") == "actionRequired" \
        and data.get("reason") == "upgradeRequired"


def refused_client(app, name, build, seed, attempts=6):
    """A client the real Core gateway refuses at CONNECT with UPGRADE_REQUIRED. The gate reads the
    policy through the tenant runtime cache, so a client that gets in while the cache is still old
    is retried."""
    for _ in range(attempts):
        client = Client(name, app, seed=seed, app_info={**RELEASE_APP, "build": build}, single_runtime=True).start()
        terminal = client.wait(upgrade_refused, 45)
        if terminal:
            return client, terminal
        client.stop()
        time.sleep(10)
    return None, None


def wait_online(client, timeout=90):
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = client.status()
        if online_on(status):
            return status
        time.sleep(1)
    return None


def command_upgrade_required(_args):
    """A client below the floor is refused by the real Core gateway at CONNECT and goes to
    `required` and read-only. The refusal is proven to be the gateway's: terminal reason
    `upgradeRequired`, the floor carried in the refusal, and the gateways' below_min_build
    counter rising. The floor is a policy change proposed
    and approved by two staff members and projected by the platform to Core; the refusal comes
    from the gateway behind the real Edge and SNI router. Policy restored afterwards. Needs `seed`
    on a lab initialized by this build."""
    facts = load_facts()
    app = facts["apps"]["main"]
    rel = release_lab(facts, app)
    rel.tauri_app()
    rel.quiet("tauri", "stable")
    build = rel.next_build("tauri", "stable")
    draft = rel.draft("tauri", "stable", build, stages=[10000])
    rel.package(draft)
    rel.go_live(draft["id"])
    wait_release(app, "tauri", "stable", lambda p: entry_of(p, build) is not None)
    min_build = build - 50
    client_build = build - 100
    seed = seed_file(app)
    result = {"build": build, "minBuild": min_build, "clientBuild": client_build}
    client = None
    try:
        # The platform has one policy per channel and projects the same floor into the signed
        # manifest and into Core: there is no way to raise the gateway's floor alone, and
        # `blockUi` false would still put `minBuild` in the manifest. What proves the gateway did
        # it is therefore what only the gateway produces: the terminal reason with the floor it
        # carried (the client's diagnostics), and the gateways' own refusal counter rising.
        rel.set_policy("tauri", "stable", min_build=min_build, block_ui=True)
        wait_projection(app, "tauri", min_build)
        result["gatewaysBefore"] = refusals_counted()
        client, terminal = refused_client(app, f"upgrade-{now_ms()}", client_build, seed)
        ls.require(client is not None, "the gateway never refused the old build")
        result["connection"] = terminal.get("data")
        failed = client.wait(lambda e: e["kind"] == "loginFailed", 20)
        result["connectError"] = (failed or {}).get("data")
        result["refusal"] = gateway_refusal(client)
        status = (client.ask("update_state", 20) or {}).get("data", {}).get("status")
        result["update"] = status
        result["gateways"] = refusals_counted()
    finally:
        if client:
            client.stop()
        rel.set_policy("tauri", "stable")
        rel.withdraw(draft["id"])
    status = result["update"] or {}
    result["checks"] = {
        "connectRefusedForUpgrade": upgrade_refused({"kind": "connection", "data": result["connection"]}),
        "gatewayCarriedTheFloor": (result.get("refusal") or {}).get("minBuild") == min_build,
        "gatewayCountedTheRefusal": result["gateways"]["refusals"] > result["gatewaysBefore"]["refusals"],
        "stateRequired": status.get("state") == "required",
        "readOnly": status.get("readOnly") is True}
    result["pass"] = all(result["checks"].values())
    evidence("s4-upgrade-required", result)
    log(json.dumps(result["checks"]) + " " + json.dumps(
        {k: status.get(k) for k in ("state", "readOnly", "blockUi", "prompt")}))


def evictions_counted(reason="below_min_build"):
    """Core gateways' connections_evicted_total{reason}, summed over the gateways that answered."""
    total, reachable = 0, 0
    for _name, gateway in ls.GATEWAYS.items():
        status, raw, _ = request("GET", f"http://127.0.0.1:{gateway[2]}/metrics", None, {})
        if status != 200:
            continue
        reachable += 1
        for line in raw.decode(errors="replace").splitlines():
            if line.startswith("connections_evicted_total") and f'reason="{reason}"' in line:
                total += int(float(line.rsplit(" ", 1)[1]))
    return {"reachableGateways": reachable, "evictions": total}


# The evictor's defaults (flare-signaling-gateway build_floor_evictor): a notice of the policy
# change sweeps at once, the periodic sweep runs every 60 s, and one sweep's closes are spread over
# 30 s. A session below the floor is closed at the latest one sweep plus one window after Core
# holds the raised floor.
EVICTION_LIMIT_MS = (60 + 30) * 1000


def command_floor_eviction(_args):
    """A floor raised while an old build is online. The gateway closes that session with the same
    typed UPGRADE_REQUIRED refusal CONNECT sends, the client takes it as terminal (actionRequired,
    upgradeRequired, no reconnect), and connections_evicted_total{below_min_build} rises; a build
    at the floor stays online on its connection. Policy restored afterwards. Needs `seed` on a lab
    initialized by this build."""
    facts = load_facts()
    app = facts["apps"]["main"]
    rel = release_lab(facts, app)
    rel.tauri_app()
    rel.quiet("tauri", "stable")
    first = rel.next_build("tauri", "stable")
    draft = rel.draft("tauri", "stable", first, stages=[10000])
    rel.package(draft)
    rel.go_live(draft["id"])
    wait_release(app, "tauri", "stable", lambda p: entry_of(p, first) is not None)
    seed = seed_file(app)
    result = {"floor": first, "oldBuild": first - 100}
    old = Client(f"evict-old-{now_ms()}", app, seed=seed, app_info={**RELEASE_APP, "build": first - 100},
                 single_runtime=True).start()
    kept = Client(f"evict-kept-{now_ms()}", app, seed=seed, app_info={**RELEASE_APP, "build": first},
                  single_runtime=True).start()
    try:
        ls.require(wait_online(old) and wait_online(kept), "the clients did not come online before the floor")
        kept_mark = len(kept.events)
        result["before"] = evictions_counted()
        rel.set_policy("tauri", "stable", min_build=first, block_ui=True)
        approved = now_ms()
        wait_projection(app, "tauri", first)
        projected = now_ms()
        terminal = old.wait(upgrade_refused, EVICTION_LIMIT_MS / 1000 + 30)
        closed = terminal["ts"] if terminal else None
        result["closed"] = {"approvedToProjectedMs": projected - approved,
                            "projectedToClosedMs": closed - projected if closed else None,
                            "approvedToClosedMs": closed - approved if closed else None,
                            "terminal": (terminal or {}).get("data")}
        result["refusal"] = gateway_refusal(old)
        # Terminal: the old build does not dial again on its own.
        time.sleep(20)
        result["afterTerminal"] = [e["data"].get("phase") for e in old.events
                                   if closed and e["kind"] == "connection" and e["ts"] > closed]
        result["oldUpdate"] = release_summary((old.ask("update_state", 20) or {}).get("data", {}).get("status"))
        status = kept.status()
        result["kept"] = {"online": online_on(status),
                          "phases": [e["data"].get("phase") for e in kept.events[kept_mark:] if e["kind"] == "connection"]}
        result["after"] = evictions_counted()
    finally:
        old.stop()
        kept.stop()
        rel.set_policy("tauri", "stable")
        rel.withdraw(draft["id"])
    closed = result.get("closed") or {}
    result["checks"] = {
        "evictedWithUpgradeRequired": closed.get("terminal") is not None,
        # Measured from the approval: the projection poll (every 2 s) can see Core's row after
        # the gateway already acted on the notice.
        "evictedWithinLimit": closed.get("approvedToClosedMs") is not None
        and closed["approvedToClosedMs"] <= closed["approvedToProjectedMs"] + EVICTION_LIMIT_MS,
        "refusalCarriedTheFloor": (result.get("refusal") or {}).get("minBuild") == first,
        "noReconnectAfterEviction": not any(phase in ("connecting", "online") for phase in result.get("afterTerminal", [])),
        "clientRequiredAndReadOnly": result.get("oldUpdate", {}).get("state") == "required"
        and result.get("oldUpdate", {}).get("readOnly") is True,
        "buildAtFloorStaysOnline": result.get("kept", {}).get("online") is True
        and not any(phase != "online" for phase in result.get("kept", {}).get("phases", [])),
        "gatewayCountedTheEviction": result.get("after", {}).get("evictions", 0)
        > result.get("before", {}).get("evictions", 0),
    }
    result["pass"] = all(result["checks"].values())
    evidence("s7-floor-eviction", result)
    log(json.dumps(result["checks"]) + " " + json.dumps(closed.get("approvedToClosedMs")))


def command_forced_update(_args):
    """The whole forced-update cycle against the real platform and gateways. Build N is on offer;
    a policy proposed by one staff member and approved by another raises the floor to N with
    effectiveFrom now. An old build is refused at CONNECT (UPGRADE_REQUIRED, the client goes
    read-only) and on Social REST with 426 and the documented body; a newer build is published;
    the same client restarts on it and reconnects. Policy restored afterwards. Needs `seed` on a
    lab initialized by this build."""
    facts = load_facts()
    app = facts["apps"]["main"]
    rel = release_lab(facts, app)
    rel.tauri_app()
    rel.quiet("tauri", "stable")
    first = rel.next_build("tauri", "stable")
    draft = rel.draft("tauri", "stable", first, stages=[10000])
    published = [draft["id"]]
    rel.package(draft)
    rel.go_live(draft["id"])
    wait_release(app, "tauri", "stable", lambda p: entry_of(p, first) is not None)
    old_build = first - 100
    seed = seed_file(app)
    name = f"forced-{now_ms()}"
    token = social_user(app)
    result = {"firstBuild": first, "oldBuild": old_build}
    client = upgraded = None
    try:
        # Before the floor: the old build connects and Social answers it.
        before = Client(name, app, seed=seed, app_info={**RELEASE_APP, "build": old_build}, single_runtime=True).start()
        try:
            result["onlineBeforeFloor"] = bool(wait_online(before))
        finally:
            before.stop()
        result["restBeforeFloor"] = social_probe(token, old_build)[0]
        rel.set_policy("tauri", "stable", min_build=first, block_ui=True)
        wait_projection(app, "tauri", first)
        result["gatewaysBefore"] = refusals_counted()
        client, terminal = refused_client(app, name, old_build, seed)
        ls.require(client is not None, "the gateway never refused the old build")
        result["connection"] = terminal.get("data")
        failed = client.wait(lambda e: e["kind"] == "loginFailed", 20)
        result["connectError"] = (failed or {}).get("data")
        result["refusal"] = gateway_refusal(client)
        result["readOnlyUpdate"] = (client.ask("update_state", 20) or {}).get("data", {}).get("status")
        result["gateways"] = refusals_counted()
        status, body, headers = social_probe(token, old_build)
        result["rest"] = {"status": status, "body": body, "contentType": (headers or {}).get("Content-Type")}
        result["restAtFloor"] = social_probe(token, first)[0]
        result["restLogoutExempt"] = social_probe(token, old_build, "/auth/logout")[0]
        # The newer build is published; the refused client learns it is the target.
        newer = first + 10
        draft2 = rel.draft("tauri", "stable", newer, stages=[10000])
        published.append(draft2["id"])
        rel.package(draft2)
        rel.go_live(draft2["id"])
        wait_release(app, "tauri", "stable", lambda p: entry_of(p, newer) is not None)
        checked = release_summary(settled_update(client))
        result["offeredToRefusedClient"] = checked
        # The upgrade: the same installation, now on the newer build.
        client.stop()
        client = None
        upgraded = Client(name, app, seed=seed, app_info={**RELEASE_APP, "build": newer}, single_runtime=True).start()
        online = wait_online(upgraded)
        result["afterUpgrade"] = {"online": bool(online), "connection": (online or {}).get("connection"),
                                  "update": release_summary((upgraded.ask("update_state", 20) or {}).get("data", {}).get("status"))}
        token = social_user(app)
        result["restAfterUpgrade"] = social_probe(token, newer)[0]
    finally:
        for item in (client, upgraded):
            if item:
                item.stop()
        rel.set_policy("tauri", "stable")
        for release_id in published:
            rel.withdraw(release_id)
    body = (result.get("rest") or {}).get("body")
    status = result.get("readOnlyUpdate") or {}
    result["checks"] = {
        "onlineBeforeFloor": result.get("onlineBeforeFloor") is True,
        "restBeforeFloorAllowed": result.get("restBeforeFloor") == 200,
        "connectRefusedForUpgrade": upgrade_refused({"kind": "connection", "data": result.get("connection")}),
        "gatewayCarriedTheFloor": (result.get("refusal") or {}).get("minBuild") == first,
        "gatewayCountedTheRefusal": (result.get("gateways") or {}).get("refusals", 0)
        > (result.get("gatewaysBefore") or {}).get("refusals", 0),
        "clientReadOnly": status.get("state") == "required" and status.get("readOnly") is True,
        "rest426": (result.get("rest") or {}).get("status") == 426,
        "rest426Body": isinstance(body, dict) and body.get("code") == "UPGRADE_REQUIRED"
        and (body.get("data") or {}).get("minBuild") == first and (body.get("data") or {}).get("platform") == "tauri",
        "restAtFloorAllowed": result.get("restAtFloor") == 200,
        "logoutExempt": result.get("restLogoutExempt") not in (426, 0),
        "newerBuildOffered": (result.get("offeredToRefusedClient") or {}).get("releaseBuild") == first + 10,
        "reconnectsAfterUpgrade": (result.get("afterUpgrade") or {}).get("online") is True,
        "restAfterUpgrade": result.get("restAfterUpgrade") == 200,
    }
    result["pass"] = all(result["checks"].values())
    evidence("s5-forced-update", result)
    log(json.dumps(result["checks"]))


# ------------------------------------------- secondary: Service Worker, real browser --
WEB_SITE_PORT = ls.port(43620)
WEB_HOST = f"web.{st.HOST_SUFFIX}"
WEB_ORIGIN = f"https://{WEB_HOST}:{WEB_SITE_PORT}"
# The web app under test, per `web-sw --app`; its node_modules also provide @playwright/test.
WEB_APPS = {"social": ls.ROOT / "flare-social-examples/apps/flare-social-web-app",
            "core": ls.ROOT / "flare-core-examples/apps/flare-core-web-app"}
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


class Browser:
    def __init__(self, profile, web_app):
        env = {**os.environ, "FLARE_WEB_APP_PACKAGE": str(web_app / "package.json"), "FLARE_WEB_SITE": WEB_ORIGIN + "/",
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


def web_register(rel, release, build_id, sha256):
    """The web bundle of a draft release: the platform fetches the asset manifest from the site
    and requires the declared hash. (status, body) of the register call."""
    revision = rel.view(release["id"])["revision"]
    return rel.author.call("POST", f"{rel.base}/releases/{release['id']}/artifacts:register", {
        "revision": revision, "distribution": "web", "artifact": {
            "kind": "web_bundle", "buildId": build_id, "sha256": sha256,
            "urls": [f"{WEB_ORIGIN}/builds/{build_id}/asset-manifest.json"]}})


def web_live(rel, app, build_id, sha256, packs=()):
    """One more web release on offer for everybody: draft, register the bundle, two-person
    approval, publish, and wait until the mirrors carry a manifest that lists it."""
    build = rel.next_build("web", "stable")
    draft = rel.draft("web", "stable", build, packs=packs, stages=[10000])
    status, body = web_register(rel, draft, build_id, sha256)
    ls.require(status == 200, f"register web bundle {build_id}: {status} {fault_of(body)[:400]}")
    rel.go_live(draft["id"])
    wait_release(app, "web", "stable", lambda p: entry_of(p, build) is not None)
    return draft, build


def offered_web(app):
    """Build ids on offer, in manifest order (the first one is what a web client moves to)."""
    _, payload = read_release(app, "web", "stable")
    return [r["artifacts"]["web"]["buildId"] for r in (payload or {}).get("releases", [])
            if not r["rollout"]["halted"] and "web" in r["artifacts"]]


def command_web_sw(args):
    """The update path a browser takes: signed manifests from the mirrors through fetch and CORS,
    the worker installed from a new query string, every open tab reloaded, then the same way back;
    and what a reload has to keep. The web releases are made through the real platform (bundle
    registered by hash, two-person approval, platform-signed manifest); rolling back is halting
    the newest release. Runs on the `forge` application, whose access documents nothing else
    reads, so it can also serve an older access manifest on purpose. Needs `seed` on a lab
    initialized by this build, and the two bundles in ~/.flare-access-lab/web/build-lab-a and
    build-lab-b: `vite build --outDir <dir>` of the web app with VITE_FLARE_WEB_BUILD_ID=lab-a (or
    lab-b), VITE_TENANT_ID=<forge appId> and VITE_FLARE_ACCESS_JSON={"trustMode":"production",
    "bootstrapId":<forge>,"trustRoots":<trust/trust-roots.json>,"seed":<a forge seed>}. The platform
    fetches the site's asset manifests from loopback, which access_stack allows for this one host
    only (FLARE_RELEASE_PRIVATE_FETCH_HOSTS). `--app core` runs the Core reference web app
    (flare-core-examples/apps/flare-core-web-app, with the engine's wasm binding built into
    bindings/wasm/pkg): bundles in core-build-lab-a and core-build-lab-b, built the same way
    except that the tenant variable is VITE_FLARE_TENANT_ID."""
    WEB_BUILD_PREFIX[0] = "core-build-" if args.app == "core" else "build-"
    facts = load_facts()
    app = facts["apps"]["forge"]
    bootstrap = app["bootstrapId"]
    lab = st.Lab()
    key = lab_key("good")
    cert = certify("ok-forge-good", key["publicKey"], "forge-good.cert")
    rel = release_lab(facts, app)
    rel.client_app("web", {"web": {"kind": "web_bundle"}})
    rel.quiet("web", "stable")
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
    release_a, _ = web_live(rel, app, "lab-a", sha_a)
    browser = Browser(profile, WEB_APPS[args.app])
    live = [release_a]
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
        # The platform refuses a bundle whose asset manifest is not the declared one.
        refused_draft = rel.draft("web", "stable", rel.next_build("web", "stable"), stages=[10000])
        refused_status, refused_body = web_register(rel, refused_draft, "lab-b", "0" * 64)
        steps["platformRefusesWrongHash"] = {"status": refused_status, "body": fault_of(refused_body)[:300],
                                             "draft": refused_draft["id"]}
        browser.ask("reload", tab=0)
        steps["reloadAfterDeploy"] = tab_view(browser.tabs()[0], entries)
        put_everywhere(lab, app, newer)
        release_b, _ = web_live(rel, app, "lab-b", sha_b, [pack])
        live.append(release_b)
        steps["offeredAfterB"] = offered_web(app)
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
        # The mirrors fall back to the older, validly signed access manifest. The serial on the
        # page's own disk is what refuses it; the seed in the bundle alone would take it.
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
        # Rollback is halting the newest release: the manifest then lists the previous bundle first.
        rel.halt(release_b["id"])
        wait_release(app, "web", "stable", lambda p: [r["artifacts"]["web"]["buildId"] for r in p["releases"]
                                                       if not r["rollout"]["halted"]][:1] == ["lab-a"])
        steps["offeredAfterRollback"] = offered_web(app)
        steps["rollback"] = web_update_to(browser, entries, "lab-a", 21 * HOUR_MS)
        # A bundle that is not what the manifest declares must not install: registered with the
        # right hash, then the site's file changes under it.
        release_c, _ = web_live(rel, app, "lab-b", sha_b)
        live.append(release_c)
        served = root / "builds" / "lab-b" / "asset-manifest.json"
        original = served.read_bytes()
        served.write_bytes(original + b"\n")
        try:
            browser.ask("clock", offsetMs=28 * HOUR_MS)
            browser.ask("reload", tab=0)
            browser.ask("reload", tab=1)
            index, _ = wait_prompt(browser, entries, 30, "bad prompt")
            if index is not None:
                browser.ask("click", tab=index, action="update")
                time.sleep(6)
            steps["wrongHash"] = {"prompted": index is not None,
                                  "after": [tab_view(t, entries) for t in browser.tabs()]}
        finally:
            served.write_bytes(original)
        # Withdrawn from the manifest, the pack leaves the page's store (it went with the halted release).
        wait_store(browser, lambda s: f"resource/{bootstrap}/web-strings" not in s, 15, "pack withdrawal")
        steps["storeAfterWithdrawal"] = store_facts(web_store(browser), bootstrap)
        steps["logs"] = browser.ask("logs")
        browser.ask("screenshot", tab=0, path=str(web / "last.png"))
    finally:
        browser.quit()
        for release in live:
            rel.withdraw(release["id"])
        delete_everywhere(lab, app, "res/web-strings-1.json")
        put_everywhere(lab, app, genuine)
    update, rollback, wrong = steps["update"], steps["rollback"], steps["wrongHash"]
    adoption, reload_facts = steps["storeAfterAdoption"], steps["storeAfterReload"]
    attempt = steps["storeAfterRollbackAttempt"]
    checks = {
        "platformRefusesWrongHash": steps["platformRefusesWrongHash"]["status"] == 409
        and "error.release.artifact.digestMismatch" in steps["platformRefusesWrongHash"]["body"],
        "newestOfferedFirst": steps["offeredAfterB"][:1] == ["lab-b"],
        "haltOffersPreviousBundle": steps["offeredAfterRollback"][:1] == ["lab-a"],
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
    sub.add_parser("release-rollout").set_defaults(func=command_release_rollout)
    sub.add_parser("forced-update").set_defaults(func=command_forced_update)
    sub.add_parser("floor-eviction").set_defaults(func=command_floor_eviction)
    sub.add_parser("apk-negative").set_defaults(func=command_apk_negative)
    sub.add_parser("recovery").set_defaults(func=command_recovery)
    total = sub.add_parser("total-outage", help="every access point down; new nodes through the emergency publish")
    total.add_argument("--clients", type=int, default=20)
    total.set_defaults(func=command_total_outage)
    glass = sub.add_parser("break-glass", help="access points and control plane down; the offline runbook")
    glass.add_argument("--clients", type=int, default=20)
    glass.set_defaults(func=command_break_glass)
    sub.add_parser("serial-continuity", help="reserved recovery serial; an unannounced serial is not overwritten"
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
