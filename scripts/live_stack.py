#!/usr/bin/env python3
"""Isolated local stack for the engine's live link smoke test (tests/live_stack_smoke.rs).

One Cell with two signaling gateways as two access points of the same Cell, the Core
services a send needs, the Push Proxy, Push Server and Push Worker (offline tasks go to
the development ``outbox`` backend, a Redis Stream the reachability acceptance reads), and
the Social gateway + user service for the idempotency checks (skipped when their binaries
are not built). Everything is owned by this script and isolated from the other local stacks:

- its own infrastructure: PostgreSQL (TimescaleDB image) and Consul containers named
  ``flare-livesmoke-*`` (with a named data volume), Redis and NATS host processes;
- its own port block 41xxx/42xxx/43xxx (checked free before anything starts);
- its own databases inside its own PostgreSQL, its own Redis and NATS (no shared prefix
  or account to collide with);
- binaries straight from the shared ``target/debug`` (nothing is built here), each with the
  per-process environment the shipped single-container manifests give it.

Processes are only ever signalled by recorded PID after checking that PID still runs the
recorded binary; nothing is stopped by name.

  python3 scripts/live_stack.py up            # infra, migrations, Core, Social
  python3 scripts/live_stack.py infra         # infra + migrations only
  python3 scripts/live_stack.py status
  eval "$(python3 scripts/live_stack.py env)"   # variables the smoke test reads
  python3 scripts/live_stack.py ctl kill|term|stop|start|restart <service> [--env K=V ...]
  python3 scripts/live_stack.py prove-s5      # Social idempotency over plain HTTP
  python3 scripts/live_stack.py down [--destroy]  # --destroy also removes containers + volume

Requires podman (or docker), psql, redis-server, nats-server, openssl 3 and the cached
image docker.io/timescale/timescaledb:latest-pg16 + docker.io/hashicorp/consul:1.17
(``--pull never``). Core migrations run through flare-migrate; Social migrations are its
own SQL set (social/migrations), applied with sqlx-cli into the same ``_sqlx_migrations``
ledger its embedded migrator uses.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request

ENGINE = Path(__file__).resolve().parents[1]
ROOT = ENGINE.parent
CORE = ROOT / "flare-core-server"
SOCIAL = ROOT / "flare-social-server"
# Overrides for a second stack on the same machine: its own binaries, containers and port block.
TARGET = Path(os.environ.get("LIVE_STACK_TARGET") or ROOT / "target" / "debug")
RUN = Path(os.environ.get("LIVE_SMOKE_DIR", "~/.flare-livesmoke")).expanduser()
PREFIX = os.environ.get("LIVE_STACK_PREFIX", "flare-livesmoke")
MARKER = f"{PREFIX}/v1"
PORT_SHIFT = int(os.environ.get("LIVE_STACK_PORT_SHIFT", "0"))
RUNNER = shutil.which("podman") or shutil.which("docker") or "podman"
NO_PROXY = "localhost,127.0.0.1,::1"

# ------------------------------------------------------------------ ports --
def port(number):
    """A port of the 41xxx-43xxx block, moved as a whole by LIVE_STACK_PORT_SHIFT."""
    return number + PORT_SHIFT


# Dev defaults -> this stack's block. Applied to config text and URL-like env only.
PORTS = {p: port(p - 9000) for p in [50050, 50051, 50061, 50062, 50090, 50091, 50110, 50181, 50182,
                                     *range(50200, 50211)]}
PORTS.update({p: port(p - 18000) for p in [60051, 60052, 60060, 60081, 60082, 60083, 60084]})
PORTS.update({p: port(p + 24000) for p in range(19180, 19210)})
PORTS.update({25432: port(41432), 26379: port(41379), 24222: port(41422), 28500: port(41500),
              29000: port(41900)})
PG_PORT, REDIS_PORT, NATS_PORT, NATS_HTTP, CONSUL_PORT = map(port, (41432, 41379, 41422, 41423, 41500))
API = port(41050)
SOCIAL_API = port(41200)
GATEWAYS = {
    # name: (ws port (QUIC = ws + 1), gRPC port, metrics port, GATEWAY_ID, GATEWAY_AP_ID)
    # GATEWAY_AP_ID is the access point ID the client knows the gateway by (development mode:
    # `development` for the primary endpoint, then the `accessPoints[].id`); a different
    # reported ID is an access point mismatch the client counts as a failed probe.
    "gw-a": (port(42051), port(42060), port(43551), "m1-gw-a", "development"),
    "gw-b": (port(42151), port(42160), port(43552), "m1-gw-b", "dev-b"),
}
CORE_SERVICES = {
    # name: (binary, supervise-core.toml service name, port waited for)
    "signaling-online": ("flare-signaling-online", "signaling-online", port(41061)),
    "signaling-route": ("flare-signaling-route", "signaling-route", port(41062)),
    "conversation": ("flare-conversation", "conversation", port(41090)),
    "storage-reader": ("flare-storage-reader", "storage-reader", port(42083)),
    "storage-writer": ("flare-storage-writer", "storage-writer", port(43182)),
    "message-ingest": ("flare-message-ingest", "message-ingest", port(41182)),
    "orchestrator": ("flare-orchestrator", "orchestrator", port(41181)),
    "sync-orchestrator": ("flare-sync-orchestrator", "sync-orchestrator", port(42084)),
    "api-gateway": ("flare-api-gateway", "api-gateway", API),
    "gw-a": ("flare-signaling-gateway", "signaling-gateway", GATEWAYS["gw-a"][0]),
    "gw-b": ("flare-signaling-gateway", "signaling-gateway", GATEWAYS["gw-b"][0]),
    # Push Server and Push Worker serve no RPC; their metrics endpoint is the readiness port.
    "push-proxy": ("flare-push-proxy", "push-proxy", port(41091)),
    "push-server": ("flare-push-server", "push-server", port(43193)),
    "push-worker": ("flare-push-worker", "push-worker", port(43186)),
}
SOCIAL_SERVICES = {
    "social-user": ("flare-user", "social-user", port(41201)),
    # Optional: started when built, so a full Social app (contacts, groups, conversation
    # bridge) can sign in against this stack. The worker serves no port (None).
    "social-relation": ("flare-relation", "social-relation", port(41202)),
    "social-group": ("flare-group", "social-group", port(41203)),
    "social-moments": ("flare-moments", "social-moments", port(41204)),
    "social-moderation": ("flare-moderation", "social-moderation", port(41205)),
    "social-control": ("flare-social-control", "social-control", port(41206)),
    "social-hook": ("flare-social-hook", "social-hook", port(41210)),
    "social-worker": ("flare-social-worker", "social-worker", None),
    "social-gateway": ("flare-gateway", "social-gateway", SOCIAL_API),
}
SOCIAL_REQUIRED = ("social-user", "social-gateway")
# LIVE_STACK_LAN_HOST=<this machine's LAN address> relays the client-facing ports (Social API,
# Core HTTP, the primary gateway's WebSocket) from that address to loopback, for a phone on
# the same network. QUIC (UDP) is not relayed: clients there use WebSocket.
LAN_HOST = os.environ.get("LIVE_STACK_LAN_HOST", "")
# Offline push tasks of this stack (push-worker `outbox` backend) in the push Redis profile.
PUSH_REDIS_DB = 5
OUTBOX_STREAM = "flare:im:push:offline:outbox"


def social_enabled():
    """Social runs when its user service and gateway are built (LIVE_STACK_SOCIAL=0 turns it off)."""
    if os.environ.get("LIVE_STACK_SOCIAL", "auto") == "0":
        return False
    return all((TARGET / SOCIAL_SERVICES[name][0]).exists() for name in SOCIAL_REQUIRED)


def social_built(name):
    return (TARGET / SOCIAL_SERVICES[name][0]).exists()
ALL_PORTS = sorted({*PORTS.values(), NATS_HTTP, *(g[0] + 1 for g in GATEWAYS.values()),
                    *(p for g in GATEWAYS.values() for p in g[:3])})
DATABASES = {"core": "flare_livesmoke_core", "social": "flare_livesmoke_social"}


def log(message):
    print(f"[{PREFIX}] {message}", flush=True)


def require(condition, message):
    if not condition:
        raise SystemExit(f"[livesmoke] FAIL: {message}")


def path(*parts):
    return RUN.joinpath(*parts)


def write_private(target, data):
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with open(target, "wb" if isinstance(data, bytes) else "w") as handle:
        handle.write(data)
    os.chmod(target, 0o600)


def load_state():
    file = path("state.json")
    return json.loads(file.read_text()) if file.exists() else {"processes": {}, "containers": []}


def save_state(state):
    write_private(path("state.json"), json.dumps(state, indent=2))


def ensure_run_dir():
    RUN.mkdir(parents=True, exist_ok=True, mode=0o700)
    marker = path("owner")
    if marker.exists():
        require(marker.read_text().strip() == MARKER, f"{RUN} belongs to something else")
    else:
        require(not any(RUN.iterdir()), f"{RUN} is not empty and not ours")
        write_private(marker, MARKER + "\n")
    for sub in ("logs", "secrets", "config", "nats", "redis"):
        path(sub).mkdir(exist_ok=True, mode=0o700)


def port_open(port):
    with socket.socket() as probe:
        probe.settimeout(0.3)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def wait_for(check, timeout, what, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(interval)
    raise SystemExit(f"[livesmoke] FAIL: {what} not ready within {timeout}s")


def run(command, env=None, check=True, input_text=None):
    result = subprocess.run(command, env={**os.environ, **(env or {})}, text=True, capture_output=True,
                            input=input_text)
    if check and result.returncode != 0:
        raise SystemExit(f"[livesmoke] FAIL: {' '.join(map(str, command[:3]))}… exit {result.returncode}\n"
                         f"{result.stdout[-2000:]}\n{result.stderr[-2000:]}")
    return result


def free_gib():
    stat = os.statvfs("/System/Volumes/Data" if Path("/System/Volumes/Data").exists() else "/")
    return stat.f_bavail * stat.f_frsize / (1 << 30)


def disk_guard(what):
    gib = free_gib()
    log(f"disk free before {what}: {gib:.1f} GiB")
    require(gib >= 3.0, f"less than 3 GiB free before {what}; stopping")


# ---------------------------------------------------------------- secrets --
def secret(name, factory):
    file = path("secrets", name)
    if not file.exists():
        write_private(file, factory())
    return file.read_text().strip()


def openssl():
    for candidate in ("/opt/homebrew/opt/openssl@3/bin/openssl", shutil.which("openssl") or ""):
        if candidate and Path(candidate).exists() and run([candidate, "version"]).stdout.startswith("OpenSSL 3"):
            return candidate
    raise SystemExit("[livesmoke] FAIL: OpenSSL 3 required")


class Key:
    """Ed25519 key kept as PKCS#8 PEM in the private run directory."""

    def __init__(self, name):
        self.file = path("secrets", name + ".pem")
        if not self.file.exists():
            write_private(self.file, run([openssl(), "genpkey", "-algorithm", "Ed25519"]).stdout)

    def _out(self, *args):
        return subprocess.run([openssl(), "pkey", "-in", str(self.file), *args], check=True,
                              capture_output=True).stdout

    def pem_b64(self):
        return base64.b64encode(self.file.read_bytes()).decode()

    def public_pem_b64(self):
        return base64.b64encode(self._out("-pubout")).decode()

    def private_der_b64(self):
        return base64.b64encode(self._out("-outform", "DER")).decode()

    def public_der_b64(self):
        return base64.b64encode(self._out("-pubout", "-outform", "DER")).decode()


class Secrets:
    def __init__(self):
        self.pg = secret("postgres-password", lambda: secrets.token_hex(16))
        self.token = secret("im-token-secret", lambda: secrets.token_urlsafe(48))
        self.cell = Key("cell-context")
        self.gateway_ctx = Key("social-gateway-context")
        self.platform = Key("platform")
        self.write_proof = Key("write-proof")
        self.introspection = secret("connect-introspection", lambda: secrets.token_hex(32))
        self.app_root = secret("social-application-root",
                               lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("="))
        self.verification = secret("social-verification-hmac", lambda: secrets.token_urlsafe(48))
        self.pepper = secret("api-key-pepper",
                             lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("="))
        self.tenant = secret("tenant-id", lambda: "e2e-" + secrets.token_hex(10))
        # Push processes run only on a broker that admits authenticated clients
        # (`broker-isolated` provenance), so the stack's NATS requires a password.
        self.nats = secret("nats-password", lambda: secrets.token_hex(16))

    def nats_url(self):
        return f"nats://flare:{self.nats}@127.0.0.1:{NATS_PORT}"

    def db_url(self, key):
        return f"postgres://flare:{self.pg}@127.0.0.1:{PG_PORT}/{DATABASES[key]}"

    def ring(self):
        return json.dumps({"gateway": self.gateway_ctx.public_der_b64(), "legacy": self.cell.public_der_b64()},
                          separators=(",", ":"), sort_keys=True)


# -------------------------------------------------------------- processes --
def process_alive(pid, needle):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    command = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True).stdout
    return needle in command


def spawn(state, name, command, env, cwd=None):
    known = state["processes"].get(name)
    require(not (known and process_alive(known["pid"], known["needle"])), f"{name} already runs (pid {known and known['pid']})")
    log_file = path("logs", f"{name}.log")
    with open(log_file, "ab") as handle:
        handle.write(f"\n===== livesmoke start {name} {time.strftime('%Y-%m-%dT%H:%M:%S')} =====\n".encode())
    handle = open(log_file, "ab")
    environment = {"PATH": os.environ["PATH"], "HOME": os.environ["HOME"], "NO_PROXY": NO_PROXY,
                   "no_proxy": NO_PROXY, "LANG": "en_US.UTF-8", **env}
    process = subprocess.Popen(command, env=environment, cwd=str(cwd or RUN), stdout=handle,
                               stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
    state["processes"][name] = {"pid": process.pid, "needle": command[0], "log": str(log_file),
                                "started_at_ms": int(time.time() * 1000)}
    save_state(state)
    log(f"started {name} pid {process.pid} -> {log_file}")
    return process.pid


def signal_process(state, name, sig, wait=None):
    known = state["processes"].get(name)
    if not known or not process_alive(known["pid"], known["needle"]):
        log(f"{name} is not running")
        return None
    os.kill(known["pid"], sig)
    log(f"sent {signal.Signals(sig).name} to {name} pid {known['pid']}")
    if wait:
        deadline = time.time() + wait
        while time.time() < deadline and process_alive(known["pid"], known["needle"]):
            time.sleep(0.2)
        if process_alive(known["pid"], known["needle"]):
            os.kill(known["pid"], signal.SIGKILL)
            log(f"{name} did not exit within {wait}s; SIGKILL")
    return known["pid"]


# ----------------------------------------------------------- infrastructure --
def container_exists(name):
    return run([RUNNER, "container", "exists", name], check=False).returncode == 0


def container(state, name, image, arguments, command=()):
    full = f"{PREFIX}-{name}"
    if container_exists(full):
        label = run([RUNNER, "inspect", "--format", '{{index .Config.Labels "flare.livesmoke"}}', full]).stdout.strip()
        require(label == MARKER, f"container {full} exists without our label; refusing")
        run([RUNNER, "start", full])
    else:
        run([RUNNER, "run", "-d", "--pull", "never", "--name", full, "--label", f"flare.livesmoke={MARKER}",
             *arguments, image, *command])
    if full not in state["containers"]:
        state["containers"].append(full)
        save_state(state)
    log(f"container {full} up")


def psql(keys, sql, database="postgres", check=True):
    env = {"PGHOST": "127.0.0.1", "PGPORT": str(PG_PORT), "PGUSER": "flare", "PGPASSWORD": keys.pg}
    return run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-At", "-d", database], env=env, check=check,
               input_text=sql).stdout.strip()


def infrastructure(state, keys):
    if not state["processes"] and not state["containers"]:
        busy = [p for p in ALL_PORTS if port_open(p)]
        require(not busy, f"ports of this stack are already in use: {busy}")
    disk_guard("infrastructure")
    container(state, "postgres", "docker.io/timescale/timescaledb:latest-pg16",
              ["-p", f"127.0.0.1:{PG_PORT}:5432", "-e", "POSTGRES_USER=flare",
               "-e", f"POSTGRES_PASSWORD={keys.pg}", "-v", f"{PREFIX}-pgdata:/var/lib/postgresql/data"],
              ["postgres", "-c", "max_connections=300", "-c", "shared_buffers=128MB"])
    if f"{PREFIX}-pgdata" not in state.setdefault("volumes", []):
        state["volumes"].append(f"{PREFIX}-pgdata")
        save_state(state)
    wait_for(lambda: run(["psql", "-X", "-At", "-c", "SELECT 1", "postgres"], check=False,
                         env={"PGHOST": "127.0.0.1", "PGPORT": str(PG_PORT), "PGUSER": "flare",
                              "PGPASSWORD": keys.pg}).returncode == 0, 90, "PostgreSQL")
    before = time.time()
    database_now = float(psql(keys, "SELECT extract(epoch FROM clock_timestamp())"))
    skew = database_now - (before + time.time()) / 2
    log(f"PostgreSQL clock minus host clock: {skew:+.3f}s")
    # ID and runtime leases are fenced on the clocks; the podman VM often runs ~1 s ahead,
    # which the smoke run tolerates. Fix the host clock (sudo sntp -sS time.apple.com) beyond 2 s.
    if abs(skew) >= 1.0:
        log("WARNING: host and PostgreSQL clocks differ by 1 s or more")
    require(abs(skew) < 2.0, "host and PostgreSQL clocks differ by 2 s or more (ID leases are fenced)")
    container(state, "consul", "docker.io/hashicorp/consul:1.17", ["-p", f"127.0.0.1:{CONSUL_PORT}:8500"],
              ["agent", "-dev", "-client=0.0.0.0"])
    wait_for(lambda: http("GET", f"http://127.0.0.1:{CONSUL_PORT}/v1/status/leader")[0] == 200, 60, "Consul")
    if not live(state, "redis"):
        spawn(state, "redis", [shutil.which("redis-server"), "--bind", "127.0.0.1", "--port", str(REDIS_PORT),
                               "--save", "", "--appendonly", "no", "--dir", str(path("redis"))], {})
    wait_for(lambda: port_open(REDIS_PORT), 20, "Redis")
    if not live(state, "nats"):
        config = path("nats", "nats.conf")
        write_private(config, json.dumps({
            "host": "127.0.0.1", "port": NATS_PORT, "http_port": NATS_HTTP,
            "authorization": {"user": "flare", "password": keys.nats},
            # Streams reserve their max_bytes up front (accounting, not disk); services
            # create their own DLQ streams with deployment-sized limits.
            "jetstream": {"store_dir": str(path("nats", "jetstream")), "max_file_store": 8 << 30,
                          "max_memory_store": 64 << 20}}, indent=2))
        spawn(state, "nats", [shutil.which("nats-server"), "-c", str(config)], {})
    wait_for(lambda: port_open(NATS_PORT), 20, "NATS")


def live(state, name):
    known = state["processes"].get(name)
    return bool(known and process_alive(known["pid"], known["needle"]))


def http(method, url, body=None, headers=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"content-type": "application/json", **(headers or {})})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as error:
        return error.code, error.read(), dict(error.headers)
    except OSError as error:
        return 0, str(error).encode(), {}


# ------------------------------------------------------------- migrations --
def databases(state, keys):
    if state.get("migrated"):
        log("databases already migrated")
        return
    disk_guard("database creation")
    social = social_enabled()
    for key, name in DATABASES.items():
        if key == "social" and not social:
            continue
        if psql(keys, f"SELECT 1 FROM pg_database WHERE datname='{name}'") != "1":
            psql(keys, f"CREATE DATABASE {name} TEMPLATE template0 ENCODING 'UTF8'")
            psql(keys, f"COMMENT ON DATABASE {name} IS '{MARKER}'")
    evidence = path("logs", "migrations.log")
    lines = []
    core_env = {"DATABASE_URL": keys.db_url("core")}
    # The working tree's own migration set first: the gate compares the result with the
    # reviewed latest schema profile.
    tree = run(["python3", str(CORE / "tools/flare-migrate/flare_migrate.py"), "run"], env=core_env, check=False)
    lines.append(f"$ flare-migrate run (working tree)\nexit {tree.returncode}\n{tree.stdout}{tree.stderr}")
    if tree.returncode != 0:
        # Untracked migrations of other work in progress (0021+) are not in the reviewed
        # profiles yet: run the committed set through the gate, then apply the rest.
        tracked = run(["git", "-C", str(CORE), "ls-files", "migrations"]).stdout.split()
        staging = path("core-migrate")
        shutil.rmtree(staging, ignore_errors=True)
        (staging / "migrations").mkdir(parents=True)
        shutil.copytree(CORE / "tools/flare-migrate", staging / "tools/flare-migrate")
        for name in tracked:
            shutil.copy2(CORE / name, staging / name)
        gated = run(["python3", str(staging / "tools/flare-migrate/flare_migrate.py"), "run"], env=core_env)
        lines.append(f"$ flare-migrate run (committed {len(tracked)} migrations)\n{gated.stdout}{gated.stderr}")
        verify = run(["python3", str(staging / "tools/flare-migrate/flare_migrate.py"), "verify"], env=core_env)
        lines.append(f"$ flare-migrate verify\n{verify.stdout}{verify.stderr}")
        extra = sorted(p for p in (CORE / "migrations").glob("*.sql") if f"migrations/{p.name}" not in tracked)
        for file in extra:
            psql(keys, "BEGIN;\n" + file.read_text() + "\nCOMMIT;\n", DATABASES["core"])
            lines.append(f"$ psql (untracked work-in-progress migration) {file.name}: applied")
    psql(keys, f"""INSERT INTO tenants (tenant_id, name, status, config, quota, version)
        VALUES ('{keys.tenant}', 'm1-live-smoke', 'active', '{{"fixture":"m1-live-smoke"}}', '{{}}', 1)
        ON CONFLICT (tenant_id) DO NOTHING;""", DATABASES["core"])
    if social:
        migrated = run([shutil.which("sqlx") or "sqlx", "migrate", "run", "--source",
                        str(SOCIAL / "social/migrations"), "--database-url", keys.db_url("social")])
        lines.append(f"$ sqlx migrate run --source social/migrations\n{migrated.stdout}{migrated.stderr}")
        psql(keys, f"""INSERT INTO flare_tenants (tenant_id, name, status, settings, quota, version)
            VALUES ('{keys.tenant}', 'm1-live-smoke', 1, '{{"fixture":"m1-live-smoke"}}', '{{}}', 1)
            ON CONFLICT (tenant_id) DO NOTHING;""", DATABASES["social"])
        lines.append(f"tenant {keys.tenant} inserted into Core tenants and Social flare_tenants")
    else:
        lines.append(f"tenant {keys.tenant} inserted into Core tenants (Social not built: skipped)")
    write_private(evidence, "\n".join(lines) + "\n")
    print("\n".join(lines))
    state["migrated"] = True
    save_state(state)


# ---------------------------------------------------------- configuration --
def remap(text):
    return re.sub(r"\b(?:" + "|".join(map(str, PORTS)) + r")\b", lambda m: str(PORTS[int(m[0])]), text)


def configuration():
    target = path("config")
    for source in (CORE / "config").rglob("*.toml"):
        dest = target / source.relative_to(CORE / "config")
        dest.parent.mkdir(exist_ok=True, parents=True)
        text = remap(source.read_text()).replace('"0.0.0.0"', '"127.0.0.1"')
        text = re.sub(r"max_connections = \d+", "max_connections = 6", text)
        text = re.sub(r"min_connections = \d+", "min_connections = 1", text)
        # Stream reservations sized for a smoke run, not for a deployment.
        text = text.replace('max_bytes = "1073741824"', 'max_bytes = "67108864"')
        dest.write_text(text)
    text = remap((SOCIAL / "config/social.example.toml").read_text()).replace("0.0.0.0:", "127.0.0.1:")
    text = text.replace('delivery = "smtp"', 'delivery = "file"\nfile_path = "logs/verification_codes.log"')
    (target / "social.toml").write_text(text)


def manifest_env(manifest_path, service_name, environment):
    """The shipped manifest's per-process environment (inherited names, bindings, fixed values)."""
    manifest = tomllib.loads(manifest_path.read_text())
    service = next(s for s in manifest["services"] if s["name"] == service_name)
    names = set(manifest.get("shared_env", []) + service.get("inherit_env", []))
    prefixes = tuple(manifest.get("shared_env_prefixes", []) + service.get("inherit_env_prefixes", []))
    result = {k: v for k, v in environment.items() if k in names or (prefixes and k.startswith(prefixes))}
    for target, source in service.get("environment_bindings", {}).items():
        require(source in environment, f"missing {service_name} binding {source}")
        result[target] = environment[source]
    for conditional in service.get("environment_defaults", []):
        if environment.get(conditional["when_env"]) == conditional["equals"]:
            for key, value in conditional.get("values", {}).items():
                result.setdefault(key, remap(value))
    result.update({k: remap(v).replace("0.0.0.0", "127.0.0.1") for k, v in service.get("environment", {}).items()})
    for key in manifest.get("forbidden_env", []):
        require(key not in result, f"forbidden {service_name} credential {key}")
    return result


def core_environment(keys):
    hook = f"http://127.0.0.1:{SOCIAL_API}/internal/identity/connect"
    env = {
        "FLARE_CONFIG_PATH": str(path("config")), "FLARE_ENV": "development", "FLARE_MQ_DEFAULT_BACKEND": "nats",
        "FLARE_POSTGRES_URL": keys.db_url("core"), "FLARE_REDIS_URL_BASE": f"redis://127.0.0.1:{REDIS_PORT}",
        "FLARE_NATS_URL": keys.nats_url(), "FLARE_CONSUL_ENDPOINT": f"http://127.0.0.1:{CONSUL_PORT}",
        "SERVICE_ADVERTISE_HOST": "127.0.0.1", "CONSUL_DISCOVERY_REFRESH_INTERVAL": "10",
        "CONSUL_DISCOVER_CACHE_TTL_SECS": "5", "RUST_LOG": "info,async_nats=warn",
        "ACCESS_GATEWAY_TOKEN_SECRET": keys.token, "FLARE_API_GATEWAY_TOKEN_SECRET": keys.token,
        "FLARE_INTERNAL_CTX_PRIVATE_KEY": keys.cell.pem_b64(),
        "FLARE_INTERNAL_CTX_PUBLIC_KEY": keys.cell.public_pem_b64(),
        "FLARE_INTERNAL_CTX_PUBLIC_KEYS": keys.ring(),
        "FLARE_WRITE_PROOF_PRIVATE_KEY": keys.write_proof.private_der_b64(),
        "FLARE_WRITE_PROOF_PUBLIC_KEY": keys.write_proof.public_der_b64(),
        "MESSAGE_INGEST_HOOKS_CONFIG": str(path("config", "hooks.core.toml")),
        "ONLINE_SERVICE_ENDPOINT": f"http://127.0.0.1:{port(41061)}",
        "ROUTE_SERVICE_ENDPOINT": f"http://127.0.0.1:{port(41062)}",
        "CONVERSATION_STORAGE_READER_SERVICE_URL": f"http://127.0.0.1:{port(42083)}",
        "FLARE_API_GATEWAY_SERVER_BIND": "127.0.0.1", "FLARE_API_GATEWAY_SERVER_PORT": str(API),
        "FLARE_API_GATEWAY_GRPC_MESSAGE_INGEST_SERVICE_URL": f"http://127.0.0.1:{port(41182)}",
        "FLARE_API_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_SERVICE_URL": f"http://127.0.0.1:{port(41181)}",
        "FLARE_API_GATEWAY_GRPC_CONVERSATION_SERVICE_URL": f"http://127.0.0.1:{port(41090)}",
        "FLARE_API_GATEWAY_GRPC_ONLINE_SERVICE_URL": f"http://127.0.0.1:{port(41061)}",
        "FLARE_API_GATEWAY_GRPC_STORAGE_READER_SERVICE_URL": f"http://127.0.0.1:{port(42083)}",
        "FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL": f"http://127.0.0.1:{port(41091)}",
        "SIGNALING_ONLINE_PUSH_PROXY_URL": f"http://127.0.0.1:{port(41091)}",
        **{f"{role}_EVENT_PROVENANCE_MODE": "broker-isolated" for role in ("PUSH_PROXY", "PUSH_SERVER", "PUSH_WORKER")},
        # Offline tasks land in a Redis Stream instead of a vendor: what the acceptance asserts.
        "PUSH_WORKER_OFFLINE_DELIVERY_BACKEND": "outbox",
        "PUSH_WORKER_OFFLINE_REDIS_URL": f"redis://127.0.0.1:{REDIS_PORT}/{PUSH_REDIS_DB}",
        "PUSH_WORKER_OFFLINE_OUTBOX_STREAM": OUTBOX_STREAM,
        # Both Core gateways verify Core-signed tokens locally (core_jwt) and fall back to
        # the Social identity hook for Social application tokens (chained validator).
        **{f"{prefix}_AUTH_{name}": value for prefix in ("ACCESS_GATEWAY", "FLARE_API_GATEWAY")
           for name, value in (("MODE", "core_jwt"), ("HOOK_URL", hook), ("HOOK_TIMEOUT_MS", "3000"),
                               ("HOOK_SECRET_HEADER", "x-flare-auth-hook-secret"),
                               ("HOOK_SECRET", keys.introspection))},
        "TOKIO_WORKER_THREADS": "2",
    }
    return env


def gateway_overrides(name):
    ws, grpc, metrics, gateway_id, ap_id = GATEWAYS[name]
    return {"PORT": str(ws), "GRPC_PORT": str(grpc), "GATEWAY_ID": gateway_id, "GATEWAY_AP_ID": ap_id,
            "ACCESS_GATEWAY_METRICS_ADDRESS": "127.0.0.1", "ACCESS_GATEWAY_METRICS_PORT": str(metrics)}


def social_environment(keys):
    return {
        "SOCIAL_POSTGRES_URL": keys.db_url("social"), "SOCIAL_POSTGRES_MAX_CONNECTIONS": "6",
        "SOCIAL_CONSUL_ENDPOINT": f"http://127.0.0.1:{CONSUL_PORT}", "SERVICE_ADVERTISE_HOST": "127.0.0.1",
        "SOCIAL_APPLICATION_SIGNING_ROOT": keys.app_root, "SOCIAL_VERIFICATION_HMAC_KEY": keys.verification,
        "SOCIAL_CELL_CTX_PRIVATE_KEY": keys.cell.pem_b64(), "SOCIAL_GATEWAY_CTX_PRIVATE_KEY": keys.gateway_ctx.pem_b64(),
        "FLARE_INTERNAL_CTX_PUBLIC_KEYS": keys.ring(), "FLARE_PLATFORM_PUBLIC_KEY": keys.platform.public_pem_b64(),
        "FLARE_CONNECT_INTROSPECTION_SECRET": keys.introspection, "FLARE_API_KEY_PEPPER": keys.pepper,
        "FLARE_CONVERSATION_GRPC": f"http://127.0.0.1:{port(41090)}",
        "FLARE_ORCHESTRATOR_GRPC": f"http://127.0.0.1:{port(41181)}",
        "FLARE_MESSAGE_INGEST_GRPC": f"http://127.0.0.1:{port(41182)}",
        "RUST_LOG": "info,async_nats=warn",
    }


def service_command(keys, name, extra=None):
    """(argv, env) for one service, from the shipped manifest plus this stack's values."""
    state_overrides = dict(extra or {})
    if name in CORE_SERVICES:
        binary, manifest_name, _ = CORE_SERVICES[name]
        env = manifest_env(CORE / "release/supervise-core.toml", manifest_name, core_environment(keys))
        if name == "api-gateway":
            # SDK-managed tokens (`auth: {}`) need the development issuer of the API gateway.
            env["FLARE_API_GATEWAY_AUTH_DEV_ISSUE"] = "true"
        if name in GATEWAYS:
            env.update(gateway_overrides(name))
        env["TOKIO_WORKER_THREADS"] = "2"
    else:
        binary, manifest_name, _ = SOCIAL_SERVICES[name]
        env = manifest_env(SOCIAL / "deploy/supervise-social.toml", manifest_name, social_environment(keys))
    for key, value in state_overrides.items():
        # `@public-pem:<key>`: the PEM text of one of this stack's public keys (for a
        # binary built before its key input moved to Base64 DER).
        if value.startswith("@public-pem:"):
            value = Key(value.split(":", 1)[1])._out("-pubout").decode()
        env[key] = value
    binary_path = TARGET / binary
    require(binary_path.exists(), f"missing binary {binary_path}")
    return [str(binary_path)], env


def start_service(state, keys, name, extra=None):
    command, env = service_command(keys, name, extra)
    spawn(state, name, command, env)
    state["processes"][name]["overrides"] = dict(extra or {})
    save_state(state)


def wait_service(state, name, timeout=90):
    port = (CORE_SERVICES.get(name) or SOCIAL_SERVICES.get(name))[2]
    known = state["processes"][name]

    def alive():
        require(process_alive(known["pid"], known["needle"]),
                f"{name} exited during startup; see {known['log']}")
        return True
    if port is None:
        # No port to wait for: it counts as started when it is still running a few seconds later.
        time.sleep(3)
        alive()
        log(f"{name} running")
        return
    wait_for(lambda: alive() and port_open(port), timeout, f"{name} :{port}")
    log(f"{name} listening on {port}")


def backends(state, keys):
    configuration()
    order = ["push-proxy", "signaling-online", "signaling-route", "conversation", "storage-reader",
             "storage-writer", "message-ingest", "orchestrator", "sync-orchestrator", "push-server",
             "push-worker", "api-gateway", *SOCIAL_SERVICES, "gw-a", "gw-b"]
    if not social_enabled():
        log("Social binaries are not built (or LIVE_STACK_SOCIAL=0): Social services skipped")
        order = [name for name in order if name not in SOCIAL_SERVICES]
    else:
        skipped = [name for name in SOCIAL_SERVICES if not social_built(name)]
        if skipped:
            log(f"Social services not built, skipped: {' '.join(skipped)}")
        order = [name for name in order if name not in skipped]
    for name in order:
        if live(state, name):
            log(f"{name} already running")
            continue
        start_service(state, keys, name)
        if name == "storage-writer" and not writer_accepts_der(state):
            # A Storage Writer built from a tree that still reads the PEM key (HEAD before the
            # Base64 DER key inputs) refuses the DER public key at startup; give it the same
            # key as PEM.
            log("storage-writer refused the Base64 DER write proof key; restarting it with the PEM form")
            start_service(state, keys, name, {"FLARE_WRITE_PROOF_PUBLIC_KEY": "@public-pem:write-proof"})
        wait_service(state, name)
    default_port_bridge(state)
    lan_bridge(state)


# Some calls go only to the single-Cell default address and never through discovery: the
# Orchestrator and Message Ingest reach the Conversation service and the Storage Reader at
# their fixed ports (`default_static_grpc_fallback`). Without these the Orchestrator refuses
# every read receipt and recall (membership lookup unavailable). Bridged on loopback only,
# only while the default port is free.
DEFAULT_PORT_BRIDGES = {50090: port(41090), 60083: port(42083)}

BRIDGE_SCRIPT = """\
import asyncio, sys

async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except OSError:
        pass
    finally:
        writer.close()

async def handle(target, client_reader, client_writer):
    try:
        server_reader, server_writer = await asyncio.open_connection("127.0.0.1", target)
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(pipe(client_reader, server_writer), pipe(server_reader, client_writer))

async def main():
    servers = []
    host, pairs = "127.0.0.1", sys.argv[1:]
    if pairs and pairs[0].startswith("--host="):
        host, pairs = pairs[0].split("=", 1)[1], pairs[1:]
    for pair in pairs:
        listen, target = map(int, pair.split(":"))
        servers.append(await asyncio.start_server(
            lambda r, w, t=target: handle(t, r, w), host, listen))
    await asyncio.gather(*(server.serve_forever() for server in servers))

asyncio.run(main())
"""


def default_port_bridge(state):
    if live(state, "default-bridge"):
        return
    pairs = []
    for default, actual in DEFAULT_PORT_BRIDGES.items():
        if port_open(default):
            log(f"default port {default} is in use: calls that only use it stay unanswered here")
        else:
            pairs.append(f"{default}:{actual}")
    if not pairs:
        return
    script = path("default_bridge.py")
    write_private(script, BRIDGE_SCRIPT)
    spawn(state, "default-bridge", [sys.executable, "-I", str(script), *pairs], {})
    # A framework Python runs under another executable path than sys.executable: recognise
    # the bridge by its script, so status and down find it.
    state["processes"]["default-bridge"]["needle"] = str(script)
    save_state(state)
    wait_for(lambda: all(port_open(int(pair.split(":")[0])) for pair in pairs), 10, "default port bridge")
    log(f"default port bridge {' '.join(pairs)}")


def lan_bridge(state):
    if not LAN_HOST or live(state, "lan-bridge"):
        return
    ports = [SOCIAL_API, API, GATEWAYS["gw-a"][0]] if social_enabled() else [API, GATEWAYS["gw-a"][0]]
    script = path("lan_bridge.py")
    write_private(script, BRIDGE_SCRIPT)
    spawn(state, "lan-bridge", [sys.executable, "-I", str(script), f"--host={LAN_HOST}",
                                *(f"{p}:{p}" for p in ports)], {})
    state["processes"]["lan-bridge"]["needle"] = str(script)
    save_state(state)

    def reachable():
        try:
            for p in ports:
                socket.create_connection((LAN_HOST, p), timeout=1).close()
            return True
        except OSError:
            return False
    wait_for(reachable, 10, "LAN bridge")
    log(f"LAN bridge on {LAN_HOST}: {' '.join(map(str, ports))}")


def writer_accepts_der(state):
    known = state["processes"]["storage-writer"]
    deadline = time.time() + 15
    while time.time() < deadline:
        if not process_alive(known["pid"], known["needle"]):
            tail = Path(known["log"]).read_text(errors="replace")[-2000:]
            return "invalid write proof public key" not in tail
        if port_open(CORE_SERVICES["storage-writer"][2]):
            return True
        time.sleep(0.3)
    return True


# ---------------------------------------------------------------- commands --
def command_up(_args):
    ensure_run_dir()
    state = load_state()
    keys = Secrets()
    infrastructure(state, keys)
    databases(state, keys)
    backends(state, keys)
    command_env(None)


def command_infra(_args):
    ensure_run_dir()
    state = load_state()
    keys = Secrets()
    infrastructure(state, keys)
    databases(state, keys)


def command_env(_args):
    keys = Secrets()
    values = {
        "LIVE_SMOKE_WS_A": f"ws://127.0.0.1:{GATEWAYS['gw-a'][0]}",
        "LIVE_SMOKE_WS_B": f"ws://127.0.0.1:{GATEWAYS['gw-b'][0]}",
        "LIVE_SMOKE_CORE_API": f"http://127.0.0.1:{API}",
        "LIVE_SMOKE_SOCIAL_API": f"http://127.0.0.1:{SOCIAL_API}",
        "LIVE_SMOKE_TENANT": keys.tenant,
        "LIVE_SMOKE_CTL": f"python3 {Path(__file__).resolve()}",
        "LIVE_SMOKE_DIR": str(RUN),
        "LIVE_SMOKE_CORE_DB": keys.db_url("core"),
        "LIVE_SMOKE_SOCIAL_DB": keys.db_url("social"),
        # Push Redis (device bindings, decision trace, offline outbox) without credentials:
        # host process bound to loopback.
        "LIVE_SMOKE_PUSH_REDIS": f"127.0.0.1:{REDIS_PORT}/{PUSH_REDIS_DB}",
        "LIVE_SMOKE_PUSH_OUTBOX": OUTBOX_STREAM,
    }
    for key, value in values.items():
        print(f"export {key}='{value}'")


def command_status(_args):
    state = load_state()
    report = {name: {"pid": p["pid"], "alive": process_alive(p["pid"], p["needle"]),
                     "overrides": p.get("overrides", {})}
              for name, p in state["processes"].items()}
    containers = {}
    for name in state["containers"]:
        result = run([RUNNER, "inspect", "--format", "{{.State.Status}}", name], check=False)
        containers[name] = result.stdout.strip() or "absent"
    print(json.dumps({"dir": str(RUN), "processes": report, "containers": containers}, indent=2))


def command_ctl(args):
    state = load_state()
    keys = Secrets()
    name = args.service
    require(name in CORE_SERVICES or name in SOCIAL_SERVICES or name in ("redis", "nats"), f"unknown {name}")
    extra = dict(item.split("=", 1) for item in args.env or [])
    if args.action == "kill":
        signal_process(state, name, signal.SIGKILL)
    elif args.action == "term":
        signal_process(state, name, signal.SIGTERM)
    elif args.action == "stop":
        signal_process(state, name, signal.SIGTERM, wait=30)
    elif args.action in ("start", "restart"):
        if args.action == "restart":
            signal_process(state, name, signal.SIGTERM, wait=30)
        known = state["processes"].get(name)
        require(not (known and process_alive(known["pid"], known["needle"])), f"{name} still running")
        if not path("config", "base.toml").exists():
            configuration()
        start_service(state, keys, name, extra)
        wait_service(state, name)


def command_down(args):
    state = load_state()
    for name in list(state["processes"])[::-1]:
        signal_process(state, name, signal.SIGTERM, wait=30)
        state["processes"].pop(name, None)
        save_state(state)
    for name in state["containers"]:
        if not container_exists(name):
            continue
        label = run([RUNNER, "inspect", "--format", '{{index .Config.Labels "flare.livesmoke"}}', name]).stdout.strip()
        require(label == MARKER, f"container {name} is not ours")
        run([RUNNER, "stop", "-t", "10", name], check=False)
        if args.destroy:
            run([RUNNER, "rm", "-v", name])
            log(f"removed container {name}")
    if args.destroy:
        for volume in state.get("volumes", []):
            if run([RUNNER, "volume", "exists", volume], check=False).returncode == 0:
                run([RUNNER, "volume", "rm", volume])
                log(f"removed volume {volume}")
        state["containers"], state["volumes"], state["migrated"] = [], [], False
        save_state(state)
        shutil.rmtree(path("nats", "jetstream"), ignore_errors=True)
        shutil.rmtree(path("core-migrate"), ignore_errors=True)


def command_prove_s5(_args):
    """Social Client-partition idempotency over plain HTTP (what curl would send)."""
    keys = Secrets()
    base = f"http://127.0.0.1:{SOCIAL_API}/api/v1"
    run_id = f"{int(time.time() * 1000):x}"
    login_key, password = f"smoke-{run_id}@smoke.flare.test", f"Smoke#{run_id}aA"
    device = f"smoke-device-{run_id}"
    results = {}

    def call(method, route, body=None, token=None, key=None):
        headers = {"x-flare-platform": "web"}
        if token:
            headers["Authorization"] = "Bearer " + token
        if key:
            headers["Idempotency-Key"] = key
        status, raw, response_headers = http(method, base + route, body, headers)
        try:
            payload = json.loads(raw or b"null")
        except ValueError:
            payload = raw.decode(errors="replace")
        replayed = {k.lower(): v for k, v in response_headers.items()}.get("idempotent-replayed")
        return status, payload, replayed, raw

    status, payload, _, _ = call("POST", "/auth/verification-codes",
                                 {"tenant_id": keys.tenant, "login_type": 1, "login_key": login_key, "purpose": 1})
    require(status == 200, f"verification code: {status} {payload}")
    code_file = path("logs", "verification_codes.log")

    def code():
        if not code_file.exists():
            return None
        for line in reversed(code_file.read_text().splitlines()):
            parts = line.split("\t")
            if len(parts) == 6 and parts[1] == keys.tenant and parts[3] == login_key:
                return parts[5]
        return None
    verification = wait_for(code, 20, "verification code")
    register = {"tenant_id": keys.tenant, "login_type": 1, "login_key": login_key, "password": password,
                "device_id": device, "platform": "web", "display_name": "M1 S5 HTTP",
                "verification_code": verification}
    status, payload, _, _ = call("POST", "/auth/register", register)
    require(status == 200, f"register: {status} {payload}")
    data = payload["data"]
    tokens = data.get("tokens", data)
    token, user = tokens["access_token"], data.get("user_id") or tokens.get("user_id")
    login = {k: register[k] for k in ("tenant_id", "login_type", "login_key", "password", "device_id", "platform")}

    # 1. Authentication route with a key: refused, nothing executed.
    status, payload, _, _ = call("POST", "/auth/login", login, key="m1-auth-" + run_id)
    results["authWithKey"] = {"status": status, "reason": payload.get("reason") if isinstance(payload, dict) else payload}
    # 2. Signed-in write without a key.
    status, payload, _, _ = call("PUT", "/users/me/profile", {"nickname": "no key"}, token=token)
    results["signedInWriteWithoutKey"] = {"status": status,
                                          "reason": payload.get("reason") if isinstance(payload, dict) else payload}

    def profile_row():
        return psql(keys, f"SELECT nickname || '|' || profile_version || '|' || updated_at FROM flare_user_profiles "
                          f"WHERE tenant_id = '{keys.tenant}' AND user_id = '{user}'", DATABASES["social"])
    before = profile_row()
    key = "m1-s5-" + run_id
    first = call("PUT", "/users/me/profile", {"nickname": "s5 first", "signature": "m1"}, token=token, key=key)
    after_first = profile_row()
    # Same key, same request, JSON keys in another order: replayed from the record.
    second = call("PUT", "/users/me/profile", {"signature": "m1", "nickname": "s5 first"}, token=token, key=key)
    after_second = profile_row()
    record = psql(keys, f"SELECT state || '|' || status || '|' || length(body) FROM gateway.client_idempotency "
                        f"WHERE tenant_id = '{keys.tenant}' AND user_id = '{user}' AND idem_key = '{key}'",
                  DATABASES["social"])
    results["keyedWrite"] = {
        "first": {"status": first[0], "replayed": first[2]},
        "second": {"status": second[0], "replayed": second[2], "sameBytes": first[3] == second[3]},
        "profile": {"before": before, "afterFirst": after_first, "afterReplay": after_second},
        "record": record,
    }
    results["userId"], results["tenant"] = user, keys.tenant
    print("EVIDENCE " + json.dumps({"case": "S5-http", "result": results}, ensure_ascii=False))
    require(results["authWithKey"]["status"] == 400
            and results["authWithKey"]["reason"] == "IDEMPOTENCY_NOT_SUPPORTED", "auth route with a key")
    require(results["signedInWriteWithoutKey"]["status"] == 400
            and results["signedInWriteWithoutKey"]["reason"] == "IDEMPOTENCY_KEY_REQUIRED", "write without a key")
    require(first[0] == 200 and first[2] is None, "first keyed write executes")
    require(second[0] == 200 and second[2] == "true" and first[3] == second[3], "replay carries the header")
    require(after_first == after_second and after_first != before, "the business write ran once")
    require(record.startswith("1|200|"), "result stored")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up").set_defaults(func=command_up)
    sub.add_parser("infra").set_defaults(func=command_infra)
    sub.add_parser("env").set_defaults(func=command_env)
    sub.add_parser("status").set_defaults(func=command_status)
    sub.add_parser("prove-s5").set_defaults(func=command_prove_s5)
    ctl = sub.add_parser("ctl")
    ctl.add_argument("action", choices=["kill", "term", "stop", "start", "restart"])
    ctl.add_argument("service")
    ctl.add_argument("--env", action="append", help="K=V override for start/restart")
    ctl.set_defaults(func=command_ctl)
    down = sub.add_parser("down")
    down.add_argument("--destroy", action="store_true")
    down.set_defaults(func=command_down)
    args = parser.parse_args()
    if args.command not in ("up", "infra"):
        require(path("owner").exists(), f"{RUN} has no stack")
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
