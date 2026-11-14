#!/usr/bin/env python3
"""Isolated lab for the plugin client hot delivery acceptance (plugin_hot_delivery_acceptance.py
--profile isolated).

It reuses the workspace's plugin online lab module (scripts/plugin-online-lab.py: keys, platform
API, release signing, the platform deployment path, accounts and proofs) and moves everything it
starts to a block of its own, so it never meets the online lab, the wallet sandbox or the dev
defaults:

- containers `<prefix>-pg` (TimescaleDB pg16), `<prefix>-consul` and `<prefix>-objects` (RustFS,
  two buckets: one per simulated artifact mirror), labelled with the lab's nonce; Redis, the lab
  NATS and nginx (mirror TLS fronts and the plugin sandbox) are host processes recorded by pid;
- databases `<db prefix>*` inside its own PostgreSQL only;
- Core/Social dev ports 50xxx/60xxx moved by --port-offset (default -36000: 14xxx/24xxx), every
  other listener in the --block (default 47000: 47xxx), metrics 19180-19209 into the block;
- binaries from --binaries (Core, Social, flare-platform, flarectl and the task, wallet and red
  packet plugins with their schema tools);
- the plugins the acceptance needs: task (the dashboard web bundle), wallet (the payment provider,
  `payments.ticket` approved with the installation) and red packet (minClient floors);
- the access control plane a web bundle needs: offline trust roots (flarectl), a certified online
  key, two access points, two independent https mirrors with their manifest and artifact
  credentials, the lab app's placement (its bootstrap id) and a published manifest, all through
  the platform's two-person flows;
- the flare-deploy plugin sandbox server block (edge/plugin-sandbox), filled for this lab, in
  front of the primary mirror's bucket, with a private CA.

State, secrets, certificates and logs stay in --lab-dir (mode 0700). Processes are stopped only by
recorded pid; containers only when they carry this lab's label.

  python3 scripts/plugin_hot_delivery_lab.py --lab-dir DIR --binaries DIR up
  python3 scripts/plugin_hot_delivery_lab.py --lab-dir DIR status
  python3 scripts/plugin_hot_delivery_lab.py --lab-dir DIR down     # processes and containers go
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SCHEMA = "flare.plugin-hot-delivery-lab/v1"

lab = None  # the online lab module, configured by configure()
settings = None


class Settings:
    """Where this lab lives: directory, names and its port block."""

    def __init__(self, lab_dir, binaries, offset=-36000, block=47000, prefix="flare-phdlab",
                 db_prefix="flare_phdlab_", web_origin=None, lab_script=None):
        self.dir = Path(lab_dir).expanduser().resolve()
        self.binaries = Path(binaries).expanduser().resolve() if binaries else None
        self.offset, self.block = offset, block
        self.prefix, self.db_prefix = prefix, db_prefix
        self.lab_script = Path(lab_script or os.environ.get("FLARE_PLUGIN_ONLINE_LAB_SCRIPT")
                               or ROOT / "scripts" / "plugin-online-lab.py")
        b = block
        self.pg, self.consul, self.redis, self.nats = b + 432, b + 500, b + 379, b + 252
        self.platform = b + 88
        self.objects = b + 900
        self.mirror_base = {"a": b + 611, "b": b + 612}
        self.mirror_upload = {"a": b + 621, "b": b + 622}
        self.sandbox = b + 444
        self.web = b + 431
        self.task, self.wallet, self.redpacket = b + 909, b + 911, b + 902
        self.realname, self.realname_material = b + 928, b + 929
        self.wallet_health = self.wallet + 1000
        self.access_metrics = b + 551
        self.web_origin = (web_origin or f"http://127.0.0.1:{self.web}").rstrip("/")

    def dev(self, port):
        """A Core/Social dev port in this lab's block."""
        return port + self.offset

    def metrics(self, port):
        return port - 19180 + self.block + 180


HOST_SUFFIX = "phdlab.localhost"
REGION, CELL = "phd-r1", "phd-c1"
MIRRORS = {"a": {"id": "phd-m-a", "provider": "sim-aws", "tld": "com", "bucket": "flare-phd-a"},
           "b": {"id": "phd-m-b", "provider": "sim-aliyun", "tld": "cn", "bucket": "flare-phd-b"}}
ACCESS_POINTS = ["phd-ap-1", "phd-ap-2"]
AP_SERVICES = {"imWs": {"path": "/ws"}, "ping": {"path": "/.well-known/flare/ping"}}
SANDBOX_HOST = f"sandbox.{HOST_SUFFIX}"
LABEL = "flare.plugin-hot-delivery.lab"


def mirror_host(key):
    return f"cfg-{key}.{MIRRORS[key]['provider']}.{HOST_SUFFIX}"


def load_lab(path):
    spec = importlib.util.spec_from_file_location("plugin_online_lab_isolated", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configure(lab_dir, binaries=None, **options):
    """Load and point the online lab module at this lab. Returns the module."""
    global lab, settings
    settings = Settings(lab_dir, binaries, **options)
    s = settings
    if not s.lab_script.exists():
        raise SystemExit(f"plugin online lab module not found at {s.lab_script} "
                         "(--lab-script or FLARE_PLUGIN_ONLINE_LAB_SCRIPT)")
    os.environ["FLARE_PLUGIN_LAB_DIR"] = str(s.dir)
    os.environ["FLARE_PLUGIN_LAB_DB_PREFIX"] = s.db_prefix
    lab = load_lab(s.lab_script)
    s.dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    s.dir.chmod(0o700)
    owner = s.dir / "owner.json"
    if owner.exists():
        lab.require(json.loads(owner.read_text()).get("schema") == SCHEMA, f"{s.dir} belongs to another tool")
    else:
        lab.require(not any(s.dir.iterdir()), f"a new lab directory must be empty: {s.dir}")
        lab.write_private(owner, json.dumps({"schema": SCHEMA, "nonce": secrets.token_hex(5)}))
    s.nonce = json.loads(owner.read_text())["nonce"]
    lab.LAB = s.dir
    lab.ensure_lab()
    # The workspace's repositories, wherever this checkout keeps them.
    lab.CORE, lab.SOCIAL = ROOT / "flare-core-server", ROOT / "flare-social-server"
    lab.PLUGIN, lab.PROTO = ROOT / "flare-plugins-commercial", ROOT / "flare-protocol-grpc" / "proto"
    lab.SDK_PLUGIN, lab.CALL_PLUGIN = lab.PLUGIN / "sdk", ROOT / "flare-plugin-call"
    lab.PROTO_IMPORTS = [ROOT / "flare-protocol-grpc" / "proto", ROOT / "flare-protocol" / "proto"]
    if s.binaries:
        lab.TARGET = s.binaries

    class WalletSpec(lab.PluginSpec):
        @property
        def template(self):
            return super().template.with_name("platform-plugin.sandbox.template.json")

    class RealnameSpec(lab.PluginSpec):
        """The realname metaservice: its stream definition and broker permissions live in deploy/,
        not in an initial/ directory, so the lab writes them in the shape the other plugins use."""

        @property
        def events(self):
            directory = s.dir / "realname-events"
            directory.mkdir(exist_ok=True, mode=0o700)
            stream = json.loads((self.layout / "deploy" / "nats-stream.json").read_text())
            (directory / "events-stream.json").write_text(json.dumps(stream))
            (directory / "events-permissions.json").write_text(json.dumps({
                "publish": {"allow": ["flare.plugins.realname.*.>", f"$JS.API.STREAM.INFO.{stream['name']}"]},
                "subscribe": {"allow": ["_INBOX.>"]}}))
            return directory

    plugin = lab.PLUGIN
    lab.PLUGINS = [
        lab.PluginSpec("task", "flare.task", "flare-task", "flare-sdk-plugin-task", s.task, "FLARE_TASK", "flare_task",
                       layout=plugin / "plugins/task"),
        WalletSpec("wallet", "flare.wallet", "flare-wallet", "flare-sdk-plugin-wallet", s.wallet, "FLARE_WALLET",
                   "flare_wallet", layout=plugin / "plugins/wallet", configuration={}),
        lab.PluginSpec("redpacket", "flare.redpacket", "flare-red-packet", "flare-sdk-plugin-red-packet", s.redpacket,
                       "FLARE_RED_PACKET", "flare_redpacket", layout=plugin / "plugins/red-packet", configuration={
                           "activeAssetId": "cash", "packetLifetimeSeconds": 3600,
                           "fundingAssets": [{"assetId": "cash", "sourceDefinitionId": "main",
                                              "recipientDefinitionId": "main", "maxAmount": "100000",
                                              "dailyLimit": "1000000"}],
                           "maxRecipients": 100, "maxLifetimeSeconds": 86400, "maxPacketsPerMinute": 60}),
        RealnameSpec("realname", "flare.realname", "flare-realname", "flare-sdk-plugin-realname", s.realname,
                     "FLARE_REALNAME", "flare_realname", layout=plugin / "metaservices/realname",
                     configuration={"acceptApplications": True}),
    ]
    lab.SPECS = {spec.key: spec for spec in lab.PLUGINS}
    lab.FIXED_PLUGIN_PORTS = {spec.port for spec in lab.PLUGINS} | {s.wallet_health, s.realname_material}
    lab.LAB_ROLES = ["core_initialize", "core_runtime", "social_initialize", "social_runtime",
                     "flare_lab_platform_migrate", "flare_lab_platform_api", "flare_lab_platform_worker",
                     *[role for spec in lab.PLUGINS for role in (spec.owner, spec.runtime)]]
    lab.DATABASES = {key: f"{s.db_prefix}{key}" for key in ("core", "social", "platform", *lab.SPECS)}
    lab.PG_HOST, lab.PG_PORT = "127.0.0.1", s.pg
    password = lab.random_secret("postgres-admin", lambda: secrets.token_urlsafe(32))
    lab.PG_ADMIN = {"PGHOST": "127.0.0.1", "PGPORT": str(s.pg), "PGUSER": "flare", "PGPASSWORD": password}
    p = s.dev
    lab.CORE_ADMIN_ADDR = f"127.0.0.1:{p(50051)}"
    lab.CORE_PORTS = {"api-gateway": p(50050), "capability": p(50110), "signaling-online": p(50061),
                      "access-gateway-ws": p(60051), "push-proxy": p(50091), "admin-gateway": p(50051)}
    lab.SOCIAL_PORTS = {"gateway": p(50200), "control": p(50207), "projection": p(50206), "user": p(50201),
                        "hook": p(50210)}
    lab.PLATFORM_ADDR = f"127.0.0.1:{s.platform}"
    lab.LAB_NATS_PORT = s.nats
    lab.CORE_INGEST_GRPC = f"127.0.0.1:{p(50182)}"
    lab.CORE_READER_GRPC = f"127.0.0.1:{p(60083)}"
    lab.CORE_ORCHESTRATOR_GRPC = f"http://127.0.0.1:{p(50181)}"
    lab.CORE_CONVERSATION_GRPC = f"http://127.0.0.1:{p(50090)}"
    lab.SOCIAL_GROUP_GRPC = f"http://127.0.0.1:{p(50203)}"
    lab.CORE_CAPABILITY_GRPC = f"http://127.0.0.1:{p(50110)}"
    lab.CORE_PUSH_PROXY_GRPC = f"http://127.0.0.1:{p(50091)}"
    lab.SOCIAL_API = f"http://127.0.0.1:{p(50200)}/api/v1"
    lab.SOCIAL_ADMIN = f"http://127.0.0.1:{p(50200)}/admin/v1"
    lab.CODE_FILE = s.dir / "logs" / "verification_codes.log"
    lab.PlatformApi.base = f"http://{lab.PLATFORM_ADDR}/api/v1/admin"
    lab.PlatformApi.origin = f"https://{lab.PLATFORM_ADDR}"
    lab.start_core = start_core
    lab.start_platform = start_platform
    lab.start_social = start_social
    deploy_plugin = lab.deploy_plugin

    def deploy_and_publish_features(state, keys, tenant, spec):
        release = deploy_plugin(state, keys, tenant, spec)
        publish_features(keys, tenant, spec)
        return release
    lab.deploy_plugin = deploy_and_publish_features
    global PORTS
    PORTS = {d: p(d) for d in [50050, 50051, 50061, 50062, 50090, 50091, 50110, 50181, 50182, 50200, 50201, 50202,
                               50203, 50204, 50205, 50206, 50207, 50210, 60051, 60052, 60060, 60081, 60082, 60083,
                               60084]}
    PORTS.update({d: s.metrics(d) for d in range(19180, 19210)})
    PORTS.update({24252: s.nats, 26379: s.redis, 28500: s.consul, 29000: s.objects, 25432: s.pg})
    return lab


PORTS = {}


def all_ports():
    s = settings
    return sorted(set(PORTS.values()) | {s.pg, s.consul, s.redis, s.nats, s.nats + 1, s.platform, s.objects,
                                         s.sandbox, s.task, s.wallet, s.wallet_health, s.redpacket,
                                         s.realname, s.realname_material,
                                         s.access_metrics, *s.mirror_base.values(), *s.mirror_upload.values()})


def remap(text):
    return re.sub(r"\b(?:" + "|".join(map(str, PORTS)) + r")\b", lambda m: str(PORTS[int(m[0])]), text)


def endpoint_environment(environment):
    result = dict(environment)
    for key, value in result.items():
        if key.endswith(("_URL", "_GRPC", "_ENDPOINT", "_LISTEN", "_ADDRESS")):
            result[key] = remap(value)
    return result


def deployment_environment(manifest_path, binary, environment):
    """The shipped supervise manifest's per-process environment rules."""
    manifest = tomllib.loads(manifest_path.read_text())
    service = next(s for s in manifest["services"] if Path(s["command"]).name == binary)
    names = set(manifest.get("shared_env", []) + service.get("inherit_env", []))
    prefixes = tuple(manifest.get("shared_env_prefixes", []) + service.get("inherit_env_prefixes", []))
    result = {k: v for k, v in environment.items() if k in names or (prefixes and k.startswith(prefixes))}
    for target, source in service.get("environment_bindings", {}).items():
        lab.require(source in environment, f"missing {binary} purpose key binding {source}")
        result[target] = environment[source]
    for conditional in service.get("environment_defaults", []):
        if environment.get(conditional["when_env"]) == conditional["equals"]:
            for key, value in conditional.get("values", {}).items():
                result.setdefault(key, remap(value))
    result.update({k: remap(v).replace("0.0.0.0", "127.0.0.1") for k, v in service.get("environment", {}).items()})
    result["TOKIO_WORKER_THREADS"] = "2"
    for key in manifest.get("forbidden_env", []):
        lab.require(key not in result, f"forbidden {binary} credential {key}")
    return result


def run(command, check=True, **kwargs):
    result = subprocess.run([str(c) for c in command], text=True, capture_output=True, **kwargs)
    if check and result.returncode:
        raise lab.LabError(f"{' '.join(str(c) for c in command[:3])} failed: {(result.stderr or result.stdout)[-1500:]}")
    return result.stdout.strip()


# ----------------------------------------------------------- infrastructure --

def container(state, kind, image, arguments, environment=None, command=()):
    name = f"{settings.prefix}-{kind}"
    existing = subprocess.run(["podman", "inspect", "--format", '{{index .Config.Labels "%s"}}' % LABEL, name],
                              text=True, capture_output=True)
    if existing.returncode == 0:
        lab.require(existing.stdout.strip() == settings.nonce, f"foreign container {name}; refusing to adopt it")
        running = run(["podman", "inspect", "--format", "{{.State.Running}}", name])
        if running != "true":
            run(["podman", "start", name])
    else:
        args = ["podman", "run", "--detach", "--name", name, "--label", f"{LABEL}={settings.nonce}"]
        if environment:
            path = lab.lab_path("secrets", f"{kind}.env")
            lab.write_private(path, "".join(f"{k}={v}\n" for k, v in environment.items()))
            args += ["--env-file", str(path)]
        run(args + arguments + [image, *command])
        lab.log(f"started container {name}")
    state.setdefault("containers", {})[kind] = name
    lab.save_state(state)


def objects_secret():
    access = lab.random_secret("objects-access", lambda: "phdlab" + secrets.token_hex(8))
    secret = lab.random_secret("objects-secret", lambda: secrets.token_urlsafe(24))
    return access, secret


def infrastructure(state, keys):
    s = settings
    if not state.get("containers") and not state.get("processes"):
        busy = [port for port in all_ports() if lab.port_open(port)]
        lab.require(not busy, f"lab ports already in use: {busy}; pick another --block/--port-offset")
    container(state, "pg", "docker.io/timescale/timescaledb:latest-pg16", ["--publish", f"127.0.0.1:{s.pg}:5432"],
              {"POSTGRES_USER": "flare", "POSTGRES_PASSWORD": lab.PG_ADMIN["PGPASSWORD"]},
              ["postgres", "-c", "max_connections=300", "-c", "shared_buffers=96MB"])
    lab.wait_for(lambda: subprocess.run(["psql", "-X", "-At", "-c", "SELECT 1", "postgres"],
                                        env={**os.environ, **lab.PG_ADMIN}, capture_output=True).returncode == 0,
                 90, "lab PostgreSQL")
    container(state, "consul", "docker.io/hashicorp/consul:1.17", ["--publish", f"127.0.0.1:{s.consul}:8500"],
              command=["agent", "-dev", "-client=0.0.0.0", "-log-level=WARN"])
    lab.wait_for(lambda: lab.http("GET", f"http://127.0.0.1:{s.consul}/v1/status/leader")[0] == 200, 60, "lab Consul")
    access, secret = objects_secret()
    container(state, "objects", "docker.io/rustfs/rustfs:latest", ["--publish", f"127.0.0.1:{s.objects}:9000"],
              {"RUSTFS_ACCESS_KEY": access, "RUSTFS_SECRET_KEY": secret, "RUSTFS_ADDRESS": ":9000",
               "RUSTFS_CONSOLE_ENABLE": "false", "RUSTFS_HEALTH_ENDPOINT_ENABLE": "true"}, ["/data"])
    lab.spawn(state, "redis", [shutil.which("redis-server"), "--bind", "127.0.0.1", "--port", str(s.redis),
                               "--save", "", "--appendonly", "no"], {}, f"127.0.0.1:{s.redis}")
    lab.wait_for(lambda: lab.port_open(s.redis), 20, "lab Redis")
    lab.start_nats(state, keys)
    lab.wait_for(lambda: lab.http("GET", f"http://127.0.0.1:{s.objects}/health/ready")[0] == 200, 90, "lab RustFS")
    buckets(state)


def s3_request(method, path, body=b"", query="", headers=None):
    """Signature Version 4, path style, against the lab object store."""
    access, secret_key = objects_secret()
    endpoint = f"http://127.0.0.1:{settings.objects}"
    host = urllib.parse.urlsplit(endpoint).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, date = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload = hashlib.sha256(body).hexdigest()
    signed = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": stamp,
              **{k.lower(): v for k, v in (headers or {}).items()}}
    names = sorted(signed)
    canonical = "\n".join([method, urllib.parse.quote(path, safe="/-_.~"), query,
                           "".join(f"{n}:{signed[n]}\n" for n in names), ";".join(names), payload])
    scope = f"{date}/us-east-1/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = ("AWS4" + secret_key).encode()
    for part in (date, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    authorization = (f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={';'.join(names)}, "
                     f"Signature={signature}")
    request = urllib.request.Request(f"{endpoint}{path}" + (f"?{query}" if query else ""), data=body or None,
                                     method=method, headers={**{k: v for k, v in signed.items() if k != "host"},
                                                             "Authorization": authorization})
    try:
        with lab.OPENER.open(request, timeout=15) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


CORS = """<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>
<AllowedMethod>HEAD</AllowedMethod><AllowedHeader>*</AllowedHeader><ExposeHeader>ETag</ExposeHeader>
<MaxAgeSeconds>600</MaxAgeSeconds></CORSRule></CORSConfiguration>"""


def buckets(state):
    """One bucket per simulated mirror: anonymous reads (clients and the sandbox read the public
    address), CORS for GET. RustFS has one root key per instance: the lab cannot scope the
    artifact credential to `{bootstrapId}/artifacts/`, `platform/artifacts/` and DeleteObject on
    `*/artifacts/plugins/*` as production does; the root key allows all three."""
    report = {}
    for key, mirror in MIRRORS.items():
        bucket = mirror["bucket"]
        status, body = s3_request("PUT", f"/{bucket}")
        lab.require(status in (200, 409), f"create bucket {bucket}: {status} {body[:200]!r}")
        policy = json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{bucket}/*"]}]}).encode()
        status, body = s3_request("PUT", f"/{bucket}", policy, "policy=", {"content-type": "application/json"})
        lab.require(status in (200, 204), f"bucket policy {bucket}: {status} {body[:300]!r}")
        document = CORS.encode()
        digest = base64.b64encode(hashlib.md5(document).digest()).decode()
        cors, _ = s3_request("PUT", f"/{bucket}", document, "cors=",
                             {"content-type": "application/xml", "content-md5": digest})
        report[key] = {"bucket": bucket, "policy": status, "cors": cors}
    state["buckets"] = report
    lab.save_state(state)


# ---------------------------------------------------------------- tls, nginx --

def tls_dir():
    directory = lab.lab_path("tls")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    return directory


def ca_file():
    return tls_dir() / "ca.crt"


def ensure_ca():
    directory, openssl = tls_dir(), lab.openssl()
    if ca_file().exists():
        return
    run([openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", directory / "ca.key"])
    os.chmod(directory / "ca.key", 0o600)
    run([openssl, "req", "-x509", "-new", "-key", directory / "ca.key", "-sha256", "-days", "30",
         "-subj", "/CN=Flare plugin hot delivery lab CA", "-addext", "basicConstraints=critical,CA:TRUE",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-out", ca_file()])


def certificate(name, hosts):
    ensure_ca()
    directory, openssl = tls_dir(), lab.openssl()
    cert, key = directory / f"{name}.crt", directory / f"{name}.key"
    if cert.exists() and key.exists():
        return cert, key
    run([openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", key])
    os.chmod(key, 0o600)
    extensions = directory / f"{name}.ext"
    extensions.write_text("basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n"
                          "subjectAltName=" + ",".join(f"DNS:{h}" for h in hosts) + "\n")
    request = directory / f"{name}.csr"
    run([openssl, "req", "-new", "-key", key, "-subj", f"/CN={hosts[0]}", "-out", request])
    run([openssl, "x509", "-req", "-in", request, "-CA", ca_file(), "-CAkey", directory / "ca.key", "-CAcreateserial",
         "-days", "30", "-sha256", "-extfile", extensions, "-out", cert])
    request.unlink()
    extensions.unlink()
    return cert, key


def nginx_common(directory):
    return (f"worker_processes 1;\npid {directory}/nginx.pid;\nerror_log {directory}/error.log warn;\n"
            "events { worker_connections 512; }\n")


def nginx_temp(directory):
    return "".join(f"    {kind}_temp_path {directory}/{kind};\n" for kind in ("client_body", "proxy", "fastcgi",
                                                                               "uwsgi", "scgi"))


def mirror_front_source(directory):
    """The https faces of both mirrors: the public base (`/flare/v1/` of its bucket) and the S3
    API itself for presigned uploads and the platform's copies (Host passed through)."""
    s = settings
    servers = []
    for key, mirror in MIRRORS.items():
        cert, cert_key = certificate(f"mirror-{key}", [mirror_host(key)])
        servers.append(f"""    server {{
        listen 127.0.0.1:{s.mirror_base[key]} ssl;
        listen [::1]:{s.mirror_base[key]} ssl;
        server_name {mirror_host(key)};
        ssl_certificate {cert};
        ssl_certificate_key {cert_key};
        location /flare/v1/ {{
            proxy_pass http://127.0.0.1:{s.objects}/{mirror['bucket']}/flare/v1/;
            proxy_set_header Host 127.0.0.1:{s.objects};
            proxy_http_version 1.1;
            proxy_set_header Connection "";
        }}
        location / {{ return 404; }}
    }}
    server {{
        listen 127.0.0.1:{s.mirror_upload[key]} ssl;
        listen [::1]:{s.mirror_upload[key]} ssl;
        server_name {mirror_host(key)};
        ssl_certificate {cert};
        ssl_certificate_key {cert_key};
        client_max_body_size 64m;
        location / {{
            proxy_pass http://127.0.0.1:{s.objects};
            proxy_set_header Host $http_host;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_request_buffering off;
        }}
    }}
""")
    return (nginx_common(directory) + "http {\n"
            "    log_format access '$msec $status $request_method $uri $body_bytes_sent';\n"
            f"    access_log {directory}/access.log access;\n" + nginx_temp(directory) + "".join(servers) + "}\n")


SANDBOX_VARIABLES = ("FLARE_PLUGIN_SANDBOX_HOST", "FLARE_PLUGIN_SANDBOX_PORT", "FLARE_PLUGIN_SANDBOX_ORIGIN",
                     "FLARE_PLUGIN_SANDBOX_FRAME_ANCESTORS", "FLARE_PLUGIN_SANDBOX_REVIEW_FRAME_ANCESTORS",
                     "FLARE_PLUGIN_SANDBOX_REVIEW_SECRET", "FLARE_PLUGIN_SANDBOX_UPSTREAM",
                     "FLARE_PLUGIN_SANDBOX_CERT", "FLARE_PLUGIN_SANDBOX_CERT_KEY")


def sandbox_origin():
    return f"https://{SANDBOX_HOST}:{settings.sandbox}"


def review_secret():
    return lab.random_secret("sandbox-review-secret", lambda: secrets.token_hex(32))


def sandbox_source(directory):
    """flare-deploy edge/plugin-sandbox, filled with only its own variables (as its README's
    envsubst does), at the http level of a server of its own. Loopback listeners only (the
    lab names resolve to ::1 and 127.0.0.1)."""
    s = settings
    cert, key = certificate("sandbox", [SANDBOX_HOST])
    values = {"FLARE_PLUGIN_SANDBOX_HOST": SANDBOX_HOST, "FLARE_PLUGIN_SANDBOX_PORT": str(s.sandbox),
              "FLARE_PLUGIN_SANDBOX_ORIGIN": sandbox_origin(),
              "FLARE_PLUGIN_SANDBOX_FRAME_ANCESTORS": " ".join(sorted({s.web_origin,
                                                                         s.web_origin.replace("127.0.0.1", "localhost")})),
              "FLARE_PLUGIN_SANDBOX_REVIEW_FRAME_ANCESTORS": f"http://127.0.0.1:{s.platform}",
              "FLARE_PLUGIN_SANDBOX_REVIEW_SECRET": review_secret(),
              "FLARE_PLUGIN_SANDBOX_UPSTREAM": f"http://127.0.0.1:{s.objects}/{MIRRORS['a']['bucket']}/flare/v1",
              "FLARE_PLUGIN_SANDBOX_CERT": str(cert), "FLARE_PLUGIN_SANDBOX_CERT_KEY": str(key)}
    template = (ROOT / "flare-deploy" / "edge" / "plugin-sandbox" / "plugin-sandbox.conf.template").read_text()
    for name in SANDBOX_VARIABLES:
        template = template.replace("${" + name + "}", values[name])
    lab.require("${FLARE_PLUGIN_SANDBOX_" not in template, "sandbox template has variables this lab does not fill")
    template = template.replace(f"listen {s.sandbox} ssl;", f"listen 127.0.0.1:{s.sandbox} ssl;")
    template = template.replace(f"listen [::]:{s.sandbox} ssl;", f"listen [::1]:{s.sandbox} ssl;")
    return nginx_common(directory) + "http {\n" + nginx_temp(directory) + template + "\n}\n"


def nginx(state, name, source):
    directory = lab.lab_path("nginx", name)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    file = directory / "nginx.conf"
    changed = not file.exists() or file.read_text() != source
    lab.write_private(file, source)
    binary = shutil.which("nginx") or "/opt/homebrew/bin/nginx"
    checked = subprocess.run([binary, "-t", "-c", str(file), "-p", str(directory)], capture_output=True, text=True)
    lab.require(checked.returncode == 0, f"nginx {name}: {checked.stderr[-800:]}")
    known = state.get("processes", {}).get(f"nginx-{name}")
    if changed and known and lab.process_alive(known["pid"], known["needle"]):
        # A configuration rendered by an older run (or a changed template): reload in place.
        os.kill(known["pid"], signal.SIGHUP)
        lab.log(f"nginx {name}: configuration changed; reloaded")
    lab.spawn(state, f"nginx-{name}", [binary, "-c", str(file), "-p", str(directory), "-g", "daemon off;"], {},
              str(file))


def fronts(state):
    nginx(state, "mirrors", mirror_front_source(lab.lab_path("nginx", "mirrors")))
    nginx(state, "sandbox", sandbox_source(lab.lab_path("nginx", "sandbox")))
    lab.wait_for(lambda: lab.port_open(settings.sandbox) and lab.port_open(settings.mirror_base["a"]), 20, "nginx")


# -------------------------------------------------------- configuration, dbs --

def configuration():
    target = lab.lab_path("config")
    target.mkdir(exist_ok=True)
    for source in (lab.CORE / "config").rglob("*.toml"):
        dest = target / source.relative_to(lab.CORE / "config")
        dest.parent.mkdir(exist_ok=True, parents=True)
        text = remap(source.read_text()).replace('"0.0.0.0"', '"127.0.0.1"')
        text = re.sub(r"max_connections = \d+", "max_connections = 8", text)
        text = re.sub(r"min_connections = \d+", "min_connections = 1", text)
        dest.write_text(text)
    text = remap((lab.SOCIAL / "config/social.example.toml").read_text()).replace("0.0.0.0:", "127.0.0.1:")
    text = text.replace('delivery = "smtp"', 'delivery = "file"\nfile_path = "logs/verification_codes.log"')
    web = settings.web_origin
    origins = sorted({web, web.replace("127.0.0.1", "localhost")})
    text = text.replace('cors_allowed_origins = ["https://im.example.com"]', "cors_allowed_origins = " + json.dumps(origins))
    text = text.replace("public_per_ip_per_minute = 60", "public_per_ip_per_minute = 600")
    text = text.replace("verification_code_per_ip_per_hour = 20", "verification_code_per_ip_per_hour = 200")
    (target / "social.toml").write_text(text)


def databases(state, keys):
    if state.get("initialized"):
        return
    completed = state.setdefault("initialized_databases", [])

    def record(key):
        if key not in completed:
            completed.append(key)
        lab.save_state(state)
    for role in lab.LAB_ROLES:
        lab.ensure_login_role(role, keys.db[role])
    lab.ensure_platform_roles()
    for key in ("core", "social"):
        if key in completed:
            continue
        lab.ensure_database(key, f"{key}_initialize", [f"{key}_runtime"])
        if key == "core":
            lab.psql("CREATE EXTENSION IF NOT EXISTS timescaledb", lab.DATABASES[key])
        prefix = key.upper()
        result = lab.run([str(lab.TARGET / f"flare-{key}-initialize")],
                         env={f"{prefix}_INITIAL_DATABASE_URL": keys.url(f"{key}_initialize", key),
                              f"{prefix}_INITIAL_DATABASE": lab.DATABASES[key]})
        lab.log(f"{key} fresh initializer: {result.stdout.strip()[-300:]}")
        record(key)
    if "platform" not in completed:
        lab.ensure_database("platform", "flare_lab_platform_migrate", ["flare_lab_platform_api", "flare_lab_platform_worker"])
        lab.run([str(lab.TARGET / "flare-platform"), "initialize"], cwd=lab.LAB,
                env={"DATABASE_URL": keys.url("flare_lab_platform_migrate", "platform")})
        lab.run([str(lab.TARGET / "flare-platform"), "bootstrap"], cwd=lab.LAB, env={
            "DATABASE_URL": keys.url("flare_lab_platform_migrate", "platform"),
            "FLARE_IAM_SIGNING_KEY": keys.iam.pem_b64(), "FLARE_IAM_KEY_ID": "iam-1",
            "BOOTSTRAP_USERNAME": "lab_platform_admin", "BOOTSTRAP_PASSWORD": keys.platform_admin_initial})
        record("platform")
    specs = lab.SPECS
    if "task" not in completed:
        lab.setup_plugin_database(keys, specs["task"])
        record("task")
    if "wallet" not in completed:
        wallet = specs["wallet"]
        lab.ensure_database("wallet", wallet.owner, [wallet.runtime])
        lab.run([str(lab.TARGET / "flare-wallet-initialize"), wallet.runtime, "--sandbox"],
                env={"FLARE_WALLET_SCHEMA_OWNER_URL": keys.url(wallet.owner, "wallet")})
        record("wallet")
    if "redpacket" not in completed:
        packet = specs["redpacket"]
        lab.ensure_database("redpacket", packet.owner, [packet.runtime])
        lab.run([str(lab.TARGET / "flare-red-packet-initialize"), packet.runtime],
                env={"FLARE_RED_PACKET_SCHEMA_OWNER_URL": keys.url(packet.owner, "redpacket")})
        record("redpacket")
    if "realname" not in completed:
        lab.setup_plugin_database(keys, specs["realname"])
        record("realname")
    state["initialized"] = True
    lab.save_state(state)


# ----------------------------------------------------------------- services --

# The launcher's Core services but flare-media (no media in these cases).
CORE_BINARIES = ["flare-signaling-online", "flare-signaling-route", "flare-capability", "flare-conversation",
                 "flare-message-ingest", "flare-orchestrator", "flare-storage-writer", "flare-storage-reader",
                 "flare-sync-orchestrator", "flare-push-proxy", "flare-push-server", "flare-push-worker",
                 "flare-api-gateway", "flare-signaling-gateway", "flare-admin-gateway"]


def core_environment(keys, state):
    p = settings.dev
    # lab.core_env is read at call time: switch-off (case 8) patches it.
    core = endpoint_environment(lab.core_env(keys, state))
    core.setdefault("FLARE_PLUGIN_WEB_BUNDLES_ENABLED", "true")
    core.update({"FLARE_CONFIG_PATH": str(lab.lab_path("config")),
                 "FLARE_CONSUL_ENDPOINT": f"http://127.0.0.1:{settings.consul}",
                 "FLARE_REDIS_URL_BASE": f"redis://127.0.0.1:{settings.redis}", "SERVICE_ADVERTISE_HOST": "127.0.0.1",
                 "RUST_LOG": "info,async_nats=warn",
                 "MESSAGE_INGEST_HOOKS_CONFIG": str(lab.lab_path("config/hooks.social.toml")),
                 "FLARE_CAPABILITY_GRPC_URI": lab.CORE_CAPABILITY_GRPC,
                 "FLARE_ADMIN_GATEWAY_BIND_ADDRESS": lab.CORE_ADMIN_ADDR,
                 "ONLINE_SERVICE_ENDPOINT": f"http://127.0.0.1:{p(50061)}",
                 "ROUTE_SERVICE_ENDPOINT": f"http://127.0.0.1:{p(50062)}",
                 "ACCESS_GATEWAY_GRPC_ENDPOINT": f"http://127.0.0.1:{p(60060)}",
                 "CONVERSATION_ROSTER_POLICY_GRPC_URL": f"http://127.0.0.1:{p(50203)}",
                 "FLARE_API_GATEWAY_SERVER_BIND": "127.0.0.1", "FLARE_API_GATEWAY_SERVER_PORT": str(p(50050)),
                 "FLARE_ADMIN_GATEWAY_SERVER_BIND": "127.0.0.1", "FLARE_ADMIN_GATEWAY_SERVER_PORT": str(p(50051)),
                 "ACCESS_GATEWAY_METRICS_ADDRESS": "127.0.0.1",
                 "ACCESS_GATEWAY_METRICS_PORT": str(settings.access_metrics)})
    for prefix in ("ACCESS_GATEWAY", "FLARE_API_GATEWAY"):
        core[prefix + "_AUTH_HOOK_URL"] = f"http://127.0.0.1:{p(50200)}/internal/identity/connect"
    for service, port in [("MEDIA", p(60081)), ("MESSAGE_INGEST", p(50182)), ("MESSAGE_ORCHESTRATOR", p(50181)),
                          ("CONVERSATION", p(50090)), ("ONLINE", p(50061)), ("STORAGE_READER", p(60083)),
                          ("CAPABILITY", p(50110))]:
        for gateway in ("FLARE_API_GATEWAY", "FLARE_ADMIN_GATEWAY"):
            core[f"{gateway}_GRPC_{service}_SERVICE_URL"] = f"http://127.0.0.1:{port}"
    core["FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL"] = f"http://127.0.0.1:{p(50091)}"
    return core


def core_process_env(keys, state, name):
    env = core_environment(keys, state)
    if name == "flare-signaling-gateway":
        env.update({"PORT": str(settings.dev(60051)), "GRPC_PORT": str(settings.dev(60060))})
    return deployment_environment(lab.CORE / "release" / "supervise-core.toml", name, env)


def start_core(state, keys):
    """Every Core service of the lab as its own process; one whose environment changed (a
    runtime key, the web bundle switch) is restarted by spawn."""
    for name in CORE_BINARIES:
        lab.spawn(state, name, [str(lab.TARGET / name)], core_process_env(keys, state, name), str(lab.TARGET / name))
    for name, port in lab.CORE_PORTS.items():
        lab.wait_for(lambda q=port: lab.port_open(q), 90, f"Core {name} :{port}")
    for name in CORE_BINARIES:
        known = state["processes"][name]
        lab.require(lab.process_alive(known["pid"], known["needle"]), f"{name} exited; see {known['log']}")
    state["core_started"] = True
    lab.save_state(state)


def social_environment(keys):
    p = settings.dev
    social = endpoint_environment(lab.social_env(keys))
    social.pop("SOCIAL_JWT_SECRET", None)
    social.pop("IM_CONNECT_TOKEN_SECRET", None)
    social.update({"SOCIAL_CONSUL_ENDPOINT": f"http://127.0.0.1:{settings.consul}", "SERVICE_ADVERTISE_HOST": "127.0.0.1",
                   "SOCIAL_CELL_CTX_PRIVATE_KEY": keys.internal_ctx.pem_b64(),
                   "SOCIAL_POSTGRES_MAX_CONNECTIONS": "8", "FLARE_CONVERSATION_GRPC": lab.CORE_CONVERSATION_GRPC,
                   "FLARE_ORCHESTRATOR_GRPC": lab.CORE_ORCHESTRATOR_GRPC,
                   "FLARE_MESSAGE_INGEST_GRPC": "http://" + lab.CORE_INGEST_GRPC,
                   "FLARE_SOCIAL_CONTROL_HTTP_ADDRESS": f"127.0.0.1:{p(50207)}", "RUST_LOG": "info,async_nats=warn",
                   # The gateway's private material route in front of the realname service.
                   "FLARE_REALNAME_MATERIAL_HTTP": f"http://127.0.0.1:{settings.realname_material}"})
    return social


def start_social(state, keys):
    social = social_environment(keys)
    for name in lab.SOCIAL_LAUNCHED_BINARIES:
        env = deployment_environment(lab.SOCIAL / "deploy" / "supervise-social.toml", name, social)
        lab.spawn(state, name, [str(lab.TARGET / name)], env, str(lab.TARGET / name))
    for name, port in lab.SOCIAL_PORTS.items():
        lab.wait_for(lambda q=port: lab.port_open(q), 90, f"Social {name} :{port}")
    state["social_started"] = True
    lab.save_state(state)


def platform_environment(keys):
    p, s = settings.dev, settings
    env = endpoint_environment(lab.platform_env(keys))
    env.setdefault("FLARE_PLUGIN_WEB_BUNDLES_ENABLED", "true")
    env.update({"BIND_ADDRESS": lab.PLATFORM_ADDR, "CORE_CAPABILITY_GRPC": lab.CORE_CAPABILITY_GRPC,
                "CORE_ONLINE_GRPC": f"http://127.0.0.1:{p(50061)}",
                "SOCIAL_CONTROL_GRPC": f"http://127.0.0.1:{p(50206)}",
                "SOCIAL_GATEWAY_URL": f"http://127.0.0.1:{p(50207)}",
                # Access control plane: two people per publication, sealed mirror credentials, the
                # lab's trust roots and private CA (mirrors and the sandbox).
                "FLARE_ACCESS_ENVIRONMENT": "production",
                "FLARE_ACCESS_CREDENTIAL_ENCRYPTION_KEY": lab.random_secret(
                    "access-credential-key", lambda: base64.b64encode(secrets.token_bytes(32)).decode()),
                "FLARE_ACCESS_TRUST_ROOTS": json.dumps(trust_roots(), separators=(",", ":")),
                "FLARE_ACCESS_EXTRA_CA_FILE": str(ca_file()),
                "FLARE_ACCESS_PROBE_REGION": REGION,
                "FLARE_ACCESS_PROBE_HTTPS_PORT": str(s.sandbox),
                # The plugin sandbox server block above: bundle file base and review copies.
                "FLARE_PLUGIN_SANDBOX_ORIGIN": sandbox_origin(),
                "FLARE_PLUGIN_SANDBOX_REVIEW_SECRET": review_secret(),
                "RUST_LOG": "info,sqlx=warn,async_nats=warn"})
    return env


def start_platform(state, keys):
    """lab.platform_env is read at call time: switch-off (case 8) patches it."""
    lab.spawn(state, "platform", [str(lab.TARGET / "flare-platform"), "--role=all"], platform_environment(keys),
              str(lab.TARGET / "flare-platform"))
    lab.wait_for(lambda: lab.http("GET", f"http://{lab.PLATFORM_ADDR}/ready")[0] == 200, 90, "platform /ready")


# -------------------------------------------------------------------- trust --

def flarectl(*arguments):
    binary = lab.TARGET / "flarectl"
    lab.require(binary.exists(), f"missing binary {binary}")
    return run([binary, *arguments])


def trust_roots():
    directory = lab.lab_path("trust")
    if not (directory / "trust-roots.json").exists():
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        flarectl("trust", "init", "--output-dir", directory, "--unencrypted")
        lab.log(f"created the lab's offline root keys in {directory}")
    return json.loads((directory / "trust-roots.json").read_text())


def certify(key_id, public_key, name):
    certs = lab.lab_path("trust", "certs")
    certs.mkdir(parents=True, exist_ok=True, mode=0o700)
    file = certs / name
    if not file.exists():
        flarectl("trust", "certify", "--roots", lab.lab_path("trust", "trust-roots.json"),
                 "--root-key", lab.lab_path("trust", "root-a.key.pem"), "--key-id", key_id, "--public-key", public_key,
                 "--usage", "access", "--usage", "release", "--bootstrap-id", "*", "--output", file)
    return file.read_text().strip()


class Partition:
    """A staff member's calls through the gateway admin partition (where access management is)."""

    def __init__(self, staff):
        self.staff = staff

    def call(self, method, path, body=None, reason="plugin hot delivery lab"):
        headers = {"Authorization": "Bearer " + self.staff.token, "Origin": lab.PlatformApi.origin,
                   "X-Audit-Reason": urllib.parse.quote(reason), "Idempotency-Key": str(uuid.uuid4())}
        status, value, _ = lab.http(method, lab.SOCIAL_ADMIN + path, body, headers)
        return status, value

    def ok(self, method, path, body=None, reason="plugin hot delivery lab"):
        status, value = self.call(method, path, body, reason)
        lab.require(status == 200, f"{method} {path}: {status} {str(value)[:800]}")
        return value["data"] if isinstance(value, dict) and "data" in value else value


def second_staff(keys, username):
    """Another platform staff member (platform_admin role)."""
    admin = lab.platform_admin(keys)
    initial = lab.random_secret(f"staff-{username}-initial", lambda: "Lab-" + uuid.uuid4().hex[:20])
    password = lab.random_secret(f"staff-{username}-password", lambda: "Lab-" + uuid.uuid4().hex[:20])
    _, listing = admin.call("GET", f"/rbac/employees?keyword={username}&limit=20&offset=0")
    if not [row for row in listing["data"] if row["username"] == username]:
        _, created = admin.call("POST", "/rbac/employees", {"username": username, "display_name": username,
                                                            "password": initial}, reason="lab access approver")
        _, roles = admin.call("GET", "/rbac/roles")
        role = next(r for r in roles["data"] if r.get("builtin_key") == "platform_admin")
        admin.call("POST", f"/rbac/employees/{created['data']['id']}/roles", {"role_id": role["id"]},
                   reason="lab access approver")
    return lab.PlatformApi(username, initial, password, lab.lab_path("secrets", f"{username}.pat"),
                           token_name="phd-lab")


def access_plane(state, keys):
    """Trust, an online key, topology with two mirrors, the app's placement and a published
    manifest; then each mirror's artifact credential."""
    s = settings
    access = state.setdefault("access", {})
    author = Partition(lab.platform_admin(keys))
    approver = Partition(second_staff(keys, "phd_access_approver"))
    if not access.get("onlineKey"):
        listed = author.ok("GET", "/platform/signing/keys")["keys"]
        key = next((k for k in listed if k["status"] == "pending_cert"), None) \
            or author.ok("POST", "/platform/signing/keys:rotate", reason="first online key")
        document = certify(key["keyId"], key["publicKey"], "online-1.cert")
        approver.ok("POST", f"/platform/signing/keys/{key['keyId']}/cert", {"document": document},
                    "second person uploads the certificate")
        access["onlineKey"] = key["keyId"]
        lab.save_state(state)
    if not access.get("topology"):
        author.ok("POST", "/platform/access/regions", {"id": REGION, "displayName": {"zh-CN": "热交付实验区",
                                                                                      "en-US": "Hot delivery lab"}})
        author.ok("POST", "/platform/access/cells", {"id": CELL, "regionId": REGION, "status": "enabled",
                                                     "coreAdminUrl": f"http://{lab.CORE_ADMIN_ADDR}"})
        for ap in ACCESS_POINTS:
            author.ok("POST", "/platform/access/access-points", {
                "id": ap, "cellId": CELL, "host": f"{ap}.{HOST_SUFFIX}", "services": AP_SERVICES, "status": "active"})
        object_access, object_secret = objects_secret()
        for key, mirror in MIRRORS.items():
            author.ok("POST", "/platform/access/mirrors", {
                "id": mirror["id"], "kind": "s3", "baseUrl": f"https://{mirror_host(key)}:{s.mirror_base[key]}/flare/v1",
                "provider": mirror["provider"], "tld": mirror["tld"], "enabled": True, "credentials": {
                    "endpoint": f"http://127.0.0.1:{s.objects}", "region": "us-east-1", "bucket": mirror["bucket"],
                    "prefix": "flare/v1", "accessKeyId": object_access, "secretAccessKey": object_secret}})
        access["topology"] = True
        lab.save_state(state)
    if not access.get("bootstrapId"):
        placement = author.ok("PUT", f"/apps/{state['app_id']}/access/placement", {
            "expectedRevision": 0, "homeCellId": CELL, "allowedRegions": [REGION], "clientPolicy": {}})
        access["bootstrapId"] = placement["bootstrapId"]
        lab.save_state(state)
    if not access.get("keystate"):
        file = lab.lab_path("trust", "keystate-1.dsse.json")
        flarectl("trust", "keystate", "--roots", lab.lab_path("trust", "trust-roots.json"),
                 "--root-key", lab.lab_path("trust", "root-a.key.pem"), "--serial", "1",
                 "--certificate", lab.lab_path("trust", "certs", "online-1.cert"), "--output", file)
        document = base64.urlsafe_b64encode(file.read_bytes()).decode().rstrip("=")
        author.ok("POST", "/platform/signing/keystate", {"document": document}, "first key state")
        access["keystate"] = 1
        lab.save_state(state)
    if not access.get("published"):
        change = author.ok("POST", "/platform/access/change-sets", reason="lab publication")
        detail = author.ok("POST", f"/platform/access/change-sets/{change['id']}:submit", reason="lab publication")
        lab.require(detail["status"] == "pending_approval",
                    f"submit left {detail['status']}: {json.dumps(detail.get('lint'))[:1200]}")
        approver.ok("POST", f"/platform/access/change-sets/{change['id']}:approve", reason="lab publication")

        def finished():
            status, body = author.call("GET", f"/platform/access/change-sets/{change['id']}")
            return body["data"] if status == 200 and body["data"]["status"] in ("published", "failed") else None
        final = lab.wait_for(finished, 300, "the access manifest publication", 1)
        lab.require(final["status"] == "published", f"publication failed: {final.get('failure')}")
        access["published"] = change["id"]
        lab.save_state(state)
    if not access.get("artifactCredentials"):
        object_access, object_secret = objects_secret()
        mirrors = {m["id"]: m for m in author.ok("GET", "/platform/access/mirrors")}
        for key, mirror in MIRRORS.items():
            author.ok("PUT", f"/platform/access/mirrors/{mirror['id']}/artifact-credential", {
                "expectedRevision": mirrors[mirror["id"]]["revision"],
                "credentials": {"endpoint": f"https://{mirror_host(key)}:{s.mirror_upload[key]}", "region": "us-east-1",
                                "bucket": mirror["bucket"], "prefix": "flare/v1", "accessKeyId": object_access,
                                "secretAccessKey": object_secret}}, "lab package credential")
        access["artifactCredentials"] = True
        lab.save_state(state)
    lab.evidence("access-plane.json", {k: v for k, v in access.items()})
    return access


# ------------------------------------------------------------------ plugins --

def deploy(state, keys, tenant, only=None):
    """task, wallet, red packet through the whole platform path. The task version declares its
    dashboard web bundle: with bundles on, its apply waits for the approved bundle, so it is
    registered and started here and enabled by the acceptance's bundle-publish."""
    for spec in lab.PLUGINS:
        if only and spec.key not in only:
            continue
        version = lab.template_version(spec)
        release = lab.signed_release(state, keys, version, spec)
        lab.register_release(lab.platform_admin(keys), release)
        env = lab.plugin_env(keys, release, tenant)
        if spec.key == "wallet":
            env.update({"FLARE_WALLET_MODE": "sandbox",
                        "FLARE_WALLET_HEALTH_LISTEN": f"127.0.0.1:{settings.wallet_health}"})
        if spec.key == "realname":
            env.update(realname_environment(keys))
        lab.spawn(state, lab.process_name(spec, version), [str(release["binary"])], env, str(release["binary"]))
        lab.wait_for(lambda a=release["address"]: lab.port_open(int(a.split(":")[1])), 60, spec.plugin_id)
        if spec.key == "task":
            continue
        lab.ensure_installation(state, keys, tenant, version, spec)
        publish_features(keys, tenant, spec)
        lab.approval_check(tenant, release)
        lab.staff_grants_check(state, keys, tenant, release)


def realname_environment(keys):
    """The realname service's private material: encryption and fingerprint keys, its vault, the
    material listener behind the Social gateway and the facts callers' public keys (the Social
    hook's, which this lab does not wire: facts are not part of these cases)."""
    secrets_dir = lab.lab_path("realname")
    secrets_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    vault = secrets_dir / "vault"
    vault.mkdir(exist_ok=True, mode=0o700)
    pii = secrets_dir / "pii-keys.json"
    if not pii.exists():
        lab.write_private(pii, json.dumps({"active_key_id": "v1",
                                           "keys": {"v1": base64.b64encode(secrets.token_bytes(32)).decode()},
                                           "fingerprint_key": base64.b64encode(secrets.token_bytes(32)).decode()}))
    workload = secrets_dir / "workload-keys.json"
    if not workload.exists():
        key = lab.Ed25519Key(secrets_dir / "social-hook-workload.pem")
        lab.write_private(workload, json.dumps([{"kid": "social-hook-realname-lab", "source": "flare-social-hook",
                                                 "publicKeyPem": key.public_pem().decode()}]))
    for path in (pii, workload):
        path.chmod(0o600)
    return {"FLARE_REALNAME_MATERIAL_LISTEN": f"127.0.0.1:{settings.realname_material}",
            "FLARE_REALNAME_PII_KEYS_FILE": str(pii), "FLARE_REALNAME_WORKLOAD_KEYS_FILE": str(workload),
            "FLARE_REALNAME_VAULT_DIRECTORY": str(vault), "FLARE_REALNAME_VAULT_QUOTA_BYTES": str(64 << 20),
            "FLARE_REALNAME_PUBLIC_HTTP_BASE_URL": f"http://127.0.0.1:{settings.dev(50200)}"}


def publish_features(keys, tenant, spec):
    """The tenant publishes the features the installed version declares, each at its default. An
    unpublished feature is closed (its pages, entries and operations never reach a client), and
    a fresh installation publishes none by itself: the console's features page does this."""
    manifest = lab.template_manifest(spec)
    features = [f for f in manifest.get("features") or [] if f.get("defaultEnabled")]
    if not features:
        return []
    owner = lab.org_owner(keys)
    items = [{"key": f"{spec.plugin_id}.{feature['id']}"} for feature in features]
    _, body = owner.call("POST", f"/apps/{tenant}/features/publish", {"publish": items},
                         reason=f"publish the {spec.plugin_id} features")
    lab.evidence(f"features-{spec.key}.json", body)
    lab.log(f"published {spec.plugin_id} features {[item['key'] for item in items]}")
    return items


# ----------------------------------------------------------------- commands --

def down(state):
    for name in list(state.get("processes", {}))[::-1]:
        lab.stop_process(state, name)
    for kind, name in list(state.get("containers", {}).items()):
        label = subprocess.run(["podman", "inspect", "--format", '{{index .Config.Labels "%s"}}' % LABEL, name],
                               text=True, capture_output=True).stdout.strip()
        if label == settings.nonce:
            subprocess.run(["podman", "rm", "--force", "--volumes", name], capture_output=True)
            lab.log(f"removed container {name}")
        state["containers"].pop(kind)
        lab.save_state(state)


def web_access(tenant):
    p = settings.dev
    return {"VITE_TENANT_ID": tenant, "VITE_FLARE_ACCESS_JSON": json.dumps({
        "trustMode": "development", "developmentEndpoints": {
            "wsUrl": f"ws://127.0.0.1:{p(60051)}", "coreApiUrl": f"http://127.0.0.1:{p(50050)}",
            "mediaUrl": f"http://127.0.0.1:{p(50050)}", "socialApiUrl": f"http://127.0.0.1:{p(50200)}"}},
        separators=(",", ":"))}


def command_up(args):
    state = lab.load_state()
    keys = lab.Secrets()
    configuration()
    infrastructure(state, keys)
    ensure_ca()
    trust_roots()
    fronts(state)
    databases(state, keys)
    start_social(state, keys)
    start_core(state, keys)
    start_platform(state, keys)
    tenant = lab.ensure_tenant(state, keys)
    lab.ensure_accounts(state, keys, tenant)
    access = access_plane(state, keys)
    deploy(state, keys, tenant, only=args.only)
    summary = {"tenant": tenant, "bootstrapId": access.get("bootstrapId"), "sandbox": sandbox_origin(),
               "web": web_access(tenant), "webOrigin": settings.web_origin, "platform": f"http://{lab.PLATFORM_ADDR}",
               "metrics": {"capability": settings.metrics(19189)}, "accounts": state.get("accounts"),
               "group": state.get("group"), "conversation": state.get("conversation_id")}
    lab.evidence("lab.json", summary)
    print(json.dumps(summary, indent=1, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lab-dir", required=True)
    parser.add_argument("--binaries", help="directory with the built Core, Social, platform, flarectl and plugins")
    parser.add_argument("--lab-script", help="scripts/plugin-online-lab.py of the workspace root")
    parser.add_argument("--port-offset", type=int, default=-36000)
    parser.add_argument("--block", type=int, default=47000)
    parser.add_argument("--web-origin")
    parser.add_argument("command", choices=["up", "status", "down", "web-env"])
    parser.add_argument("--only", action="append",
                        help="deploy only these plugins (task, wallet, redpacket, realname)")
    args = parser.parse_args()
    configure(args.lab_dir, args.binaries, offset=args.port_offset, block=args.block, web_origin=args.web_origin,
              lab_script=args.lab_script)
    state = lab.load_state()
    if args.command == "status":
        print(json.dumps({"processes": {n: lab.process_alive(v["pid"], v["needle"])
                                        for n, v in state.get("processes", {}).items()},
                          "containers": state.get("containers", {})}, indent=1))
    elif args.command == "down":
        down(state)
    elif args.command == "web-env":
        print(json.dumps(web_access(state["app_id"])))
    else:
        command_up(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - one line for the operator, the trace in the log
        if lab is not None and isinstance(error, lab.LabError):
            print(f"[phd-lab] FAILED: {error}", file=sys.stderr)
            sys.exit(1)
        raise
