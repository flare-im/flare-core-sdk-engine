#!/usr/bin/env python3
"""Isolated local stack for the bootstrap and access-console acceptance (access_acceptance.py).

Everything live_stack.py starts (one Cell, Core and Social), moved to its own port block,
containers and directory, plus what the access control plane needs:

- four signaling gateways, one per access point (a gateway reports the access point it serves);
- flare-capability (client policy projection, tenant notices) and flare-social-control;
- flare-platform as three processes: --role=api, --role=worker, --role=prober;
- the Social gateway's admin partition in front of the platform;
- one Edge per access point, rendered from the flare-deploy production template, behind an SNI
  router: each access point needs its own address, which a single machine has only by name;
- RustFS containers: one per simulated cloud mirror (TLS in front, private CA) and the Cell's own.

Every listener binds the loopback address. Names resolve for real: macOS answers *.localhost.
Clients trust the private CA through the SDK's `tls_ca_cert_path` setting; no system trust store
or system setting is touched.

Production manifests fix wss://{host}{path} on port 443, which an unprivileged process here cannot
bind on loopback. So the SNI router sits on a high loopback port: the prober reaches it through
FLARE_ACCESS_PROBE_HTTPS_PORT, the test client through AccessLab (examples/access_lab_client.rs).

  python3 scripts/access_stack.py up
  python3 scripts/access_stack.py status
  python3 scripts/access_stack.py ctl stop|start|restart|kill <process>
  python3 scripts/access_stack.py mirror stop|start <a|b|c>   # the mirror's container and TLS front
  python3 scripts/access_stack.py ap stop|start all|<ap-id>...  # an access point's Edge and gateway
  python3 scripts/access_stack.py down [--destroy]

Secrets stay in ~/.flare-access-lab (mode 0700); nothing secret reaches the repository or the logs.
"""
import argparse
import base64
import dataclasses
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.sax.saxutils

LAB = Path(os.environ.get("FLARE_ACCESS_LAB_DIR", "~/.flare-access-lab")).expanduser()
os.environ.setdefault("LIVE_STACK_PREFIX", "flare-access-lab")
os.environ.setdefault("LIVE_STACK_PORT_SHIFT", "4000")
os.environ.setdefault("LIVE_SMOKE_DIR", str(LAB / "stack"))

sys.path.insert(0, str(Path(__file__).resolve().parent))
import live_stack as ls  # noqa: E402

sys.path.insert(0, str(ls.ROOT / "flare-deploy" / "edge"))
import render_prod  # noqa: E402

SOCIAL = ls.SOCIAL
PG_DATABASE_PLATFORM = "flare_accesslab_platform"
PLATFORM_API, PLATFORM_WORKER, PLATFORM_PROBER = ls.port(41088), ls.port(41089), ls.port(41087)
SOCIAL_CONTROL = ls.port(41206)
SOCIAL_CONTROL_HTTP = ls.port(41207)
CAPABILITY = ls.port(41110)
SNI_PORT = ls.port(43440)
HOST_SUFFIX = "accesslab.localhost"
REGION, CELL = "cn-h", "cell-h1"
TOKEN_TTL_SECONDS = 6 * 3600

ls.GATEWAYS.clear()
ls.GATEWAYS.update({
    "gw-a1": (ls.port(42051), ls.port(42060), ls.port(43551), "h-gw-a1", "ap-a1"),
    "gw-a2": (ls.port(42151), ls.port(42160), ls.port(43552), "h-gw-a2", "ap-a2"),
    "gw-b1": (ls.port(42251), ls.port(42260), ls.port(43553), "h-gw-b1", "ap-b1"),
    "gw-b2": (ls.port(42351), ls.port(42360), ls.port(43554), "h-gw-b2", "ap-b2"),
})
for _name in ("gw-a", "gw-b"):
    ls.CORE_SERVICES.pop(_name)
for _name, _gateway in ls.GATEWAYS.items():
    ls.CORE_SERVICES[_name] = ("flare-signaling-gateway", "signaling-gateway", _gateway[0])
ls.CORE_SERVICES["capability"] = ("flare-capability", "capability", CAPABILITY)
ls.SOCIAL_SERVICES["social-control"] = ("flare-social-control", "social-control", SOCIAL_CONTROL)


@dataclasses.dataclass(frozen=True)
class AccessPointSite:
    ap_id: str
    gateway: str
    tls_port: int
    http_ports: tuple
    health_port: int
    udp_port: int

    @property
    def host(self):
        return f"{self.ap_id}.{HOST_SUFFIX}"

    @property
    def edge(self):
        return f"edge-{self.ap_id}"


SITES = {
    "ap-a1": AccessPointSite("ap-a1", "gw-a1", ls.port(43441), (ls.port(43451), ls.port(43461)), ls.port(43471), ls.port(43481)),
    "ap-a2": AccessPointSite("ap-a2", "gw-a2", ls.port(43442), (ls.port(43452), ls.port(43462)), ls.port(43472), ls.port(43482)),
    "ap-b1": AccessPointSite("ap-b1", "gw-b1", ls.port(43443), (ls.port(43453), ls.port(43463)), ls.port(43473), ls.port(43483)),
    "ap-b2": AccessPointSite("ap-b2", "gw-b2", ls.port(43444), (ls.port(43454), ls.port(43464)), ls.port(43474), ls.port(43484)),
}


@dataclasses.dataclass(frozen=True)
class MirrorSite:
    key: str
    provider: str
    tld: str
    s3_port: int
    tls_port: int

    @property
    def id(self):
        return f"m-{self.key}"

    @property
    def container(self):
        return f"rustfs-{self.key}"

    @property
    def bucket(self):
        return f"flare-bootstrap-{self.key}"

    @property
    def host(self):
        return f"cfg-{self.key}.{self.provider}.{HOST_SUFFIX}"

    @property
    def base_url(self):
        return f"https://{self.host}:{self.tls_port}/flare/v1"

    @property
    def s3_endpoint(self):
        return f"http://127.0.0.1:{self.s3_port}"

    @property
    def front(self):
        return f"mirror-{self.key}"


# The provider and top-level domain are what the control plane records for a mirror. All of them
# are simulated here: separate RustFS instances on this machine, not separate clouds.
MIRRORS = {
    "a": MirrorSite("a", "sim-aws", "com", ls.port(43901), ls.port(43611)),
    "b": MirrorSite("b", "sim-aliyun", "cn", ls.port(43902), ls.port(43612)),
    "c": MirrorSite("c", "sim-tencent", "net", ls.port(43903), ls.port(43613)),
}
CELL_S3_PORT = ls.port(43904)
CELL_BUCKET = "flare-bootstrap"
# The third-domain DoH pointer. The resolver and the rescue mirror it points to during an incident
# are served by access_acceptance.py itself, only while a case needs them; the platform audits the
# record against this resolver and every manifest lists it as the last source.
DOH_ZONE = f"boot.{HOST_SUFFIX}"
DOH_HOST = f"doh.{HOST_SUFFIX}"
DOH_PORT = ls.port(43630)
DOH_RESOLVER = f"https://{DOH_HOST}:{DOH_PORT}/dns-query"
RESCUE_MIRROR_HOST = f"cfg-rescue.sim-rescue.{HOST_SUFFIX}"
RESCUE_MIRROR_PORT = ls.port(43631)
EXTRA_PORTS = sorted({PLATFORM_API, PLATFORM_WORKER, PLATFORM_PROBER, SOCIAL_CONTROL, CAPABILITY, CELL_S3_PORT, SNI_PORT,
                      DOH_PORT, RESCUE_MIRROR_PORT,
                      *(g[0] + 1 for g in ls.GATEWAYS.values()),
                      *(p for g in ls.GATEWAYS.values() for p in g[:3]),
                      *(m.s3_port for m in MIRRORS.values()), *(m.tls_port for m in MIRRORS.values()),
                      *(s.tls_port for s in SITES.values()), *(s.health_port for s in SITES.values()),
                      *(s.udp_port for s in SITES.values()), *(p for s in SITES.values() for p in s.http_ports)})
ls.ALL_PORTS = sorted({*ls.ALL_PORTS, *EXTRA_PORTS})


def lab_path(*parts):
    return LAB.joinpath(*parts)


def lab_secret(name, factory):
    file = lab_path("secrets", name)
    if not file.exists():
        ls.write_private(file, factory())
    return file.read_text().strip()


class Lab:
    """Secrets beyond the Core and Social ones of live_stack.Secrets."""

    def __init__(self):
        lab_path("secrets").mkdir(parents=True, exist_ok=True, mode=0o700)
        self.core = ls.Secrets()
        self.iam = ls.Key("iam-signing")
        self.db = {role: lab_secret(f"db-{role}", lambda: secrets.token_hex(16))
                   for role in ("accesslab_platform_api", "accesslab_platform_worker", "accesslab_platform_migrate")}
        self.access_cipher = lab_secret("access-credential-key",
                                        lambda: base64.b64encode(secrets.token_bytes(32)).decode())
        self.rustfs = {key: (lab_secret(f"rustfs-{key}-access", lambda: "accesslab" + secrets.token_hex(8)),
                             lab_secret(f"rustfs-{key}-secret", lambda: secrets.token_urlsafe(24)))
                       for key in [*MIRRORS, "cell"]}
        self.admin_initial = lab_secret("staff-admin-initial", lambda: "Lab-" + secrets.token_urlsafe(18))
        self.admin_password = lab_secret("staff-admin-password", lambda: "Lab-" + secrets.token_urlsafe(18))

    def platform_url(self, role):
        return f"postgres://{role}:{self.db[role]}@127.0.0.1:{ls.PG_PORT}/{PG_DATABASE_PLATFORM}"


# ------------------------------------------------------------------- trust --
def flarectl(*arguments):
    binary = ls.TARGET / "flarectl"
    ls.require(binary.exists(), f"missing binary {binary}")
    return ls.run([str(binary), *map(str, arguments)])


def trust_ready():
    """The two offline root keys. `trust-roots.json` is what apps and the control plane embed."""
    directory = lab_path("trust")
    if not (directory / "trust-roots.json").exists():
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        flarectl("trust", "init", "--output-dir", directory, "--unencrypted")
        ls.log(f"created offline root keys in {directory}")
    return json.loads((directory / "trust-roots.json").read_text())


# --------------------------------------------------------------------- tls --
def tls_dir():
    directory = lab_path("tls")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    return directory


def ca_file():
    return tls_dir() / "ca.crt"


def ensure_ca():
    directory = tls_dir()
    if ca_file().exists():
        return
    openssl = ls.openssl()
    ls.run([openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", str(directory / "ca.key")])
    os.chmod(directory / "ca.key", 0o600)
    ls.run([openssl, "req", "-x509", "-new", "-key", str(directory / "ca.key"), "-sha256", "-days", "365",
            "-subj", "/CN=Flare access lab CA", "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-out", str(ca_file())])
    ls.log("created the private CA")


def issue_certificate(name, hosts, days=200):
    """A leaf certificate of the private CA; (certificate, key) paths."""
    directory = tls_dir()
    certificate, key = directory / f"{name}.crt", directory / f"{name}.key"
    if certificate.exists() and key.exists():
        return certificate, key
    openssl = ls.openssl()
    ls.run([openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", str(key)])
    os.chmod(key, 0o600)
    extensions = directory / f"{name}.ext"
    extensions.write_text("basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\n"
                          "extendedKeyUsage=serverAuth\nsubjectAltName="
                          + ",".join(f"DNS:{host}" for host in hosts) + "\n")
    request = directory / f"{name}.csr"
    ls.run([openssl, "req", "-new", "-key", str(key), "-subj", f"/CN={hosts[0]}", "-out", str(request)])
    ls.run([openssl, "x509", "-req", "-in", str(request), "-CA", str(ca_file()), "-CAkey", str(directory / "ca.key"),
            "-CAcreateserial", "-days", str(days), "-sha256", "-extfile", str(extensions), "-out", str(certificate)])
    request.unlink()
    extensions.unlink()
    return certificate, key


# ---------------------------------------------------------------- rustfs ----
def rustfs_container(state, lab, key, s3_port):
    name = f"rustfs-{key}"
    access, secret_key = lab.rustfs[key]
    volume = f"{ls.PREFIX}-{name}-data"
    ls.container(state, name, "docker.io/rustfs/rustfs:latest",
                 ["-p", f"127.0.0.1:{s3_port}:9000", "-e", f"RUSTFS_ACCESS_KEY={access}",
                  "-e", f"RUSTFS_SECRET_KEY={secret_key}", "-e", "RUSTFS_ADDRESS=:9000",
                  "-e", "RUSTFS_CONSOLE_ENABLE=false", "-e", "RUSTFS_HEALTH_ENDPOINT_ENABLE=true",
                  "-v", f"{volume}:/data"])
    if volume not in state.setdefault("volumes", []):
        state["volumes"].append(volume)
        ls.save_state(state)


def s3_request(method, endpoint, access, secret_key, path, body=b"", query="", headers=None, timeout=10):
    """A Signature Version 4 request, path-style. (status, body, headers)."""
    parsed = urllib.parse.urlsplit(endpoint)
    host = parsed.netloc
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
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as error:
        return error.code, error.read(), dict(error.headers)
    except OSError as error:
        return 0, str(error).encode(), {}


CORS = """<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>
<AllowedMethod>HEAD</AllowedMethod><AllowedHeader>*</AllowedHeader><ExposeHeader>ETag</ExposeHeader>
<MaxAgeSeconds>600</MaxAgeSeconds></CORSRule></CORSConfiguration>"""


def read_policy(bucket):
    return json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["s3:GetObject"],
        "Resource": [f"arn:aws:s3:::{bucket}/*"]}]})


def prepare_bucket(endpoint, credentials, bucket, cors=True):
    """What an operator does once per cloud bucket: the worker's credential is write-only."""
    access, secret_key = credentials
    outcome = {}
    status, body, _ = s3_request("PUT", endpoint, access, secret_key, f"/{bucket}")
    ls.require(status in (200, 409), f"create bucket {bucket}: {status} {body[:200]!r}")
    outcome["bucket"] = status
    policy = read_policy(bucket).encode()
    status, body, _ = s3_request("PUT", endpoint, access, secret_key, f"/{bucket}", body=policy, query="policy=",
                                 headers={"content-type": "application/json"})
    ls.require(status in (200, 204), f"bucket policy {bucket}: {status} {body[:300]!r}")
    outcome["policy"] = status
    if cors:
        document = CORS.encode()
        digest = base64.b64encode(hashlib.md5(document).digest()).decode()
        status, body, _ = s3_request("PUT", endpoint, access, secret_key, f"/{bucket}", body=document, query="cors=",
                                     headers={"content-type": "application/xml", "content-md5": digest})
        outcome["cors"] = status
        outcome["corsBody"] = body[:200].decode(errors="replace") if status not in (200, 204) else ""
    return outcome


def mirrors_up(state, lab):
    for key, site in MIRRORS.items():
        rustfs_container(state, lab, key, site.s3_port)
    rustfs_container(state, lab, "cell", CELL_S3_PORT)
    report = {}
    for key, site in MIRRORS.items():
        ls.wait_for(lambda: ls.http("GET", f"{site.s3_endpoint}/health/ready")[0] == 200, 90, f"RustFS {key}")
        report[key] = prepare_bucket(site.s3_endpoint, lab.rustfs[key], site.bucket)
    ls.wait_for(lambda: ls.http("GET", f"http://127.0.0.1:{CELL_S3_PORT}/health/ready")[0] == 200, 90, "RustFS cell")
    ls.log(f"mirror buckets: {json.dumps(report)}")
    state["mirrorBuckets"] = report
    ls.save_state(state)


# ------------------------------------------------------------------ nginx ----
def nginx_binary():
    return shutil.which("nginx") or "/opt/homebrew/bin/nginx"


def nginx_process(state, name, source):
    directory = ls.path("nginx", name)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    file = directory / "nginx.conf"
    file.write_text(source)
    checked = ls.run([nginx_binary(), "-t", "-c", str(file), "-p", str(directory)], check=False)
    ls.require(checked.returncode == 0, f"nginx {name}: {checked.stderr[-800:]}")
    ls.spawn(state, name, [nginx_binary(), "-c", str(file), "-p", str(directory), "-g", "daemon off;"], {})


def mirror_front_source(site):
    certificate, key = issue_certificate(f"mirror-{site.key}", [site.host])
    directory = ls.path("nginx", site.front)
    return f"""worker_processes 1;
pid {directory}/nginx.pid;
error_log {directory}/error.log warn;
events {{ worker_connections 512; }}
http {{
    log_format access '$msec $status $request_method $uri $body_bytes_sent';
    access_log {directory}/access.log access;
    client_body_temp_path {directory}/body;
    proxy_temp_path {directory}/proxy;
    fastcgi_temp_path {directory}/fastcgi;
    uwsgi_temp_path {directory}/uwsgi;
    scgi_temp_path {directory}/scgi;
    server {{
        listen 127.0.0.1:{site.tls_port} ssl;
        server_name {site.host};
        ssl_certificate {certificate};
        ssl_certificate_key {key};
        location /flare/v1/ {{
            proxy_pass http://127.0.0.1:{site.s3_port}/{site.bucket}/flare/v1/;
            proxy_set_header Host 127.0.0.1:{site.s3_port};
            proxy_http_version 1.1;
            proxy_set_header Connection "";
        }}
        location / {{ return 404; }}
    }}
}}
"""


def edge_source(site):
    certificate, key = issue_certificate(f"edge-{site.ap_id}", [site.host])
    gateway = ls.GATEWAYS[site.gateway]
    directory = ls.path("nginx", site.edge)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    base = render_prod.load_config(ls.ROOT / "flare-deploy/templates/edge/single.example.toml")
    config = dataclasses.replace(
        base, tls_certificate=str(certificate), tls_private_key=str(key), ap_id=site.ap_id, cell_id=CELL,
        local_dev_hosts=(site.host,), public_https_port=SNI_PORT,
        upstreams={"management": f"127.0.0.1:{ls.port(41208)}", "social": f"127.0.0.1:{ls.SOCIAL_API}",
                   "core_api": f"127.0.0.1:{ls.API}", "websocket": f"127.0.0.1:{gateway[0]}",
                   "quic": f"127.0.0.1:{gateway[0] + 1}", "media": f"127.0.0.1:{CELL_S3_PORT}"})
    source = render_prod.render(config)
    (directory / "resolver.conf").write_text("resolver 127.0.0.1 valid=30s ipv6=off;\n")
    for old, new in ((render_prod.RESOLVER_INCLUDE, str(directory / "resolver.conf")),
                     ("listen 80 default_server;", f"listen 127.0.0.1:{site.http_ports[0]} default_server;"),
                     ("listen 80;", f"listen 127.0.0.1:{site.http_ports[1]};"),
                     ("listen 443 ssl default_server;", f"listen 127.0.0.1:{site.tls_port} ssl default_server;"),
                     ("listen 443 ssl;", f"listen 127.0.0.1:{site.tls_port} ssl;"),
                     ("listen 443 udp;", f"listen 127.0.0.1:{site.udp_port} udp;"),
                     (render_prod.HEALTH_LISTEN, f"127.0.0.1:{site.health_port}"),
                     ("pid /tmp/flare-edge.pid;", f"pid {directory}/nginx.pid;"),
                     ("worker_processes auto;", "worker_processes 1;"),
                     ("worker_connections 16384;", "worker_connections 1024;"),
                     ("worker_rlimit_nofile 32768;", ""),
                     ("/tmp/flare-edge-client-body", f"{directory}/client-body"),
                     ("/tmp/flare-edge-proxy", f"{directory}/proxy"),
                     ("error_log /dev/stderr crit;", f"error_log {directory}/error.log warn;"),
                     ("access_log /dev/stdout edge_safe;", f"access_log {directory}/access.log edge_safe;")):
        source = source.replace(old, new)
    return source


def sni_source():
    directory = ls.path("nginx", "sni-router")
    entries = "\n".join(f"        {site.host} 127.0.0.1:{site.tls_port};" for site in SITES.values())
    return f"""worker_processes 1;
pid {directory}/nginx.pid;
error_log {directory}/error.log warn;
events {{ worker_connections 2048; }}
stream {{
    map $ssl_preread_server_name $site {{
{entries}
        default 127.0.0.1:1;
    }}
    server {{
        listen 127.0.0.1:{SNI_PORT};
        ssl_preread on;
        proxy_pass $site;
        proxy_connect_timeout 2s;
        proxy_timeout 3h;
    }}
}}
"""


def fronts_up(state):
    ensure_ca()
    for site in MIRRORS.values():
        if not ls.live(state, site.front):
            ls.path("nginx", site.front).mkdir(parents=True, exist_ok=True, mode=0o700)
            nginx_process(state, site.front, mirror_front_source(site))
    for site in SITES.values():
        if not ls.live(state, site.edge):
            nginx_process(state, site.edge, edge_source(site))
    if not ls.live(state, "sni-router"):
        ls.path("nginx", "sni-router").mkdir(parents=True, exist_ok=True, mode=0o700)
        nginx_process(state, "sni-router", sni_source())
    ls.wait_for(lambda: ls.port_open(next(iter(SITES.values())).tls_port), 20, "Edge")


# --------------------------------------------------------------- platform ----
def platform_env(lab, role):
    roots = json.dumps(trust_ready(), separators=(",", ":"))
    primary = "accesslab_platform_worker" if role == "prober" else "accesslab_platform_api"
    env = {
        "DATABASE_URL": lab.platform_url("accesslab_platform_worker" if role == "worker" else primary),
        "FLARE_PLATFORM_SIGNING_KEY": lab.core.platform.pem_b64(),
        "FLARE_IAM_SIGNING_KEY": lab.iam.pem_b64(), "FLARE_IAM_KEY_ID": "iam-1",
        "CONTROL_CREDENTIALS_ENABLED": "true", "FLARE_API_KEY_PEPPER": lab.core.pepper,
        "CONTROL_SOCIAL_ENABLED": "true",
        "SOCIAL_CONTROL_GRPC": f"http://127.0.0.1:{SOCIAL_CONTROL}",
        "SOCIAL_GATEWAY_URL": f"http://127.0.0.1:{SOCIAL_CONTROL_HTTP}",
        "CORE_CAPABILITY_GRPC": f"http://127.0.0.1:{CAPABILITY}",
        "CORE_ONLINE_GRPC": f"http://127.0.0.1:{ls.port(41061)}",
        "BIND_ADDRESS": f"127.0.0.1:{dict(api=PLATFORM_API, worker=PLATFORM_WORKER, prober=PLATFORM_PROBER)[role]}",
        "APP_MODE": "multi", "APP_ENTRY_HOSTS": "localhost,127.0.0.1", "APP_DEFAULT_TENANT": "default",
        "TRUSTED_PROXIES": "", "FLARE_PLUGIN_PUBLISHER_KEYS": "[]", "FLARE_PLUGIN_HOST_VERSION": "1.0.0",
        "FLARE_INTERNAL_CTX_PUBLIC_KEYS": json.dumps({"gateway": base64.b64decode(lab.core.gateway_ctx.public_pem_b64()).decode()},
                                                     separators=(",", ":")),
        "FLARE_ACCESS_ENVIRONMENT": "production", "FLARE_ACCESS_CREDENTIAL_ENCRYPTION_KEY": lab.access_cipher,
        "FLARE_ACCESS_TRUST_ROOTS": roots, "FLARE_ACCESS_EXTRA_CA_FILE": str(ca_file()),
        "FLARE_ACCESS_PROBE_REGION": REGION, "FLARE_ACCESS_PROBE_HTTPS_PORT": str(SNI_PORT),
        "FLARE_ACCESS_DOH_ZONE": DOH_ZONE, "FLARE_ACCESS_DOH_RESOLVERS": DOH_RESOLVER,
        "RUST_LOG": "info,sqlx=warn,async_nats=warn",
    }
    if role == "api":
        env["FLARE_PLATFORM_WORKER_DATABASE_URL"] = lab.platform_url("accesslab_platform_worker")
    return env


def platform_process(state, lab, role):
    binary = ls.TARGET / "flare-platform"
    ls.require(binary.exists(), f"missing binary {binary}")
    ls.spawn(state, f"platform-{role}", [str(binary), f"--role={role}"], platform_env(lab, role))
    port = dict(api=PLATFORM_API, worker=PLATFORM_WORKER, prober=PLATFORM_PROBER)[role]
    known = state["processes"][f"platform-{role}"]

    def ready():
        ls.require(ls.process_alive(known["pid"], known["needle"]), f"platform-{role} exited; see {known['log']}")
        return ls.port_open(port)
    ls.wait_for(ready, 60, f"platform-{role} :{port}")


def platform_database(state, lab):
    if state.get("platformInitialized"):
        return
    keys = lab.core
    for role, password in lab.db.items():
        exists = ls.psql(keys, f"SELECT 1 FROM pg_roles WHERE rolname='{role}'")
        verb = "ALTER" if exists == "1" else "CREATE"
        ls.psql(keys, f"{verb} ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE "
                      "NOREPLICATION NOBYPASSRLS;")
    binary = str(ls.TARGET / "flare-platform")
    roles = ls.run([binary, "db-roles", "--migrator", "accesslab_platform_migrate"], env={"PATH": os.defpath}).stdout
    ls.psql(keys, "SET client_min_messages = warning;\n" + roles)
    ls.psql(keys, "SET client_min_messages = warning;\n"
                  "GRANT platform_api TO accesslab_platform_api WITH INHERIT TRUE;\n"
                  "GRANT platform_worker TO accesslab_platform_worker WITH INHERIT TRUE;")
    if ls.psql(keys, f"SELECT 1 FROM pg_database WHERE datname='{PG_DATABASE_PLATFORM}'") != "1":
        ls.psql(keys, f'CREATE DATABASE "{PG_DATABASE_PLATFORM}" OWNER accesslab_platform_migrate ENCODING \'UTF8\' '
                      "TEMPLATE template0;")
        ls.psql(keys, f'REVOKE CONNECT, TEMPORARY ON DATABASE "{PG_DATABASE_PLATFORM}" FROM PUBLIC;\n'
                + "".join(f'GRANT CONNECT ON DATABASE "{PG_DATABASE_PLATFORM}" TO "{role}";\n' for role in lab.db))
    result = subprocess.run([binary, "initialize"], cwd=str(ls.RUN), text=True, capture_output=True,
                            env={"PATH": os.defpath, "DATABASE_URL": lab.platform_url("accesslab_platform_migrate")})
    ls.require(result.returncode == 0, f"platform initialize: {result.stdout[-500:]} {result.stderr[-800:]}")
    ls.log(f"platform initializer: {result.stdout.strip()}")
    result = subprocess.run([binary, "bootstrap"], cwd=str(ls.RUN), text=True, capture_output=True,
                            env={"PATH": os.defpath, "DATABASE_URL": lab.platform_url("accesslab_platform_migrate"),
                                 "FLARE_IAM_SIGNING_KEY": lab.iam.pem_b64(), "FLARE_IAM_KEY_ID": "iam-1",
                                 "BOOTSTRAP_USERNAME": "accesslab_admin", "BOOTSTRAP_PASSWORD": lab.admin_initial})
    ls.require(result.returncode == 0, f"platform bootstrap: {result.stderr[-800:]}")
    ls.log("platform bootstrap done")
    state["platformInitialized"] = True
    ls.save_state(state)


# ------------------------------------------------------------- core, social --
base_core_environment = ls.core_environment
base_social_environment = ls.social_environment


def core_environment(keys):
    env = base_core_environment(keys)
    env.update({
        # CONNECT_ACK reports the serial only with the tenant runtime database configured.
        "ACCESS_GATEWAY_TENANT_RUNTIME_POSTGRES_URL": keys.db_url("core"),
        "FLARE_PLATFORM_PUBLIC_KEY": keys.platform.public_pem_b64(),
        "FLARE_CAPABILITY_NOTICE_REDIS_URL": f"redis://127.0.0.1:{ls.REDIS_PORT}/1",
    })
    return env


def social_environment(keys):
    env = base_social_environment(keys)
    env.update({
        "FLARE_ADMIN_PARTITION_ENABLED": "true",
        "FLARE_PLATFORM_URL": f"http://127.0.0.1:{PLATFORM_API}",
        "FLARE_ADMIN_NATS_URL": f"nats://127.0.0.1:{ls.NATS_PORT}",
    })
    return env


ls.core_environment = core_environment
ls.social_environment = social_environment


def gateway_overrides(name):
    ws, grpc, metrics, gateway_id, ap_id = ls.GATEWAYS[name]
    return {"PORT": str(ws), "GRPC_PORT": str(grpc), "GATEWAY_ID": gateway_id, "GATEWAY_AP_ID": ap_id,
            "ACCESS_GATEWAY_METRICS_ADDRESS": "127.0.0.1", "ACCESS_GATEWAY_METRICS_PORT": str(metrics)}


ls.gateway_overrides = gateway_overrides


def long_lived_tokens():
    """Clients here cannot renew a token (renewal is REST through Edge on 443): one token has to
    outlast the longest case."""
    for name in ("api-gateway", "access-gateway"):
        file = ls.path("config", "services", f"{name}.toml")
        file.write_text(file.read_text().replace("token_ttl_seconds = 3600", f"token_ttl_seconds = {TOKEN_TTL_SECONDS}"))


def core_and_social(state, keys):
    ls.configuration()
    long_lived_tokens()
    order = ["signaling-online", "signaling-route", "conversation", "storage-reader", "storage-writer",
             "message-ingest", "orchestrator", "sync-orchestrator", "capability", "api-gateway", "social-user",
             "social-control", "social-gateway", *ls.GATEWAYS]
    for name in order:
        if ls.live(state, name):
            continue
        ls.start_service(state, keys, name)
        ls.wait_service(state, name, timeout=120)


# ---------------------------------------------------------------- commands --
def command_up(_args):
    LAB.mkdir(parents=True, exist_ok=True, mode=0o700)
    ls.ensure_run_dir()
    state = ls.load_state()
    lab = Lab()
    trust_ready()
    ls.infrastructure(state, lab.core)
    ls.databases(state, lab.core)
    platform_database(state, lab)
    core_and_social(state, lab.core)
    mirrors_up(state, lab)
    fronts_up(state)
    for role in ("api", "worker", "prober"):
        if not ls.live(state, f"platform-{role}"):
            platform_process(state, lab, role)
    ls.log("stack is up")


def command_status(_args):
    ls.command_status(None)


def command_ctl(args):
    state = ls.load_state()
    lab = Lab()
    name = args.process
    if args.action in ("stop", "restart", "kill"):
        sig = signal.SIGKILL if args.action == "kill" else signal.SIGTERM
        ls.signal_process(state, name, sig, wait=None if args.action == "kill" else 30)
    if args.action in ("start", "restart"):
        if name.startswith("platform-"):
            platform_process(state, lab, name.removeprefix("platform-"))
        elif name in ls.CORE_SERVICES or name in ls.SOCIAL_SERVICES:
            ls.start_service(state, lab.core, name)
            ls.wait_service(state, name, timeout=120)
        else:
            nginx_start(state, name)


def nginx_start(state, name):
    """An nginx process again from the configuration `up` rendered for it."""
    file = ls.path("nginx", name, "nginx.conf")
    ls.require(file.exists(), f"unknown process {name}")
    ls.spawn(state, name, [nginx_binary(), "-c", str(file), "-p", str(file.parent), "-g", "daemon off;"], {})


def mirror_stop(state, site):
    ls.signal_process(state, site.front, signal.SIGTERM, wait=10)
    ls.run([ls.RUNNER, "stop", "-t", "5", f"{ls.PREFIX}-{site.container}"], check=False)


def mirror_start(state, site):
    ls.run([ls.RUNNER, "start", f"{ls.PREFIX}-{site.container}"])
    ls.wait_for(lambda: ls.http("GET", f"{site.s3_endpoint}/health/ready")[0] == 200, 60, f"RustFS {site.key}")
    nginx_start(state, site.front)


def access_point_stop(state, ap_id):
    """The access point goes away as a whole: its Edge first, so nothing is proxied to a gateway
    that is shutting down, then its gateway."""
    site = SITES[ap_id]
    ls.signal_process(state, site.edge, signal.SIGTERM, wait=10)
    ls.signal_process(state, site.gateway, signal.SIGTERM, wait=30)


def access_point_start(state, ap_id):
    """A node that comes up: its gateway, then the Edge in front of it."""
    site = SITES[ap_id]
    if not ls.live(state, site.gateway):
        ls.start_service(state, Lab().core, site.gateway)
        ls.wait_service(state, site.gateway, timeout=120)
    if not ls.live(state, site.edge):
        nginx_start(state, site.edge)
    ls.wait_for(lambda: ls.port_open(site.tls_port), 20, f"Edge of {ap_id}")


def command_ap(args):
    state = ls.load_state()
    ids = sorted(SITES) if args.ids == ["all"] else args.ids
    unknown = [ap_id for ap_id in ids if ap_id not in SITES]
    ls.require(not unknown, f"unknown access points {unknown}; known: {sorted(SITES)}")
    for ap_id in ids:
        (access_point_stop if args.action == "stop" else access_point_start)(state, ap_id)


def command_mirror(args):
    state = ls.load_state()
    site = MIRRORS[args.key]
    (mirror_stop if args.action == "stop" else mirror_start)(state, site)


def command_down(args):
    ls.command_down(args)
    if args.destroy:
        # All of this describes the databases and buckets that just went away. Certificates are
        # cached by name, so a stale one would be handed to a freshly rotated key.
        state = ls.load_state()
        for key in ("platformInitialized", "mirrorBuckets"):
            state.pop(key, None)
        ls.save_state(state)
        for name in ("clients", "seeds"):
            shutil.rmtree(lab_path(name), ignore_errors=True)
        shutil.rmtree(lab_path("trust", "certs"), ignore_errors=True)
        for file in (lab_path("acceptance.json"), *lab_path("trust").glob("keystate-*.dsse.json"),
                     *lab_path("secrets").glob("pat-*"), *lab_path("secrets").glob("forge-*.json")):
            file.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up").set_defaults(func=command_up)
    sub.add_parser("status").set_defaults(func=command_status)
    ctl = sub.add_parser("ctl")
    ctl.add_argument("action", choices=["stop", "start", "restart", "kill"])
    ctl.add_argument("process")
    ctl.set_defaults(func=command_ctl)
    mirror = sub.add_parser("mirror")
    mirror.add_argument("action", choices=["stop", "start"])
    mirror.add_argument("key", choices=sorted(MIRRORS))
    mirror.set_defaults(func=command_mirror)
    access_points = sub.add_parser("ap", help="stop or start access points (Edge and gateway)")
    access_points.add_argument("action", choices=["stop", "start"])
    access_points.add_argument("ids", nargs="+", metavar="all|ap-id")
    access_points.set_defaults(func=command_ap)
    down = sub.add_parser("down")
    down.add_argument("--destroy", action="store_true")
    down.set_defaults(func=command_down)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
