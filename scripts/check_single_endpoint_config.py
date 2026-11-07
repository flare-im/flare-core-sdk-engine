#!/usr/bin/env python3
"""单地址配置门禁：引擎、绑定、平台包、示例里不再出现 `wsUrl` / `gateway_url` 这类单地址接入配置。

接入地址只有两种来源：生产的签名接入清单（`access`），以及开发模式的 `access.developmentEndpoints`
（主接入点 + `accessPoints`）。旧的顶层 `wsUrl` / `httpUrl` / `quicUrl`、社交的 `gateway_url`、
「创建时固化网关地址」都已删除，不留兼容层；这道门禁防止它们从文档、测试替身或示例里长回来。

扫描范围（相对工作区根，即本仓的上一级目录）：
  flare-core-sdk-engine: src bindings tests examples
  flare-core-sdk:        packages sdk-spec examples
  flare-social-sdk:      src bindings packages
  flare-social-examples: apps
  flare-core-examples:   apps

只看各仓 git 认得的文件（已跟踪 + 未忽略的新文件），所以 .gitignore 里的旧克隆、构建产物不在内；
另外显式跳过 node_modules / build / target / .dart_tool / Pods / .build / .gradle 与二进制文件。
不在本机的仓（CI 只检出了本仓）记为跳过，不算失败。

每一处命中归为三类之一：
  dev       允许：开发模式端点（developmentEndpoints / accessPoints 的键、读取它们的地方、给它们取值的
            示例与测试输入）；
  guard     允许：断言旧键已删除、不再出现的测试；
  other-url 不是接入配置：同名但属于别的业务的地址（媒体源地址、卡片头像等）；
其余一律算违规（要清理），脚本以非零退出。新增合法用法时在 RULES 里写明理由，不要放宽通用规则。

用法：
    python3 scripts/check_single_endpoint_config.py          # 只列违规与各类计数
    python3 scripts/check_single_endpoint_config.py --all    # 列出每一处命中及其归类
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PATTERN = re.compile(r"ws_url|wsUrl|gateway_url|gatewayUrl|http_url|httpUrl|quic_url|quicUrl")

SCOPES = {
    "flare-core-sdk-engine": ["src", "bindings", "tests", "examples"],
    "flare-core-sdk": ["packages", "sdk-spec", "examples"],
    "flare-social-sdk": ["src", "bindings", "packages"],
    "flare-social-examples": ["apps"],
    "flare-core-examples": ["apps"],
}

EXCLUDED_DIRS = {"node_modules", "build", "target", ".dart_tool", "Pods", ".build", ".gradle"}
BINARY_SUFFIXES = {
    ".a", ".so", ".dylib", ".dll", ".wasm", ".jar", ".aar", ".png", ".jpg", ".jpeg", ".gif",
    ".webp", ".ico", ".icns", ".ttf", ".otf", ".woff", ".woff2", ".zip", ".gz", ".db", ".sqlite",
}

# 开发模式的标记：命中行本身或它前面 DEV_CONTEXT_LINES 行内出现其一，即视为开发模式端点。
DEV_MARKER = re.compile(
    r"developmentEndpoints|DevelopmentEndpoint|DevelopmentAccessPoint|development_access|_endpoints\("
    r"|accessPoints|FLARE_ACCESS_JSON|FLARE_DEV_(?:WS|QUIC)_URL|LocalDevelopment|kDevWsPort"
)
DEV_CONTEXT_LINES = 12
# 只看命中行本身：读取开发模式端点对象的写法（endpoints["wsUrl"]、endpoints.quic_url 等）。
DEV_READ = re.compile(r"\bendpoints\b|_endpoints\(|\.ws_url\(\)")

# (类别, 路径正则, 行正则, 理由)。行正则对命中行做 search。
RULES = [
    ("other-url", r"(^|/)(native_file_service|progress|dispatch_support)\.rs$", r"source_http_url",
     "媒体下载的来源地址（sourceHttpUrl），不是接入配置"),
    ("other-url", r"flare-social-sdk/src/modules/card/", r"http_url_or_empty",
     "卡片头像地址的校验函数，不是接入配置"),
    ("guard", r"flare-core-sdk-engine/src/client/config\.rs$", r'for key in \[.*"ws_url"',
     "断言 SdkConfig 序列化后不再有旧地址键"),
    ("guard", r"flare-core-sdk-engine/src/client/lifecycle\.rs$",
     r'^\s*"(wsUrl|httpUrl|quicUrl)",\s*$|from_value::<SdkConfigOverlay>\(serde_json::json!\(\{ "wsUrl"',
     "断言 SdkConfigOverlay 不再写出、也不再接受旧地址键"),
    ("guard", r"flare-social-flutter-sdk/test/init_config_test\.dart$", r"^\s*'(wsUrl|quicUrl|httpUrl)',\s*$",
     "断言社交 Flutter 初始化配置不再写出旧地址键"),
    ("dev", r"flare-core-sdk-engine/examples/common/server_message_proof\.rs$", r".",
     "消息证明脚本自己的输入文件格式（flare-sdk-proof-config/1），只映射到 developmentEndpoints"),
    ("dev", r"flare-core-sdk-engine/examples/partition_message_proof\.rs$", r"config\.(ws|http)_url",
     "把证明脚本输入映射到 developmentEndpoints"),
    ("dev", r"flare-core-sdk-engine/examples/[a-z_]+\.rs$",
     r"let ws_url =|ws_url: &str|ws_url = %ws_url|println!\(\"  ws_url: \{ws_url\}\"\)|^\s*&ws_url,$",
     "压测/联调示例从自己的环境变量取地址，只交给 developmentEndpoints"),
    ("dev", r"flare-core-sdk-engine/tests/", r"fn get_ws_url\(\)|\(ws_url: &str|let \(Some\(ws_url\)|let http_url = env_var"
     r"|http_url\.(is_some|clone|as_ref)\(\)|\(&ws_url, |let \(server, ws_url\)|fn ws_url\(&self\)",
     "测试从实验台或环境变量取地址，只交给 developmentEndpoints"),
    ("dev", r"flare-core-sdk-engine/bindings/contract/generated/model_schemas/sdk_config_overlay\.schema\.json$",
     r'"(wsUrl|quicUrl)": \{|DevelopmentEndpoints',
     "生成的 developmentEndpoints / accessPoints 结构定义"),
    ("dev", r"flare-core-sdk-engine/bindings/shared/src/generated/client_config\.rs$",
     r"CLIENT_CONFIG_CONTRACT_JSON",
     "client_config.json 的内嵌副本（地址都在 developmentEndpoints 里）"),
    ("dev", r"flare-(core|social)-sdk/.*\.(rs|dart|ts)$", r"^\s*(///|\*|//).*developmentEndpoints",
     "文档注释描述 developmentEndpoints 的键"),
    ("dev", r"flare-social-examples/apps/flare-social-flutter-app/lib/host/access_config\.dart$",
     r"'(wsUrl|quicUrl)':", "开发模式端点对象的键"),
    ("dev", r"flare-social-examples/apps/flare-social-flutter-app/integration_test/", r"'wsUrl'|\['wsUrl'\]",
     "实验台用例读写开发模式端点"),
    ("dev", r"flare-core-examples/apps/flare-core-flutter-app/(lib/shared/config/access_config|test/support/dev_access)\.dart$",
     r"'(wsUrl|quicUrl)':", "开发模式端点对象的键"),
    ("dev", r"flare-core-examples/apps/flare-core-tauri-app/tests/access-config\.test\.mjs$", r"wsUrl",
     "断言示例的开发模式端点"),
    ("dev", r"flare-core-examples/apps/[^/]+/README\.md$", r"quicUrl",
     "README 说明 developmentEndpoints.quicUrl"),
]
COMPILED_RULES = [(c, re.compile(p), re.compile(l), why) for c, p, l, why in RULES]


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def listed_files(repo: Path, dirs: list[str]) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", *dirs],
        cwd=repo,
        capture_output=True,
        check=True,
    ).stdout
    return sorted({name for name in out.decode().split("\0") if name})


def is_text(path: Path) -> bool:
    if path.suffix.lower() in BINARY_SUFFIXES:
        return False
    with path.open("rb") as handle:
        return b"\0" not in handle.read(8192)


def classify(rel: str, lines: list[str], index: int) -> tuple[str, str]:
    line = lines[index]
    for category, path_re, line_re, why in COMPILED_RULES:
        if path_re.search(rel) and line_re.search(line):
            return category, why
    window = lines[max(0, index - DEV_CONTEXT_LINES) : index + 1]
    if any(DEV_MARKER.search(text) for text in window) or DEV_READ.search(line):
        return "dev", "开发模式端点"
    return "violation", ""


def scan(root: Path):
    hits, skipped = [], []
    for repo_name, dirs in SCOPES.items():
        repo = root / repo_name
        if not (repo / ".git").exists():
            skipped.append(repo_name)
            continue
        present = [d for d in dirs if (repo / d).exists()]
        if not present:
            continue
        for name in listed_files(repo, present):
            if any(part in EXCLUDED_DIRS for part in name.split("/")):
                continue
            path = repo / name
            if not path.is_file() or not is_text(path):
                continue
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            rel = f"{repo_name}/{name}"
            for index, line in enumerate(lines):
                if PATTERN.search(line):
                    category, why = classify(rel, lines, index)
                    hits.append((category, rel, index + 1, line.strip(), why))
    return hits, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--all", action="store_true", help="list every hit with its category")
    args = parser.parse_args()

    hits, skipped = scan(workspace_root())
    counts: dict[str, int] = {}
    for category, *_ in hits:
        counts[category] = counts.get(category, 0) + 1
    for repo in skipped:
        print(f"skip: {repo} is not checked out here")
    if args.all:
        for category, rel, line_no, text, why in hits:
            note = f"  [{why}]" if why else ""
            print(f"{category:9} {rel}:{line_no}: {text[:160]}{note}")
    violations = [hit for hit in hits if hit[0] == "violation"]
    if violations and not args.all:
        print("single-address config that must go:")
        for _, rel, line_no, text, _ in violations:
            print(f"  {rel}:{line_no}: {text[:160]}")
    summary = ", ".join(f"{name} {counts.get(name, 0)}" for name in ("dev", "guard", "other-url", "violation"))
    print(f"single-endpoint gate: {len(hits)} hits ({summary})")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
