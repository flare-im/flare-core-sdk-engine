#!/usr/bin/env python3
"""体积预算门禁：WASM 产物有没有悄悄变胖。

为什么单独立一道：包体是**用户直接付钱的性能指标**（首屏要下多少字节、
浏览器要编译多少字节），但它没有任何运行时信号——加一个胖依赖、开一个
本该关掉的 feature，测试全绿、CI 全绿，只有真实用户的首屏慢下来。
`wasm-opt = ["-Oz", ...]` 已经在 release profile 里开着，但没有任何东西
拦得住「优化开着、输入却涨了一倍」。

与性能耗时门禁的关键区别：**体积是确定的，没有 runner 噪声**。同一份源码 +
同一套工具链，产物字节数每次都一样。所以这里可以设紧的阈值，而不像
`flare-transport/scripts/check_perf_baseline.py` 那样必须留 50~100 倍余量。

量两个数，两个都要卡：

- **gzip**：用户实际下载的字节数。是「首屏要等多久」的直接来源。
- **raw**：浏览器要解析和编译的字节数。gzip 再小，解压后的编译成本照付，
  所以不能只卡压缩后的。

预算怎么定：按当轮实测值上浮一档。它**会**因为正常的功能增长而需要上调——
那正是这道门禁的用处：把「包体又涨了」变成一次需要有人点头的显式改动，
而不是没人注意的漂移。上调时把新数字和理由一起写进 BUDGET。

用法：
    python3 scripts/check_wasm_budget.py                # 量默认产物
    python3 scripts/check_wasm_budget.py path/to.wasm   # 量指定文件
"""

import gzip
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "pkg"

# 预算（字节）。数字后面写的是设定时的实测值，方便判断余量还剩多少。
#
# 2026-08-20 实测，同一份源码两个平台：
#   本机 macOS/aarch64（rustc 1.94.1 + wasm-opt 117，清空 pkg 后重跑两次同值）
#       raw 4,759,230 / gzip 1,611,874
#   CI   ubuntu-latest（rustc stable）
#       raw 4,520,778 / gzip 1,572,307     ← 比本机小 5.0%
#
# 预算按**较大的那个**（本机）上浮 10%。这 5% 的平台差就是余量必须留的理由：
# 按 CI 数字卡紧，本机跑同一个脚本就会红；按本机数字卡紧，换台机器又可能红。
# 剩下的余量用来抓「多了一个胖依赖」这种量级的增长——它一次就是几百 KB，
# 不会被 5% 的平台差淹掉。
#
# 定这个数之前排掉过一个假线索：`dist/wasm/` 和 pkg 里躺着 3.37MB 的旧产物，
# 一度看着像「包体两天涨了 41%」。核下来那些文件既没入库、也没有任何代码引用，
# 是本机遗留的陈旧副本（同目录还混着 6 月的 .js glue）。真实基数以本仓唯一那条
# 构建命令（bindings/wasm 的 npm run build）的可复现输出为准。
BUDGET = {
    "raw": 5_235_000,  # 实测 4,759,230 → 余量 +10.0%
    "gzip": 1_773_000,  # 实测 1,611,874 → 余量 +10.0%
}

DEFAULT_ARTIFACT = PKG / "flare_im_core_sdk_bg.wasm"


def human(n: int) -> str:
    return f"{n:,} B ({n / 1024 / 1024:.2f} MB)"


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ARTIFACT

    if not target.exists():
        print(f"✗ 找不到产物：{target}", file=sys.stderr)
        print(
            "  先构建：npm run build"
            "（wasm-pack build --target web --out-dir pkg --out-name flare_im_core_sdk）",
            file=sys.stderr,
        )
        # 产物不在就判红，不跳过：「没量到」和「量了没超」必须是两种结果，
        # 否则构建步骤一旦坏掉，这道门禁会安静地退化成永远绿。
        return 1

    raw_bytes = target.read_bytes()
    # mtime=0 让输出与时间无关，同一份 wasm 每次得到同一个 gzip 字节数。
    gzip_size = len(gzip.compress(raw_bytes, compresslevel=9, mtime=0))
    actual = {"raw": len(raw_bytes), "gzip": gzip_size}

    print(f"体积（{target.name}）：")
    problems = []
    for key in ("raw", "gzip"):
        budget = BUDGET[key]
        used = actual[key] / budget * 100
        mark = "✓" if actual[key] <= budget else "✗"
        print(
            f"  {mark} {key:<5} {human(actual[key]):>26}"
            f"   预算 {human(budget):>26}   用掉 {used:5.1f}%"
        )
        if actual[key] > budget:
            over = actual[key] - budget
            problems.append(
                f"  {key} 超预算 {human(over)}（{used - 100:.1f}%）"
            )

    print()
    if problems:
        print("包体超预算：", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        print("", file=sys.stderr)
        print(
            "  体积没有运行时信号——测试和 CI 都不会因为它变红，只有用户的首屏会变慢。\n"
            "  要么把增长压回去，要么把 BUDGET 连同新数字与理由一起调高：\n"
            "  这道门禁要的就是「包体变大」必须有人点头，而不是没人注意地漂移。",
            file=sys.stderr,
        )
        return 1

    print("  ✓ raw 与 gzip 均在预算内")
    return 0


if __name__ == "__main__":
    sys.exit(main())
