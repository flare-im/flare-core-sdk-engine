"""Measure first-install cold starts, SQLite batch commits and offline outbox depth."""

import json
import math
import os
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET = "cold_start_baseline"


def build_harness():
    build = subprocess.run(
        ["cargo", "test", "--locked", "--features", "lifecycle-sqlite,e2ee",
         "--test", TARGET, "--no-run", "--message-format=json-render-diagnostics"],
        cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True,
    )
    for line in build.stdout.splitlines():
        record = json.loads(line)
        if (record["reason"] == "compiler-artifact"
                and record["target"]["name"] == TARGET and record["executable"]):
            return record["executable"]
    raise RuntimeError("Cargo did not produce the cold_start_baseline test executable")


def measure(executable, case, root, marker):
    env = dict(os.environ, FLARE_COLD_START_ROOT=str(root))
    env.pop("FLARE_COLD_START_FRESH", None)
    output = subprocess.check_output(
        [executable, "--ignored", "--exact", case, "--nocapture"],
        env=env, cwd=ROOT, text=True,
    )
    for line in output.splitlines():
        if line.startswith(marker):
            return json.loads(line.removeprefix(marker))
    raise RuntimeError(f"{case} produced no measurement")


def main():
    executable = build_harness()
    with tempfile.TemporaryDirectory(prefix="flare-engine-baseline-") as directory:
        root = Path(directory)
        starts = [
            measure(executable, "cold_start_once", root / f"cold-{i}", "COLD_START ")["totalWallUs"]
            for i in range(20)
        ]
        batch = measure(executable, "sqlite_batch_and_offline_queue", root, "SQLITE_BASELINE ")
        reopened = measure(executable, "read_reopened_outbox", root, "REOPENED_OUTBOX ")
        if reopened != batch.pop("pendingIds"):
            raise RuntimeError("Outbox IDs or order changed across process restart")
    print(json.dumps({
        "coldStartSamplesUs": starts,
        "coldStartP95Us": sorted(starts)[math.ceil(0.95 * len(starts)) - 1],
        "reopenedPending": len(reopened),
        **batch,
    }))


if __name__ == "__main__":
    main()
