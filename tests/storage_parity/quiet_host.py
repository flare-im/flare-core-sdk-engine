"""Run an ACK benchmark after host builds settle. Save the witness even on failure.

Usage: python3 quiet_host.py witness.json command [arg ...]
Requires ps and the shared sccache daemon used by this workspace. Exit 75 means
discard this database and repeat the arm. Sampling cannot see every short-lived
uncached process; cache requests between samples are covered by the counter.
"""
import json
from pathlib import Path
import re
import subprocess
import sys
import time

INTERVAL = 0.25
QUIET_SECONDS = 10
COMPILER = re.compile(
    r"(rustc|rustdoc|cc|c\+\+|gcc|g\+\+|clang(\+\+)?|cc1(plus)?|collect2|"
    r"ld(64|\.lld|-classic)?|lld|rust-lld|swiftc|swift-frontend|wasm-opt)(-[0-9.]+)?"
)


def active_compilers():
    lines = subprocess.check_output(["ps", "-axo", "stat=,comm="], text=True)
    found = set()
    for line in lines.splitlines():
        state, command = line.strip().split(None, 1)
        name = Path(command).name.strip("()")
        if not state.startswith("Z") and COMPILER.fullmatch(name):
            found.add(name)
    return found


def requests():
    raw = subprocess.check_output(["sccache", "--show-stats", "--stats-format=json"])
    return int(json.loads(raw)["stats"]["compile_requests"])


def main():
    path, *command = sys.argv[1:]
    if not command:
        raise SystemExit(__doc__)
    wait_start = time.monotonic()
    quiet_since = wait_start
    initial = requests()
    while time.monotonic() - quiet_since < QUIET_SECONDS:
        if active_compilers():
            quiet_since = time.monotonic()
        time.sleep(INTERVAL)
        if time.monotonic() - quiet_since >= QUIET_SECONDS:
            count = requests()
            if initial != count:
                quiet_since = time.monotonic()
            initial = count

    start = last = time.monotonic()
    max_gap = 0
    samples = 0
    observed = set()
    with subprocess.Popen(command) as child:
        while True:
            observed.update(active_compilers())
            now = time.monotonic()
            max_gap = max(max_gap, now - last)
            last = now
            samples += 1
            if child.poll() is not None:
                break
            time.sleep(INTERVAL)
    final = requests()
    valid = not observed and initial == final and child.returncode == 0
    report = dict(clean=valid, preflightWaitSeconds=round(start - wait_start, 2),
                  quietSeconds=QUIET_SECONDS, elapsedSeconds=round(last - start, 2),
                  samples=samples, maxSampleGapSeconds=round(max_gap, 3),
                  compilerNames=sorted(observed), sccacheBefore=initial, sccacheAfter=final,
                  commandExit=child.returncode)
    Path(path).write_text(json.dumps(report, indent=2) + "\n")
    return child.returncode or (0 if valid else 75)


if __name__ == "__main__":
    sys.exit(main())
