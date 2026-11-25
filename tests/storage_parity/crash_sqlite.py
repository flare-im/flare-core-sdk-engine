"""Crash the test driver's own SQLite connection inside a targeted write, before COMMIT."""
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time


def contains_marker(wal, marker):
    if not wal.is_file():
        return False
    with wal.open("rb") as frames:
        tail = b""
        while chunk := frames.read(256 * 1024):
            if marker in tail + chunk:
                return True
            tail = chunk[-len(marker):]
    return False


def crash(data_root, pid, client_id, point, target):
    # Only a private database created by withBindings is ever instrumented.
    db_file, = Path(data_root).rglob("flare_im_core_sdk.db")
    marker = (client_id + ":before-commit").encode()
    wal = db_file.with_name(db_file.name + "-wal")
    with sqlite3.connect(db_file, timeout=3) as observer:
        assert observer.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        quoted, = observer.execute("SELECT quote(?)", (client_id,)).fetchone()
        if point in ("outgoing", "incoming"):
            table = "pending_sends" if point == "outgoing" else "messages"
            action = f"AFTER INSERT ON {table}"
            predicate = f"NEW.client_msg_id = {quoted}"
        else:
            cid, user = observer.execute("SELECT quote(?), quote(?)", (target["cid"], target["user"])).fetchone()
            seq = int(target["seq"])
            if point == "before-projection":
                action = "BEFORE UPDATE OF ext ON conversations"
                receipt = """json_extract(NEW.ext, '$."_local.messageProjectionRanges"')"""
                predicate = f"NEW.conversation_id = {cid} AND json_extract({receipt}, '$.materializedSeq') = {seq}"
            elif point == "before-cursor":
                # The repository uses INSERT ... ON CONFLICT DO UPDATE, also for an existing row.
                action = "BEFORE INSERT ON sync_conversation_cursors"
                predicate = f"NEW.user_id = {user} AND NEW.conversation_id = {cid} AND NEW.last_seq = {seq}"
            else:
                raise ValueError(f"unknown crash point {point}")
        # Exceed the production 64 MiB SQLite page cache in this disposable transaction to spill
        # the marker into uncommitted WAL frames. The final SELECT holds the writer
        # inside the trigger long enough for observation; it has a finite input.
        observer.executescript(f"""
            CREATE TABLE parity_spill (marker TEXT, padding BLOB);
            CREATE TRIGGER parity_crash {action}
            WHEN {predicate} BEGIN
                INSERT INTO parity_spill
                SELECT {quoted} || ':before-commit', zeroblob(1000)
                FROM (WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 98304)
                      SELECT i FROM n);
                SELECT max(i) FROM (
                    WITH RECURSIVE gate(i) AS (
                        VALUES(0) UNION ALL SELECT i+1 FROM gate WHERE i < 50000000
                    ) SELECT i FROM gate
                );
            END;
        """)
        try:
            print("armed", flush=True)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if contains_marker(wal, marker):
                    break
                time.sleep(0.02)
            else:
                raise RuntimeError("no staged WAL frame")
            assert not observer.execute("SELECT 1 FROM pending_sends WHERE client_msg_id=?", (client_id,)).fetchone()
            message = observer.execute("SELECT 1 FROM messages WHERE client_msg_id=?", (client_id,)).fetchone()
            assert bool(message) == (point in ("before-projection", "before-cursor"))

            # Leave more than two 200 ms receipt coalescing windows with the receiver live.
            if point == "incoming":
                time.sleep(0.5)
            os.kill(pid, signal.SIGKILL)
            # Wait for rollback/recovery before removing the fixture. A committed marker
            # here would mean the process was killed too late.
            observer.execute("BEGIN IMMEDIATE")
            assert not observer.execute("SELECT 1 FROM parity_spill LIMIT 1").fetchone()
            observer.commit()
            print("uncommitted WAL frame observed; writer killed", flush=True)
        except BaseException:
            # Release a writer stuck in the fixture before dropping its private trigger.
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise
        finally:
            observer.executescript("DROP TRIGGER parity_crash; DROP TABLE parity_spill;")


if __name__ == "__main__":
    crash(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], json.loads(sys.argv[5]))
