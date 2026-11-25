"""Read one consistent view of a private native receiver before SDK recovery."""
import json
from pathlib import Path
import sqlite3
import sys


def snapshot(data_root, user, cid):
    db_file, = Path(data_root).rglob("flare_im_core_sdk.db")
    with sqlite3.connect(db_file.as_uri() + "?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        messages = db.execute(
            "SELECT client_msg_id, server_id, conversation_seq FROM messages "
            "WHERE conversation_id=? ORDER BY conversation_seq", (cid,)
        ).fetchall()
        last_id, sender, at, preview, unread, last_read, max_seq, ext = db.execute(
            "SELECT last_message_id, last_sender_id, last_message_at, last_message_preview, unread_count, last_read_seq, max_seq, ext "
            "FROM conversations WHERE conversation_id=?", (cid,)
        ).fetchone()
        cursor = db.execute(
            "SELECT last_seq, synced_at FROM sync_conversation_cursors WHERE user_id=? AND conversation_id=?",
            (user, cid)
        ).fetchone()
        outbox = db.execute(
            "SELECT client_msg_id, conversation_id FROM pending_sends ORDER BY enqueued_at_ms, rowid"
        ).fetchall()
    return dict(messages=messages, lastId=last_id, lastSender=sender, lastAt=at, preview=preview, unread=unread, readSeq=last_read, maxSeq=max_seq,
                pendingRead=json.loads(ext or "{}").get("_local.pendingReadSeq"),
                receipt=json.loads(json.loads(ext or "{}").get("_local.messageProjectionRanges", "null")),
                cursor=dict(seq=cursor[0], syncedAt=cursor[1]) if cursor else None, outbox=outbox)


if __name__ == "__main__":
    print(json.dumps(snapshot(*sys.argv[1:])))
