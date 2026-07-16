"""Stage 2b temporal fold: one ticket record -> event rows + attr_history rows.

Pure, deterministic, no I/O. The fold consumes BOTH the snapshot fields and the
changelog (see fold_attr_history) so early-date and never-changed values are not
lost.
"""
from __future__ import annotations

SENTINEL = "9999-12-31"


def extract_events(record: dict) -> list[dict]:
    key = record["key"]
    out: list[dict] = []
    for entry in (record.get("changelog") or []):
        ts = entry.get("created")
        author = (entry.get("author") or {}).get("accountId")
        for it in (entry.get("items") or []):
            out.append({
                "ticket_id": key, "ts": ts, "author": author,
                "field": it.get("field"),
                "from_id": it.get("from"), "from_val": it.get("fromString"),
                "to_id": it.get("to"), "to_val": it.get("toString"),
            })
    return out
