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


ATTR_FIELDS = {
    "status": ("status", "string"),
    "priority": ("priority", "string"),
    "assignee": ("assignee", "id"),
}


def snapshot_value(record: dict, attr: str):
    f = record.get("fields") or {}
    if attr == "assignee":
        return (f.get("assignee") or {}).get("accountId")
    return (f.get(attr) or {}).get("name")


def _row(key, attr, value, vfrom, vto, source):
    return {"node_id": key, "attr": attr, "value": value,
            "valid_from": vfrom, "valid_to": vto, "source": source}


def fold_attr_history(record: dict):
    f = record.get("fields") or {}
    key = record["key"]
    created = f.get("created")
    changelog = record.get("changelog") or []
    rows: list[dict] = []
    discrepancies: list[dict] = []

    for jira_field, (attr, kind) in ATTR_FIELDS.items():
        # collect this field's events, sorted by timestamp
        evs = []
        for entry in changelog:
            ts = entry.get("created")
            for it in (entry.get("items") or []):
                if it.get("field") == jira_field:
                    evs.append((ts, it))
        # NOTE: lexicographic ts sort assumes a uniform timezone offset (this Jira
        # instance emits +0530 throughout). Not valid for mixed-offset/UTC-normalized data.
        evs.sort(key=lambda x: x[0])

        def val_of(it, side):  # side: "from" | "to"
            return it.get(side) if kind == "id" else it.get(side + "String")

        snap = snapshot_value(record, attr)

        if not evs:
            rows.append(_row(key, attr, snap, created, SENTINEL, "snapshot-seed"))
            continue

        prev_ts = created
        prev_val = val_of(evs[0][1], "from")
        for ts, it in evs:
            if prev_ts != ts:  # skip zero-length intervals
                rows.append(_row(key, attr, prev_val, prev_ts, ts, "changelog"))
            prev_ts, prev_val = ts, val_of(it, "to")
        rows.append(_row(key, attr, prev_val, prev_ts, SENTINEL, "changelog"))

        if prev_val != snap:
            discrepancies.append({"ticket": key, "attr": attr,
                                  "folded": prev_val, "snapshot": snap})

    return rows, discrepancies
