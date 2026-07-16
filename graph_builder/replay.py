"""Stage 2b temporal fold: one ticket record -> event rows + attr_history rows.

Pure, deterministic, no I/O. The fold consumes BOTH the snapshot fields and the
changelog (see fold_attr_history) so early-date and never-changed values are not
lost.
"""
from __future__ import annotations
import re

from graph_builder.extract import normalize_link_type

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


_LINK_VERB_MAP = [
    ("blocks", "BLOCKS"), ("blocked by", "BLOCKS"),
    ("duplicat", "DUPLICATES"),
    ("clones", "CLONES"), ("cloned by", "CLONES"),
    ("caused by", "CAUSES"), ("causes", "CAUSES"),
    ("problem", "CAUSES"), ("incident", "CAUSES"),
    ("reviews", "REVIEWS"), ("reviewed by", "REVIEWS"),
    ("relates to", "RELATES_TO"), ("related to", "RELATES_TO"),
]


def map_link_phrase(phrase):
    if not phrase:
        return "RELATED"
    p = phrase.lower()
    for verb, canon in _LINK_VERB_MAP:
        if verb in p:
            return canon
    # custom link type: drop the ticket key and leading "this issue ", UPPER_SNAKE
    core = re.sub(r"\b[A-Z][A-Z0-9]+-\d+\b", "", phrase)
    core = re.sub(r"(?i)^\s*this issue\s+", "", core).strip()
    core = re.sub(r"[^A-Za-z0-9]+", "_", core).strip("_").upper()
    return core or "RELATED"


def extract_key_aliases(record: dict) -> list[dict]:
    key = record["key"]
    out = []
    for entry in (record.get("changelog") or []):
        for it in (entry.get("items") or []):
            if it.get("field") == "Key":
                old = it.get("fromString")
                if old and old != key:
                    out.append({"old_key": old, "current_key": key})
    return out


def extract_link_events(record: dict) -> list[dict]:
    key = record["key"]
    out = []
    for entry in (record.get("changelog") or []):
        ts = entry.get("created")
        for it in (entry.get("items") or []):
            if it.get("field") != "Link":
                continue
            if it.get("to"):
                target, phrase, action = it.get("to"), it.get("toString"), "add"
            elif it.get("from"):
                target, phrase, action = it.get("from"), it.get("fromString"), "remove"
            else:
                continue
            out.append({"ticket_id": key, "ts": ts, "action": action,
                        "target_key": target, "type_phrase": phrase,
                        "mapped_type": map_link_phrase(phrase)})
    return out


def current_links(record: dict) -> dict:
    """Extract {target_key: canonical_type} from issuelinks snapshot."""
    f = record.get("fields") or {}
    out = {}
    for l in (f.get("issuelinks") or []):
        other = l.get("outwardIssue") or l.get("inwardIssue") or {}
        k = other.get("key")
        if k:
            out[k] = normalize_link_type(((l.get("type") or {}).get("name")) or "")
    return out


def _lh_row(key, target, ltype, vfrom, vto, source):
    """Helper to build a link history row with exact shape."""
    return {"node_id": key, "target_key": target, "link_type": ltype,
            "valid_from": vfrom, "valid_to": vto, "source": source}


def fold_link_history(record: dict):
    """Fold link add/remove events + current snapshot into validity intervals.

    Returns: (rows, discrepancies)
    - rows: list of {"node_id","target_key","link_type","valid_from","valid_to","source"}
    - discrepancies: list of {"ticket","target","reason"} for snapshot/changelog mismatches
    """
    key = record["key"]
    created = (record.get("fields") or {}).get("created")
    snapshot = current_links(record)             # {target_key: canonical_type}
    events = extract_link_events(record)
    by_target: dict[str, list] = {}
    for ev in events:
        by_target.setdefault(ev["target_key"], []).append(ev)

    rows: list[dict] = []
    discrepancies: list[dict] = []
    targets = set(by_target) | set(snapshot)
    for target in sorted(targets):
        # NOTE: lexicographic ts sort assumes a uniform timezone offset (this Jira
        # instance emits +0530 throughout). Not valid for mixed-offset/UTC-normalized data.
        evs = sorted(by_target.get(target, []), key=lambda e: e["ts"])
        # NOTE: link_type is best-effort. Changelog links use phrase-mapped types
        # (map_link_phrase) while snapshot-seeded links use normalize_link_type(name);
        # the two can diverge, so type-filtered as-of queries are best-effort.
        # choose link_type: first add's mapped_type, else snapshot canonical, else first event's
        ltype = None
        for e in evs:
            if e["action"] == "add":
                ltype = e["mapped_type"]
                break
        if ltype is None:
            ltype = snapshot.get(target) or (evs[0]["mapped_type"] if evs else "RELATED")

        if not evs:
            rows.append(_lh_row(key, target, ltype, created, SENTINEL, "snapshot-seed"))
            continue

        present_since = created if evs[0]["action"] == "remove" else None
        for e in evs:
            if e["action"] == "add":
                if present_since is None:
                    present_since = e["ts"]
            else:  # remove
                if present_since is not None:
                    if present_since != e["ts"]:      # skip zero-length
                        rows.append(_lh_row(key, target, ltype, present_since, e["ts"], "changelog"))
                    present_since = None
        if present_since is not None:
            rows.append(_lh_row(key, target, ltype, present_since, SENTINEL, "changelog"))
            if target not in snapshot:
                discrepancies.append({"ticket": key, "target": target,
                                      "reason": "open-in-changelog-but-absent-from-snapshot"})
        elif target in snapshot:
            # changelog says removed, but snapshot still has it -> discrepancy
            discrepancies.append({"ticket": key, "target": target,
                                  "reason": "present-in-snapshot-but-closed"})

    return rows, discrepancies
