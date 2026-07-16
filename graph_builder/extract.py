"""Pure extraction: one Jira ticket record (dict) -> node/edge dicts.

No I/O, no LLM, no network. Deterministic. Node/edge dicts are plain and
serializable; the build driver writes them to SQLite.
"""
from __future__ import annotations

import re

EPIC_TYPES = {"Epic"}


def ticket_type(record: dict) -> str:
    name = ((record.get("fields") or {}).get("issuetype") or {}).get("name")
    return "Epic" if name in EPIC_TYPES else "Ticket"


def user_id(user: dict | None) -> str | None:
    if not user:
        return None
    acc = user.get("accountId")
    if acc:
        return f"user:{acc}"
    dn = user.get("displayName")
    return f"user:name:{dn}" if dn else None


def _node(id_, type_, attrs=None, created_ts=None):
    return {"id": id_, "type": type_, "attrs": attrs or {}, "created_ts": created_ts}


def extract_nodes(record: dict) -> list[dict]:
    f = record.get("fields") or {}
    key = record["key"]
    nodes: list[dict] = []

    # the ticket / epic itself
    nodes.append(_node(
        key, ticket_type(record),
        attrs={"summary": f.get("summary"),
               "issuetype": (f.get("issuetype") or {}).get("name"),
               "project": (f.get("project") or {}).get("key")},
        created_ts=f.get("created")))

    proj = f.get("project") or {}
    if proj.get("key"):
        nodes.append(_node(f"project:{proj['key']}", "Project",
                           {"name": proj.get("name"), "key": proj.get("key")}))

    for who in (f.get("reporter"), f.get("assignee")):
        uid = user_id(who)
        if uid:
            nodes.append(_node(uid, "User", {"displayName": who.get("displayName")}))

    for c in (f.get("components") or []):
        if c.get("id"):
            nodes.append(_node(f"component:{c['id']}", "Component", {"name": c.get("name")}))

    for lb in (f.get("labels") or []):
        nodes.append(_node(f"label:{lb}", "Label", {"name": lb}))

    spr = f.get("sprint")
    spr_list = spr if isinstance(spr, list) else ([spr] if isinstance(spr, dict) else [])
    for s in spr_list:
        if isinstance(s, dict) and s.get("id") is not None:
            nodes.append(_node(f"sprint:{s['id']}", "Sprint", {"name": s.get("name")}))

    for v in (f.get("fixVersions") or []):
        if v.get("id"):
            nodes.append(_node(f"version:{v['id']}", "FixVersion", {"name": v.get("name")}))

    return nodes
