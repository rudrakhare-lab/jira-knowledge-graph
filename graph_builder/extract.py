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


def _edge(src, dst, type_, valid_from, type_confidence="exact", link_id=None):
    return {"src": src, "dst": dst, "type": type_, "valid_from": valid_from,
            "type_confidence": type_confidence, "link_id": link_id}


def extract_membership_people_edges(record: dict) -> list[dict]:
    f = record.get("fields") or {}
    key = record["key"]
    vf = f.get("created")
    edges: list[dict] = []

    proj = f.get("project") or {}
    if proj.get("key"):
        edges.append(_edge(key, f"project:{proj['key']}", "IN_PROJECT", vf))

    for c in (f.get("components") or []):
        if c.get("id"):
            edges.append(_edge(key, f"component:{c['id']}", "HAS_COMPONENT", vf))

    for lb in (f.get("labels") or []):
        edges.append(_edge(key, f"label:{lb}", "HAS_LABEL", vf))

    spr = f.get("sprint")
    spr_list = spr if isinstance(spr, list) else ([spr] if isinstance(spr, dict) else [])
    for s in spr_list:
        if isinstance(s, dict) and s.get("id") is not None:
            edges.append(_edge(key, f"sprint:{s['id']}", "IN_SPRINT", vf))

    for v in (f.get("fixVersions") or []):
        if v.get("id"):
            edges.append(_edge(key, f"version:{v['id']}", "HAS_FIXVERSION", vf))

    rid = user_id(f.get("reporter"))
    if rid:
        edges.append(_edge(key, rid, "REPORTED_BY", vf))
    aid = user_id(f.get("assignee"))
    if aid:
        edges.append(_edge(key, aid, "ASSIGNED_TO", vf))

    return edges


LINK_TYPE_MAP = {
    "Blocks": "BLOCKS",
    "Relates": "RELATES_TO",
    "Duplicate": "DUPLICATES",
    "Cloners": "CLONES",
    "Problem/Incident": "CAUSES",
    "Post-Incident Reviews": "REVIEWS",
}


def normalize_link_type(name: str) -> str:
    if name in LINK_TYPE_MAP:
        return LINK_TYPE_MAP[name]
    # UPPER_SNAKE of anything else (e.g. "SIM Outbound link")
    return re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip()).strip("_").upper()


def extract_link_edges(record: dict) -> list[dict]:
    f = record.get("fields") or {}
    key = record["key"]
    vf = f.get("created")
    edges: list[dict] = []
    for link in (f.get("issuelinks") or []):
        etype = normalize_link_type(((link.get("type") or {}).get("name")) or "")
        lid = link.get("id")
        out = link.get("outwardIssue")
        inw = link.get("inwardIssue")
        if out and out.get("key"):
            edges.append(_edge(key, out["key"], etype, vf, link_id=lid))
        elif inw and inw.get("key"):
            edges.append(_edge(inw["key"], key, etype, vf, link_id=lid))
    return edges


def extract_hierarchy_edges(record: dict) -> list[dict]:
    f = record.get("fields") or {}
    key = record["key"]
    vf = f.get("created")
    edges: list[dict] = []
    parent = f.get("parent") or {}
    if parent.get("key"):
        edges.append(_edge(parent["key"], key, "PARENT_OF", vf))
    for sub in (f.get("subtasks") or []):
        if sub.get("key"):
            edges.append(_edge(sub["key"], key, "SUBTASK_OF", vf))
    return edges
