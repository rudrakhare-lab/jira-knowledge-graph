# Stage 2a — Deterministic Nodes/Edges Graph Builder — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream `data/tickets.jsonl` (723k tickets, 46 GB) and build the *current-state* knowledge graph — nodes and typed edges — into a local SQLite database, deterministically, with no LLM.

**Architecture:** Pure extraction functions turn one ticket record (dict) into node/edge rows; a streaming driver reads the file line-by-line (constant memory), dedups via `INSERT OR IGNORE`, and creates indexes after bulk load. Temporal columns (`valid_from`, `valid_to`, `type_confidence`) are populated with creation-time defaults now so Stage 2b (changelog fold) can refine link validity without a schema change.

**Tech Stack:** Python 3.13 (`.venv`), stdlib `sqlite3` + `json` + `re`, `pytest` for tests. Fully offline.

## Global Constraints

- **Fully local / offline. No LLM, no network calls in the builder.** (Copied from `docs/design.md` §2.)
- **Deterministic construction** — graph derives only from JSON fields; re-running on the same input yields the same graph. (`docs/temporal-kg-architecture.md` §1.)
- **Constant memory** — the 46 GB file MUST be streamed line-by-line; never `read()` it whole or hold all records/edges in RAM. Node/edge dedup happens in SQLite, not Python sets.
- **Additive schema** — extend the locked `nodes`/`edges` shape (`id, type, attrs JSON`) with real, indexed temporal columns; never bury timestamps in `attrs`. (`docs/temporal-kg-architecture.md` §1, §3.)
- **v1 is bulk backfill + re-run = overwrite** — `init_db` drops and recreates tables; the builder is idempotent.
- **Idempotent dedup** — edges are unique on `(src, dst, type)`; nodes unique on `id`.
- **Python package location:** all code under `graph_builder/`; tests under `tests/`.

### Node ID scheme (used across all tasks — canonical)
| Node type | `id` format | Source |
|---|---|---|
| Ticket | the issue key, e.g. `SUP-269421` | `record["key"]` |
| Epic | the issue key (type distinguishes it) | `record["key"]` when issuetype name == `Epic` |
| Project | `project:<projectKey>` | `fields.project.key` |
| User | `user:<accountId>` (fallback `user:name:<displayName>`) | `fields.assignee`, `fields.reporter` |
| Component | `component:<id>` | `fields.components[].id` |
| Label | `label:<name>` | `fields.labels[]` |
| Sprint | `sprint:<id>` | `fields.sprint[].id` (customfield_10006) |
| FixVersion | `version:<id>` | `fields.fixVersions[].id` |

### Edge type taxonomy (used across all tasks — canonical)
Structural: `IN_PROJECT`, `HAS_COMPONENT`, `HAS_LABEL`, `IN_SPRINT`, `HAS_FIXVERSION`, `REPORTED_BY`, `ASSIGNED_TO`, `PARENT_OF`, `SUBTASK_OF`, `MENTIONS`.
Link (from `issuelinks`, canonical direction src→dst): `BLOCKS`, `RELATES_TO`, `DUPLICATES`, `CLONES`, `CAUSES`, `REVIEWS`, else the normalized link-type name.

---

## File Structure

- `graph_builder/schema.py` — SQLite DDL + `init_db()` / `create_indexes()` / pragmas.
- `graph_builder/adf.py` — iterative Atlassian-Document-Format → plain-text extractor.
- `graph_builder/extract.py` — pure functions: one ticket dict → node dicts + edge dicts (the heart; fully unit-testable).
- `graph_builder/build.py` — streaming driver + CLI (`python -m graph_builder.build`).
- `graph_builder/__init__.py` — already exists (empty).
- `tests/conftest.py` — shared fixtures (sample ticket dicts).
- `tests/test_schema.py`, `tests/test_adf.py`, `tests/test_extract.py`, `tests/test_build.py`.
- `tests/fixtures/sample_tickets.jsonl` — 4 crafted records for the end-to-end build test.

---

### Task 1: Project setup + SQLite schema

**Files:**
- Create: `graph_builder/schema.py`
- Create: `tests/test_schema.py`
- Modify: `requirements.txt` (add `pytest`)

**Interfaces:**
- Produces:
  - `SCHEMA_SQL: str` — DDL for `nodes` + `edges` tables.
  - `INDEX_SQL: str` — DDL for indexes (applied after bulk load).
  - `init_db(db_path: str) -> sqlite3.Connection` — drops & recreates tables, sets bulk-load pragmas, returns an open connection.
  - `create_indexes(conn: sqlite3.Connection) -> None` — runs `INDEX_SQL`.

- [ ] **Step 1: Install pytest and add to requirements**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pip install pytest && printf 'pytest\n' >> requirements.txt
```
Expected: pytest installs; `requirements.txt` gains a `pytest` line.

- [ ] **Step 2: Write the failing test**

Create `tests/test_schema.py`:
```python
import sqlite3
from graph_builder import schema


def test_init_db_creates_tables_and_is_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    conn = schema.init_db(db)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"nodes", "edges"} <= tables
    # columns present
    node_cols = {r[1] for r in conn.execute("PRAGMA table_info(nodes)")}
    edge_cols = {r[1] for r in conn.execute("PRAGMA table_info(edges)")}
    assert {"id", "type", "attrs", "created_ts"} <= node_cols
    assert {"src", "dst", "type", "valid_from", "valid_to",
            "type_confidence", "link_id"} <= edge_cols
    conn.close()
    # re-init drops & recreates without error (overwrite semantics)
    conn2 = schema.init_db(db)
    assert conn2.execute("SELECT count(*) FROM nodes").fetchone()[0] == 0
    conn2.close()


def test_create_indexes(tmp_path):
    conn = schema.init_db(str(tmp_path / "g.db"))
    schema.create_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_edges_src_asof" in idx
    assert "ix_edges_dst_asof" in idx
    conn.close()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.schema'`.

- [ ] **Step 4: Write minimal implementation**

Create `graph_builder/schema.py`:
```python
"""SQLite schema for the Stage-2a knowledge graph (nodes + edges).

Temporal columns exist now with creation-time defaults; Stage 2b (changelog
fold) refines link validity without a schema change.
"""
from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS nodes;

CREATE TABLE nodes (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL,
  attrs       TEXT,                 -- JSON, immutable/descriptive only
  created_ts  TEXT,                 -- ticket creation; NULL for non-ticket nodes
  recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE edges (
  src             TEXT NOT NULL,
  dst             TEXT NOT NULL,
  type            TEXT NOT NULL,
  attrs           TEXT,
  link_id         TEXT,             -- Jira issuelink id (provenance; NULL for non-link edges)
  valid_from      TEXT,             -- creation-time default in 2a; refined in 2b
  valid_to        TEXT NOT NULL DEFAULT '9999-12-31',
  type_confidence TEXT DEFAULT 'exact',
  recorded_at     TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (src, dst, type)
) WITHOUT ROWID;
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_edges_src_asof ON edges(src, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS ix_edges_dst_asof ON edges(dst, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS ix_nodes_type      ON nodes(type);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # bulk-load pragmas (safe: v1 rebuilds from scratch on any failure)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript(SCHEMA_SQL)
    return conn


def create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(INDEX_SQL)
    conn.commit()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_schema.py -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/schema.py tests/test_schema.py requirements.txt && git commit -m "feat(graph): stage-2a SQLite schema (nodes/edges) + init"
```

---

### Task 2: ADF → plain-text extractor

**Files:**
- Create: `graph_builder/adf.py`
- Create: `tests/test_adf.py`

**Interfaces:**
- Produces: `adf_to_text(node) -> str` — accepts an ADF dict, a list, a str, or None; returns concatenated text. Iterative (explicit stack) to survive deeply nested ADF without `RecursionError`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_adf.py`:
```python
from graph_builder.adf import adf_to_text


def test_extracts_nested_text():
    doc = {"type": "doc", "version": 1, "content": [
        {"type": "paragraph", "content": [
            {"type": "text", "text": "See SUP-100"},
            {"type": "text", "text": " and PB-42"}]}]}
    assert "SUP-100" in adf_to_text(doc)
    assert "PB-42" in adf_to_text(doc)


def test_handles_none_and_str():
    assert adf_to_text(None) == ""
    assert adf_to_text("plain") == "plain"


def test_deeply_nested_does_not_recurse_error():
    node = {"type": "text", "text": "x"}
    for _ in range(5000):
        node = {"type": "doc", "content": [node]}
    assert adf_to_text(node) == "x"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_adf.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.adf'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/adf.py`:
```python
"""Iterative Atlassian Document Format (ADF) -> plain text.

Iterative (explicit stack) because Jira ADF can nest tables/lists deeply
enough to exceed Python's recursion limit.
"""
from __future__ import annotations


def adf_to_text(node) -> str:
    out: list[str] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if cur is None:
            continue
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, list):
            stack.extend(reversed(cur))
        elif isinstance(cur, dict):
            if cur.get("type") == "text" and isinstance(cur.get("text"), str):
                out.append(cur["text"])
            if "content" in cur:
                stack.append(cur["content"])
    return " ".join(out)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_adf.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/adf.py tests/test_adf.py && git commit -m "feat(graph): iterative ADF text extractor"
```

---

### Task 3: Extract nodes from a ticket

**Files:**
- Create: `graph_builder/extract.py`
- Create: `tests/conftest.py`
- Create: `tests/test_extract.py`

**Interfaces:**
- Consumes: node-ID scheme (Global Constraints).
- Produces:
  - `extract_nodes(record: dict) -> list[dict]` — each node dict: `{"id": str, "type": str, "attrs": dict, "created_ts": str | None}`.
  - Helper id functions used by later tasks: `user_id(user: dict) -> str | None`, `ticket_type(record: dict) -> str`.

- [ ] **Step 1: Write the shared fixture and failing test**

Create `tests/conftest.py`:
```python
import pytest


@pytest.fixture
def ticket_epic():
    return {
        "key": "PB-1",
        "fields": {
            "summary": "Payments epic",
            "created": "2018-01-01T10:00:00.000+0530",
            "issuetype": {"name": "Epic"},
            "project": {"key": "PB", "name": "Payments"},
            "reporter": {"accountId": "acc-r", "displayName": "Rita"},
            "assignee": None,
            "components": [], "labels": [], "fixVersions": [],
            "issuelinks": [], "subtasks": [],
        },
        "comments": [], "changelog": [], "attachments": [],
    }


@pytest.fixture
def ticket_full():
    """A bug with links (inward+outward), parent, membership, mentions."""
    return {
        "key": "SUP-500",
        "fields": {
            "summary": "Login timeout",
            "description": {"type": "doc", "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Related to PB-1 regression"}]}]},
            "created": "2020-06-01T09:00:00.000+0530",
            "issuetype": {"name": "Bug"},
            "project": {"key": "SUP", "name": "Support"},
            "reporter": {"accountId": "acc-r", "displayName": "Rita"},
            "assignee": {"accountId": "acc-a", "displayName": "Amit"},
            "components": [{"id": "c9", "name": "Auth"}],
            "labels": ["timeout", "prod"],
            "sprint": [{"id": 77, "name": "S-77"}],
            "fixVersions": [{"id": "v3", "name": "3.1"}],
            "parent": {"key": "PB-1"},
            "subtasks": [{"key": "SUP-501"}],
            "issuelinks": [
                {"id": "L1", "type": {"name": "Blocks",
                    "inward": "is blocked by", "outward": "blocks"},
                 "outwardIssue": {"key": "SUP-999"}},
                {"id": "L2", "type": {"name": "Relates",
                    "inward": "relates to", "outward": "relates to"},
                 "inwardIssue": {"key": "SUP-100"}},
            ],
        },
        "comments": [{"author": {"accountId": "acc-c", "displayName": "Cara"},
                      "body": {"type": "doc", "content": [
                          {"type": "paragraph", "content": [
                              {"type": "text", "text": "dup of TB-7"}]}]}}],
        "changelog": [], "attachments": [],
    }
```

Create `tests/test_extract.py`:
```python
from graph_builder import extract


def _nodes_by_type(nodes):
    d = {}
    for n in nodes:
        d.setdefault(n["type"], []).append(n)
    return d


def test_epic_node_typed_as_epic(ticket_epic):
    nodes = extract.extract_nodes(ticket_epic)
    by = _nodes_by_type(nodes)
    epic = [n for n in by.get("Epic", []) if n["id"] == "PB-1"]
    assert len(epic) == 1
    assert epic[0]["created_ts"] == "2018-01-01T10:00:00.000+0530"
    # project + reporter user nodes present
    assert any(n["id"] == "project:PB" for n in by.get("Project", []))
    assert any(n["id"] == "user:acc-r" for n in by.get("User", []))


def test_full_ticket_emits_all_node_types(ticket_full):
    nodes = extract.extract_nodes(ticket_full)
    ids = {n["id"] for n in nodes}
    assert "SUP-500" in ids                 # Ticket
    assert "project:SUP" in ids             # Project
    assert "user:acc-a" in ids              # assignee
    assert "user:acc-r" in ids              # reporter
    assert "component:c9" in ids            # Component
    assert "label:timeout" in ids and "label:prod" in ids
    assert "sprint:77" in ids
    assert "version:v3" in ids
    types = {n["type"] for n in nodes}
    assert {"Ticket", "Project", "User", "Component",
            "Label", "Sprint", "FixVersion"} <= types
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.extract'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/extract.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/extract.py tests/conftest.py tests/test_extract.py && git commit -m "feat(graph): extract nodes from ticket record"
```

---

### Task 4: Extract membership + people edges

**Files:**
- Modify: `graph_builder/extract.py`
- Modify: `tests/test_extract.py`

**Interfaces:**
- Consumes: `user_id()` (Task 3); node-ID scheme.
- Produces: `extract_membership_people_edges(record: dict) -> list[dict]` — each edge dict: `{"src", "dst", "type", "valid_from", "type_confidence", "link_id"}`. `valid_from` = ticket `created`. Emits `IN_PROJECT, HAS_COMPONENT, HAS_LABEL, IN_SPRINT, HAS_FIXVERSION, REPORTED_BY, ASSIGNED_TO`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_extract.py`:
```python
def _edge_set(edges):
    return {(e["src"], e["dst"], e["type"]) for e in edges}


def test_membership_and_people_edges(ticket_full):
    edges = extract.extract_membership_people_edges(ticket_full)
    es = _edge_set(edges)
    assert ("SUP-500", "project:SUP", "IN_PROJECT") in es
    assert ("SUP-500", "component:c9", "HAS_COMPONENT") in es
    assert ("SUP-500", "label:timeout", "HAS_LABEL") in es
    assert ("SUP-500", "sprint:77", "IN_SPRINT") in es
    assert ("SUP-500", "version:v3", "HAS_FIXVERSION") in es
    assert ("SUP-500", "user:acc-r", "REPORTED_BY") in es
    assert ("SUP-500", "user:acc-a", "ASSIGNED_TO") in es
    # valid_from carries the ticket creation date
    assert all(e["valid_from"] == "2020-06-01T09:00:00.000+0530" for e in edges)


def test_no_assignee_no_assigned_edge(ticket_epic):
    es = _edge_set(extract.extract_membership_people_edges(ticket_epic))
    assert not any(t == "ASSIGNED_TO" for (_, _, t) in es)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: FAIL — `AttributeError: module 'graph_builder.extract' has no attribute 'extract_membership_people_edges'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/extract.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/extract.py tests/test_extract.py && git commit -m "feat(graph): membership + people edges"
```

---

### Task 5: Extract issue-link edges (canonical direction + type mapping + dedup)

**Files:**
- Modify: `graph_builder/extract.py`
- Modify: `tests/test_extract.py`

**Interfaces:**
- Consumes: edge type taxonomy (Global Constraints).
- Produces:
  - `LINK_TYPE_MAP: dict[str, str]` — Jira link-type name → canonical edge type.
  - `normalize_link_type(name: str) -> str` — mapped canonical, else UPPER_SNAKE of the name.
  - `extract_link_edges(record: dict) -> list[dict]` — one edge per issuelink, canonical direction `src → dst`, carrying `link_id`. For `outwardIssue`: `src=thisKey, dst=outwardKey`. For `inwardIssue`: `src=inwardKey, dst=thisKey`. This makes the mirror copy on the other ticket produce the identical `(src,dst,type)` row (deduped downstream).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_extract.py`:
```python
def test_link_edges_direction_and_type(ticket_full):
    edges = extract.extract_link_edges(ticket_full)
    es = _edge_set(edges)
    # outward "blocks": this ticket -> target
    assert ("SUP-500", "SUP-999", "BLOCKS") in es
    # inward "relates to": target -> this ticket (canonical src=inward)
    assert ("SUP-100", "SUP-500", "RELATES_TO") in es
    # link_id preserved for provenance
    assert {e["link_id"] for e in edges} == {"L1", "L2"}


def test_normalize_link_type():
    assert extract.normalize_link_type("Blocks") == "BLOCKS"
    assert extract.normalize_link_type("Problem/Incident") == "CAUSES"
    assert extract.normalize_link_type("SIM Outbound link") == "SIM_OUTBOUND_LINK"


def test_mirror_links_produce_same_canonical_edge():
    # Ticket A says it blocks B (outward); Ticket B says it is blocked by A (inward).
    a = {"key": "A-1", "fields": {"created": "t", "issuelinks": [
        {"id": "L9", "type": {"name": "Blocks", "inward": "is blocked by",
         "outward": "blocks"}, "outwardIssue": {"key": "B-2"}}]}}
    b = {"key": "B-2", "fields": {"created": "t", "issuelinks": [
        {"id": "L9", "type": {"name": "Blocks", "inward": "is blocked by",
         "outward": "blocks"}, "inwardIssue": {"key": "A-1"}}]}}
    ea = extract.extract_link_edges(a)[0]
    eb = extract.extract_link_edges(b)[0]
    assert (ea["src"], ea["dst"], ea["type"]) == (eb["src"], eb["dst"], eb["type"])
    assert (ea["src"], ea["dst"], ea["type"]) == ("A-1", "B-2", "BLOCKS")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: FAIL — `AttributeError: ... 'extract_link_edges'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/extract.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/extract.py tests/test_extract.py && git commit -m "feat(graph): issue-link edges (canonical direction + type map)"
```

---

### Task 6: Extract hierarchy edges (parent / subtask)

**Files:**
- Modify: `graph_builder/extract.py`
- Modify: `tests/test_extract.py`

**Interfaces:**
- Produces: `extract_hierarchy_edges(record: dict) -> list[dict]` — `PARENT_OF` from `fields.parent` (parent → this) and `SUBTASK_OF` from `fields.subtasks` (subtask → this).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_extract.py`:
```python
def test_hierarchy_edges(ticket_full):
    es = _edge_set(extract.extract_hierarchy_edges(ticket_full))
    # parent PB-1 is parent of SUP-500
    assert ("PB-1", "SUP-500", "PARENT_OF") in es
    # subtask SUP-501 is a subtask of SUP-500
    assert ("SUP-501", "SUP-500", "SUBTASK_OF") in es
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: FAIL — `AttributeError: ... 'extract_hierarchy_edges'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/extract.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/extract.py tests/test_extract.py && git commit -m "feat(graph): parent/subtask hierarchy edges"
```

---

### Task 7: Extract MENTIONS edges + `extract_all` aggregator

**Files:**
- Modify: `graph_builder/extract.py`
- Modify: `tests/test_extract.py`

**Interfaces:**
- Consumes: `adf_to_text` (Task 2); all edge extractors (Tasks 4–6).
- Produces:
  - `KEY_RE` — compiled `re` for Jira keys (`[A-Z][A-Z0-9]+-\d+`).
  - `extract_mentions_edges(record: dict) -> list[dict]` — scans description + comment bodies; emits `MENTIONS` to each referenced key except self; `type_confidence="mention"`; deduped within the record.
  - `extract_all(record: dict) -> tuple[list[dict], list[dict]]` — returns `(nodes, edges)` combining every extractor.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_extract.py`:
```python
def test_mentions_from_description_and_comments(ticket_full):
    es = _edge_set(extract.extract_mentions_edges(ticket_full))
    assert ("SUP-500", "PB-1", "MENTIONS") in es    # from description
    assert ("SUP-500", "TB-7", "MENTIONS") in es    # from comment
    # never mentions itself
    assert ("SUP-500", "SUP-500", "MENTIONS") not in es


def test_extract_all_combines(ticket_full):
    nodes, edges = extract.extract_all(ticket_full)
    assert any(n["id"] == "SUP-500" for n in nodes)
    types = {e["type"] for e in edges}
    assert {"IN_PROJECT", "BLOCKS", "RELATES_TO", "PARENT_OF",
            "SUBTASK_OF", "MENTIONS", "ASSIGNED_TO"} <= types
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: FAIL — `AttributeError: ... 'extract_mentions_edges'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/extract.py` (add `from graph_builder.adf import adf_to_text` at the top import block):
```python
KEY_RE = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")


def extract_mentions_edges(record: dict) -> list[dict]:
    f = record.get("fields") or {}
    key = record["key"]
    vf = f.get("created")
    text_parts = [adf_to_text(f.get("description"))]
    for c in (record.get("comments") or []):
        text_parts.append(adf_to_text(c.get("body")))
    blob = " ".join(text_parts)
    seen: set[str] = set()
    edges: list[dict] = []
    for m in KEY_RE.findall(blob):
        if m == key or m in seen:
            continue
        seen.add(m)
        edges.append(_edge(key, m, "MENTIONS", vf, type_confidence="mention"))
    return edges


def extract_all(record: dict) -> tuple[list[dict], list[dict]]:
    nodes = extract_nodes(record)
    edges = (extract_membership_people_edges(record)
             + extract_link_edges(record)
             + extract_hierarchy_edges(record)
             + extract_mentions_edges(record))
    return nodes, edges
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_extract.py -v`
Expected: PASS (all extract tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/extract.py tests/test_extract.py && git commit -m "feat(graph): MENTIONS edges + extract_all aggregator"
```

---

### Task 8: Streaming build driver + CLI

**Files:**
- Create: `graph_builder/build.py`
- Create: `tests/fixtures/sample_tickets.jsonl`
- Create: `tests/test_build.py`

**Interfaces:**
- Consumes: `schema.init_db`, `schema.create_indexes`, `extract.extract_all`.
- Produces:
  - `build(jsonl_path: str, db_path: str, batch_size: int = 5000, limit: int | None = None) -> dict` — streams the file, writes nodes/edges with `INSERT OR IGNORE`, creates indexes after load, returns counts `{"records", "nodes", "edges"}` (counts = distinct rows in DB after dedup).
  - `main()` — CLI reading paths from argv/defaults (`data/tickets.jsonl` → `data/graph.db`).

- [ ] **Step 1: Create the fixture and write the failing test**

Create `tests/fixtures/sample_tickets.jsonl` (exactly 4 lines, one JSON object per line):
```json
{"key":"PB-1","fields":{"summary":"Payments epic","created":"2018-01-01T10:00:00.000+0530","issuetype":{"name":"Epic"},"project":{"key":"PB","name":"Payments"},"reporter":{"accountId":"acc-r","displayName":"Rita"},"assignee":null,"components":[],"labels":[],"fixVersions":[],"issuelinks":[],"subtasks":[]},"comments":[],"changelog":[],"attachments":[]}
{"key":"SUP-500","fields":{"summary":"Login timeout","description":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Related to PB-1 regression"}]}]},"created":"2020-06-01T09:00:00.000+0530","issuetype":{"name":"Bug"},"project":{"key":"SUP","name":"Support"},"reporter":{"accountId":"acc-r","displayName":"Rita"},"assignee":{"accountId":"acc-a","displayName":"Amit"},"components":[{"id":"c9","name":"Auth"}],"labels":["timeout","prod"],"sprint":[{"id":77,"name":"S-77"}],"fixVersions":[{"id":"v3","name":"3.1"}],"parent":{"key":"PB-1"},"subtasks":[{"key":"SUP-501"}],"issuelinks":[{"id":"L1","type":{"name":"Blocks","inward":"is blocked by","outward":"blocks"},"outwardIssue":{"key":"SUP-999"}},{"id":"L2","type":{"name":"Relates","inward":"relates to","outward":"relates to"},"inwardIssue":{"key":"SUP-100"}}]},"comments":[{"author":{"accountId":"acc-c","displayName":"Cara"},"body":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"dup of TB-7"}]}]}}],"changelog":[],"attachments":[]}
{"key":"SUP-501","fields":{"summary":"Sub of 500","created":"2020-06-02T09:00:00.000+0530","issuetype":{"name":"Sub-task"},"project":{"key":"SUP","name":"Support"},"reporter":{"accountId":"acc-r","displayName":"Rita"},"assignee":null,"components":[],"labels":[],"fixVersions":[],"issuelinks":[],"subtasks":[]},"comments":[],"changelog":[],"attachments":[]}
{"key":"SUP-999","fields":{"summary":"Downstream","created":"2020-05-01T09:00:00.000+0530","issuetype":{"name":"Bug"},"project":{"key":"SUP","name":"Support"},"reporter":{"accountId":"acc-r","displayName":"Rita"},"assignee":null,"components":[],"labels":[],"fixVersions":[],"issuelinks":[{"id":"L1","type":{"name":"Blocks","inward":"is blocked by","outward":"blocks"},"inwardIssue":{"key":"SUP-500"}}],"subtasks":[]},"comments":[],"changelog":[],"attachments":[]}
```

Create `tests/test_build.py`:
```python
from pathlib import Path

from graph_builder import build

FIX = Path(__file__).parent / "fixtures" / "sample_tickets.jsonl"


def test_build_counts_and_dedup(tmp_path):
    db = str(tmp_path / "g.db")
    counts = build.build(str(FIX), db, batch_size=2)
    assert counts["records"] == 4
    import sqlite3
    conn = sqlite3.connect(db)
    # 4 ticket nodes + entities (project SUP, PB; users acc-r/acc-a/... ; component, labels, sprint, version)
    n_tickets = conn.execute(
        "SELECT count(*) FROM nodes WHERE type IN ('Ticket','Epic')").fetchone()[0]
    assert n_tickets == 4
    # the mirror BLOCKS link (present on SUP-500 outward AND SUP-999 inward) dedups to ONE row
    blocks = conn.execute(
        "SELECT count(*) FROM edges WHERE type='BLOCKS'").fetchone()[0]
    assert blocks == 1
    assert conn.execute(
        "SELECT count(*) FROM edges WHERE src='SUP-500' AND dst='SUP-999' AND type='BLOCKS'"
    ).fetchone()[0] == 1
    # indexes exist (created after load)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_edges_src_asof" in idx
    conn.close()


def test_build_is_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build.build(str(FIX), db, batch_size=2)
    b = build.build(str(FIX), db, batch_size=2)   # re-run overwrites
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.build'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/build.py`:
```python
"""Stage 2a driver: stream tickets.jsonl -> SQLite graph (nodes + edges).

Constant memory: read line-by-line, batch INSERT OR IGNORE, build indexes
after bulk load. Deterministic; re-running overwrites (init_db drops tables).
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from graph_builder import schema
from graph_builder.extract import extract_all

logger = logging.getLogger("graph_builder.build")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, node_rows, edge_rows):
    if node_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO nodes(id,type,attrs,created_ts) VALUES(?,?,?,?)",
            node_rows)
    if edge_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO edges(src,dst,type,valid_from,type_confidence,link_id) "
            "VALUES(?,?,?,?,?,?)",
            edge_rows)


def build(jsonl_path: str, db_path: str, batch_size: int = 5000,
          limit: int | None = None) -> dict:
    conn = schema.init_db(db_path)
    records = 0
    node_rows: list[tuple] = []
    edge_rows: list[tuple] = []
    with open(jsonl_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            records += 1
            nodes, edges = extract_all(rec)
            for n in nodes:
                node_rows.append((n["id"], n["type"],
                                  json.dumps(n["attrs"], ensure_ascii=False),
                                  n["created_ts"]))
            for e in edges:
                edge_rows.append((e["src"], e["dst"], e["type"], e["valid_from"],
                                  e["type_confidence"], e["link_id"]))
            if len(node_rows) >= batch_size or len(edge_rows) >= batch_size:
                _flush(conn, node_rows, edge_rows)
                node_rows.clear()
                edge_rows.clear()
            if records % 50000 == 0:
                conn.commit()
                logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, node_rows, edge_rows)
    conn.commit()
    schema.create_indexes(conn)
    counts = {
        "records": records,
        "nodes": conn.execute("SELECT count(*) FROM nodes").fetchone()[0],
        "edges": conn.execute("SELECT count(*) FROM edges").fetchone()[0],
    }
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    counts = build(jsonl, db, limit=limit)
    logger.info("done: %s", counts)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/build.py tests/test_build.py tests/fixtures/sample_tickets.jsonl && git commit -m "feat(graph): streaming build driver + CLI"
```

---

### Task 9: Real-data smoke test + live-Jira spot-check

**Files:**
- None created (verification task). Uses the real `data/tickets.jsonl`.

**Interfaces:**
- Consumes: `build.build` with `limit`.

- [ ] **Step 1: Full test suite green**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest -v`
Expected: PASS (all tests across schema/adf/extract/build).

- [ ] **Step 2: Build a 20k-record slice from the real file**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -m graph_builder.build data/tickets.jsonl /tmp/graph_smoke.db 20000
```
Expected: log ends with `done: {'records': 20000, 'nodes': <N>, 'edges': <M>}` with N, M > 0.

- [ ] **Step 3: Sanity-check the slice graph**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -c "import sqlite3;c=sqlite3.connect('/tmp/graph_smoke.db');print('nodes by type:',dict(c.execute('SELECT type,count(*) FROM nodes GROUP BY type').fetchall()));print('edges by type:',dict(c.execute('SELECT type,count(*) FROM edges GROUP BY type').fetchall()))"
```
Expected: node types include `Ticket, Project, User, Component, Label`; edge types include `IN_PROJECT, REPORTED_BY, BLOCKS, RELATES_TO, MENTIONS`.

- [ ] **Step 4: Spot-check one ticket's edges against live Jira**

Pick a ticket key that appears in the slice with issue-links (e.g. from the edge dump), fetch it live via the Atlassian MCP connector (`getJiraIssue`, cloudId `a6e123b0-3842-4bc0-b88a-8a83e1c5458f`), and confirm its `issuelinks` / parent / assignee match the edges in `/tmp/graph_smoke.db` for that key. (Manual reconciliation per `docs/design.md` §6.) Record the checked key + result in the commit message.

- [ ] **Step 5: Commit the verification note**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git commit --allow-empty -m "test(graph): stage-2a smoke on 20k slice + live-Jira spot-check <KEY> OK"
```

---

## Notes for Stage 2b (out of scope here, do NOT build now)
- Load `events` table (changelog 1:1) and fold snapshot+changelog → `attr_history` + refine `edges.valid_from/valid_to` (link add/remove events; `type_confidence='phrase-mapped'` for historical link types).
- Build the key-alias map (e.g. `EIM-5998` → `EIMV2-14875`) and resolve `MENTIONS`/link targets across renames.
- Assert per-field `final toString == snapshot` completeness check.
