# Stage 2b (part 1) — Temporal Replay: events + attr_history — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Jira changelog (7.65M timestamped events across 95% of tickets) into two SQLite tables — an append-only `events` log and a derived `attr_history` of valid-time intervals for single-valued scalar fields (status, priority, assignee) — so any ticket's status/priority/assignee can be queried *as-of any date*.

**Architecture:** A pure fold turns one ticket record (snapshot `fields` + `changelog`) into event rows and attribute-interval rows. The fold consumes BOTH the snapshot and the changelog: fields with change-events are seeded with the pre-change value from `created_ts` and then walked; fields with NO events get a single interval `[created_ts, sentinel] = snapshot value`. A streaming driver opens the *existing* `data/graph.db` (built by Stage 2a), adds the two temporal tables without touching `nodes`/`edges`, and populates them in constant memory.

**Tech Stack:** Python 3.13 (`.venv`), stdlib `sqlite3`+`json`, `pytest`. Fully offline, no LLM.

## Global Constraints

- **Fully local/offline. No LLM, no network.** (`docs/design.md` §2.)
- **Deterministic.** Same input → same `events`/`attr_history`. (`docs/temporal-kg-architecture.md` §1.)
- **Constant memory.** Stream `data/tickets.jsonl` (46 GB) line-by-line; never read it whole; batch-insert; dedup/PK in SQLite.
- **Additive & non-destructive to Stage 2a.** The temporal builder MUST NOT drop or modify `nodes`/`edges`. It creates/refreshes ONLY `events` and `attr_history`. Re-running drops & rebuilds just those two tables.
- **Snapshot+changelog fold (correctness-critical).** The fold MUST consume the current snapshot value AND the changelog. Fields with events: seed `[created_ts, first_event.ts)` with the first event's *from* value, then walk each event's *to* value; the final folded value should equal the snapshot — record a discrepancy when it doesn't. Fields with no events: emit one interval `[created_ts, sentinel] = snapshot value` with `source='snapshot-seed'`. (This is the bug the advisor caught: event-only replay drops early-date and never-changed values.)
- **Half-open intervals `[valid_from, valid_to)` with strict `>` in as-of predicates; sentinel `valid_to='9999-12-31'`** (never NULL). Skip zero-length intervals (`valid_from == valid_to`).
- **Temporal fields are real, indexed columns** — never inside JSON.
- **Tracked fields (this plan):** `status`, `priority` (string-valued: use `fromString`/`toString`; snapshot = `.name`); `assignee` (id-valued: use `from`/`to` accountId; snapshot = `fields.assignee.accountId`). `resolution` and multi-valued fields (labels/components/sprint/fixVersion) and edge-validity are OUT OF SCOPE (2b-part-2).

### Changelog record shape (validated against live Jira this session)
`record["changelog"]` = list of entries `{ "created": ts, "author": {"accountId":...}, "items": [ {"field","fieldtype","from","fromString","to","toString"} ] }`. For `assignee`, `from`/`to` hold accountIds and `fromString`/`toString` hold display names. For `status`/`priority`, `fromString`/`toString` hold the names.

### Table shapes (additive to Stage 2a's `nodes`/`edges`)
```sql
events(
  event_id INTEGER PRIMARY KEY,   -- rowid autoincrement
  ticket_id TEXT, ts TEXT, author TEXT, field TEXT,
  from_id TEXT, from_val TEXT, to_id TEXT, to_val TEXT
);
attr_history(
  node_id TEXT, attr TEXT, value TEXT,
  valid_from TEXT, valid_to TEXT DEFAULT '9999-12-31', source TEXT,
  PRIMARY KEY (node_id, attr, valid_from)
) WITHOUT ROWID;
```

---

## File Structure

- `graph_builder/temporal_schema.py` — DDL + `init_temporal(conn)` (drops & recreates ONLY events/attr_history) + `create_temporal_indexes(conn)`.
- `graph_builder/replay.py` — pure functions: `extract_events(record)` and `fold_attr_history(record)` (+ helpers `snapshot_value`, `ATTR_FIELDS`, `SENTINEL`).
- `graph_builder/build_temporal.py` — streaming driver + CLI (`python -m graph_builder.build_temporal`).
- `tests/test_temporal_schema.py`, `tests/test_replay.py`, `tests/test_build_temporal.py`.
- `tests/fixtures/temporal_tickets.jsonl` — 3 crafted records for the end-to-end test.

---

### Task 1: Temporal schema (events + attr_history)

**Files:**
- Create: `graph_builder/temporal_schema.py`
- Create: `tests/test_temporal_schema.py`

**Interfaces:**
- Produces:
  - `TEMPORAL_SCHEMA_SQL: str`, `TEMPORAL_INDEX_SQL: str`
  - `init_temporal(conn: sqlite3.Connection) -> None` — runs `TEMPORAL_SCHEMA_SQL` (drops & recreates ONLY `events`, `attr_history`; leaves other tables untouched).
  - `create_temporal_indexes(conn: sqlite3.Connection) -> None`

- [ ] **Step 1: Write the failing test**

Create `tests/test_temporal_schema.py`:
```python
import sqlite3
from graph_builder import temporal_schema


def test_init_temporal_creates_only_temporal_tables(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    # simulate a Stage-2a db with a nodes table holding a row
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    temporal_schema.init_temporal(conn)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"events", "attr_history", "nodes"} <= tables
    # nodes was NOT dropped
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    ecols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    acols = {r[1] for r in conn.execute("PRAGMA table_info(attr_history)")}
    assert {"event_id","ticket_id","ts","author","field",
            "from_id","from_val","to_id","to_val"} <= ecols
    assert {"node_id","attr","value","valid_from","valid_to","source"} <= acols
    # idempotent: re-init drops & recreates temporal tables only
    conn.execute("INSERT INTO events(ticket_id,ts,field) VALUES('X','t','status')")
    conn.commit()
    temporal_schema.init_temporal(conn)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()


def test_create_temporal_indexes(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    temporal_schema.init_temporal(conn)
    temporal_schema.create_temporal_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_events_ticket_field_ts" in idx
    assert "ix_attr_asof" in idx
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.temporal_schema'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/temporal_schema.py`:
```python
"""Stage 2b temporal tables: events (changelog mirror) + attr_history (folded
valid-time intervals). Additive to Stage 2a's nodes/edges — init_temporal drops
& recreates ONLY these two tables.
"""
from __future__ import annotations

import sqlite3

TEMPORAL_SCHEMA_SQL = """
DROP TABLE IF EXISTS attr_history;
DROP TABLE IF EXISTS events;

CREATE TABLE events (
  event_id  INTEGER PRIMARY KEY,
  ticket_id TEXT,
  ts        TEXT,
  author    TEXT,
  field     TEXT,
  from_id   TEXT,
  from_val  TEXT,
  to_id     TEXT,
  to_val    TEXT
);

CREATE TABLE attr_history (
  node_id    TEXT,
  attr       TEXT,
  value      TEXT,
  valid_from TEXT,
  valid_to   TEXT NOT NULL DEFAULT '9999-12-31',
  source     TEXT,
  PRIMARY KEY (node_id, attr, valid_from)
) WITHOUT ROWID;
"""

TEMPORAL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_events_ticket_field_ts ON events(ticket_id, field, ts);
CREATE INDEX IF NOT EXISTS ix_events_ts               ON events(ts);
CREATE INDEX IF NOT EXISTS ix_attr_asof               ON attr_history(node_id, attr, valid_from, valid_to);
"""


def init_temporal(conn: sqlite3.Connection) -> None:
    conn.executescript(TEMPORAL_SCHEMA_SQL)
    conn.commit()


def create_temporal_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(TEMPORAL_INDEX_SQL)
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/temporal_schema.py tests/test_temporal_schema.py && git commit -m "feat(temporal): stage-2b events + attr_history schema"
```

---

### Task 2: `extract_events` — mirror the changelog

**Files:**
- Create: `graph_builder/replay.py`
- Create: `tests/test_replay.py`

**Interfaces:**
- Produces: `extract_events(record: dict) -> list[dict]` — one row per changelog item: `{"ticket_id","ts","author","field","from_id","from_val","to_id","to_val"}`. `author` = the entry's `author.accountId`. Preserves ALL fields (not just tracked ones) — `events` is a faithful 1:1 mirror.

- [ ] **Step 1: Write the failing test**

Create `tests/test_replay.py`:
```python
from graph_builder import replay


def _entry(ts, field, frm=None, frms=None, to=None, tos=None, author="acc-x"):
    return {"created": ts, "author": {"accountId": author},
            "items": [{"field": field, "from": frm, "fromString": frms,
                       "to": to, "toString": tos}]}


def test_extract_events_mirrors_all_items():
    rec = {"key": "SUP-1", "fields": {"created": "2020-01-01T00:00:00.000+0000"},
           "changelog": [
               _entry("2020-02-01T00:00:00.000+0000", "status",
                      frms="Open", tos="Done"),
               _entry("2020-03-01T00:00:00.000+0000", "Custom Field",
                      frms="a", tos="b", author="acc-y"),
           ]}
    evs = replay.extract_events(rec)
    assert len(evs) == 2
    e0 = evs[0]
    assert e0["ticket_id"] == "SUP-1"
    assert e0["ts"] == "2020-02-01T00:00:00.000+0000"
    assert e0["author"] == "acc-x"
    assert e0["field"] == "status"
    assert e0["from_val"] == "Open" and e0["to_val"] == "Done"
    # non-tracked fields are still mirrored (events is 1:1)
    assert evs[1]["field"] == "Custom Field" and evs[1]["author"] == "acc-y"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.replay'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/replay.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/replay.py tests/test_replay.py && git commit -m "feat(temporal): extract_events changelog mirror"
```

---

### Task 3: `fold_attr_history` — the snapshot+changelog fold

**Files:**
- Modify: `graph_builder/replay.py`
- Modify: `tests/test_replay.py`

**Interfaces:**
- Consumes: `SENTINEL` (Task 2).
- Produces:
  - `ATTR_FIELDS: dict[str, tuple[str, str]]` — jira changelog field name → `(attr_name, kind)` where kind ∈ `{"string","id"}`. Value: `{"status": ("status","string"), "priority": ("priority","string"), "assignee": ("assignee","id")}`.
  - `snapshot_value(record: dict, attr: str) -> str | None` — current value: status→`fields.status.name`, priority→`fields.priority.name`, assignee→`fields.assignee.accountId`.
  - `fold_attr_history(record: dict) -> tuple[list[dict], list[dict]]` — returns `(rows, discrepancies)`. Row: `{"node_id","attr","value","valid_from","valid_to","source"}`. Discrepancy: `{"ticket","attr","folded","snapshot"}` (only when a field with events folds to a value ≠ the snapshot). Skips zero-length intervals (`valid_from == valid_to`).

**Fold rules (per (ticket, tracked-field)):**
- `kind="string"` uses `fromString`/`toString`; `kind="id"` uses `from`/`to`.
- Events present: seed interval `[created_ts, first.ts) = first.from`; then for each event, close the current interval at `event.ts` and open the next with `event.to`; final interval `[last.ts, SENTINEL) = last.to`, `source="changelog"`. If the final value ≠ snapshot → append a discrepancy.
- No events: one interval `[created_ts, SENTINEL) = snapshot`, `source="snapshot-seed"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_replay.py`:
```python
def _rec(created, snap_status=None, snap_priority=None, snap_assignee=None, changelog=None):
    fields = {"created": created}
    if snap_status is not None:
        fields["status"] = {"name": snap_status}
    if snap_priority is not None:
        fields["priority"] = {"name": snap_priority}
    fields["assignee"] = {"accountId": snap_assignee} if snap_assignee else None
    return {"key": "SUP-1", "fields": fields, "changelog": changelog or []}


def _by_attr(rows, attr):
    return sorted([r for r in rows if r["attr"] == attr], key=lambda r: r["valid_from"])


def test_status_fold_seeds_initial_and_walks():
    rec = _rec("2020-01-01", snap_status="Done", changelog=[
        _entry("2020-02-01", "status", frms="Open", tos="In Progress"),
        _entry("2020-03-01", "status", frms="In Progress", tos="Done"),
    ])
    rows, disc = replay.fold_attr_history(rec)
    s = _by_attr(rows, "status")
    assert [(r["value"], r["valid_from"], r["valid_to"], r["source"]) for r in s] == [
        ("Open", "2020-01-01", "2020-02-01", "changelog"),
        ("In Progress", "2020-02-01", "2020-03-01", "changelog"),
        ("Done", "2020-03-01", replay.SENTINEL, "changelog"),
    ]
    assert disc == []


def test_zero_event_field_seeded_from_snapshot():
    rec = _rec("2020-01-01", snap_status="Open", snap_priority="Major")
    rows, disc = replay.fold_attr_history(rec)
    # priority never changed -> single snapshot-seed interval (the bug this fixes)
    p = _by_attr(rows, "priority")
    assert p == [{"node_id": "SUP-1", "attr": "priority", "value": "Major",
                  "valid_from": "2020-01-01", "valid_to": replay.SENTINEL,
                  "source": "snapshot-seed"}]
    st = _by_attr(rows, "status")
    assert st[0]["value"] == "Open" and st[0]["source"] == "snapshot-seed"


def test_assignee_uses_id_form():
    rec = _rec("2020-01-01", snap_assignee=None, changelog=[
        _entry("2020-05-01", "assignee", frm=None, frms=None, to="acc-a", tos="Amit"),
        _entry("2020-06-01", "assignee", frm="acc-a", frms="Amit", to=None, tos=None),
    ])
    a = _by_attr(replay.fold_attr_history(rec)[0], "assignee")
    assert [(r["value"], r["valid_from"], r["valid_to"]) for r in a] == [
        (None, "2020-01-01", "2020-05-01"),
        ("acc-a", "2020-05-01", "2020-06-01"),
        (None, "2020-06-01", replay.SENTINEL),
    ]


def test_completeness_discrepancy_flagged():
    # changelog says final status "Reopened" but snapshot says "Done" -> gap
    rec = _rec("2020-01-01", snap_status="Done", changelog=[
        _entry("2020-02-01", "status", frms="Open", tos="Reopened"),
    ])
    _, disc = replay.fold_attr_history(rec)
    assert {"ticket": "SUP-1", "attr": "status",
            "folded": "Reopened", "snapshot": "Done"} in disc
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: FAIL — `AttributeError: module 'graph_builder.replay' has no attribute 'fold_attr_history'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/replay.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: PASS (all replay tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/replay.py tests/test_replay.py && git commit -m "feat(temporal): fold_attr_history snapshot+changelog fold"
```

---

### Task 4: Streaming temporal build driver + CLI

**Files:**
- Create: `graph_builder/build_temporal.py`
- Create: `tests/fixtures/temporal_tickets.jsonl`
- Create: `tests/test_build_temporal.py`

**Interfaces:**
- Consumes: `temporal_schema.init_temporal`/`create_temporal_indexes`, `replay.extract_events`/`fold_attr_history`.
- Produces:
  - `build_temporal(jsonl_path: str, db_path: str, batch_size: int = 5000, limit: int | None = None) -> dict` — opens the EXISTING db, (re)creates temporal tables, streams the file, inserts `events` + `attr_history`, creates indexes after load, returns `{"records","events","attr_rows","discrepancies"}`. Does NOT touch nodes/edges.
  - `main()` — CLI: argv `[jsonl] [db] [limit]`, defaults `data/tickets.jsonl` → `data/graph.db`.

- [ ] **Step 1: Create the fixture and write the failing test**

Create `tests/fixtures/temporal_tickets.jsonl` (exactly 3 lines, one JSON object per line):
```json
{"key":"T-1","fields":{"created":"2020-01-01T00:00:00.000+0000","status":{"name":"Done"},"priority":{"name":"Major"},"assignee":null},"changelog":[{"created":"2020-02-01T00:00:00.000+0000","author":{"accountId":"acc-x"},"items":[{"field":"status","from":null,"fromString":"Open","to":null,"toString":"Done"}]}],"comments":[],"attachments":[]}
{"key":"T-2","fields":{"created":"2019-06-01T00:00:00.000+0000","status":{"name":"Open"},"priority":{"name":"Minor"},"assignee":{"accountId":"acc-a"}},"changelog":[],"comments":[],"attachments":[]}
{"key":"T-3","fields":{"created":"2018-01-01T00:00:00.000+0000","status":{"name":"Open"},"priority":{"name":"Minor"},"assignee":null},"changelog":[{"created":"2019-05-01T00:00:00.000+0000","author":{"accountId":"acc-b"},"items":[{"field":"assignee","from":null,"fromString":null,"to":"acc-b","toString":"Bob"}]},{"created":"2019-05-02T00:00:00.000+0000","author":{"accountId":"acc-b"},"items":[{"field":"assignee","from":"acc-b","fromString":"Bob","to":null,"toString":null}]}],"comments":[],"attachments":[]}
```

Create `tests/test_build_temporal.py`:
```python
import sqlite3
from pathlib import Path

from graph_builder import build_temporal

FIX = Path(__file__).parent / "fixtures" / "temporal_tickets.jsonl"


def test_build_temporal_populates_events_and_history(tmp_path):
    db = str(tmp_path / "g.db")
    # pre-existing Stage-2a table must survive
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('T-1','Ticket')")
    c0.commit(); c0.close()

    counts = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    assert counts["records"] == 3
    assert counts["events"] == 3          # T-1 status, T-3 two assignee
    conn = sqlite3.connect(db)
    # nodes untouched
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1

    # T-1 status: seeded Open then Done (2 intervals)
    t1 = conn.execute(
        "SELECT value,valid_from,valid_to,source FROM attr_history "
        "WHERE node_id='T-1' AND attr='status' ORDER BY valid_from").fetchall()
    assert t1 == [("Open","2020-01-01T00:00:00.000+0000","2020-02-01T00:00:00.000+0000","changelog"),
                  ("Done","2020-02-01T00:00:00.000+0000","9999-12-31","changelog")]

    # T-2 zero-event fields -> snapshot-seed single intervals
    t2p = conn.execute(
        "SELECT value,source FROM attr_history WHERE node_id='T-2' AND attr='priority'").fetchall()
    assert t2p == [("Minor","snapshot-seed")]

    # as-of query: who was assigned to T-3 on 2019-05-01T12:00? -> acc-b
    asof = conn.execute(
        "SELECT value FROM attr_history WHERE node_id='T-3' AND attr='assignee' "
        "AND valid_from <= ? AND valid_to > ?",
        ("2019-05-01T12:00:00.000+0000","2019-05-01T12:00:00.000+0000")).fetchone()
    assert asof[0] == "acc-b"
    conn.close()


def test_build_temporal_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    b = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_temporal.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.build_temporal'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/build_temporal.py`:
```python
"""Stage 2b driver: stream tickets.jsonl -> events + attr_history in the EXISTING
graph.db. Never touches nodes/edges. Constant memory; re-run rebuilds temporal
tables only.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import extract_events, fold_attr_history

logger = logging.getLogger("graph_builder.build_temporal")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, ev_rows, ah_rows):
    if ev_rows:
        conn.executemany(
            "INSERT INTO events(ticket_id,ts,author,field,from_id,from_val,to_id,to_val) "
            "VALUES(:ticket_id,:ts,:author,:field,:from_id,:from_val,:to_id,:to_val)", ev_rows)
    if ah_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO attr_history(node_id,attr,value,valid_from,valid_to,source) "
            "VALUES(:node_id,:attr,:value,:valid_from,:valid_to,:source)", ah_rows)


def build_temporal(jsonl_path: str, db_path: str, batch_size: int = 5000,
                   limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    temporal_schema.init_temporal(conn)

    records = ev_count = ah_count = disc_count = 0
    ev_rows: list[dict] = []
    ah_rows: list[dict] = []
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
            evs = extract_events(rec)
            ah, disc = fold_attr_history(rec)
            ev_rows.extend(evs); ah_rows.extend(ah)
            ev_count += len(evs); ah_count += len(ah); disc_count += len(disc)
            if len(ev_rows) >= batch_size or len(ah_rows) >= batch_size:
                _flush(conn, ev_rows, ah_rows); ev_rows.clear(); ah_rows.clear()
            if records % 50000 == 0:
                conn.commit()
                logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, ev_rows, ah_rows)
    conn.commit()
    temporal_schema.create_temporal_indexes(conn)
    counts = {"records": records, "events": ev_count,
              "attr_rows": ah_count, "discrepancies": disc_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_temporal(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()
```

Note: `attr_history` INSERT uses `INSERT OR IGNORE` to be safe against the `(node_id,attr,valid_from)` PK; the fold never emits duplicate keys, but this keeps re-runs and any same-timestamp oddity crash-free.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_temporal.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/build_temporal.py tests/test_build_temporal.py tests/fixtures/temporal_tickets.jsonl && git commit -m "feat(temporal): streaming build_temporal driver + CLI"
```

---

### Task 5: Real-data smoke + reconcile a known ticket's history

**Files:** none (verification). Uses real `data/tickets.jsonl` and the Stage-2a `data/graph.db` if present (else a temp db).

- [ ] **Step 1: Full suite green**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest -q`
Expected: PASS (all Stage 2a + 2b tests).

- [ ] **Step 2: Build temporal tables on a 20k slice**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -m graph_builder.build_temporal data/tickets.jsonl /tmp/graph_temporal_smoke.db 20000
```
Expected: log ends with `done: {'records': 20000, 'events': <E>, 'attr_rows': <A>, 'discrepancies': <D>}`, with E and A > 0. Note the discrepancy count (data-quality signal; expected small relative to records).

- [ ] **Step 3: Reconcile EIMV2-14875's assignee history (validated live earlier this session)**

The first ticket in the file is `EIMV2-14875`: created 2014-08-14, no status events (→ status seeded "Open" for its whole life), and assignee set to Naveen Kumar's accountId on 2019-05-07T23:10 then unset at 23:16 (current: None).

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -c "
import sqlite3; c=sqlite3.connect('/tmp/graph_temporal_smoke.db')
print('status intervals:', c.execute(\"SELECT value,valid_from,valid_to,source FROM attr_history WHERE node_id='EIMV2-14875' AND attr='status'\").fetchall())
print('assignee intervals:', c.execute(\"SELECT value,valid_from,valid_to FROM attr_history WHERE node_id='EIMV2-14875' AND attr='assignee' ORDER BY valid_from\").fetchall())
print('assignee AS-OF 2019-05-07T23:12:', c.execute(\"SELECT value FROM attr_history WHERE node_id='EIMV2-14875' AND attr='assignee' AND valid_from<=? AND valid_to>?\", ('2019-05-07T23:12:00.000+0530',)*2).fetchone())
"
```
Expected: status = one `Open` interval `[2014-08-14…, 9999-12-31)` with `source='snapshot-seed'`; assignee intervals = `None [created→2019-05-07T23:10)`, `<Naveen accountId> [23:10→23:16)`, `None [23:16→sentinel)`; and the AS-OF 2019-05-07T23:12 query returns Naveen's accountId. Record the result in the commit message.

- [ ] **Step 4: Commit the verification note**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git commit --allow-empty -m "test(temporal): stage-2b smoke on 20k slice; EIMV2-14875 status(seeded Open)+assignee(None->Naveen 2019-05-07->None) reconciled, as-of query correct"
```

---

## Notes for Stage 2b-part-2 (do NOT build now)
- Edge-validity refinement: use `Link`/`Epic Link`/membership (`Component`,`labels`,`Sprint`,`Fix version`) change events to set `edges.valid_from`/`valid_to`; `type_confidence='phrase-mapped'` for historical link types (target key is deterministic from `item.to`/`item.from`).
- Key-alias map from `Key` change events (`EIM-5998`→`EIMV2-14875`); resolve `MENTIONS`/link targets across renames.
- `resolution` attr_history once a snapshot source is available (ingestor fetched `resolutiondate`, not `resolution`).
- Multi-valued field history (labels/components) as temporal membership edges rather than attr_history.
- Optional: surface `discrepancies` count in `index_status()` as a data-quality metric.
```
