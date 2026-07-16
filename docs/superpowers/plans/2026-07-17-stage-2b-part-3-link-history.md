# Stage 2b (part 3) — Link History (temporal link intervals) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fold the changelog's link add/remove events together with each ticket's current `issuelinks` snapshot into a `link_history` table of valid-time intervals — one row per period a link of a given type existed between a ticket and a target — so the graph can answer "what was linked to X as-of date D" and "when was the link between X and Y active."

**Architecture:** A pure fold (`fold_link_history`) turns one ticket record into link-interval rows, using the same snapshot+changelog pattern as `fold_attr_history`: a `remove` event (or a link present-now with no add event) implies the link existed since `created_ts`; adds/removes pair into `[valid_from, valid_to)` intervals. A streaming driver opens the *existing* `data/graph.db` and populates ONE new table (`link_history`) without touching any other table.

**Tech Stack:** Python 3.13 (`.venv`), stdlib `sqlite3`+`json`+`re`, `pytest`. Fully offline, no LLM.

## Global Constraints

- **Fully local/offline. No LLM, no network.**
- **Deterministic.** Same input → same `link_history`. Target key + timing are exact (from `link_events`); the link *type* is best-effort (phrase-mapped from the add event, else the current `issuelinks` canonical type).
- **Constant memory.** Stream `data/tickets.jsonl` (46 GB) line-by-line; batch-insert; buffer cleared after each flush.
- **Additive & non-destructive.** The driver creates/refreshes ONLY `link_history`. It MUST NOT drop or modify `nodes`, `edges`, `events`, `attr_history`, `key_alias`, or `link_events`. Re-running rebuilds only `link_history`.
- **`PRAGMA synchronous=NORMAL`** (the shared db holds non-rebuildable `nodes`/`edges`).
- **Half-open intervals `[valid_from, valid_to)` with strict `>`; sentinel `valid_to='9999-12-31'`** (never NULL). Skip zero-length intervals (`valid_from == valid_to`).
- **Per-ticket perspective.** `link_history` is keyed on the ticket that recorded the event (`node_id`), not canonicalized/deduped across endpoints (that reconciliation onto `edges` is deferred). `target_key` is stored raw (resolve via `key_alias` at query time).

### Fold rules (per (ticket, target_key))
Inputs per ticket: `created_ts` (`fields.created`), the changelog-derived link events (via `extract_link_events`, which already gives `{action, target_key, mapped_type, ts}`), and the current links snapshot (`fields.issuelinks` → `{target_key: canonical_type}`).

For each `target_key` appearing in either the events or the current snapshot:
- **Type selection** (`link_type`): the `mapped_type` of the first `add` event for that target; else the current snapshot's canonical type; else the first event's `mapped_type`.
- **No events** but present in current snapshot → one interval `[created_ts, SENTINEL)`, `source="snapshot-seed"`.
- **Events present:** walk them sorted by `ts`. Initial presence: if the first event is a `remove`, the link existed since creation → `present_since = created_ts`; if the first event is an `add`, the link was absent before → `present_since = None`. Then for each event: `add` sets `present_since = ts` (if not already open); `remove` closes the open interval `[present_since, ts)` and sets `present_since = None`. After the walk, if still open, emit `[present_since, SENTINEL)`. `source="changelog"`. Skip any zero-length interval.
- **Discrepancy:** if a target is present in the current snapshot but the fold leaves it *closed* (last event was a remove, never re-added), append `{"ticket","target","reason":"present-in-snapshot-but-closed"}` (changelog gap) — do NOT fabricate an interval.

### Table shape (additive)
```sql
link_history(
  node_id    TEXT,
  target_key TEXT,
  link_type  TEXT,
  valid_from TEXT,
  valid_to   TEXT NOT NULL DEFAULT '9999-12-31',
  source     TEXT,
  PRIMARY KEY (node_id, target_key, link_type, valid_from)
) WITHOUT ROWID;
```

---

## File Structure

- `graph_builder/temporal_schema.py` — ADD `LINK_HISTORY_SCHEMA_SQL`, `LINK_HISTORY_INDEX_SQL`, `init_link_history(conn)`, `create_link_history_indexes(conn)`.
- `graph_builder/replay.py` — ADD `current_links(record)` and `fold_link_history(record)` (reusing `extract_link_events`, `SENTINEL`, and `normalize_link_type` from `graph_builder.extract`).
- `graph_builder/build_link_history.py` — streaming driver + CLI.
- `tests/test_temporal_schema.py`, `tests/test_replay.py`, `tests/test_build_link_history.py` — new tests.
- `tests/fixtures/link_history_tickets.jsonl` — 2 crafted records.

---

### Task 1: `link_history` schema

**Files:**
- Modify: `graph_builder/temporal_schema.py`
- Modify: `tests/test_temporal_schema.py`

**Interfaces:**
- Produces: `LINK_HISTORY_SCHEMA_SQL`, `LINK_HISTORY_INDEX_SQL`, `init_link_history(conn) -> None` (drops & recreates ONLY `link_history`), `create_link_history_indexes(conn) -> None`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_temporal_schema.py`:
```python
def test_init_link_history_creates_only_its_table(tmp_path):
    import sqlite3
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    temporal_schema.init_link_history(conn)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"link_history", "nodes"} <= tables
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    cols = {r[1] for r in conn.execute("PRAGMA table_info(link_history)")}
    assert {"node_id","target_key","link_type","valid_from","valid_to","source"} <= cols
    temporal_schema.create_link_history_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_link_history_asof" in idx
    # idempotent
    conn.execute("INSERT INTO link_history(node_id,target_key,link_type,valid_from) "
                 "VALUES('A','B','BLOCKS','t')")
    conn.commit()
    temporal_schema.init_link_history(conn)
    assert conn.execute("SELECT count(*) FROM link_history").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py::test_init_link_history_creates_only_its_table -v`
Expected: FAIL — `AttributeError: module 'graph_builder.temporal_schema' has no attribute 'init_link_history'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/temporal_schema.py`:
```python
LINK_HISTORY_SCHEMA_SQL = """
DROP TABLE IF EXISTS link_history;

CREATE TABLE link_history (
  node_id    TEXT,
  target_key TEXT,
  link_type  TEXT,
  valid_from TEXT,
  valid_to   TEXT NOT NULL DEFAULT '9999-12-31',
  source     TEXT,
  PRIMARY KEY (node_id, target_key, link_type, valid_from)
) WITHOUT ROWID;
"""

LINK_HISTORY_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_link_history_asof   ON link_history(node_id, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS ix_link_history_target ON link_history(target_key);
"""


def init_link_history(conn: sqlite3.Connection) -> None:
    conn.executescript(LINK_HISTORY_SCHEMA_SQL)
    conn.commit()


def create_link_history_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(LINK_HISTORY_INDEX_SQL)
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py -v`
Expected: PASS (all temporal_schema tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/temporal_schema.py tests/test_temporal_schema.py && git commit -m "feat(temporal): link_history schema"
```

---

### Task 2: `current_links` + `fold_link_history`

**Files:**
- Modify: `graph_builder/replay.py`
- Modify: `tests/test_replay.py`

**Interfaces:**
- Consumes: `SENTINEL`, `extract_link_events` (replay.py); `normalize_link_type` (from `graph_builder.extract`).
- Produces:
  - `current_links(record: dict) -> dict[str, str]` — `{target_key: canonical_type}` from `fields.issuelinks` (target = `outwardIssue.key` or `inwardIssue.key`; type = `normalize_link_type(type.name)`).
  - `fold_link_history(record: dict) -> tuple[list[dict], list[dict]]` — `(rows, discrepancies)`. Row: `{"node_id","target_key","link_type","valid_from","valid_to","source"}`. Discrepancy: `{"ticket","target","reason"}`. Applies the fold rules in Global Constraints.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_replay.py`:
```python
def _link_entry(ts, action, target, phrase):
    it = {"field": "Link"}
    if action == "add":
        it.update({"from": None, "fromString": None, "to": target, "toString": phrase})
    else:
        it.update({"from": target, "fromString": phrase, "to": None, "toString": None})
    return {"created": ts, "author": {"accountId": "a"}, "items": [it]}


def _link_rec(created, issuelinks=None, changelog=None):
    return {"key": "SUP-1",
            "fields": {"created": created, "issuelinks": issuelinks or []},
            "changelog": changelog or []}


def _link_by_target(rows, target):
    return sorted([r for r in rows if r["target_key"] == target],
                  key=lambda r: r["valid_from"])


def test_link_added_then_removed_is_closed_interval():
    rec = _link_rec("2014-08-14", issuelinks=[], changelog=[
        _link_entry("2014-10-24", "add", "EIM-1", "This issue devices linked to EIM-1"),
        _link_entry("2014-11-24", "remove", "EIM-1", "This issue devices linked to EIM-1"),
    ])
    rows, disc = replay.fold_link_history(rec)
    assert _link_by_target(rows, "EIM-1") == [
        {"node_id": "SUP-1", "target_key": "EIM-1", "link_type": "DEVICES_LINKED_TO",
         "valid_from": "2014-10-24", "valid_to": "2014-11-24", "source": "changelog"}]
    assert disc == []


def test_link_added_and_still_present_is_open_interval():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Blocks"},
                                 "outwardIssue": {"key": "SUP-9"}}],
                    changelog=[_link_entry("2020-01-01", "add", "SUP-9",
                                           "This issue blocks SUP-9")])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-9") == [
        {"node_id": "SUP-1", "target_key": "SUP-9", "link_type": "BLOCKS",
         "valid_from": "2020-01-01", "valid_to": replay.SENTINEL, "source": "changelog"}]


def test_link_present_at_creation_no_events_seeded():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Relates"},
                                 "inwardIssue": {"key": "SUP-5"}}])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-5") == [
        {"node_id": "SUP-1", "target_key": "SUP-5", "link_type": "RELATES_TO",
         "valid_from": "2014-08-14", "valid_to": replay.SENTINEL, "source": "snapshot-seed"}]


def test_link_present_since_creation_then_removed():
    # first event is a REMOVE -> link existed since creation
    rec = _link_rec("2014-08-14", issuelinks=[], changelog=[
        _link_entry("2015-01-01", "remove", "SUP-7", "This issue blocks SUP-7")])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-7") == [
        {"node_id": "SUP-1", "target_key": "SUP-7", "link_type": "BLOCKS",
         "valid_from": "2014-08-14", "valid_to": "2015-01-01", "source": "changelog"}]


def test_link_discrepancy_present_but_closed():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Blocks"},
                                 "outwardIssue": {"key": "SUP-9"}}],
                    changelog=[
                        _link_entry("2020-01-01", "add", "SUP-9", "This issue blocks SUP-9"),
                        _link_entry("2020-02-01", "remove", "SUP-9", "This issue blocks SUP-9")])
    rows, disc = replay.fold_link_history(rec)
    # closed interval emitted; discrepancy flagged (present in snapshot but closed)
    assert _link_by_target(rows, "SUP-9")[-1]["valid_to"] == "2020-02-01"
    assert {"ticket": "SUP-1", "target": "SUP-9",
            "reason": "present-in-snapshot-but-closed"} in disc
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -k "link_added or link_present or link_discrepancy" -v`
Expected: FAIL — `AttributeError: ... 'fold_link_history'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/replay.py` (add `from graph_builder.extract import normalize_link_type` to the top import block):
```python
def current_links(record: dict) -> dict:
    f = record.get("fields") or {}
    out = {}
    for l in (f.get("issuelinks") or []):
        other = l.get("outwardIssue") or l.get("inwardIssue") or {}
        k = other.get("key")
        if k:
            out[k] = normalize_link_type(((l.get("type") or {}).get("name")) or "")
    return out


def _lh_row(key, target, ltype, vfrom, vto, source):
    return {"node_id": key, "target_key": target, "link_type": ltype,
            "valid_from": vfrom, "valid_to": vto, "source": source}


def fold_link_history(record: dict):
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
        evs = sorted(by_target.get(target, []), key=lambda e: e["ts"])
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
        elif target in snapshot:
            # changelog says removed, but snapshot still has it -> gap
            discrepancies.append({"ticket": key, "target": target,
                                  "reason": "present-in-snapshot-but-closed"})

    return rows, discrepancies
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: PASS (all replay tests, old + new).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/replay.py tests/test_replay.py && git commit -m "feat(temporal): fold_link_history (link validity intervals)"
```

---

### Task 3: Streaming build driver + CLI

**Files:**
- Create: `graph_builder/build_link_history.py`
- Create: `tests/fixtures/link_history_tickets.jsonl`
- Create: `tests/test_build_link_history.py`

**Interfaces:**
- Consumes: `temporal_schema.init_link_history`/`create_link_history_indexes`, `replay.fold_link_history`.
- Produces:
  - `build_link_history(jsonl_path, db_path, batch_size=5000, limit=None) -> dict` — opens existing db, (re)creates `link_history`, streams, inserts (`INSERT OR IGNORE`), indexes after load, returns `{"records","intervals","discrepancies"}`. Touches only `link_history`.
  - `main()` — CLI: argv `[jsonl] [db] [limit]`, defaults `data/tickets.jsonl` → `data/graph.db`.

- [ ] **Step 1: Create the fixture and write the failing test**

Create `tests/fixtures/link_history_tickets.jsonl` (exactly 2 lines, one JSON object per line):
```json
{"key":"EIMV2-14875","fields":{"created":"2014-08-14T00:00:00.000+0530","issuelinks":[{"type":{"name":"Relates"},"outwardIssue":{"key":"EIM-15379"}}]},"changelog":[{"created":"2014-10-24T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Link","from":null,"fromString":null,"to":"EIM-15015","toString":"This issue devices linked to EIM-15015"}]},{"created":"2014-11-24T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Link","from":"EIM-15015","fromString":"This issue devices linked to EIM-15015","to":null,"toString":null}]},{"created":"2014-11-26T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Link","from":null,"fromString":null,"to":"EIM-15379","toString":"This issue devices linked to EIM-15379"}]}],"comments":[],"attachments":[]}
{"key":"SUP-2","fields":{"created":"2020-01-01T00:00:00.000+0530","issuelinks":[]},"changelog":[],"comments":[],"attachments":[]}
```

Create `tests/test_build_link_history.py`:
```python
import sqlite3
from pathlib import Path

from graph_builder import build_link_history

FIX = Path(__file__).parent / "fixtures" / "link_history_tickets.jsonl"


def test_build_link_history(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('EIMV2-14875','Ticket')")
    c0.commit(); c0.close()

    counts = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    assert counts["records"] == 2
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched

    # EIM-15015: added 10-24, removed 11-24 -> closed interval
    r1 = conn.execute("SELECT valid_from,valid_to,source FROM link_history "
                      "WHERE node_id='EIMV2-14875' AND target_key='EIM-15015'").fetchall()
    assert r1 == [("2014-10-24T00:00:00.000+0530","2014-11-24T00:00:00.000+0530","changelog")]

    # EIM-15379: added 11-26 and present in snapshot -> open interval
    r2 = conn.execute("SELECT valid_from,valid_to FROM link_history "
                      "WHERE node_id='EIMV2-14875' AND target_key='EIM-15379'").fetchall()
    assert r2 == [("2014-11-26T00:00:00.000+0530","9999-12-31")]

    # as-of 2014-11-01: EIM-15015 active, EIM-15379 not yet
    d = "2014-11-01T00:00:00.000+0530"
    active = {r[0] for r in conn.execute(
        "SELECT target_key FROM link_history WHERE node_id='EIMV2-14875' "
        "AND valid_from<=? AND valid_to>?", (d, d))}
    assert active == {"EIM-15015"}
    conn.close()


def test_build_link_history_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    b = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_link_history.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.build_link_history'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/build_link_history.py`:
```python
"""Stage 2b-part-3 driver: stream tickets.jsonl -> link_history in the EXISTING
graph.db. Never touches other tables. Constant memory; re-run rebuilds only
link_history.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import fold_link_history

logger = logging.getLogger("graph_builder.build_link_history")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, rows):
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO link_history"
            "(node_id,target_key,link_type,valid_from,valid_to,source) "
            "VALUES(:node_id,:target_key,:link_type,:valid_from,:valid_to,:source)", rows)


def build_link_history(jsonl_path: str, db_path: str, batch_size: int = 5000,
                       limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    temporal_schema.init_link_history(conn)

    records = interval_count = disc_count = 0
    rows: list[dict] = []
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
            lh, disc = fold_link_history(rec)
            rows.extend(lh)
            interval_count += len(lh); disc_count += len(disc)
            if len(rows) >= batch_size:
                _flush(conn, rows); rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, rows)
    conn.commit()
    temporal_schema.create_link_history_indexes(conn)
    counts = {"records": records, "intervals": interval_count, "discrepancies": disc_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_link_history(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_link_history.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/build_link_history.py tests/test_build_link_history.py tests/fixtures/link_history_tickets.jsonl && git commit -m "feat(temporal): build_link_history driver + CLI"
```

---

### Task 4: Real-data smoke + reconcile EIMV2-14875

**Files:** none (verification).

- [ ] **Step 1: Full suite green**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest -q`
Expected: PASS (all Stage 2a + 2b parts 1/2/3 tests).

- [ ] **Step 2: Build on a 20k slice**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -m graph_builder.build_link_history data/tickets.jsonl /tmp/graph_link_history_smoke.db 20000
```
Expected: log ends with `done: {'records': 20000, 'intervals': <N>, 'discrepancies': <D>}`, N > 0.

- [ ] **Step 3: Reconcile EIMV2-14875 (validated against live Jira earlier this session)**

EIMV2-14875 link changelog: add EIM-15015 (2014-10-24) then remove EIM-15015 (2014-11-24); add EIM-15379 (2014-11-26); add ARM-597 (2015-08-07). EIM-15015 should be a CLOSED interval; EIM-15379 and ARM-597 should be OPEN (still present).

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -c "
import sqlite3; c=sqlite3.connect('/tmp/graph_link_history_smoke.db')
print('EIMV2-14875 link_history:')
for r in c.execute(\"SELECT target_key,link_type,valid_from,valid_to,source FROM link_history WHERE node_id='EIMV2-14875' ORDER BY valid_from\").fetchall(): print('  ', r)
d='2014-11-01T00:00:00.000+0530'
print('active links AS-OF 2014-11-01:', {r[0] for r in c.execute(\"SELECT target_key FROM link_history WHERE node_id='EIMV2-14875' AND valid_from<=? AND valid_to>?\", (d,d))})
"
```
Expected: EIM-15015 closed `[2014-10-24 … 2014-11-24)`; EIM-15379 and ARM-597 open to sentinel; and the as-of 2014-11-01 set = `{EIM-15015}` (added, not yet removed; the others added later). Record the result in the commit message.

- [ ] **Step 4: Commit the verification note**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git commit --allow-empty -m "test(temporal): stage-2b-part-3 smoke on 20k slice; EIMV2-14875 link_history EIM-15015 closed [10-24..11-24), EIM-15379/ARM-597 open, as-of 2014-11-01 = {EIM-15015} reconciled"
```

---

## Notes for later (do NOT build now)
- Materialize link validity onto the canonical deduped `edges` table (direction reconciliation between per-ticket `link_history` and `(src,dst,type)` edges).
- Membership-edge history (labels/components/sprint/fixVersion) via set-diff of consecutive changelog `toString` states.
- `resolution` attr_history (ingestor fetched `resolutiondate`, not `resolution`).
- `index_status()` surfacing counts for events/attr_history/key_alias/link_events/link_history + discrepancy totals.
- Resolve `target_key` through `key_alias` at query time (Stage 4) so old-key link targets join to current nodes.
```
