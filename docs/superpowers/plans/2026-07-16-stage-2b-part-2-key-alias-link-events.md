# Stage 2b (part 2) — Key-Alias Map + Link-Event Capture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture two more deterministic facts from the Jira changelog into SQLite: (1) a **key-alias map** so references to a ticket's old key (e.g. `EIM-5998`) resolve to its current key (`EIMV2-14875`), and (2) a **`link_events`** log of every issue-link add/remove with deterministic target + timing and a best-effort mapped link type.

**Architecture:** Pure functions turn one ticket record into key-alias rows and link-event rows; a streaming driver opens the *existing* `data/graph.db` and populates two NEW tables (`key_alias`, `link_events`) without touching `nodes`/`edges`/`events`/`attr_history`. No folding into interval form and no mutation of the canonical `edges` table — that (direction/dedup reconciliation, link snapshot-seeding, membership validity) is Stage 2b-part-3.

**Tech Stack:** Python 3.13 (`.venv`), stdlib `sqlite3`+`json`+`re`, `pytest`. Fully offline, no LLM.

## Global Constraints

- **Fully local/offline. No LLM, no network.**
- **Deterministic.** Same input → same `key_alias`/`link_events`. Target key and timing are exact (from `item.to`/`item.from`/entry `created`); only the link *type* is best-effort (phrase-mapped from the changelog's free-text `toString`/`fromString`).
- **Constant memory.** Stream `data/tickets.jsonl` (46 GB) line-by-line; batch-insert; buffers cleared after each flush.
- **Additive & non-destructive.** The driver creates/refreshes ONLY `key_alias` and `link_events`. It MUST NOT drop or modify `nodes`, `edges`, `events`, or `attr_history`. Re-running rebuilds only these two tables.
- **No mutation of `edges`.** Old-key resolution is provided as a lookup (`key_alias` + a helper) applied at query time (Stage 4), NOT by rewriting `edges` rows.

### Changelog facts used (validated against live Jira this session, ticket EIMV2-14875)
- `Key` changelog item: `fromString` = an old key, and the ticket's current key = `record["key"]`. (EIMV2-14875 has `Key` `EIM-5998`→`EIMV2-14875`.)
- `Link` changelog item: `to` = target key + `toString` = phrase (an ADD); `from` = target key + `fromString` = phrase (a REMOVE). Target key is in `to`/`from` deterministically; the phrase (e.g. `"This issue devices linked to EIM-15015"`, `"This issue blocks FOO-1"`) carries the type. (EIMV2-14875: add EIM-15015, remove EIM-15015, add EIM-15379, add ARM-597 — all custom "devices linked to".)

### Table shapes (additive)
```sql
key_alias(
  old_key TEXT PRIMARY KEY,
  current_key TEXT
);
link_events(
  link_event_id INTEGER PRIMARY KEY,
  ticket_id TEXT, ts TEXT, action TEXT,      -- 'add' | 'remove'
  target_key TEXT, type_phrase TEXT, mapped_type TEXT
);
```

---

## File Structure

- `graph_builder/temporal_schema.py` — ADD `ALIAS_LINK_SCHEMA_SQL`, `ALIAS_LINK_INDEX_SQL`, `init_alias_link(conn)`, `create_alias_link_indexes(conn)` (separate from the existing events/attr_history init so the two 2b builds are independent).
- `graph_builder/replay.py` — ADD `map_link_phrase(phrase)`, `extract_key_aliases(record)`, `extract_link_events(record)`.
- `graph_builder/build_alias_link.py` — streaming driver + CLI (`python -m graph_builder.build_alias_link`).
- `tests/test_temporal_schema.py` — ADD a test for the new init/indexes.
- `tests/test_replay.py` — ADD tests for the three new functions.
- `tests/test_build_alias_link.py` — end-to-end test.
- `tests/fixtures/alias_link_tickets.jsonl` — 2 crafted records.

---

### Task 1: Schema for key_alias + link_events

**Files:**
- Modify: `graph_builder/temporal_schema.py`
- Modify: `tests/test_temporal_schema.py`

**Interfaces:**
- Produces: `ALIAS_LINK_SCHEMA_SQL`, `ALIAS_LINK_INDEX_SQL`, `init_alias_link(conn) -> None` (drops & recreates ONLY `key_alias`+`link_events`), `create_alias_link_indexes(conn) -> None`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_temporal_schema.py`:
```python
def test_init_alias_link_creates_only_its_tables(tmp_path):
    import sqlite3
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    temporal_schema.init_alias_link(conn)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"key_alias", "link_events", "nodes"} <= tables
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    kcols = {r[1] for r in conn.execute("PRAGMA table_info(key_alias)")}
    lcols = {r[1] for r in conn.execute("PRAGMA table_info(link_events)")}
    assert {"old_key", "current_key"} <= kcols
    assert {"link_event_id","ticket_id","ts","action","target_key",
            "type_phrase","mapped_type"} <= lcols
    temporal_schema.create_alias_link_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_link_events_ticket_ts" in idx
    # idempotent
    conn.execute("INSERT INTO key_alias VALUES('OLD-1','NEW-1')")
    conn.commit()
    temporal_schema.init_alias_link(conn)
    assert conn.execute("SELECT count(*) FROM key_alias").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py::test_init_alias_link_creates_only_its_tables -v`
Expected: FAIL — `AttributeError: module 'graph_builder.temporal_schema' has no attribute 'init_alias_link'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/temporal_schema.py`:
```python
ALIAS_LINK_SCHEMA_SQL = """
DROP TABLE IF EXISTS key_alias;
DROP TABLE IF EXISTS link_events;

CREATE TABLE key_alias (
  old_key     TEXT PRIMARY KEY,
  current_key TEXT
);

CREATE TABLE link_events (
  link_event_id INTEGER PRIMARY KEY,
  ticket_id     TEXT,
  ts            TEXT,
  action        TEXT,
  target_key    TEXT,
  type_phrase   TEXT,
  mapped_type   TEXT
);
"""

ALIAS_LINK_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_link_events_ticket_ts ON link_events(ticket_id, ts);
CREATE INDEX IF NOT EXISTS ix_link_events_target    ON link_events(target_key);
"""


def init_alias_link(conn: sqlite3.Connection) -> None:
    conn.executescript(ALIAS_LINK_SCHEMA_SQL)
    conn.commit()


def create_alias_link_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(ALIAS_LINK_INDEX_SQL)
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_temporal_schema.py -v`
Expected: PASS (all temporal_schema tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/temporal_schema.py tests/test_temporal_schema.py && git commit -m "feat(temporal): key_alias + link_events schema"
```

---

### Task 2: `map_link_phrase`, `extract_key_aliases`, `extract_link_events`

**Files:**
- Modify: `graph_builder/replay.py`
- Modify: `tests/test_replay.py`

**Interfaces:**
- Produces:
  - `map_link_phrase(phrase: str | None) -> str` — map a changelog link phrase to a canonical type. Known verbs → `BLOCKS`/`RELATES_TO`/`DUPLICATES`/`CLONES`/`CAUSES`/`REVIEWS`; otherwise strip the ticket key and leading `"this issue "` and UPPER_SNAKE the remainder (e.g. `"This issue devices linked to EIM-15015"` → `DEVICES_LINKED_TO`); empty/None → `RELATED`.
  - `extract_key_aliases(record: dict) -> list[dict]` — one row `{"old_key","current_key"}` per `Key` changelog item (`old_key`=`fromString`, `current_key`=`record["key"]`), skipping items whose `fromString` is empty or already equals the current key.
  - `extract_link_events(record: dict) -> list[dict]` — one row per `Link` changelog item: `{"ticket_id","ts","action","target_key","type_phrase","mapped_type"}`. ADD when `to` set (`target_key`=`to`, phrase=`toString`); REMOVE when `from` set (`target_key`=`from`, phrase=`fromString`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_replay.py`:
```python
def test_map_link_phrase_known_and_custom():
    assert replay.map_link_phrase("This issue blocks FOO-1") == "BLOCKS"
    assert replay.map_link_phrase("This issue is blocked by FOO-1") == "BLOCKS"
    assert replay.map_link_phrase("This issue relates to FOO-1") == "RELATES_TO"
    assert replay.map_link_phrase("This issue duplicates FOO-1") == "DUPLICATES"
    assert replay.map_link_phrase("This issue devices linked to EIM-15015") == "DEVICES_LINKED_TO"
    assert replay.map_link_phrase("") == "RELATED"


def test_extract_key_aliases():
    rec = {"key": "EIMV2-14875", "fields": {"created": "t"}, "changelog": [
        {"created": "2015-06-08", "author": {"accountId": "a"}, "items": [
            {"field": "Key", "from": None, "fromString": "EIM-5998",
             "to": None, "toString": "EIMV2-14875"},
            {"field": "project", "fromString": "old", "toString": "new"}]}]}
    aliases = replay.extract_key_aliases(rec)
    assert aliases == [{"old_key": "EIM-5998", "current_key": "EIMV2-14875"}]


def test_extract_link_events_add_and_remove():
    rec = {"key": "T-1", "fields": {"created": "t"}, "changelog": [
        {"created": "2014-10-24", "author": {"accountId": "a"}, "items": [
            {"field": "Link", "from": None, "fromString": None,
             "to": "EIM-15015", "toString": "This issue devices linked to EIM-15015"}]},
        {"created": "2014-11-24", "author": {"accountId": "a"}, "items": [
            {"field": "Link", "from": "EIM-15015",
             "fromString": "This issue devices linked to EIM-15015",
             "to": None, "toString": None}]}]}
    evs = replay.extract_link_events(rec)
    assert evs == [
        {"ticket_id": "T-1", "ts": "2014-10-24", "action": "add",
         "target_key": "EIM-15015",
         "type_phrase": "This issue devices linked to EIM-15015",
         "mapped_type": "DEVICES_LINKED_TO"},
        {"ticket_id": "T-1", "ts": "2014-11-24", "action": "remove",
         "target_key": "EIM-15015",
         "type_phrase": "This issue devices linked to EIM-15015",
         "mapped_type": "DEVICES_LINKED_TO"},
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -k "link_phrase or key_aliases or link_events" -v`
Expected: FAIL — `AttributeError: ... 'map_link_phrase'`.

- [ ] **Step 3: Write minimal implementation**

Append to `graph_builder/replay.py` (the module already has `import` of stdlib only; add `import re` at the top import block if not present):
```python
import re  # (add to the top of the file if not already imported)

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_replay.py -v`
Expected: PASS (all replay tests, old + new).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/replay.py tests/test_replay.py && git commit -m "feat(temporal): key-alias + link-event extraction + phrase->type map"
```

---

### Task 3: Streaming build driver + CLI

**Files:**
- Create: `graph_builder/build_alias_link.py`
- Create: `tests/fixtures/alias_link_tickets.jsonl`
- Create: `tests/test_build_alias_link.py`

**Interfaces:**
- Consumes: `temporal_schema.init_alias_link`/`create_alias_link_indexes`, `replay.extract_key_aliases`/`extract_link_events`.
- Produces:
  - `resolve_key(conn, key) -> str` — returns the current key for `key` via `key_alias`, or `key` unchanged if not aliased.
  - `build_alias_link(jsonl_path, db_path, batch_size=5000, limit=None) -> dict` — opens existing db, (re)creates the two tables, streams, inserts (`key_alias` via `INSERT OR IGNORE` on its PK; `link_events` plain), indexes after load, returns `{"records","aliases","link_events"}`. Does NOT touch other tables.
  - `main()` — CLI: argv `[jsonl] [db] [limit]`, defaults `data/tickets.jsonl` → `data/graph.db`.

- [ ] **Step 1: Create the fixture and write the failing test**

Create `tests/fixtures/alias_link_tickets.jsonl` (exactly 2 lines, one JSON object per line):
```json
{"key":"EIMV2-14875","fields":{"created":"2014-08-14T00:00:00.000+0530"},"changelog":[{"created":"2014-10-24T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Link","from":null,"fromString":null,"to":"EIM-15015","toString":"This issue devices linked to EIM-15015"}]},{"created":"2014-11-24T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Link","from":"EIM-15015","fromString":"This issue devices linked to EIM-15015","to":null,"toString":null}]},{"created":"2015-06-08T00:00:00.000+0530","author":{"accountId":"a"},"items":[{"field":"Key","from":null,"fromString":"EIM-5998","to":null,"toString":"EIMV2-14875"}]}],"comments":[],"attachments":[]}
{"key":"SUP-2","fields":{"created":"2020-01-01T00:00:00.000+0530"},"changelog":[{"created":"2020-02-01T00:00:00.000+0530","author":{"accountId":"b"},"items":[{"field":"Link","from":null,"fromString":null,"to":"SUP-9","toString":"This issue blocks SUP-9"}]}],"comments":[],"attachments":[]}
```

Create `tests/test_build_alias_link.py`:
```python
import sqlite3
from pathlib import Path

from graph_builder import build_alias_link

FIX = Path(__file__).parent / "fixtures" / "alias_link_tickets.jsonl"


def test_build_alias_link(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('EIMV2-14875','Ticket')")
    c0.commit(); c0.close()

    counts = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    assert counts["records"] == 2
    assert counts["aliases"] == 1
    assert counts["link_events"] == 3      # EIMV2: add+remove; SUP-2: add

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    # alias resolves old key -> current
    assert build_alias_link.resolve_key(conn, "EIM-5998") == "EIMV2-14875"
    assert build_alias_link.resolve_key(conn, "SUP-2") == "SUP-2"     # unaliased passthrough
    # link event mapped type
    row = conn.execute("SELECT action,target_key,mapped_type FROM link_events "
                       "WHERE ticket_id='SUP-2'").fetchone()
    assert row == ("add", "SUP-9", "BLOCKS")
    conn.close()


def test_build_alias_link_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    b = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_alias_link.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'graph_builder.build_alias_link'`.

- [ ] **Step 3: Write minimal implementation**

Create `graph_builder/build_alias_link.py`:
```python
"""Stage 2b-part-2 driver: stream tickets.jsonl -> key_alias + link_events in the
EXISTING graph.db. Never touches nodes/edges/events/attr_history. Constant memory;
re-run rebuilds only these two tables.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import extract_key_aliases, extract_link_events

logger = logging.getLogger("graph_builder.build_alias_link")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def resolve_key(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute(
        "SELECT current_key FROM key_alias WHERE old_key = ?", (key,)).fetchone()
    return row[0] if row else key


def _flush(conn, alias_rows, link_rows):
    if alias_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO key_alias(old_key,current_key) "
            "VALUES(:old_key,:current_key)", alias_rows)
    if link_rows:
        conn.executemany(
            "INSERT INTO link_events(ticket_id,ts,action,target_key,type_phrase,mapped_type) "
            "VALUES(:ticket_id,:ts,:action,:target_key,:type_phrase,:mapped_type)", link_rows)


def build_alias_link(jsonl_path: str, db_path: str, batch_size: int = 5000,
                     limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    temporal_schema.init_alias_link(conn)

    records = alias_count = link_count = 0
    alias_rows: list[dict] = []
    link_rows: list[dict] = []
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
            al = extract_key_aliases(rec)
            le = extract_link_events(rec)
            alias_rows.extend(al); link_rows.extend(le)
            alias_count += len(al); link_count += len(le)
            if len(alias_rows) >= batch_size or len(link_rows) >= batch_size:
                _flush(conn, alias_rows, link_rows)
                alias_rows.clear(); link_rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, alias_rows, link_rows)
    conn.commit()
    temporal_schema.create_alias_link_indexes(conn)
    counts = {"records": records, "aliases": alias_count, "link_events": link_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_alias_link(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()
```

Note: `aliases` count is the number of alias rows *emitted* (before `INSERT OR IGNORE` dedup); for the fixture that equals the distinct count. That is the intended contract for the idempotency test.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_alias_link.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add graph_builder/build_alias_link.py tests/test_build_alias_link.py tests/fixtures/alias_link_tickets.jsonl && git commit -m "feat(temporal): build_alias_link driver + resolve_key + CLI"
```

---

### Task 4: Real-data smoke + reconcile EIMV2-14875

**Files:** none (verification).

- [ ] **Step 1: Full suite green**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest -q`
Expected: PASS (all Stage 2a + 2b + 2b-part-2 tests).

- [ ] **Step 2: Build on a 20k slice**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -m graph_builder.build_alias_link data/tickets.jsonl /tmp/graph_alias_link_smoke.db 20000
```
Expected: log ends with `done: {'records': 20000, 'aliases': <A>, 'link_events': <L>}`, with A and L > 0.

- [ ] **Step 3: Reconcile EIMV2-14875 (validated against live Jira earlier this session)**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -c "
import sqlite3; c=sqlite3.connect('/tmp/graph_alias_link_smoke.db')
print('EIM-5998 resolves to:', c.execute(\"SELECT current_key FROM key_alias WHERE old_key='EIM-5998'\").fetchone())
print('EIMV2-14875 link_events:')
for r in c.execute(\"SELECT ts,action,target_key,mapped_type FROM link_events WHERE ticket_id='EIMV2-14875' ORDER BY ts\").fetchall(): print('  ', r)
"
```
Expected: `EIM-5998` → `EIMV2-14875`; link_events show add `EIM-15015`, remove `EIM-15015`, add `EIM-15379`, add `ARM-597` (all `mapped_type='DEVICES_LINKED_TO'`), matching the live-Jira changelog. Record the result in the commit message.

- [ ] **Step 4: Commit the verification note**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git commit --allow-empty -m "test(temporal): stage-2b-part-2 smoke on 20k slice; EIMV2-14875 alias EIM-5998->EIMV2-14875 + link_events (add/remove EIM-15015, add EIM-15379, add ARM-597 = DEVICES_LINKED_TO) reconciled"
```

---

## Notes for Stage 2b-part-3 (do NOT build now)
- Fold `link_events` into per-(ticket,target,mapped_type) validity intervals (`link_history`), snapshot-seeding links present at creation (reconcile current `issuelinks` type with phrase-mapped type — the fuzzy step deferred here).
- Materialize validity onto the canonical `edges` table (direction/dedup reconciliation).
- Membership-edge validity (labels/components/sprint/fixVersion add/remove via set-diff of changelog `toString`).
- `resolution` attr_history (needs a snapshot source; ingestor fetched `resolutiondate`, not `resolution`).
- Surface `discrepancies` + alias/link counts in `index_status()`.
```
