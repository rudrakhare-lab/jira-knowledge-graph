# Stage 3 (part 1) — FTS5 Keyword Search (BM25) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a SQLite FTS5 full-text index (`tickets_fts`) over each ticket's summary + description + comments, and expose a BM25-ranked `search_bm25(query)` — giving keyword search over all 723k tickets, fully offline, with no new dependencies.

**Architecture:** A pure text-assembler turns one ticket record into `{key, summary, description, comments}` (ADF descriptions/comments flattened to text via the existing `graph_builder.adf.adf_to_text`). A streaming driver opens the *existing* `data/graph.db` and populates the `tickets_fts` virtual table without touching any other table. Query sanitizes user input into a safe FTS5 MATCH expression and ranks by `bm25()`. This is the BM25 leg of the eventual hybrid (BM25 + vectors + RRF) search; vectors are Stage 3-part-2.

**Tech Stack:** Python 3.13 (`.venv`), stdlib `sqlite3` (FTS5 confirmed compiled in: SQLite 3.53.2) + `json` + `re`, `pytest`. Fully offline, no LLM, no embeddings.

## Global Constraints

- **Fully local/offline. No LLM, no network, no embeddings** (embeddings are Stage 3-part-2).
- **Deterministic.** Same input → same `tickets_fts` content.
- **Constant memory.** Stream `data/tickets.jsonl` (46 GB) line-by-line; batch-insert; buffer cleared after each flush.
- **Additive & non-destructive.** The build driver creates/refreshes ONLY `tickets_fts` (and its FTS5 shadow tables). It MUST NOT drop or modify `nodes`, `edges`, `events`, `attr_history`, `key_alias`, `link_events`, or `link_history`. Re-running rebuilds only `tickets_fts`.
- **`PRAGMA synchronous=NORMAL`** (the shared db holds non-rebuildable `nodes`/`edges`).
- **Core text only.** Index `summary` + `description` + `comments`. Do NOT index attachment extracted text (17.2 GB, mostly xlsx dumps) — that gating is intentional (`docs/temporal-kg-architecture.md` §5).
- **Query safety.** Raw user queries MUST be sanitized before being passed to FTS5 `MATCH` (a bare `-` or `"` is FTS5 syntax and would raise). Tokenize to word characters, quote each term.

### Table shape (additive; FTS5 virtual table in the shared graph.db)
```sql
CREATE VIRTUAL TABLE tickets_fts USING fts5(
  key UNINDEXED,   -- stored, returned in results, not tokenized
  summary,
  description,
  comments
);
```
`bm25(tickets_fts)` returns a score where **more negative = better**; order ascending.

---

## File Structure

- `search/fts.py` — `FTS_SCHEMA_SQL`, `init_fts(conn)`, `searchable_text(record)`, `_fts_query(raw)`, `search_bm25(conn, query, limit)`. (The `search/` package already exists with an empty `__init__.py`.)
- `search/build_fts.py` — streaming driver + CLI (`python -m search.build_fts`).
- `tests/test_fts.py` — schema + text-assembly + query-sanitize + in-memory search tests.
- `tests/test_build_fts.py` — end-to-end build + search test.
- `tests/fixtures/fts_tickets.jsonl` — 3 crafted records.

---

### Task 1: FTS5 schema + non-destructive init

**Files:**
- Create: `search/fts.py`
- Create: `tests/test_fts.py`

**Interfaces:**
- Produces: `FTS_SCHEMA_SQL: str`, `init_fts(conn: sqlite3.Connection) -> None` (drops & recreates ONLY `tickets_fts`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_fts.py`:
```python
import sqlite3
from search import fts


def test_init_fts_creates_only_tickets_fts(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    fts.init_fts(conn)
    # tickets_fts exists and is queryable
    conn.execute("INSERT INTO tickets_fts(key,summary,description,comments) "
                 "VALUES('SUP-1','login timeout','','')")
    conn.commit()
    assert conn.execute("SELECT key FROM tickets_fts WHERE tickets_fts MATCH 'login'"
                        ).fetchone()[0] == "SUP-1"
    # nodes untouched
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    # idempotent: re-init clears tickets_fts, leaves nodes
    fts.init_fts(conn)
    assert conn.execute("SELECT count(*) FROM tickets_fts").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'search.fts'`.

- [ ] **Step 3: Write minimal implementation**

Create `search/fts.py`:
```python
"""Stage 3 (part 1) — FTS5 keyword search over ticket text.

`tickets_fts` is an FTS5 virtual table in the shared graph.db; init_fts drops &
recreates ONLY it (its shadow tables go with it). BM25 ranking via bm25().
"""
from __future__ import annotations

import re
import sqlite3

FTS_SCHEMA_SQL = """
DROP TABLE IF EXISTS tickets_fts;
CREATE VIRTUAL TABLE tickets_fts USING fts5(
  key UNINDEXED,
  summary,
  description,
  comments
);
"""


def init_fts(conn: sqlite3.Connection) -> None:
    conn.executescript(FTS_SCHEMA_SQL)
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add search/fts.py tests/test_fts.py && git commit -m "feat(search): FTS5 tickets_fts schema + init"
```

---

### Task 2: `searchable_text` — assemble per-ticket searchable text

**Files:**
- Modify: `search/fts.py`
- Modify: `tests/test_fts.py`

**Interfaces:**
- Consumes: `graph_builder.adf.adf_to_text`.
- Produces: `searchable_text(record: dict) -> dict` — `{"key","summary","description","comments"}`. `summary` = `fields.summary or ""`; `description` = `adf_to_text(fields.description)`; `comments` = space-joined `adf_to_text(c["body"])` over `record["comments"]`. Attachment text is NOT included.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fts.py`:
```python
def test_searchable_text_flattens_adf_and_comments():
    rec = {"key": "SUP-5", "fields": {
        "summary": "Login timeout on mobile",
        "description": {"type": "doc", "content": [
            {"type": "paragraph", "content": [
                {"type": "text", "text": "Users hit a 504 after 30s"}]}]}},
        "comments": [
            {"body": {"type": "doc", "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "reproduced on staging"}]}]}}],
        "attachments": [{"extracted_text": "SHOULD NOT BE INDEXED"}]}
    t = fts.searchable_text(rec)
    assert t["key"] == "SUP-5"
    assert t["summary"] == "Login timeout on mobile"
    assert "504" in t["description"]
    assert "reproduced on staging" in t["comments"]
    # attachment text is excluded
    assert "SHOULD NOT BE INDEXED" not in (t["summary"]+t["description"]+t["comments"])


def test_searchable_text_handles_missing_fields():
    t = fts.searchable_text({"key": "X-1", "fields": {}})
    assert t == {"key": "X-1", "summary": "", "description": "", "comments": ""}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: FAIL — `AttributeError: module 'search.fts' has no attribute 'searchable_text'`.

- [ ] **Step 3: Write minimal implementation**

Append to `search/fts.py` (add `from graph_builder.adf import adf_to_text` to the top import block):
```python
def searchable_text(record: dict) -> dict:
    f = record.get("fields") or {}
    summary = f.get("summary") or ""
    description = adf_to_text(f.get("description"))
    parts = []
    for c in (record.get("comments") or []):
        parts.append(adf_to_text(c.get("body")))
    return {"key": record["key"], "summary": summary,
            "description": description, "comments": " ".join(parts)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add search/fts.py tests/test_fts.py && git commit -m "feat(search): searchable_text (summary+description+comments)"
```

---

### Task 3: `_fts_query` sanitizer + `search_bm25`

**Files:**
- Modify: `search/fts.py`
- Modify: `tests/test_fts.py`

**Interfaces:**
- Produces:
  - `_fts_query(raw: str) -> str` — tokenize `raw` to word-character terms (lowercased), double-quote each, join with spaces (implicit AND). Empty/no-terms → `""`.
  - `search_bm25(conn, query: str, limit: int = 20) -> list[tuple[str, float]]` — returns `[(key, score)]` ranked best-first (bm25 ascending). Returns `[]` for an empty sanitized query.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fts.py`:
```python
def test_fts_query_sanitizes():
    assert fts._fts_query("login timeout") == '"login" "timeout"'
    # hyphen / punctuation split into safe terms (no FTS5 syntax injection)
    assert fts._fts_query("EIM-15015") == '"eim" "15015"'
    assert fts._fts_query("   ") == ""
    assert fts._fts_query('"; DROP') == '"drop"'


def _mk_fts(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    fts.init_fts(conn)
    rows = [
        ("A-1", "login timeout on mobile", "user sees 504 gateway timeout", ""),
        ("A-2", "payment gateway error", "card declined at checkout", "timeout seen once"),
        ("A-3", "dashboard slow", "charts take 10s to render", ""),
    ]
    conn.executemany("INSERT INTO tickets_fts(key,summary,description,comments) "
                     "VALUES(?,?,?,?)", rows)
    conn.commit()
    return conn


def test_search_bm25_ranks_and_filters(tmp_path):
    conn = _mk_fts(tmp_path)
    res = fts.search_bm25(conn, "timeout", limit=10)
    keys = [k for k, _ in res]
    # A-1 (timeout in summary+desc) and A-2 (timeout in comments) match; A-3 does not
    assert set(keys) == {"A-1", "A-2"}
    assert keys[0] == "A-1"                      # stronger match ranks first
    assert all(isinstance(s, float) for _, s in res)
    # empty query -> no results, no crash
    assert fts.search_bm25(conn, "   ") == []
    # multi-term AND
    assert [k for k, _ in fts.search_bm25(conn, "gateway timeout")] == ["A-1"]
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: FAIL — `AttributeError: ... '_fts_query'`.

- [ ] **Step 3: Write minimal implementation**

Append to `search/fts.py`:
```python
def _fts_query(raw: str) -> str:
    terms = re.findall(r"\w+", (raw or "").lower())
    return " ".join('"%s"' % t for t in terms)


def search_bm25(conn: sqlite3.Connection, query: str, limit: int = 20):
    q = _fts_query(query)
    if not q:
        return []
    return conn.execute(
        "SELECT key, bm25(tickets_fts) AS score FROM tickets_fts "
        "WHERE tickets_fts MATCH ? ORDER BY score LIMIT ?", (q, limit)).fetchall()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_fts.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add search/fts.py tests/test_fts.py && git commit -m "feat(search): _fts_query sanitizer + search_bm25 (BM25 ranking)"
```

---

### Task 4: Streaming build driver + CLI

**Files:**
- Create: `search/build_fts.py`
- Create: `tests/fixtures/fts_tickets.jsonl`
- Create: `tests/test_build_fts.py`

**Interfaces:**
- Consumes: `fts.init_fts`, `fts.searchable_text`, `fts.search_bm25`.
- Produces:
  - `build_fts(jsonl_path, db_path, batch_size=2000, limit=None) -> dict` — opens existing db, (re)creates `tickets_fts`, streams, inserts, returns `{"records","indexed"}`. Touches only `tickets_fts`.
  - `main()` — CLI: argv `[jsonl] [db] [limit]`, defaults `data/tickets.jsonl` → `data/graph.db`.

- [ ] **Step 1: Create the fixture and write the failing test**

Create `tests/fixtures/fts_tickets.jsonl` (exactly 3 lines, one JSON object per line):
```json
{"key":"SUP-1","fields":{"summary":"Login timeout on mobile app","description":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Users get a 504 gateway timeout after 30 seconds on the login screen"}]}]}},"comments":[],"attachments":[]}
{"key":"SUP-2","fields":{"summary":"Payment gateway declined","description":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Card payments fail at checkout with an error"}]}]}},"comments":[{"body":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"a timeout was seen once during retry"}]}]}}],"attachments":[]}
{"key":"SUP-3","fields":{"summary":"Dashboard renders slowly","description":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"charts take ten seconds to appear"}]}]}},"comments":[],"attachments":[]}
```

Create `tests/test_build_fts.py`:
```python
import sqlite3
from pathlib import Path

from search import build_fts, fts

FIX = Path(__file__).parent / "fixtures" / "fts_tickets.jsonl"


def test_build_fts_and_search(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    c0.commit(); c0.close()

    counts = build_fts.build_fts(str(FIX), db, batch_size=2)
    assert counts == {"records": 3, "indexed": 3}

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    # known-item: "504 gateway timeout" -> SUP-1 ranks first
    res = fts.search_bm25(conn, "gateway timeout", limit=5)
    assert res[0][0] == "SUP-1"
    # comment-only term still matches (SUP-2 has 'timeout' only in a comment)
    keys = {k for k, _ in fts.search_bm25(conn, "timeout", limit=5)}
    assert {"SUP-1", "SUP-2"} <= keys and "SUP-3" not in keys
    conn.close()


def test_build_fts_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_fts.build_fts(str(FIX), db, batch_size=2)
    b = build_fts.build_fts(str(FIX), db, batch_size=2)
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_fts.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'search.build_fts'`.

- [ ] **Step 3: Write minimal implementation**

Create `search/build_fts.py`:
```python
"""Stage 3 (part 1) driver: stream tickets.jsonl -> tickets_fts in the EXISTING
graph.db. Never touches other tables. Constant memory; re-run rebuilds only
tickets_fts.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from search.fts import init_fts, searchable_text

logger = logging.getLogger("search.build_fts")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, rows):
    if rows:
        conn.executemany(
            "INSERT INTO tickets_fts(key,summary,description,comments) "
            "VALUES(:key,:summary,:description,:comments)", rows)


def build_fts(jsonl_path: str, db_path: str, batch_size: int = 2000,
              limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_fts(conn)

    records = indexed = 0
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
            rows.append(searchable_text(rec))
            indexed += 1
            if len(rows) >= batch_size:
                _flush(conn, rows); rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("indexed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, rows)
    conn.commit()
    counts = {"records": records, "indexed": indexed}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_fts(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest tests/test_build_fts.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git add search/build_fts.py tests/test_build_fts.py tests/fixtures/fts_tickets.jsonl && git commit -m "feat(search): build_fts streaming driver + CLI"
```

---

### Task 5: Real-data smoke + known-item retrieval

**Files:** none (verification).

- [ ] **Step 1: Full suite green**

Run: `cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/pytest -q`
Expected: PASS (all prior + new tests).

- [ ] **Step 2: Build FTS on a 20k slice**

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -m search.build_fts data/tickets.jsonl /tmp/graph_fts_smoke.db 20000
```
Expected: log ends with `done: {'records': 20000, 'indexed': 20000}`.

- [ ] **Step 3: Known-item retrieval check**

Pick a ticket with a distinctive multi-word summary from the index, search those words, and assert that ticket ranks in the top results (known-item retrieval per `docs/design.md` §6, Stage 3).

Run:
```bash
cd /Users/rudrakhare/jira-knowledge-graph && ./.venv/bin/python -c "
import sqlite3; from search.fts import search_bm25
c=sqlite3.connect('/tmp/graph_fts_smoke.db')
# choose a ticket with a reasonably long summary
key,summary=c.execute(\"SELECT key,summary FROM tickets_fts WHERE length(summary)>25 AND summary NOT LIKE '%null%' LIMIT 1\").fetchone()
print('probe ticket:', key, '::', summary)
res=search_bm25(c, summary, limit=10)
top=[k for k,_ in res]
print('top-10 keys:', top)
print('KNOWN-ITEM PASS:', key in top[:10])
# a couple of generic sanity queries
for q in ['login timeout','payment failed','device not working']:
    print(q, '->', [k for k,_ in search_bm25(c,q,limit=3)])
"
```
Expected: the probe ticket appears in its own summary search's top-10 (known-item retrieval works); generic queries return plausible keys. Record the probe key + PASS in the commit message.

- [ ] **Step 4: Commit the verification note**

```bash
cd /Users/rudrakhare/jira-knowledge-graph && git commit --allow-empty -m "test(search): stage-3-part-1 FTS5 smoke on 20k slice; known-item retrieval PASS (<KEY>), sanity queries reasonable"
```

---

## Notes for Stage 3-part-2 / part-3 (do NOT build now)
- **Vectors (part-2):** embed core text (summary+description+comments, ~330M tokens) with a local model (`nomic-embed-text` or `BAAI/bge-*` via sentence-transformers) into a local vector store (Qdrant local, or `sqlite-vec`/faiss) with HNSW; store `valid_from`/`valid_to`/`created_ts`/`project` as payload for temporal + facet filtering. Decoupled/resumable backfill so BM25 is usable before all vectors land. **Operational decision needed:** embedding model + vector store (Qdrant needs a local server/binary; `sqlite-vec` keeps everything in the one file).
- **RRF (part-3):** fuse BM25 + vector ranked lists via `score = Σ 1/(k+rank)` (k≈60). Add mode=hybrid|keyword|semantic. Add temporal `as_of`/`between` filters (join FTS candidates to `attr_history`/`nodes`; payload range-filter the vector leg).
- Optional BM25 tuning: column weights via `bm25(tickets_fts, wSummary, wDesc, wComments)` (weight summary higher); `porter` stemming tokenizer for recall; a `content=` external-content table to avoid duplicating text if disk matters.
```
