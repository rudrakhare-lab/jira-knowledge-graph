# Jira-Memory-MCP — Design Spec

**Date:** 2026-06-26
**Status:** Locked (design approved by user; not open to re-litigation)
**Instance:** `moveinsync.atlassian.net` (cloudId `a6e123b0-3842-4bc0-b88a-8a83e1c5458f`)
**Scale target:** ~1,000,000 tickets, fully local / offline

---

## 1. Purpose

A local, offline knowledge-graph + hybrid-search engine over the organization's
Jira tickets, exposed to LLMs via the Model Context Protocol (MCP). Inspired by
the architecture of `codebase-memory-mcp` (local SQLite graph + recursive-CTE
traversal + bundled search + MCP tools), but built fresh in **Python** and
retargeted from source code to Jira issues.

It answers four classes of question over the full ticket history:

1. **Semantic + keyword ticket search** — "find tickets about login timeouts"
2. **Relationship tracing** — blocker chains, blast radius up to the Epic
3. **Expert / ownership finding** — who owns / knows a component or area
4. **Pattern & history insight** — regression patterns, time-in-status,
   incident correlation

## 2. Constraints (final, not up for debate)

- **Fully local / offline.** No cloud services. No third-party data egress.
- **Language:** Python.
- **Ingestion:** Jira Cloud REST API, parallel fetching, resumable + checkpointed.
- **Query surface (v1):** MCP server only (Option A). API/web layers are out of
  scope for v1 and will sit on top later without reworking v1.
- **Storage:**
  - **SQLite** — graph (nodes/edges tables, recursive-CTE traversal) + FTS5 BM25
    keyword search.
  - **Qdrant** — local binary (no cloud), vector ANN search with **HNSW** at 1M
    scale.
  - **Embedding model:** `nomic-embed-text` via `sentence-transformers`
    (NOT MiniLM).
  - Hybrid search fuses BM25 + vector results with **Reciprocal Rank Fusion (RRF)**.

## 3. Architecture — four independent stages, built in dependency order

```
Stage 1 INGESTOR      Stage 2 GRAPH BUILDER     Stage 3 SEARCH            Stage 4 MCP SERVER
Jira REST API    ──▶  tickets.jsonl       ──▶   SQLite graph        ──▶   4 tool groups over
parallel,             nodes + edges             FTS5 (BM25) +             stages 2 + 3
resumable,            into SQLite               Qdrant (HNSW) +
checkpointed                                    RRF fusion
   │ tickets.jsonl        │ SQLite                │ FTS5 + Qdrant            │ MCP stdio
   ▼ (+ checkpoint)       ▼ (nodes/edges)         ▼ indexes                 ▼ tools
```

Decomposition rationale: each stage has a single responsibility, a well-defined
handoff artifact, and is independently testable. This sequences the build — it
does **not** reduce scope. All four capabilities at full 1M scale remain in
scope. Because Stage 1 emits a plain `tickets.jsonl` dump, Stages 2–4 can be
developed and tested against the first slice of real tickets while the full
backfill continues in the background.

Each stage gets its own implementation plan and is implemented in isolation. When
a stage is requested, only that stage is implemented — no future stages.

### Stage 1 — Ingestor

- Reads Jira via the REST search API using **token-based pagination**
  (`nextPageToken`, `maxResults` ≤ 100 — offset pagination is deprecated).
- Per ticket fetches **fields + comments (paginated) + changelog (paginated)**.
  Changelog is required for the `RESOLVED_BY` edge and status history.
- Parallel fetching bounded by Jira Cloud rate limits; exponential backoff on 429.
- **Resumable + checkpointed:** a checkpoint file records progress (page tokens /
  completed projects) so a run can stop and restart without re-fetching.
- Output: `tickets.jsonl` (one raw, complete ticket record per line) plus the
  checkpoint file. The ingestor knows nothing about graphs.
- Honest cost note: ~1M tickets at 100/page ≈ ≥10k search calls, plus per-ticket
  comment/changelog pagination. Realistic first-backfill wall-clock is
  hours-to-a-day-plus, rate-limit bound; checkpointing makes this survivable.

### Stage 2 — Graph Builder

- Reads `tickets.jsonl`; knows nothing about the Jira API.
- Populates SQLite `nodes` and `edges` tables (schema in §4).
- Detects whether the instance uses classic Epic-Link custom field vs. next-gen
  `parent` for `PARENT_OF` / `SUBTASK_OF`.
- Verifiable in isolation by spot-checking a node's edges against live Jira.

### Stage 3 — Search

- Reads the graph; builds two indexes:
  - `tickets_fts` — SQLite FTS5 over summary + description + comments (BM25).
  - Qdrant collection — `nomic-embed-text` embeddings, HNSW index.
- **Decoupled embedding backfill:** BM25 is queryable as soon as text lands; the
  vector collection backfills as a separate resumable stage so first queries do
  not block on embedding all 1M tickets.
- Hybrid query fuses BM25 + Qdrant ANN via RRF.

### Stage 4 — MCP Server

- Thin layer. Each tool is a SQL / recursive-CTE query or a search call.
- Knows nothing about how the data was ingested or indexed.
- Tool groups in §5.

## 4. Graph schema

**Nodes:** `Ticket`, `Epic`, `Project`, `User`, `Component`, `Sprint`, `Label`.
Stored in a `nodes` table `(id, type, attrs JSON)`. Ticket type (Bug/Story/Task/
Sub-task) carried as an attribute.

**Edges** (`edges` table `(src, dst, type, attrs JSON)`, indexed on `(type)`,
`(src,type)`, `(dst,type)`):

Issue-link edges (mapped to confirmed real instance link types):

| Edge | Jira link type (instance) |
|------|---------------------------|
| `BLOCKS` | Blocks (blocks / is blocked by) |
| `RELATES_TO` | Relates (relates to) |
| `DUPLICATES` | Duplicate (duplicates / is duplicated by) |
| `CLONES` | Cloners (clones / is cloned by) |
| `CAUSES` | Problem/Incident (causes / is caused by) |
| `REVIEWS` | Post-Incident Reviews (reviews / is reviewed by) |

Structural edges:

- `PARENT_OF` / `SUBTASK_OF` (Epic-link or `parent`, auto-detected)
- `IN_PROJECT`, `IN_SPRINT`, `HAS_COMPONENT`, `HAS_LABEL`
- `ASSIGNED_TO`, `REPORTED_BY`, `RESOLVED_BY` (resolver from changelog)
- `MENTIONS` (regex `[A-Z]+-\d+` in descriptions + comments)

Graph traversal (blocker chains, blast radius) uses recursive CTEs over `edges`.

## 5. MCP tool groups (4 groups → 4 capabilities)

**Group 1 — Search & context**
- `search_tickets(query, project?, filters?, mode=hybrid|keyword|semantic, limit)`
  — hybrid BM25 + Qdrant ANN with RRF fusion.
- `get_ticket_context(key)` — node + immediate edges + recent comments + status
  history.

**Group 2 — Relationship tracing**
- `trace_relationships(ticket, edge_types, direction, depth)` — recursive-CTE
  traversal (e.g. blocker chains over `BLOCKS`).
- `blast_radius(ticket)` — downstream-affected tickets, rolled up to Epic.

**Group 3 — Expert / ownership**
- `find_experts(scope)` where scope = component | label | project | free-text —
  ranked users blending **assignee + resolver + top commenter** signals.

**Group 4 — Pattern & history insight**
- `analyze_patterns(scope)` — regression/reopen patterns and time-in-status from
  changelog; incident correlation via `CAUSES` edges.
- `find_duplicates(ticket)` — near-duplicate detection via hybrid similarity.

**Utility**
- `index_status()` — counts of nodes/edges, FTS coverage, vector backfill progress.

## 6. Verification strategy

- **Stage 1:** fetched ticket counts reconcile against Jira's reported counts per
  project (JQL `count`).
- **Stage 2:** spot-check indexed nodes/edges for sample tickets against live Jira
  via the Atlassian MCP connector available in-session.
- **Stage 3:** known-item retrieval — a ticket whose text is known should rank in
  the top results for both BM25 and vector paths.
- **Stage 4:** each tool exercised against the spot-checked sample and reconciled
  with live Jira.

## 7. Out of scope for v1

- HTTP/API backend wrapping the Claude API (future Option B).
- Web / Slack UI (future Option C).
- Incremental webhook sync (future; v1 is bulk backfill + re-run).
- Access control / per-user repo filtering.
