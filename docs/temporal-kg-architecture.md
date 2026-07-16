# Temporal Relational Knowledge-Graph RAG — Architecture for the Jira KB

**Date:** 2026-07-16 · **Status:** Proposed, empirically validated, and article-reconciled (additive extension of the locked `docs/design.md`; the last open item — §8 concept layer — is now decided per the two Graph-RAG articles)
**Grounded in:** full scan of all 723,214 tickets + research of 8 repos/systems (Graphify, LightRAG, MS GraphRAG, MMGraphRAG, Graphiti/Zep, Cozo/XTDB, MCP memory server, SQLite-hybrid refs) + a live-Jira reconciliation of a real ticket (§10).

---

## 0. The one-sentence answer

> Build it ourselves on the locked SQLite + FTS5 + Qdrant + RRF + MCP stack, and add a **valid-time bitemporal layer sourced deterministically from the Jira changelog** — no LLM for graph construction — so every node attribute and every relationship carries a `[valid_from, valid_to)` interval and the whole graph can be queried *as it existed on any date*.

Nothing off-the-shelf fits (all 8 evaluated; best "adopt" score 3/5). The decisive facts:
1. **The graph is already in the data** — 339k typed links + parent/subtask + people + membership, deterministic, no LLM, and *more accurate* than LLM extraction.
2. **The timeline is already in the data** — 7.65M timestamped, authored changelog events (95% coverage) are a near-complete event log. We event-source it (folded with the current snapshot, §4), we don't infer it. This is our advantage over Graphiti/Zep, which spends an LLM to *guess* valid-times from prose.

---

## 1. Design principles

1. **Deterministic construction.** Graph + temporal validity come from JSON `fields` + `changelog`, zero LLM calls. (LLM entity/relation extraction — GraphRAG/LightRAG-default/MMGraphRAG — is redundant *and* infeasible at 723k docs offline.)
2. **Valid-time-dominant now; full bitemporal later.** v1 is "bulk backfill + re-run" (overwrite), so transaction time is degenerate — a single thin `recorded_at` stub. Add the 2nd axis (`expired_at`, supersede-don't-overwrite) only when incremental webhook sync / retroactive corrections arrive.
3. **Additive, not a redesign.** New columns + new tables; the four-stage handoff (`tickets.jsonl` → SQLite → indexes → MCP) is preserved. `design.md` stays intact.
4. **Split churn from structure for scale.** High-volume scalar history (status/priority/assignee) lives in `attr_history`; the ~170k distinct relationships live in `edges`. Never let status churn bloat the traversal table.
5. **Temporal fields are real, indexed columns — never inside `attrs JSON`.** SQLite cannot range-index into JSON.
6. **Offline, local, embeddable.** SQLite + Qdrant + local embeddings (Ollama / sentence-transformers). No cloud in the critical path.

---

## 2. Node & edge taxonomy

**Nodes** (stable identity, never versioned): `Ticket`, `Epic`, `Project`, `User`, `Component`, `Sprint`, `Label`, `FixVersion`.

**Structural edges** (in `edges`, with validity intervals):
- Link edges from `issuelinks` (current) + `Link` changelog events (history): `BLOCKS`, `RELATES_TO`, `DUPLICATES`, `CLONES`, `CAUSES` (Problem/Incident), `REVIEWS` (Post-Incident Reviews), plus instance-specific custom link types (e.g. "devices linked to").
- Hierarchy from `parent`/`subtasks` + `Epic Link`/`Epic Child` events: `PARENT_OF` / `SUBTASK_OF`.
- Membership: `IN_PROJECT`, `IN_SPRINT`, `HAS_COMPONENT`, `HAS_LABEL`, `HAS_FIXVERSION`.
- People: `REPORTED_BY` (stable), `ASSIGNED_TO` (temporal — from `assignee` events), `RESOLVED_BY` (from resolution / status→Done events).
- `MENTIONS` (regex `[A-Z]+-\d+` in descriptions/comments) — the only "soft" edge in v1. Note keys can migrate (validated: `EIM-5998` → `EIMV2-14875`); maintain a key-alias map so mentions/links resolve across renames.

**Evolving scalar attributes** (in `attr_history`, not edges): `status`, `priority`, `resolution`, `assignee`, `sprint`, `fixVersion`, `labels`, plus custom fields of interest. This is what the changelog is *mostly* made of.

---

## 3. Schema (additive extension of the locked `nodes`/`edges`)

```sql
-- NODES: unchanged shape; stable identity, never versioned.
nodes(
  id TEXT PRIMARY KEY,          -- "SUP-269421", "user:<accountId>", "component:<name>"...
  type TEXT,                    -- Ticket|Epic|Project|User|Component|Sprint|Label|FixVersion
  attrs JSON,                   -- IMMUTABLE/descriptive only (summary, issuetype, project key...)
  created_ts TEXT,              -- ticket creation (real, indexed)
  recorded_at TEXT             -- ingest/audit stub (thin transaction-time)
);

-- EVENTS: append-only, mirrors the Jira changelog 1:1. Ground truth + replay source. Never mutated.
events(
  event_id INTEGER PRIMARY KEY,
  ticket_id TEXT,
  ts TEXT,                      -- when the change happened (valid time)   [changelog.created]
  author TEXT,                  -- accountId
  field TEXT,                   -- status|assignee|priority|Link|Epic Link|...  [item.field]
  from_id TEXT, from_val TEXT,  -- item.from / item.fromString
  to_id TEXT,   to_val TEXT     -- item.to   / item.toString  (Link events carry target key in to_id)
);
CREATE INDEX ix_events_ticket_field_ts ON events(ticket_id, field, ts);
CREATE INDEX ix_events_ts ON events(ts);

-- ATTR_HISTORY: derived by a snapshot+changelog fold (§4). Evolving SCALAR node attributes.
attr_history(
  node_id TEXT,
  attr TEXT,                    -- status|priority|assignee|...
  value TEXT,
  valid_from TEXT,             -- created_ts for the seed segment, else event.ts
  valid_to   TEXT DEFAULT '9999-12-31',   -- successor event.ts; sentinel = still open
  source TEXT,                 -- 'changelog' | 'snapshot-seed' (provenance / confidence)
  event_id INTEGER             -- provenance → events (NULL for seeds)
);
CREATE INDEX ix_attr_asof ON attr_history(node_id, attr, valid_from, valid_to);

-- EDGES: unchanged shape + validity columns. Distinct relationships (dedup by link id).
edges(
  src TEXT, dst TEXT, type TEXT, attrs JSON,
  valid_from TEXT,             -- link-add event ts, or ticket.created_ts if present-at-creation
  valid_to   TEXT DEFAULT '9999-12-31',   -- link-remove event ts; sentinel = open
  type_confidence TEXT,        -- 'exact' (from issuelinks) | 'phrase-mapped' (from Link toString)
  recorded_at TEXT
);
CREATE INDEX ix_edges_src_asof ON edges(src, type, valid_from, valid_to);
CREATE INDEX ix_edges_dst_asof ON edges(dst, type, valid_from, valid_to);
```

**Conventions that prevent the classic bugs:**
- Half-open intervals `[valid_from, valid_to)` with **strict `>`** in as-of predicates (no boundary double-count).
- **Sentinel `valid_to = '9999-12-31'`** instead of `NULL` (keeps as-of predicate + recursive CTEs branch-free).
- ISO-8601 TEXT timestamps (lexicographically sortable); epoch-int copies in Qdrant payload for range filters.

---

## 4. Build pipeline (extends Stage 2) — CORRECTED fold

`tickets.jsonl` → **Graph Builder** → **Temporal Replay** (new sub-stage) → SQLite.

1. **Nodes/edges (current state):** parse each ticket's `fields` → nodes + current structural edges (`issuelinks` give type **exactly**; dedup by link `id` — the raw 339k link appearances ≈ ~170k distinct links, since each shows on both endpoints). ADF descriptions parsed with an iterative extractor (all 461,897 descriptions are ADF).
2. **Events:** stream every `changelog[].items[]` into `events` verbatim, preserving `from`/`to` ids (7.65M rows).
3. **Attribute fold — MUST consume snapshot + changelog together** (this is the correctness-critical part; changelog alone is insufficient):
   - **Fields WITH events:** seed the first interval `[created_ts, first_event.ts)` with the first event's `fromString` (the pre-change value), then walk each event's `toString` as the next value with `valid_from = event.ts`. The **final `toString` should equal the current snapshot value** — assert this per field as a completeness check; a mismatch flags a changelog gap.
   - **Fields WITHOUT events** (e.g. status/priority that never changed — common: Minor priority = 438k tickets): emit a single interval `[created_ts, sentinel] = snapshot value`, `source='snapshot-seed'`. *Without this, as-of queries return nothing for unchanged fields at any date — the bug this fold fixes.*
4. **Edge history:** `Link`/`Epic Link` events set edge validity. **Target key + timing are deterministic** (from `item.to`/`item.from` = the issue key, validated §10); **link type is phrase-mapped** from `toString` against the instance's bounded link-type vocabulary (mark `type_confidence='phrase-mapped'`). Links present at creation with no add-event get `valid_from = created_ts`, `type_confidence='exact'`. → *Edge **existence & timing** are exact; edge **type** for historical links is best-effort. `graph_diff` (§6) is therefore exact on structure, approximate on type labels.*
5. **Backfill embeddings** (decoupled, resumable): §5.

All steps are pure functions of the data — re-runnable, verifiable against live Jira.

---

## 5. Retrieval (Stage 3) — hybrid + temporal

Keep the locked **FTS5(BM25) + Qdrant(HNSW) + RRF** pipeline; inject a time constraint at each leg:

- **BM25:** FTS5 gives candidate ids → outer SQL applies the as-of/range predicate via join to `attr_history`/`nodes`.
- **Vector:** store `valid_from`, `valid_to`, `created_ts` (epoch ints) as **Qdrant payload**; use payload **range filters** alongside ANN. No re-embedding for temporal queries. **Sizing:** the corpus is ~723k tickets but embedding is chunk-level — summary+description+comments ≈ **a few million chunks** (2.1M comments alone); size HNSW for millions, not 723k.
- **Graph traversal:** thread the as-of date **through the recursive CTE** — apply `valid_from <= :D AND valid_to > :D` at each hop, so you traverse *the graph as it existed on D*.
- **Fuse** the three time-scoped lists with RRF (`score = Σ 1/(k+rank)`, k≈60 — ~20 lines, we own it; ref: `tailorlite/sqlite-hybrid`, `fidx`).
- **Recency:** hard scope = filter-then-rank (primary for "as-of 2018"); soft "current state" = a recency-sorted list fused as a 3rd RRF input (no score normalization needed).

**Embedding cost control (critical):** embed core text only — summary + description + comments (~330M tokens, tractable offline). **Do NOT bulk-embed the 17.2 GB of attachment text** (13× core, mostly xlsx dumps); gate by MIME/size/value.

**Context formatting (Zep's key trick):** return retrieved facts *with their valid ranges* — `FACT (valid 2018-03-01 → 2021-06-10)` — so the LLM can reason about *when* things were true.

---

## 6. MCP tool surface (Stage 4)

Modeled on the official MCP memory server (`search_nodes`/`open_nodes`/`read_graph`) + `as_of` temporal params (ref: `memento-mcp` for temporal patterns).

**Search & context**
- `search_tickets(query, mode=hybrid|keyword|semantic, as_of?, between?, project?, filters?, limit)`
- `get_ticket_context(key, as_of?)` — node + edges + recent comments + status history, reconstructed as-of a date.

**Relationship tracing (as-of aware)**
- `trace_relationships(key, edge_types, direction, depth, as_of?)` — recursive-CTE over time-valid edges.
- `blast_radius(key, as_of?)` — downstream affected, rolled up to Epic, as it existed then.

**Temporal-native (the new capabilities)**
- `ticket_timeline(key, from?, to?)` — ordered scan of `events`: every status/owner/field change with author + timestamp. *Trivial and exact because the changelog is complete.*
- `graph_diff(key, date_a, date_b)` — set-diff two as-of traversals → relationships/attributes added or removed between two dates ("how interdependencies changed 2018→2025"). Exact on edge existence/timing; type labels best-effort for historical links.
- `state_as_of(key, date)` — full reconstructed ticket state on a date.

**Expertise & patterns**
- `find_experts(scope, as_of?)` — assignee + resolver + top-commenter blend, optionally time-scoped ("who owned X *in 2020*").
- `analyze_patterns(scope)` — regression/reopen patterns + time-in-status, from `events` (reopen = status transitions back from Done; time-in-status = interval lengths in `attr_history`). Optional DuckDB sidecar for heavy aggregation over 7.65M events.

**Utility**
- `index_status()` — node/edge/event counts, FTS coverage, vector-backfill progress.

---

## 7. What we reuse vs build

| Concern | Decision | Source |
|---|---|---|
| Graph store | **Build** on SQLite (2-table nodes/edges + recursive CTE) | pattern: `dpapathanasiou/simple-graph` |
| Bitemporal model | **Borrow the model** (valid_from/valid_to + recorded_at) | Graphiti/Zep edge model; XTDB / `pg_bitemporal` |
| Hybrid + RRF | **Build** (~20-line RRF), FTS5 + Qdrant | `tailorlite/sqlite-hybrid`, `fidx` (Python, offline) |
| MCP surface | **Copy the tool naming** + add `as_of` | `modelcontextprotocol/servers/src/memory`, `memento-mcp` |
| Analytics | **Optional sidecar** for temporal aggregation | DuckDB |
| Graphiti / LightRAG / GraphRAG | **Do NOT adopt as engine** | scale/offline/typed-edge mismatches |

**Two optional, off-critical-path experiments** (post-v1, must not compromise offline v1):
- **Cozo spike** — native `@ timestamp` as-of on `Validity` keys could collapse graph+vector+FTS+temporal into one embeddable file. Gating risks: ~19 months stale, scale unproven. *Prototype the as-of query on a sample before ever betting on it.*
- **GraphRAG-BYOG** — feed our deterministic graph to get *global community-summary* search (era-level rollups: "2018 summary of component X"). The one net-new capability none of our tools cover; but needs a structured-JSON LLM (offline weak point) and flattens typed edges. Experiment only.

---

## 8. The concept / global-summary layer — DECIDED (per the two Graph-RAG articles)

The two articles (Zilliz, Aug 2024; Brian Curry, Feb 2026) describe Microsoft-style GraphRAG: chunk → **LLM extracts entities+relations** → build graph → **Leiden community detection** → **LLM community summaries** → *Global search* (thematic) + *Local search* (entity neighborhood). They explicitly flag the tradeoffs: **"entity extraction via LLM is expensive… Microsoft warns indexing can be costly, start small"**, plus fragile **entity resolution** ("Dr. John Smith" vs "J. Smith" fragments the graph), and "not always necessary — benchmark against vanilla RAG."

**What this means for us — the articles validate the plan and resolve the open question:**

1. **Confirmed: skip per-chunk LLM extraction.** The articles show extraction exists to *manufacture a graph out of unstructured text*. We already have that graph, deterministically and typed — so we skip the single most expensive, most fragile step. Our explicit `BLOCKS/CAUSES/parent-of` edges are exactly what GraphRAG pays an LLM to approximate, and ours are better. Multi-hop reasoning (their headline benefit) we already get from typed recursive-CTE traversal.
2. **Adopt the ONE additive idea — community detection + summaries — cheaply.** The genuinely new capability our design lacks is *global/thematic* answers ("top recurring root causes across all tickets 2018→2025"). We get it **without** the expensive path:
   - Run **Leiden community detection over our existing deterministic graph** (non-LLM clustering).
   - Generate **one summary per community** — LLM cost is per-community (thousands), not per-chunk (millions). Optionally era-scoped (per year) to exploit our temporal layer: "2019 summary of the Payments cluster."
   - This is the affordable slice of GraphRAG-BYOG (§7), now promoted from "maybe" to **planned post-v1 layer**.
3. **Concept edges (`ABOUT_CONCEPT`) — tiered, cheapest-first.** For the 69% of tickets with no explicit link:
   - **v1:** the hybrid search layer already links by meaning (BM25+vector) — no new structure needed.
   - **v1.5 (non-LLM):** embedding-similarity clustering + `MENTIONS` to materialize "same-topic" edges offline.
   - **Only if needed:** LLM concept extraction — but the articles' cost warning stands, so this is last resort, not default.
4. **Two cheap borrows the articles surface:**
   - **Entity resolution matters** — invest in `User` dedup (22,283 users; same person, multiple accountIds) and the key-alias map. The articles name this as the top quality lever.
   - **Personalized PageRank** over the graph (topology + statistics, no LLM) is called out as consistently strong — a good ranking signal for `find_experts` and relevance without any LLM.

**Net:** nothing in the architecture changes; the articles confirm our "no-LLM-for-construction" thesis and turn the former open question into a concrete, tiered plan (community summaries + tiered concept edges), all off the offline critical path.

---

## 9. Build order (additive to design.md)

1. Stage 2a — nodes/edges current-state builder (deterministic). *Verify against live Jira.*
2. Stage 2b — `events` loader (changelog 1:1) + snapshot+changelog fold → `attr_history` + edge validity. *Verify a few tickets' timelines against Jira history; assert final-toString == snapshot per field.*
3. Stage 3 — FTS5 first (queryable immediately), then decoupled Qdrant embedding backfill (core text only) with temporal payload.
4. Stage 4 — MCP tools, `as_of`-aware, reconciled against spot-checked tickets.
5. (Later) full bitemporal + incremental sync; concept-layer per article; optional Cozo / GraphRAG-BYOG spikes.

---

## 11. Are Jira's built-in relations enough? (plain-language)

**Short answer: keep every relation Jira gives us — they are high-quality and free — but they are NOT enough on their own. We add three more layers on top.**

### What Jira already gives us (the reliable skeleton — keep all of it)
| Relation | From | Coverage |
|---|---|---|
| Typed links (Blocks/Relates/Duplicate/Causes/Clones/Reviews) | `issuelinks` | 223,989 tickets have ≥1 link (**31%**); ~170k distinct links |
| Parent / Sub-task | `parent`, `subtasks` | 55k parent, 14k with subtasks |
| Assignee / Reporter | fields | assignee on 567k (**78%**) |
| Membership (project/component/label/sprint/fixVersion) | fields | 41 / 91 / 1,183 / 466 |
| Status history, resolver, *when* links/assignees changed | `changelog` | 95% |

These are **precise and trustworthy** (a human or the system asserted them). They power blocker-chains, blast-radius, duplicates, hierarchy.

### Why they're not enough (the gap)
1. **Sparse.** Only **31% of tickets are explicitly linked** to any other ticket. The other **69% are islands** — a bug about "login timeout" filed in 2019 and another in 2023 are almost never linked, even though they're the same problem. Explicit links alone can't answer "find everything about X across 12 years."
2. **Fields are text, not connections yet.** "assignee: John", "component: Payments" are just strings sitting on a ticket. To ask "what does John know?" or "all tickets in Payments," those strings must become **nodes** you can traverse to.
3. **No time dimension in the raw links.** The live `issuelinks` field shows only *today's* links; the history of when links/owners/status changed lives in the changelog and must be folded in.

### The three layers we add on top (this is the "new relation logic")
1. **Promote fields → nodes.** Turn assignee/reporter/component/label/sprint/project/fixVersion/epic into first-class **nodes**, and connect every ticket to them (`ASSIGNED_TO`, `HAS_COMPONENT`, `IN_PROJECT`…). *This one step creates millions of connections* and makes ownership/expertise/area questions answerable — without it the graph is just loosely-linked tickets.
2. **Derive edges from text + changelog.** `MENTIONS` (regex ticket keys in descriptions/comments) catches implicit references; `RESOLVED_BY` (from changelog); a **key-alias map** (EIM-5998 = EIMV2-14875); and **valid-time intervals on every edge/attribute** (from the changelog) for the "as-of" queries.
3. **Connect "same-topic" tickets that were never linked.** This is the real fix for the 69% islands. Two mechanisms: (a) the **search layer** (BM25 + vector similarity) links tickets by meaning at query time — already in the plan; (b) an **optional concept layer** (§8) that materializes `ABOUT_CONCEPT` edges (error signatures, feature names). Layer (a) ships in v1; layer (b) waits for the article.

**Net:** explicit Jira links = precise typed traversal; promoted-field nodes = ownership/area traversal; derived + search + concept layers = "about the same thing" discovery. **We change nothing about the existing relations — we keep them and build additional nodes/edges around them.**

---

## 10. Validation evidence (live-Jira reconciliation)

Reconciled ticket **EIMV2-14875** (created 2014-08-14) against live Jira via the Atlassian MCP connector:

- **Changelog completeness:** live `total = 13` histories == our 13 ingested entries, identical timestamps/events. Confirms ingestion is complete & exact for this ticket (the assumption the whole temporal thesis rests on).
- **Zero-event-field bug confirmed & fixed:** the ticket has **no `status` events** yet current status = `Open`; naïve event-only replay would yield no status at any date. §4 step 3 (snapshot seed) fixes this.
- **Temporal attribute validated:** `assignee` was set (`Naveen Kumar`, 2019-05-07 23:10) then unset (23:16); current = null — reconstructs correctly as three intervals.
- **Edge history is semi-deterministic (better than feared):** `Link` events carry the **target key in `item.to`/`item.from`** (`EIM-15015` added 2014-10-24, removed 2014-11-24; `EIM-15379` 2014-11-26; `ARM-597` 2015-08-07) — existence + timing exact; only the link *type* needs phrase-mapping from `toString`.
- **Key migration observed:** `EIM-5998` → `EIMV2-14875` (2015-06-08) — motivates the key-alias map (§2).
