"""Stage 1 — Jira ingestor.

Fetches every issue from a Jira Cloud site via the token-paginated search
API and writes one complete record per ticket to ``data/tickets.jsonl``:

    {"key": "PROJ-123", "fields": {...}, "comments": [...],
     "changelog": [...], "attachments": [...]}

Per ticket we fetch: the configured fields, all comments (paginated), all
changelog history (paginated), and extracted text from attachments
(see attachments.py).

The run is resumable: progress is checkpointed to ``data/checkpoint.json``
({"last_next_page_token": ..., "fetched_count": N}) and read on startup.

Auth: HTTP Basic with JIRA_EMAIL + JIRA_API_TOKEN (base64 handled by aiohttp).

System dependency (for attachment OCR, see attachments.py):
    brew install tesseract        # macOS
    apt install tesseract-ocr     # Linux

Run:
    JIRA_EMAIL=you@org.com JIRA_API_TOKEN=xxxx python -m ingestor.fetch
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp

from ingestor.attachments import process_attachments

logger = logging.getLogger("ingestor.fetch")

# --- Configuration ----------------------------------------------------------
BASE_URL = os.environ.get("JIRA_BASE_URL", "https://moveinsync.atlassian.net").rstrip("/")
EMAIL = os.environ.get("JIRA_EMAIL")
API_TOKEN = os.environ.get("JIRA_API_TOKEN")

SEARCH_URL = f"{BASE_URL}/rest/api/3/search/jql"
# /search/jql rejects unbounded queries; the created floor matches every issue
# (earliest issue on this instance is from 2014) while keeping created-ASC order.
JQL = 'created >= "1970-01-01" ORDER BY created ASC'
PAGE_SIZE = 100            # max for /search/jql
SUBFETCH_PAGE_SIZE = 100   # comments / changelog page size

CONCURRENCY = 10           # semaphore bound for comments/changelog/attachments
CHECKPOINT_EVERY = 500     # tickets between checkpoint writes
LOG_EVERY = 1000           # tickets between progress logs

MAX_RETRIES = 3
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Sprint is NOT a system field on Jira Cloud; on moveinsync.atlassian.net the
# Sprint field is customfield_10006 (verified against a live issue). We request
# it explicitly and also expose it under a friendly "sprint" key downstream.
SPRINT_FIELD = "customfield_10006"
FIELDS = [
    "summary", "description", "status", "priority", "issuetype",
    "assignee", "reporter", "components", "labels", SPRINT_FIELD,
    "project", "created", "updated", "resolutiondate",
    "attachment",   # required for Stage 1b attachment extraction
    "issuelinks",   # BLOCKS, CAUSES, RELATES_TO edges — required for graph
    "subtasks",     # SUBTASK_OF edges
    "parent",       # PARENT_OF / epic-child edges
    "fixVersions",  # version context for pattern analysis
]

# --- Paths (resolved relative to the project root, regardless of cwd) --------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
TICKETS_FILE = DATA_DIR / "tickets.jsonl"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"


# --- HTTP with retry --------------------------------------------------------
async def get_json(
    session: aiohttp.ClientSession,
    semaphore: Optional[asyncio.Semaphore],
    url: str,
    params: dict,
) -> dict:
    """GET JSON with retry on 429 (Retry-After), 5xx, and connection errors.

    ``semaphore`` bounds concurrent sub-fetches (comments/changelog); pass
    ``None`` for the top-level sequential search pagination.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if semaphore is not None:
                await semaphore.acquire()
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status in RETRYABLE_STATUS:
                        delay = _retry_delay(resp, attempt)
                        logger.warning(
                            "GET %s -> HTTP %s, retry %d/%d in %.1fs",
                            url, resp.status, attempt, MAX_RETRIES, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            finally:
                if semaphore is not None:
                    semaphore.release()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES:
                raise
            delay = float(2 ** attempt)
            logger.warning(
                "GET %s error: %s, retry %d/%d in %.1fs",
                url, exc, attempt, MAX_RETRIES, delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(f"exhausted {MAX_RETRIES} retries for {url}")


def _retry_delay(resp: aiohttp.ClientResponse, attempt: int) -> float:
    if resp.status == 429:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
    return float(2 ** attempt)


async def post_json(
    session: aiohttp.ClientSession,
    url: str,
    body: dict,
) -> dict:
    """POST JSON with retry on 429 (Retry-After), 5xx, and connection errors.

    Used for /search/jql pagination, which requires a POST with a JSON body
    (``fields`` as an array). Runs sequentially, so no semaphore.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(url, json=body) as resp:
                if resp.status in RETRYABLE_STATUS:
                    delay = _retry_delay(resp, attempt)
                    logger.warning(
                        "POST %s -> HTTP %s, retry %d/%d in %.1fs",
                        url, resp.status, attempt, MAX_RETRIES, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES:
                raise
            delay = float(2 ** attempt)
            logger.warning(
                "POST %s error: %s, retry %d/%d in %.1fs",
                url, exc, attempt, MAX_RETRIES, delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(f"exhausted {MAX_RETRIES} retries for {url}")


# --- Per-ticket sub-fetches -------------------------------------------------
async def fetch_all_comments(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    key: str,
) -> list:
    url = f"{BASE_URL}/rest/api/3/issue/{key}/comment"
    start_at = 0
    out: list = []
    while True:
        page = await get_json(
            session, semaphore, url,
            {"startAt": start_at, "maxResults": SUBFETCH_PAGE_SIZE},
        )
        comments = page.get("comments", [])
        out.extend(comments)
        total = page.get("total", len(out))
        start_at += len(comments)
        if not comments or start_at >= total:
            break
    return out


async def fetch_all_changelog(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    key: str,
) -> list:
    url = f"{BASE_URL}/rest/api/3/issue/{key}/changelog"
    start_at = 0
    out: list = []
    while True:
        page = await get_json(
            session, semaphore, url,
            {"startAt": start_at, "maxResults": SUBFETCH_PAGE_SIZE},
        )
        values = page.get("values", [])
        out.extend(values)
        start_at += len(values)
        if page.get("isLast") or not values or start_at >= page.get("total", 0):
            break
    return out


async def enrich_issue(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    issue: dict,
) -> dict:
    """Build the full tickets.jsonl record for one search-result issue."""
    key = issue["key"]
    fields = issue.get("fields", {}) or {}
    # Friendly alias so downstream stages don't need the custom-field id.
    fields["sprint"] = fields.get(SPRINT_FIELD)

    comments, changelog, attachments = await asyncio.gather(
        fetch_all_comments(session, semaphore, key),
        fetch_all_changelog(session, semaphore, key),
        process_attachments(session, semaphore, fields.get("attachment")),
    )
    return {
        "key": key,
        "fields": fields,
        "comments": comments,
        "changelog": changelog,
        "attachments": attachments,
    }


# --- Checkpoint -------------------------------------------------------------
def read_checkpoint() -> tuple[Optional[str], int]:
    if not CHECKPOINT_FILE.exists():
        return None, 0
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        return data.get("last_next_page_token"), int(data.get("fetched_count", 0))
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("could not read checkpoint (%s); starting fresh", exc)
        return None, 0


def write_checkpoint(token: Optional[str], fetched_count: int) -> None:
    """Atomically persist progress (tmp file + os.replace)."""
    tmp = CHECKPOINT_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(
        {"last_next_page_token": token, "fetched_count": fetched_count}
    ))
    os.replace(tmp, CHECKPOINT_FILE)


# --- Main ingestion loop ----------------------------------------------------
async def run() -> None:
    if not EMAIL or not API_TOKEN:
        sys.exit("JIRA_EMAIL and JIRA_API_TOKEN must be set in the environment.")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    next_page_token, fetched_count = read_checkpoint()
    if fetched_count:
        logger.info(
            "resuming from checkpoint: %d tickets fetched, token=%s",
            fetched_count, next_page_token,
        )

    semaphore = asyncio.Semaphore(CONCURRENCY)
    auth = aiohttp.BasicAuth(EMAIL, API_TOKEN)
    started = time.monotonic()
    since_checkpoint = 0
    last_logged = fetched_count - (fetched_count % LOG_EVERY)

    async with aiohttp.ClientSession(
        auth=auth, headers={"Accept": "application/json"}
    ) as session, aiofiles.open(TICKETS_FILE, mode="a") as out:
        while True:
            body = {
                "jql": JQL,
                "maxResults": PAGE_SIZE,
                "fields": FIELDS,
            }
            if next_page_token:
                body["nextPageToken"] = next_page_token

            page = await post_json(session, SEARCH_URL, body)
            issues = page.get("issues", [])
            next_page_token = page.get("nextPageToken")

            if not issues:
                break

            # Enrich the whole page concurrently; HTTP ops are gated by the
            # shared semaphore (10). Flush all lines BEFORE checkpointing so a
            # checkpoint token never points past un-persisted tickets.
            enriched = await asyncio.gather(
                *(enrich_issue(session, semaphore, issue) for issue in issues)
            )
            for record in enriched:
                await out.write(json.dumps(record, ensure_ascii=False) + "\n")
            await out.flush()

            fetched_count += len(enriched)
            since_checkpoint += len(enriched)
            last_key = enriched[-1]["key"]

            if fetched_count - last_logged >= LOG_EVERY:
                last_logged = fetched_count
                logger.info(
                    "Fetched %d tickets, last: %s, elapsed: %.1fs",
                    fetched_count, last_key, time.monotonic() - started,
                )

            if since_checkpoint >= CHECKPOINT_EVERY:
                write_checkpoint(next_page_token, fetched_count)
                since_checkpoint = 0

            if not next_page_token:
                break

        # Final checkpoint at clean completion.
        write_checkpoint(next_page_token, fetched_count)

    logger.info(
        "Done. Fetched %d tickets in %.1fs -> %s",
        fetched_count, time.monotonic() - started, TICKETS_FILE,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
