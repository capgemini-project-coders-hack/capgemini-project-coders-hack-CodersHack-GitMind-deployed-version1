"""
snowflake_etl.py — Snowflake ingestion pipeline for GitMind
============================================================
Pulls data from GitHub, Jira, Slack, and ADR markdown files,
then upserts into Snowflake via MERGE INTO (idempotent).

USAGE
-----
python -m ingest.snowflake_etl                        # full pipeline
python -m ingest.snowflake_etl --step ddl             # create tables only
python -m ingest.snowflake_etl --step commits --repo owner/repo
python -m ingest.snowflake_etl --step tickets
python -m ingest.snowflake_etl --step messages --channel C0XXXXXX
python -m ingest.snowflake_etl --step adrs --repo owner/repo
python -m ingest.snowflake_etl --step reset           # wipe all 6 tables

ENVIRONMENT VARIABLES
---------------------
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
GITHUB_TOKEN, JIRA_URL, JIRA_USER, JIRA_API_TOKEN, JIRA_DEFAULT_PROJECT
SLACK_BOT_TOKEN
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("gitmind.etl.snowflake")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")


# ---------------------------------------------------------------------------
# PERF: shared requests.Session — every GitHub/Jira/Slack call in this file
# used to be a bare requests.get(...) call, each opening a brand-new TCP+TLS
# connection. Reusing one Session lets urllib3 keep-alive connections to
# the same host (api.github.com, the Jira host, slack.com) across calls
# instead of re-handshaking every time.
#
# Thread-safety: requests.Session wraps a urllib3 connection-pooled
# HTTPAdapter, which is safe for concurrent .get() calls from multiple
# threads as long as no thread mutates session-level state (headers/
# auth/cookies) after creation -- this module never does that; every call
# site still passes its own per-call headers=/params=/auth=/timeout= as
# kwargs exactly like the plain requests.get() calls it replaces. This
# matters because ingest_commits()'s ThreadPoolExecutor (see PERF comment
# there) calls this session concurrently from up to 20 worker threads --
# pool_maxsize=20 below matches that cap so urllib3 doesn't have to keep
# discarding/reopening connections once the pool is full.
# ---------------------------------------------------------------------------

import threading

_session_lock = threading.Lock()
_SESSION = None


def _get_session():
    global _SESSION
    if _SESSION is None:
        with _session_lock:
            if _SESSION is None:
                import requests
                s = requests.Session()
                adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _SESSION = s
    return _SESSION


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_conn():
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=_require("SNOWFLAKE_ACCOUNT"),
        user=_require("SNOWFLAKE_USER"),
        password=_require("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=_require("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        login_timeout=30,
        network_timeout=60,
    )
    log.info("Connected to Snowflake.")
    return conn


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise EnvironmentError(f"Required env var '{name}' is not set.")
    return val


def _exe(cur, sql: str, params=None):
    cur.execute(sql, params or ())


# ---------------------------------------------------------------------------
# PERF: bulk upsert helper — replaces "one MERGE statement per row" with
# "N bulk-bound inserts into a TEMPORARY staging table (1 round trip) + one
# MERGE against it (1 round trip)". Same ON/WHEN MATCHED/WHEN NOT MATCHED
# semantics as the row-by-row MERGE it replaces; same target-table columns
# and values end up written, just via 2 statements total instead of N.
#
# Snowflake's connector only bulk-optimizes executemany() for plain INSERT
# statements (stage-bound, single round trip) -- MERGE isn't covered by
# that optimization, so executemany() on a MERGE directly would still be
# N round trips. Staging through a TEMPORARY table (session-scoped, auto
# dropped on connection close, and explicitly dropped in `finally` here
# too) is what actually turns this into O(1) round trips regardless of N.
# ---------------------------------------------------------------------------

def _bulk_merge(conn, table: str, columns: list[str], pk_col: str, rows: list[tuple]) -> int:
    """Upsert `rows` into `table` in one MERGE instead of one MERGE per row.

    `columns` is the exact ordered list of column identifiers each row
    tuple maps to -- same order, same quoting/casing the table's DDL uses
    (e.g. '"BRANCH"' for COMMITS, since that column is a quoted identifier
    in the original schema). `pk_col` is the column MERGE matches ON, and
    must also appear in `columns`. Returns the row count passed in (rows
    is always fully applied or the MERGE raises -- same all-or-nothing
    per-call behavior as the row-by-row version had per-row, just now
    atomic for the whole batch instead of partial-on-error).
    """
    if not rows:
        return 0

    stage = f"_stage_{table}_{os.getpid()}"
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE TEMPORARY TABLE {stage} LIKE {table}")

        col_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        cur.executemany(
            f"INSERT INTO {stage} ({col_list}) VALUES ({placeholders})", rows
        )

        update_cols = [c for c in columns if c != pk_col]
        set_clause = ", ".join(f"{c}=src.{c}" for c in update_cols)
        values_clause = ", ".join(f"src.{c}" for c in columns)
        cur.execute(f"""
            MERGE INTO {table} AS tgt
            USING {stage} AS src
            ON tgt.{pk_col} = src.{pk_col}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({col_list})
            VALUES ({values_clause})
        """)
        return len(rows)
    finally:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stage}")
        except Exception:
            pass
        cur.close()


# ---------------------------------------------------------------------------
# Schema drift check — code's DDL/MERGE statements assume these columns
# exist with a roughly-compatible type. CREATE TABLE IF NOT EXISTS silently
# no-ops on tables that already exist (wrong type, missing column, both),
# so this is the only thing that surfaces drift before a DML statement
# crashes mid-ingest. "TEXT"-expected columns that are actually TIMESTAMP_*
# are the dangerous case: an empty string sent to them blows up with
# "Timestamp '' is not recognized" (see ADR_RECORDS.created_date, fixed by
# sending NULL instead of "" — but any other TEXT-expected/TIMESTAMP-actual
# column has the same landmine if its ingest path ever sends "").
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: dict[str, dict[str, str]] = {
    "COMMITS": {
        "COMMIT_ID": "TEXT", "REPO": "TEXT", "BRANCH": "TEXT", "AUTHOR": "TEXT",
        "AUTHOR_EMAIL": "TEXT", "MESSAGE": "TEXT", "TIMESTAMP": "TEXT",
        "FILES_CHANGED": "NUMBER", "ADDITIONS": "NUMBER", "DELETIONS": "NUMBER",
        "PATCH_SUMMARY": "TEXT", "URL": "TEXT",
    },
    "TICKETS": {
        "TICKET_ID": "TEXT", "PROJECT": "TEXT", "SUMMARY": "TEXT", "TEXT": "TEXT",
        "STATUS": "TEXT", "PRIORITY": "TEXT", "ISSUE_TYPE": "TEXT", "ASSIGNEE": "TEXT",
        "CREATED_AT": "TEXT", "URL": "TEXT", "IS_BUG": "BOOLEAN",
    },
    "MESSAGES": {
        "MESSAGE_ID": "TEXT", "CHANNEL_ID": "TEXT", "CHANNEL_NAME": "TEXT",
        "USER_ID": "TEXT", "USERNAME": "TEXT", "TEXT": "TEXT", "SUMMARY": "TEXT",
        "TIMESTAMP": "TEXT", "THREAD_TS": "TEXT",
    },
    "ADR_RECORDS": {
        "ADR_ID": "TEXT", "REPO": "TEXT", "FILE_PATH": "TEXT", "TITLE": "TEXT",
        "SUMMARY": "TEXT", "STATUS": "TEXT", "CONTEXT": "TEXT", "DECISION": "TEXT",
        "CONSEQUENCES": "TEXT", "RAW_MARKDOWN": "TEXT", "CREATED_DATE": "TEXT",
    },
    "BUG_REPORTS": {
        "BUG_ID": "TEXT", "TITLE": "TEXT", "SUMMARY": "TEXT", "SEVERITY": "TEXT",
        "SOURCE": "TEXT", "STATUS": "TEXT", "AFFECTED_REPO": "TEXT",
        "COMMIT_REF": "TEXT", "TICKET_REF": "TEXT", "REPORTED_AT": "TEXT",
        "RESOLVED_AT": "TEXT",
    },
    "DECISIONS": {
        "DECISION_ID": "TEXT", "TITLE": "TEXT", "SUMMARY": "TEXT", "RATIONALE": "TEXT",
        "OUTCOME": "TEXT", "SOURCE": "TEXT", "OWNER": "TEXT", "RELATED_TICKET": "TEXT",
        "RELATED_COMMIT": "TEXT", "TIMESTAMP": "TEXT",
    },
}

_TIMESTAMP_TYPES = ("TIMESTAMP", "DATE", "TIME")
_TEXT_TYPES = ("TEXT", "VARCHAR", "CHAR", "STRING")
_NUMBER_TYPES = ("NUMBER", "INT", "FLOAT", "DECIMAL")


def _type_category(data_type: str) -> str:
    dt = data_type.upper()
    if dt.startswith(_TIMESTAMP_TYPES):
        return "TIMESTAMP"
    if dt.startswith(_TEXT_TYPES):
        return "TEXT"
    if dt.startswith(_NUMBER_TYPES):
        return "NUMBER"
    if dt.startswith("BOOLEAN"):
        return "BOOLEAN"
    return dt


def validate_schema(execute_fn) -> list[str]:
    """Compare live Snowflake schema against what the code expects.

    `execute_fn(sql) -> list[dict]` — pass `SnowflakeClient.execute`
    (server startup path) or a thin wrapper around a cursor (CLI path).
    Returns a list of human-readable drift warnings; also logs each one
    so it shows up in render/server logs at boot, before any ingest step
    runs into it as a runtime crash.
    """
    warnings: list[str] = []
    tables = list(EXPECTED_COLUMNS.keys())
    try:
        rows = execute_fn(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            f"WHERE table_name IN ({', '.join(['%s'] * len(tables))})",
            tuple(tables),
        )
    except TypeError:
        # execute_fn doesn't take params (e.g. a raw cursor wrapper) — inline instead.
        in_clause = ", ".join(f"'{t}'" for t in tables)
        rows = execute_fn(
            f"SELECT table_name, column_name, data_type FROM information_schema.columns "
            f"WHERE table_name IN ({in_clause})"
        )

    live: dict[str, dict[str, str]] = {}
    for r in rows:
        t = r["TABLE_NAME"].upper()
        c = r["COLUMN_NAME"].upper()
        live.setdefault(t, {})[c] = r["DATA_TYPE"].upper()

    for table, expected_cols in EXPECTED_COLUMNS.items():
        live_cols = live.get(table)
        if live_cols is None:
            warnings.append(f"{table}: table not found in Snowflake (not yet created?).")
            continue
        for col, expected_kind in expected_cols.items():
            actual_type = live_cols.get(col)
            if actual_type is None:
                warnings.append(
                    f"{table}.{col}: column missing from live schema — "
                    f"code's MERGE/INSERT will fail with 'invalid identifier {col}'."
                )
                continue
            actual_kind = _type_category(actual_type)
            if expected_kind == "TEXT" and actual_kind == "TIMESTAMP":
                warnings.append(
                    f"{table}.{col}: live column is {actual_type} but code treats it as text — "
                    f"an empty-string insert will fail with \"Timestamp '' is not recognized\"."
                )
            elif expected_kind != actual_kind and not (expected_kind == "TEXT" and actual_kind in ("TEXT",)):
                warnings.append(
                    f"{table}.{col}: live type {actual_type} ({actual_kind}) "
                    f"differs from code's expected {expected_kind}."
                )

    if warnings:
        log.warning("Schema drift detected (%d issue(s)) — see below:", len(warnings))
        for w in warnings:
            log.warning("  - %s", w)
    else:
        log.info("Schema check: live Snowflake schema matches code expectations for all 6 tables.")

    return warnings


# ---------------------------------------------------------------------------
# Step 1: DDL — create tables
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS COMMITS (
    commit_id       VARCHAR(40)  PRIMARY KEY,
    repo            VARCHAR(500),
    "BRANCH"        VARCHAR(255),
    author          VARCHAR(500),
    author_email    VARCHAR(500),
    message         VARCHAR(8000),
    timestamp       VARCHAR(100),
    files_changed   NUMBER,
    additions       NUMBER,
    deletions       NUMBER,
    patch_summary   VARCHAR(8000),
    url             VARCHAR(1000),
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS TICKETS (
    ticket_id       VARCHAR(100) PRIMARY KEY,
    project         VARCHAR(100),
    summary         VARCHAR(1000),
    text            VARCHAR(8000),
    status          VARCHAR(100),
    priority        VARCHAR(100),
    issue_type      VARCHAR(100),
    assignee        VARCHAR(500),
    created_at      VARCHAR(100),
    url             VARCHAR(1000),
    is_bug          BOOLEAN,
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MESSAGES (
    message_id      VARCHAR(200) PRIMARY KEY,
    channel_id      VARCHAR(100),
    channel_name    VARCHAR(200),
    user_id         VARCHAR(100),
    username        VARCHAR(200),
    text            VARCHAR(4000),
    summary         VARCHAR(1000),
    timestamp       VARCHAR(100),
    thread_ts       VARCHAR(100),
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ADR_RECORDS (
    adr_id          VARCHAR(500) PRIMARY KEY,
    repo            VARCHAR(500),
    file_path       VARCHAR(1000),
    title           VARCHAR(500),
    summary         VARCHAR(2000),
    status          VARCHAR(100),
    context         VARCHAR(4000),
    decision        VARCHAR(4000),
    consequences    VARCHAR(4000),
    raw_markdown    VARCHAR(16000),
    created_date    VARCHAR(50),
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BUG_REPORTS (
    bug_id          VARCHAR(200) PRIMARY KEY,
    title           VARCHAR(500),
    summary         VARCHAR(2000),
    severity        VARCHAR(100),
    source          VARCHAR(100),
    status          VARCHAR(100),
    affected_repo   VARCHAR(500),
    commit_ref      VARCHAR(100),
    ticket_ref      VARCHAR(100),
    reported_at     VARCHAR(100),
    resolved_at     VARCHAR(100),
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DECISIONS (
    decision_id     VARCHAR(200) PRIMARY KEY,
    title           VARCHAR(500),
    summary         VARCHAR(2000),
    rationale       VARCHAR(4000),
    outcome         VARCHAR(2000),
    source          VARCHAR(100),
    owner           VARCHAR(500),
    related_ticket  VARCHAR(100),
    related_commit  VARCHAR(100),
    timestamp       VARCHAR(100),
    inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

# All tables this pipeline owns — used by both DDL and reset_all().
ALL_TABLES = ["COMMITS", "TICKETS", "MESSAGES", "ADR_RECORDS", "BUG_REPORTS", "DECISIONS"]


_MIGRATIONS = [
    # CREATE TABLE IF NOT EXISTS is a no-op on tables that already exist,
    # so columns added later (e.g. ADR_RECORDS.summary) never reach
    # pre-existing deployments. Patch them in explicitly here.
    "ALTER TABLE ADR_RECORDS ADD COLUMN IF NOT EXISTS summary VARCHAR(2000)",
]


def run_ddl(conn) -> None:
    log.info("Creating tables (IF NOT EXISTS)...")
    cur = conn.cursor()
    try:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        for stmt in _MIGRATIONS:
            cur.execute(stmt)
        log.info("DDL complete — 6 tables ready.")

        def _exec_dict(sql: str) -> list[dict]:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

        validate_schema(_exec_dict)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Step 0: Reset — wipe all rows from all 6 tables (called on frontend refresh
# / repo switch so stale data from a previous repo never bleeds into a new
# query). TRUNCATE keeps the table+schema intact, just empties rows; cheap
# and instant in Snowflake regardless of row count.
# ---------------------------------------------------------------------------

def reset_all(conn=None) -> None:
    own_conn = conn is None
    conn = conn or _get_conn()
    cur = conn.cursor()
    try:
        for table in ALL_TABLES:
            cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
            log.info("  Truncated %s", table)
        log.info("Snowflake reset complete — all %d tables emptied.", len(ALL_TABLES))
    finally:
        cur.close()
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Step 2: Commits
# ---------------------------------------------------------------------------

def _list_branches(owner: str, repo_name: str, headers: dict, cache: dict | None = None) -> list[str]:
    cache_key = ("branches", owner, repo_name)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    branches: list[str] = []
    page = 1
    while True:
        resp = _get_session().get(
            f"https://api.github.com/repos/{owner}/{repo_name}/branches",
            params={"per_page": 100, "page": page},
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        branches.extend(b["name"] for b in batch)
        if len(batch) < 100:
            break
        page += 1

    if cache is not None:
        cache[cache_key] = branches
    return branches


def ingest_commits(repo: str, branch: str = "main", max_pages: int = 10, cache: dict | None = None) -> None:
    # max_pages=10 * per_page=100 = 1000 commits cap PER BRANCH.
    # `branch` is kept only for CLI back-compat and ignored below: commits
    # are now pulled for EVERY branch (deduped by sha), matching
    # ingest/neo4j_etl.py's behavior. Before this fix, Snowflake (Traces
    # panel) only ever saw `main`'s commits while Neo4j (Graph panel) saw
    # all branches -- two different commit sets that looked like "random
    # mismatched nodes" because they genuinely were different data.
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)

    pinned = os.getenv("GITMIND_INGEST_BRANCH", "")
    branches = _list_branches(owner, repo_name, headers, cache=cache)
    if not branches:
        branches = [branch]
    if pinned:
        if pinned in branches:
            branches = [pinned]
            log.info("Branch pinned to '%s' via GITMIND_INGEST_BRANCH", pinned)
        else:
            # GITMIND_INGEST_BRANCH is a deployment-wide env var. If it was
            # set for a previous repo (e.g. "trunk" for a repo whose default
            # branch isn't "main"), it will not exist on a different repo
            # ingested later -- pinning to it anyway 404s every commits/adrs
            # call for that repo. Ignoring a pin that doesn't apply here and
            # falling back to every real branch keeps ingest working for
            # whatever repo was actually requested, instead of hard-failing
            # because of a stale setting left over from a different one.
            log.warning(
                "GITMIND_INGEST_BRANCH='%s' does not exist on %s -- ignoring "
                "the pin and using all %d branch(es) found instead.",
                pinned, repo, len(branches),
            )

    log.info("Fetching commits from %s across %d branch(es) (max %d pages / %d commits per branch)...",
              repo, len(branches), max_pages, max_pages * 100)

    # sha -> (commit_dict, branch_name_first_seen) -- dedupe commits that
    # appear on multiple branches so each lands in Snowflake exactly once.
    commits_by_sha: dict[str, dict] = {}
    for b_name in branches:
        for page in range(1, max_pages + 1):
            page_key = ("commits_page", owner, repo_name, b_name, page)
            if cache is not None and page_key in cache:
                batch = cache[page_key]
            else:
                resp = _get_session().get(
                    f"https://api.github.com/repos/{owner}/{repo_name}/commits",
                    params={"sha": b_name, "per_page": 100, "page": page},
                    headers=headers, timeout=30,
                )
                resp.raise_for_status()
                batch = resp.json()
                if cache is not None:
                    cache[page_key] = batch
            if not batch:
                break
            for c in batch:
                sha = c["sha"]
                if sha not in commits_by_sha:
                    commits_by_sha[sha] = {"commit": c, "branch": b_name}
            if len(batch) < 100:
                break

    log.info("  %d unique commits fetched across all branches; enriching with diff stats...", len(commits_by_sha))

    # PERF: diff-stat detail fetch is one independent GitHub API call per
    # commit (no shared state, no ordering dependency -- rows are bulk
    # MERGEd into Snowflake by primary key afterward, so row order doesn't
    # matter). Running these serially made this loop O(N) sequential round
    # trips (N = unique commits, up to max_pages*100 per branch), which
    # dominated ingest wall-clock for any repo with a few hundred+ commits.
    # A bounded thread pool overlaps the network waits; worker count capped
    # at 20 to stay well under GitHub's secondary rate-limit burst ceiling.
    # Same per-commit try/except fallback (additions=deletions=files=0,
    # empty patch_summary) is preserved exactly, just per-future instead of
    # per-loop-iteration -- a failed/rate-limited detail fetch degrades a
    # single commit's row the same way it did before, it never aborts the
    # batch.
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_commit_row(sha: str, entry: dict) -> tuple:
        c             = entry["commit"]
        commit_branch = entry["branch"]
        commit_data   = c.get("commit", {})
        author_data   = commit_data.get("author") or {}
        message       = commit_data.get("message", "")

        additions = deletions = files_changed = 0
        patch_summary = ""
        try:
            detail = _get_session().get(
                f"https://api.github.com/repos/{owner}/{repo_name}/commits/{sha}",
                headers=headers, timeout=15,
            ).json()
            stats = detail.get("stats", {})
            additions = stats.get("additions", 0)
            deletions = stats.get("deletions", 0)
            files = detail.get("files", [])
            files_changed = len(files)
            patch_summary = "; ".join(
                f"{f.get('filename','?')} +{f.get('additions',0)}/-{f.get('deletions',0)}"
                for f in files[:10]
            )[:8000]
        except Exception:
            pass

        return (
            sha,
            repo,
            commit_branch,
            (c.get("author") or {}).get("login") or author_data.get("name", ""),
            author_data.get("email", ""),
            message[:8000],
            author_data.get("date", ""),
            files_changed,
            additions,
            deletions,
            patch_summary,
            c.get("html_url", ""),
        )

    rows: list[tuple] = []
    max_workers = min(20, len(commits_by_sha)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_commit_row, sha, entry): sha
            for sha, entry in commits_by_sha.items()
        }
        for fut in as_completed(futures):
            rows.append(fut.result())

    conn = _get_conn()
    try:
        # PERF: was one `cur.execute(merge_sql, row)` per commit (N MERGE
        # round trips). _bulk_merge stages all rows into a TEMPORARY table
        # via one bulk-bound INSERT, then runs exactly one MERGE with the
        # identical ON/WHEN MATCHED/WHEN NOT MATCHED clauses that were
        # spelled out inline below before -- same target columns, same
        # update-vs-insert semantics, same COMMITS rows end up written.
        _bulk_merge(
            conn, "COMMITS",
            columns=["commit_id", "repo", '"BRANCH"', "author", "author_email",
                     "message", "timestamp", "files_changed", "additions",
                     "deletions", "patch_summary", "url"],
            pk_col="commit_id",
            rows=rows,
        )
        log.info("  Upserted %d/%d commits.", len(rows), len(rows))
    finally:
        conn.close()
    log.info("ETL done.")


# ---------------------------------------------------------------------------
# Step 3: Tickets (Jira)
# ---------------------------------------------------------------------------

def _fetch_repo_local_tickets(repo: str, path: str = "jira/issues.json", cache: dict | None = None) -> dict | None:
    """Look for a committed ticket file (e.g. jira/issues.json) in the repo.

    Some repos (anything without a real, externally-hosted Jira instance)
    track their tickets as a single JSON file checked into the repo itself
    instead of a live Jira server. This is the generic fallback that makes
    ticket ingestion work for ANY public repo, not just ones pointed at a
    real JIRA_URL: if this file exists, use it; otherwise ingest_tickets()
    falls through unchanged to the live Jira API path below (so Kafka /
    other repos using a real Jira instance keep working exactly as before).

    Expected shape (minimal):
        {"project": {"key": "RRW"}, "issues": [{"id": "RRW-001",
         "type": "Story", "title": "...", "status": "Done",
         "priority": "High", "commits": ["sha1", "sha2"],
         "description": "..."}]}
    Only "id" and "title" are required per issue; everything else is
    optional and defaults to empty/false if absent.
    """
    cache_key = ("local_tickets", repo, path)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    resp = _get_session().get(
        f"https://api.github.com/repos/{owner}/{repo_name}/contents/{path}",
        headers=headers, timeout=15,
    )
    if not resp.ok:
        if cache is not None:
            cache[cache_key] = None
        return None
    try:
        raw = base64.b64decode(resp.json().get("content", "")).decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception as exc:
        log.warning("Found %s in %s but couldn't parse it as JSON: %s", path, repo, exc)
        if cache is not None:
            cache[cache_key] = None
        return None
    if not isinstance(data, dict) or not isinstance(data.get("issues"), list):
        log.warning("%s in %s doesn't match expected {project, issues:[...]} shape — ignoring.", path, repo)
        if cache is not None:
            cache[cache_key] = None
        return None
    if cache is not None:
        cache[cache_key] = data
    return data


def ingest_tickets(
    repo: str | None = None,
    project: str | None = None,
    max_results: int = 5000,
    local_tickets_path: str = "jira/issues.json",
    cache: dict | None = None,
) -> None:
    import requests
    from requests.auth import HTTPBasicAuth

    # Repo-local ticket file takes priority over the live Jira API — see
    # _fetch_repo_local_tickets() docstring for why. Falls through to the
    # unchanged live-API path below if no such file exists for this repo.
    if repo:
        local = _fetch_repo_local_tickets(repo, local_tickets_path, cache=cache)
        if local is not None:
            local_project = (local.get("project") or {}).get("key") or project or repo
            log.info("Found local ticket file '%s' in %s (project=%s) — using it instead of live Jira.",
                      local_tickets_path, repo, local_project)
            rows = []
            for issue in local["issues"]:
                ticket_id = issue.get("id")
                if not ticket_id:
                    continue
                issue_type = issue.get("type", "")
                rows.append((
                    ticket_id,
                    local_project,
                    str(issue.get("title", ""))[:1000],
                    str(issue.get("description", ""))[:8000],
                    issue.get("status", ""),
                    issue.get("priority", ""),
                    issue_type,
                    "",  # assignee — not present in the local-file format
                    "",  # created_at — not present in the local-file format
                    # "HEAD" rather than "main" -- this repo's default branch
                    # might not be main (e.g. ansh-jha2006/reni2's is "license"),
                    # and GitHub resolves /blob/HEAD/... to whatever the actual
                    # default branch is, so the link doesn't 404.
                    f"https://github.com/{repo}/blob/HEAD/{local_tickets_path}#{ticket_id}",
                    issue_type.lower() in ("bug", "incident", "defect"),
                ))
            conn = _get_conn()
            cur = conn.cursor()
            try:
                merge_sql = """
                MERGE INTO TICKETS AS tgt
                USING (SELECT %s AS ticket_id, %s AS project, %s AS summary,
                              %s AS text, %s AS status, %s AS priority,
                              %s AS issue_type, %s AS assignee, %s AS created_at,
                              %s AS url, %s AS is_bug) AS src
                ON tgt.ticket_id = src.ticket_id
                WHEN MATCHED THEN UPDATE SET
                    project=src.project, summary=src.summary, text=src.text,
                    status=src.status, priority=src.priority, issue_type=src.issue_type,
                    assignee=src.assignee, created_at=src.created_at,
                    url=src.url, is_bug=src.is_bug
                WHEN NOT MATCHED THEN INSERT
                    (ticket_id, project, summary, text, status, priority,
                     issue_type, assignee, created_at, url, is_bug)
                VALUES
                    (src.ticket_id, src.project, src.summary, src.text,
                     src.status, src.priority, src.issue_type, src.assignee,
                     src.created_at, src.url, src.is_bug)
                """
                for row in rows:
                    cur.execute(merge_sql, row)
                log.info("  Upserted %d tickets (from local file).", len(rows))
            finally:
                cur.close()
                conn.close()
            log.info("ETL done.")
            return

    jira_url = os.getenv("JIRA_URL", "").rstrip("/")
    jira_user = os.getenv("JIRA_USER", "")
    jira_token = os.getenv("JIRA_API_TOKEN", "")
    project = project or os.getenv("JIRA_DEFAULT_PROJECT", "")

    if not jira_url:
        log.warning("JIRA_URL not set — skipping ticket ingestion.")
        return
    if not project:
        log.warning("JIRA_DEFAULT_PROJECT not set — skipping ticket ingestion.")
        return

    auth = HTTPBasicAuth(jira_user, jira_token) if jira_user and jira_token else None
    is_atlassian_cloud = "atlassian.net" in jira_url
    search_url = (
        f"{jira_url}/rest/api/3/search/jql" if is_atlassian_cloud
        else f"{jira_url}/rest/api/2/search"
    )

    log.info("Fetching Jira tickets for project %s...", project)

    rows: list[tuple] = []
    start_at = 0
    batch_size = 100
    next_page_token = None

    while True:
        params: dict[str, Any] = {
            "jql": f"project={project} ORDER BY created DESC",
            "maxResults": batch_size,
            "fields": "summary,description,status,priority,issuetype,"
                      "assignee,reporter,created,updated,resolutiondate,labels",
        }
        if is_atlassian_cloud and next_page_token:
            params["nextPageToken"] = next_page_token
        elif not is_atlassian_cloud:
            params["startAt"] = start_at

        page_key = ("jira_page", search_url, tuple(sorted(params.items())))
        if cache is not None and page_key in cache:
            data = cache[page_key]
        else:
            resp = _get_session().get(search_url, params=params, auth=auth, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if cache is not None:
                cache[page_key] = data
        issues = data.get("issues", [])
        if not issues:
            break

        for issue in issues:
            key = issue["key"]
            fields = issue.get("fields", {})

            def _text(obj, *keys):
                for k in keys:
                    obj = (obj or {}).get(k)
                return obj or ""

            desc_raw = fields.get("description")
            desc_text = _adf_to_text(desc_raw) if isinstance(desc_raw, dict) else (desc_raw or "")
            issue_type = _text(fields.get("issuetype"), "name")
            is_bug = issue_type.lower() in ("bug", "incident", "defect")

            rows.append((
                key,
                project,
                fields.get("summary", "")[:1000],
                desc_text[:8000],
                _text(fields.get("status"), "name"),
                _text(fields.get("priority"), "name"),
                issue_type,
                _text(fields.get("assignee"), "displayName"),
                fields.get("created", ""),
                f"{jira_url}/browse/{key}",
                is_bug,
            ))

        start_at += len(issues)
        next_page_token = data.get("nextPageToken")
        if data.get("isLast", not next_page_token) or start_at >= max_results:
            break

    conn = _get_conn()
    cur = conn.cursor()
    try:
        merge_sql = """
        MERGE INTO TICKETS AS tgt
        USING (SELECT %s AS ticket_id, %s AS project, %s AS summary,
                      %s AS text, %s AS status, %s AS priority,
                      %s AS issue_type, %s AS assignee, %s AS created_at,
                      %s AS url, %s AS is_bug) AS src
        ON tgt.ticket_id = src.ticket_id
        WHEN MATCHED THEN UPDATE SET
            project=src.project, summary=src.summary, text=src.text,
            status=src.status, priority=src.priority, issue_type=src.issue_type,
            assignee=src.assignee, created_at=src.created_at,
            url=src.url, is_bug=src.is_bug
        WHEN NOT MATCHED THEN INSERT
            (ticket_id, project, summary, text, status, priority,
             issue_type, assignee, created_at, url, is_bug)
        VALUES
            (src.ticket_id, src.project, src.summary, src.text,
             src.status, src.priority, src.issue_type, src.assignee,
             src.created_at, src.url, src.is_bug)
        """
        for row in rows:
            cur.execute(merge_sql, row)
        log.info("  Upserted %d tickets.", len(rows))
    finally:
        cur.close()
        conn.close()
    log.info("ETL done.")


def _adf_to_text(adf: dict) -> str:
    parts = []
    for node in adf.get("content", []):
        if node.get("type") == "paragraph":
            for inline in node.get("content", []):
                if inline.get("type") == "text":
                    parts.append(inline.get("text", ""))
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Step 4: Slack → MESSAGES
# ---------------------------------------------------------------------------

def ingest_messages(channel_id: str, limit_days: int = 90, cache: dict | None = None) -> None:
    import requests

    token = _require("SLACK_BOT_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    channel_name = channel_id
    info_key = ("slack_info", channel_id)
    if cache is not None and info_key in cache:
        channel_name = cache[info_key]
    else:
        try:
            info = _get_session().get(
                "https://slack.com/api/conversations.info",
                params={"channel": channel_id}, headers=headers, timeout=15,
            ).json()
            channel_name = info.get("channel", {}).get("name", channel_id)
        except Exception:
            pass
        if cache is not None:
            cache[info_key] = channel_name

    log.info("Fetching Slack messages from #%s...", channel_name)

    oldest = str(time.time() - limit_days * 86400)
    cursor = None
    rows: list[tuple] = []

    while True:
        params: dict[str, Any] = {"channel": channel_id, "limit": 200, "oldest": oldest}
        if cursor:
            params["cursor"] = cursor

        # Cache key intentionally excludes 'oldest' (a wall-clock cutoff
        # that drifts by a few seconds between this call and the sibling
        # pipeline's call within the same run) and 'limit' (always 200
        # here) -- (channel_id, cursor) alone is enough to identify "the
        # same history page" for reuse purposes.
        page_key = ("slack_history_page", channel_id, cursor)
        if cache is not None and page_key in cache:
            data = cache[page_key]
        else:
            resp = _get_session().get(
                "https://slack.com/api/conversations.history",
                params=params, headers=headers, timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            if cache is not None:
                cache[page_key] = data
        if not data.get("ok"):
            log.error("Slack error: %s", data.get("error"))
            break

        for msg in data.get("messages", []):
            msg_id = f"{channel_id}:{msg['ts']}"
            ts_dt = datetime.fromtimestamp(float(msg.get("ts", 0)), tz=timezone.utc)
            text = msg.get("text", "")
            rows.append((
                msg_id,
                channel_id,
                channel_name,
                msg.get("user", ""),
                "",           # username — not available from history
                text[:4000],
                text[:1000],  # summary
                ts_dt.isoformat(),
                msg.get("thread_ts", ""),
            ))

        meta = data.get("response_metadata", {})
        cursor = meta.get("next_cursor")
        if not cursor:
            break

    conn = _get_conn()
    cur = conn.cursor()
    try:
        merge_sql = """
        MERGE INTO MESSAGES AS tgt
        USING (SELECT %s AS message_id, %s AS channel_id, %s AS channel_name,
                      %s AS user_id, %s AS username, %s AS text,
                      %s AS summary, %s AS timestamp, %s AS thread_ts) AS src
        ON tgt.message_id = src.message_id
        WHEN MATCHED THEN UPDATE SET
            channel_id=src.channel_id, channel_name=src.channel_name,
            user_id=src.user_id, username=src.username, text=src.text,
            summary=src.summary, timestamp=src.timestamp, thread_ts=src.thread_ts
        WHEN NOT MATCHED THEN INSERT
            (message_id, channel_id, channel_name, user_id, username,
             text, summary, timestamp, thread_ts)
        VALUES
            (src.message_id, src.channel_id, src.channel_name, src.user_id,
             src.username, src.text, src.summary, src.timestamp, src.thread_ts)
        """
        for row in rows:
            cur.execute(merge_sql, row)
        log.info("  Upserted %d messages.", len(rows))
    finally:
        cur.close()
        conn.close()
    log.info("ETL done.")


# ---------------------------------------------------------------------------
# Step 5: ADRs
# ---------------------------------------------------------------------------

def ingest_adrs(repo: str, adr_path: str = "docs/adr", cache: dict | None = None) -> None:
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    log.info("Scanning %s/%s for ADRs in '%s'...", owner, repo_name, adr_path)

    listing_key = ("adr_listing", owner, repo_name, adr_path)
    if cache is not None and listing_key in cache:
        listing = cache[listing_key]
    else:
        resp = _get_session().get(
            f"https://api.github.com/repos/{owner}/{repo_name}/contents/{adr_path}",
            headers=headers, timeout=15,
        )
        if not resp.ok:
            log.warning("  ADR path not found in %s (%s).", repo, resp.status_code)
            if cache is not None:
                cache[listing_key] = None
            return
        listing = resp.json()
        if cache is not None:
            cache[listing_key] = listing

    if listing is None:
        log.warning("  ADR path not found in %s.", repo)
        return

    files = [f for f in listing if f.get("name", "").endswith(".md")]
    log.info("  Found %d ADR files.", len(files))

    rows: list[tuple] = []
    for f in files:
        content_key = ("adr_content", f["url"])
        if cache is not None and content_key in cache:
            content_json = cache[content_key]
        else:
            content_resp = _get_session().get(f["url"], headers=headers, timeout=15)
            if not content_resp.ok:
                if cache is not None:
                    cache[content_key] = None
                continue
            content_json = content_resp.json()
            if cache is not None:
                cache[content_key] = content_json
        if content_json is None:
            continue
        raw = base64.b64decode(content_json.get("content", "")).decode("utf-8", errors="replace")
        title = _extract_section(raw, r"^#\s+(.+)$") or f["name"]
        status = _extract_section(raw, r"[Ss]tatus[:\s]+(.+)")
        context = _extract_section(raw, r"## Context\s+([\s\S]+?)(?=##|$)")
        decision = _extract_section(raw, r"## Decision\s+([\s\S]+?)(?=##|$)")
        consequences = _extract_section(raw, r"## Consequences\s+([\s\S]+?)(?=##|$)")
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
        created_date = date_match.group(1) if date_match else None

        adr_id = f"{repo}/{f['path']}"
        rows.append((
            adr_id,
            repo,
            f["path"],
            title[:500],
            title[:500],  # summary = title
            (status or "")[:50],
            context[:4000],
            decision[:4000],
            consequences[:4000],
            raw[:16000],
            created_date,
        ))

    conn = _get_conn()
    cur = conn.cursor()
    try:
        merge_sql = """
        MERGE INTO ADR_RECORDS AS tgt
        USING (SELECT %s AS adr_id, %s AS repo, %s AS file_path,
                      %s AS title, %s AS summary, %s AS status,
                      %s AS context, %s AS decision, %s AS consequences,
                      %s AS raw_markdown, %s AS created_date) AS src
        ON tgt.adr_id = src.adr_id
        WHEN MATCHED THEN UPDATE SET
            repo=src.repo, file_path=src.file_path, title=src.title,
            summary=src.summary, status=src.status, context=src.context,
            decision=src.decision, consequences=src.consequences,
            raw_markdown=src.raw_markdown, created_date=src.created_date
        WHEN NOT MATCHED THEN INSERT
            (adr_id, repo, file_path, title, summary, status, context,
             decision, consequences, raw_markdown, created_date)
        VALUES
            (src.adr_id, src.repo, src.file_path, src.title, src.summary,
             src.status, src.context, src.decision, src.consequences,
             src.raw_markdown, src.created_date)
        """
        for row in rows:
            cur.execute(merge_sql, row)
        log.info("  Upserted %d ADR records.", len(rows))
    finally:
        cur.close()
        conn.close()
    log.info("ETL done.")


def _extract_section(text: str, pattern: str) -> str:
    m = re.search(pattern, text, re.MULTILINE)
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None, cache: dict | None = None) -> int:
    parser = argparse.ArgumentParser(description="GitMind Snowflake ETL")
    parser.add_argument("--step", default="all",
                        choices=["all", "ddl", "commits", "tickets", "messages", "adrs", "reset"])
    parser.add_argument("--repo", help="owner/repo for commits and ADRs")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--channel", help="Slack channel ID")
    parser.add_argument("--adr-path", default="docs/adr")
    parser.add_argument("--project", help="Jira project key")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="GitHub commit pages to fetch (10 pages * 100/page = 1000 commits)")
    args = parser.parse_args(argv)

    step = args.step

    if step == "reset":
        try:
            reset_all()
        except Exception as exc:
            log.error("reset step failed: %s", exc)
            return 1
        log.info("ETL done.")
        return 0

    if step in ("all", "ddl"):
        try:
            run_ddl(_get_conn())
        except Exception as exc:
            log.error("ddl step failed: %s", exc)
            if step != "all":
                return 1

    if step in ("all", "commits"):
        repo = args.repo or os.getenv("GITHUB_DEFAULT_REPOS", "")
        if not repo:
            log.error("--repo required for commits step (or set GITHUB_DEFAULT_REPOS)")
            if step != "all":
                return 1
        else:
            try:
                ingest_commits(repo, branch=args.branch, max_pages=args.max_pages, cache=cache)
            except Exception as exc:
                log.error("commits step failed: %s", exc)

    if step in ("all", "tickets"):
        repo = args.repo or os.getenv("GITHUB_DEFAULT_REPOS", "")
        try:
            ingest_tickets(repo=repo or None, project=args.project, cache=cache)
        except Exception as exc:
            log.error("tickets step failed: %s", exc)

    if step in ("all", "messages"):
        channel = args.channel or os.getenv("SLACK_DEFAULT_CHANNELS", "")
        if not channel:
            log.warning("No channel set — skipping messages step.")
        else:
            try:
                ingest_messages(channel, cache=cache)
            except Exception as exc:
                log.error("messages step failed: %s", exc)

    if step in ("all", "adrs"):
        repo = args.repo or os.getenv("GITHUB_DEFAULT_REPOS", "")
        if not repo:
            log.warning("No repo set — skipping adrs step.")
        else:
            try:
                ingest_adrs(repo, adr_path=args.adr_path, cache=cache)
            except Exception as exc:
                log.error("adrs step failed: %s", exc)

    log.info("ETL done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
