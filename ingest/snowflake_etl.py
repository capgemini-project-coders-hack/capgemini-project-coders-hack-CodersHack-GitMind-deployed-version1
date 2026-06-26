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


def run_ddl(conn) -> None:
    log.info("Creating tables (IF NOT EXISTS)...")
    cur = conn.cursor()
    try:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        log.info("DDL complete — 6 tables ready.")
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

def _list_branches(owner: str, repo_name: str, headers: dict) -> list[str]:
    import requests

    branches: list[str] = []
    page = 1
    while True:
        resp = requests.get(
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
    return branches


def ingest_commits(repo: str, branch: str = "main", max_pages: int = 10) -> None:
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
    if pinned:
        branches = [pinned]
        log.info("Branch pinned to '%s' via GITMIND_INGEST_BRANCH", pinned)
    else:
        branches = _list_branches(owner, repo_name, headers)
        if not branches:
            branches = [branch]

    log.info("Fetching commits from %s across %d branch(es) (max %d pages / %d commits per branch)...",
              repo, len(branches), max_pages, max_pages * 100)

    # sha -> (commit_dict, branch_name_first_seen) -- dedupe commits that
    # appear on multiple branches so each lands in Snowflake exactly once.
    commits_by_sha: dict[str, dict] = {}
    for b_name in branches:
        for page in range(1, max_pages + 1):
            resp = requests.get(
                f"https://api.github.com/repos/{owner}/{repo_name}/commits",
                params={"sha": b_name, "per_page": 100, "page": page},
                headers=headers, timeout=30,
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for c in batch:
                sha = c["sha"]
                if sha not in commits_by_sha:
                    commits_by_sha[sha] = {"commit": c, "branch": b_name}
            if len(batch) < 100:
                break

    log.info("  %d unique commits fetched across all branches; enriching with diff stats...", len(commits_by_sha))

    rows: list[tuple] = []
    for sha, entry in commits_by_sha.items():
        c           = entry["commit"]
        commit_branch = entry["branch"]
        commit_data = c.get("commit", {})
        author_data = commit_data.get("author") or {}
        message     = commit_data.get("message", "")

        # Fetch diff stats per commit
        additions = deletions = files_changed = 0
        patch_summary = ""
        try:
            detail = requests.get(
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

        rows.append((
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
        ))

    conn = _get_conn()
    cur = conn.cursor()
    try:
        merge_sql = """
        MERGE INTO COMMITS AS tgt
        USING (SELECT %s AS commit_id, %s AS repo, %s AS "BRANCH",
                      %s AS author, %s AS author_email, %s AS message,
                      %s AS timestamp, %s AS files_changed,
                      %s AS additions, %s AS deletions,
                      %s AS patch_summary, %s AS url) AS src
        ON tgt.commit_id = src.commit_id
        WHEN MATCHED THEN UPDATE SET
            repo=src.repo, "BRANCH"=src."BRANCH", author=src.author,
            author_email=src.author_email, message=src.message,
            timestamp=src.timestamp, files_changed=src.files_changed,
            additions=src.additions, deletions=src.deletions,
            patch_summary=src.patch_summary, url=src.url
        WHEN NOT MATCHED THEN INSERT
            (commit_id, repo, "BRANCH", author, author_email, message, timestamp,
             files_changed, additions, deletions, patch_summary, url)
        VALUES
            (src.commit_id, src.repo, src."BRANCH", src.author, src.author_email,
             src.message, src.timestamp, src.files_changed, src.additions,
             src.deletions, src.patch_summary, src.url)
        """
        upserted = 0
        for row in rows:
            cur.execute(merge_sql, row)
            upserted += 1
        log.info("  Upserted %d/%d commits.", upserted, len(rows))
    finally:
        cur.close()
        conn.close()
    log.info("ETL done.")


# ---------------------------------------------------------------------------
# Step 3: Tickets (Jira)
# ---------------------------------------------------------------------------

def ingest_tickets(project: str | None = None, max_results: int = 5000) -> None:
    import requests
    from requests.auth import HTTPBasicAuth

    jira_url = os.getenv("JIRA_URL", "").rstrip("/")
    jira_user = os.getenv("JIRA_USER", "")
    jira_token = os.getenv("JIRA_API_TOKEN", "")
    project = project or os.getenv("JIRA_DEFAULT_PROJECT", "")

    if not jira_url or not jira_user or not jira_token:
        log.warning("Jira env vars (JIRA_URL, JIRA_USER, JIRA_API_TOKEN) not set — skipping ticket ingestion.")
        return
    if not project:
        log.warning("JIRA_DEFAULT_PROJECT not set — skipping ticket ingestion.")
        return

    auth = HTTPBasicAuth(jira_user, jira_token)

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
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = requests.get(
            # /rest/api/3/search was shut down by Atlassian (410 Gone, Oct 2025).
            # Replacement is /rest/api/3/search/jql with token-based pagination.
            f"{jira_url}/rest/api/3/search/jql",
            params=params, auth=auth, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
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

def ingest_messages(channel_id: str, limit_days: int = 90) -> None:
    import requests

    token = _require("SLACK_BOT_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    channel_name = channel_id
    try:
        info = requests.get(
            "https://slack.com/api/conversations.info",
            params={"channel": channel_id}, headers=headers, timeout=15,
        ).json()
        channel_name = info.get("channel", {}).get("name", channel_id)
    except Exception:
        pass

    log.info("Fetching Slack messages from #%s...", channel_name)

    oldest = str(time.time() - limit_days * 86400)
    cursor = None
    rows: list[tuple] = []

    while True:
        params: dict[str, Any] = {"channel": channel_id, "limit": 200, "oldest": oldest}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            "https://slack.com/api/conversations.history",
            params=params, headers=headers, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
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

def ingest_adrs(repo: str, adr_path: str = "docs/adr") -> None:
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    log.info("Scanning %s/%s for ADRs in '%s'...", owner, repo_name, adr_path)

    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo_name}/contents/{adr_path}",
        headers=headers, timeout=15,
    )
    if not resp.ok:
        log.warning("  ADR path not found in %s (%s).", repo, resp.status_code)
        return

    files = [f for f in resp.json() if f.get("name", "").endswith(".md")]
    log.info("  Found %d ADR files.", len(files))

    rows: list[tuple] = []
    for f in files:
        content_resp = requests.get(f["url"], headers=headers, timeout=15)
        if not content_resp.ok:
            continue
        raw = base64.b64decode(content_resp.json().get("content", "")).decode("utf-8", errors="replace")
        title = _extract_section(raw, r"^#\s+(.+)$") or f["name"]
        status = _extract_section(raw, r"[Ss]tatus[:\s]+(.+)")
        context = _extract_section(raw, r"## Context\s+([\s\S]+?)(?=##|$)")
        decision = _extract_section(raw, r"## Decision\s+([\s\S]+?)(?=##|$)")
        consequences = _extract_section(raw, r"## Consequences\s+([\s\S]+?)(?=##|$)")
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
        created_date = date_match.group(1) if date_match else ""

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

def main(argv: list[str] | None = None) -> int:
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
                ingest_commits(repo, branch=args.branch, max_pages=args.max_pages)
            except Exception as exc:
                log.error("commits step failed: %s", exc)

    if step in ("all", "tickets"):
        try:
            ingest_tickets(project=args.project)
        except Exception as exc:
            log.error("tickets step failed: %s", exc)

    if step in ("all", "messages"):
        channel = args.channel or os.getenv("SLACK_DEFAULT_CHANNELS", "")
        if not channel:
            log.warning("No channel set — skipping messages step.")
        else:
            try:
                ingest_messages(channel)
            except Exception as exc:
                log.error("messages step failed: %s", exc)

    if step in ("all", "adrs"):
        repo = args.repo or os.getenv("GITHUB_DEFAULT_REPOS", "")
        if not repo:
            log.warning("No repo set — skipping adrs step.")
        else:
            try:
                ingest_adrs(repo, adr_path=args.adr_path)
            except Exception as exc:
                log.error("adrs step failed: %s", exc)

    log.info("ETL done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
