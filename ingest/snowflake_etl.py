"""
snowflake_etl.py — DDL + ingestion pipeline for GitMind's Snowflake ledger
===========================================================================
Creates (if absent) the six tables consumed by SnowflakeDetails in main.py,
then populates them from GitHub commits, Jira tickets, Slack messages,
ADRs (markdown files in the repo), and a manual bug/decision feed.

USAGE
-----
  # One-shot: create schema + ingest everything
  python snowflake_etl.py

  # Individual steps
  python snowflake_etl.py --step ddl
  python snowflake_etl.py --step commits  --repo owner/repo
  python snowflake_etl.py --step tickets
  python snowflake_etl.py --step messages --channel C0XXXXXX
  python snowflake_etl.py --step adrs     --repo owner/repo
  python snowflake_etl.py --step bugs     --input bugs.json
  python snowflake_etl.py --step decisions --input decisions.json

ENVIRONMENT VARIABLES (same as config.py)
------------------------------------------
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or key-pair),
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE
  GITHUB_TOKEN   — for GitHub commits/ADRs
  JIRA_URL, JIRA_USER, JIRA_API_TOKEN, JIRA_DEFAULT_PROJECT
  SLACK_BOT_TOKEN
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("gitmind.etl.snowflake")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    # COMMITS — one row per GitHub commit
    """
    CREATE TABLE IF NOT EXISTS COMMITS (
        commit_id      VARCHAR(40)   NOT NULL PRIMARY KEY,
        repo           VARCHAR(255)  NOT NULL,
        branch         VARCHAR(255),
        author         VARCHAR(255),
        author_email   VARCHAR(255),
        message        TEXT,
        timestamp      TIMESTAMP_TZ  NOT NULL,
        files_changed  INTEGER       DEFAULT 0,
        additions      INTEGER       DEFAULT 0,
        deletions      INTEGER       DEFAULT 0,
        patch_summary  TEXT,
        url            VARCHAR(500),
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # TICKETS — one row per Jira issue
    """
    CREATE TABLE IF NOT EXISTS TICKETS (
        ticket_id      VARCHAR(50)   NOT NULL PRIMARY KEY,
        project        VARCHAR(20)   NOT NULL,
        summary        TEXT,
        description    TEXT,
        status         VARCHAR(50),
        priority       VARCHAR(30),
        issue_type     VARCHAR(50),
        assignee       VARCHAR(255),
        reporter       VARCHAR(255),
        created_at     TIMESTAMP_TZ  NOT NULL,
        updated_at     TIMESTAMP_TZ,
        resolved_at    TIMESTAMP_TZ,
        labels         VARIANT,
        fix_versions   VARIANT,
        url            VARCHAR(500),
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # MESSAGES — one row per Slack message
    """
    CREATE TABLE IF NOT EXISTS MESSAGES (
        message_id     VARCHAR(100)  NOT NULL PRIMARY KEY,
        channel_id     VARCHAR(50)   NOT NULL,
        channel_name   VARCHAR(100),
        user_id        VARCHAR(50),
        username       VARCHAR(100),
        text           TEXT,
        timestamp      TIMESTAMP_TZ  NOT NULL,
        thread_ts      VARCHAR(30),
        reaction_count INTEGER       DEFAULT 0,
        reply_count    INTEGER       DEFAULT 0,
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # ADR_RECORDS — architecture decision records (markdown files)
    """
    CREATE TABLE IF NOT EXISTS ADR_RECORDS (
        adr_id         VARCHAR(100)  NOT NULL PRIMARY KEY,
        repo           VARCHAR(255),
        file_path      VARCHAR(500),
        title          TEXT,
        status         VARCHAR(50),
        context        TEXT,
        decision       TEXT,
        consequences   TEXT,
        created_date   DATE,
        raw_markdown   TEXT,
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # BUG_REPORTS — bugs / incidents (from Jira bug-type issues or external feed)
    """
    CREATE TABLE IF NOT EXISTS BUG_REPORTS (
        bug_id         VARCHAR(100)  NOT NULL PRIMARY KEY,
        source         VARCHAR(50)   DEFAULT 'jira',
        title          TEXT,
        description    TEXT,
        severity       VARCHAR(30),
        status         VARCHAR(50),
        affected_repo  VARCHAR(255),
        commit_ref     VARCHAR(40),
        ticket_ref     VARCHAR(50),
        reported_at    TIMESTAMP_TZ  NOT NULL,
        resolved_at    TIMESTAMP_TZ,
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # DECISIONS — lightweight decision log (ADRs, post-mortems, manual entries)
    """
    CREATE TABLE IF NOT EXISTS DECISIONS (
        decision_id    VARCHAR(100)  NOT NULL PRIMARY KEY,
        source         VARCHAR(50)   DEFAULT 'manual',
        title          TEXT,
        rationale      TEXT,
        outcome        TEXT,
        owner          VARCHAR(255),
        related_ticket VARCHAR(50),
        related_commit VARCHAR(40),
        timestamp      TIMESTAMP_TZ  NOT NULL,
        inserted_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
    """,
]


# ---------------------------------------------------------------------------
# Snowflake connection (re-uses logic from snowflake_client.py)
# ---------------------------------------------------------------------------

def _get_connection():
    """Open a raw snowflake-connector-python connection from env vars."""
    import snowflake.connector

    connect_kwargs: dict[str, Any] = {
        "account":   _require("SNOWFLAKE_ACCOUNT"),
        "user":      _require("SNOWFLAKE_USER"),
        "warehouse": _require("SNOWFLAKE_WAREHOUSE"),
        "database":  _require("SNOWFLAKE_DATABASE"),
        "schema":    _require("SNOWFLAKE_SCHEMA"),
        "role":      os.getenv("SNOWFLAKE_ROLE", "PUBLIC"),
        "login_timeout":   30,
        "network_timeout": 30,
    }

    pk_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if pk_path:
        from cryptography.hazmat.primitives import serialization
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        with open(pk_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=passphrase.encode() if passphrase else None
            )
        connect_kwargs["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    else:
        connect_kwargs["password"] = _require("SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(**connect_kwargs)


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise EnvironmentError(f"Required env var '{name}' is not set.")
    return val


def _execute_many(conn, sql: str, rows: list[tuple]) -> int:
    """Bulk insert with MERGE-like skip on duplicate key."""
    if not rows:
        return 0
    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()


def _execute(conn, sql: str, params: tuple = ()) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        if cur.description:
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.commit()
        return []
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Step 1: DDL
# ---------------------------------------------------------------------------

def run_ddl(conn) -> None:
    log.info("Creating tables (IF NOT EXISTS)...")
    for stmt in DDL_STATEMENTS:
        _execute(conn, stmt)
    log.info("DDL complete — 6 tables ready.")


# ---------------------------------------------------------------------------
# Step 2: GitHub commits → COMMITS
# ---------------------------------------------------------------------------

def ingest_commits(conn, repo: str, branch: str = "main", max_pages: int = 10) -> None:
    """Fetch commits from GitHub and upsert into COMMITS."""
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    log.info("Fetching commits from %s @ %s (max %d pages)...", repo, branch, max_pages)

    all_commits: list[dict] = []
    for page in range(1, max_pages + 1):
        resp = requests.get(
            f"https://api.github.com/repos/{owner}/{repo_name}/commits",
            params={"sha": branch, "per_page": 100, "page": page},
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_commits.extend(batch)
        if len(batch) < 100:
            break

    log.info("  %d commits fetched; enriching with diff stats...", len(all_commits))

    upsert_sql = """
        MERGE INTO COMMITS AS tgt
        USING (SELECT %s AS commit_id) AS src ON tgt.commit_id = src.commit_id
        WHEN NOT MATCHED THEN INSERT (
            commit_id, repo, branch, author, author_email, message,
            timestamp, files_changed, additions, deletions, patch_summary, url
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    rows = []
    for c in all_commits:
        sha = c["sha"]
        commit_data = c.get("commit", {})
        author_data = commit_data.get("author") or {}
        committer_data = c.get("author") or {}

        # Fetch detailed stats for this commit (adds/deletions/files)
        detail: dict = {}
        try:
            detail_resp = requests.get(
                f"https://api.github.com/repos/{owner}/{repo_name}/commits/{sha}",
                headers=headers, timeout=15,
            )
            if detail_resp.ok:
                detail = detail_resp.json()
        except Exception as exc:
            log.debug("  Could not fetch detail for %s: %s", sha[:7], exc)

        stats = detail.get("stats", {})
        files = detail.get("files", [])
        patch_summary = "; ".join(
            f"{f['filename']} (+{f.get('additions',0)}/-{f.get('deletions',0)})"
            for f in files[:10]
        )

        ts_raw = author_data.get("date") or datetime.now(timezone.utc).isoformat()
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

        rows.append((
            sha,                                              # MERGE key
            sha,                                              # commit_id
            repo,                                             # repo
            branch,                                           # branch
            committer_data.get("login") or author_data.get("name", ""),
            author_data.get("email", ""),
            commit_data.get("message", "")[:4000],
            ts,
            stats.get("total", len(files)),
            stats.get("additions", 0),
            stats.get("deletions", 0),
            patch_summary[:4000] or None,
            c.get("html_url", ""),
        ))

    inserted = 0
    for row in rows:
        try:
            _execute(conn, upsert_sql, row)
            inserted += 1
        except Exception as exc:
            log.warning("  Skip commit %s: %s", row[1][:7], exc)

    log.info("  Upserted %d / %d commits.", inserted, len(rows))


# ---------------------------------------------------------------------------
# Step 3: Jira → TICKETS + BUG_REPORTS
# ---------------------------------------------------------------------------

def ingest_tickets(conn, project: str | None = None, max_results: int = 5000) -> None:
    """Fetch Jira issues and upsert into TICKETS (and BUG_REPORTS for bug types)."""
    import requests
    from requests.auth import HTTPBasicAuth

    jira_url   = _require("JIRA_URL").rstrip("/")
    jira_user  = _require("JIRA_USER")
    jira_token = _require("JIRA_API_TOKEN")
    project    = project or os.getenv("JIRA_DEFAULT_PROJECT", "PLAT")
    auth       = HTTPBasicAuth(jira_user, jira_token)

    log.info("Fetching Jira issues for project %s...", project)

    ticket_upsert = """
        MERGE INTO TICKETS AS tgt
        USING (SELECT %s AS ticket_id) AS src ON tgt.ticket_id = src.ticket_id
        WHEN NOT MATCHED THEN INSERT (
            ticket_id, project, summary, description, status, priority,
            issue_type, assignee, reporter, created_at, updated_at,
            resolved_at, labels, fix_versions, url
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    bug_upsert = """
        MERGE INTO BUG_REPORTS AS tgt
        USING (SELECT %s AS bug_id) AS src ON tgt.bug_id = src.bug_id
        WHEN NOT MATCHED THEN INSERT (
            bug_id, source, title, description, severity, status,
            ticket_ref, reported_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    start_at = 0
    batch_size = 100
    tickets_inserted = bugs_inserted = 0
    next_page_token = None

    while True:
        params = {
            "jql":        f"project={project} ORDER BY created DESC",
            "maxResults": batch_size,
            "fields":     "summary,description,status,priority,issuetype,"
                          "assignee,reporter,created,updated,resolutiondate,"
                          "labels,fixVersions",
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = requests.get(
            # Atlassian removed GET /rest/api/3/search entirely (410 Gone,
            # final shutdown completed Oct 2025) — /search/jql is the
            # replacement and uses nextPageToken pagination, not startAt.
            f"{jira_url}/rest/api/3/search/jql",
            params=params,
            auth=auth, timeout=30,
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

            def _ts(raw):
                if not raw:
                    return None
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))

            desc_raw = fields.get("description")
            desc_text = ""
            if isinstance(desc_raw, dict):  # Jira Cloud ADF format
                desc_text = _adf_to_text(desc_raw)
            elif isinstance(desc_raw, str):
                desc_text = desc_raw

            labels_json    = json.dumps(fields.get("labels", []))
            versions_json  = json.dumps([v.get("name") for v in fields.get("fixVersions", [])])
            created_at     = _ts(fields.get("created")) or datetime.now(timezone.utc)
            issue_type     = _text(fields.get("issuetype"), "name")

            try:
                _execute(conn, ticket_upsert, (
                    key,                                     # MERGE key
                    key,                                     # ticket_id
                    project,                                 # project
                    fields.get("summary", "")[:1000],        # summary
                    desc_text[:8000],                        # description
                    _text(fields.get("status"), "name"),     # status
                    _text(fields.get("priority"), "name"),   # priority
                    issue_type,                              # issue_type
                    _text(fields.get("assignee"), "displayName"),
                    _text(fields.get("reporter"), "displayName"),
                    created_at,                              # created_at
                    _ts(fields.get("updated")),              # updated_at
                    _ts(fields.get("resolutiondate")),       # resolved_at
                    labels_json,                             # labels (VARIANT)
                    versions_json,                           # fix_versions (VARIANT)
                    f"{jira_url}/browse/{key}",              # url
                ))
                tickets_inserted += 1
            except Exception as exc:
                log.warning("  Skip ticket %s: %s", key, exc)

            # Mirror bugs into BUG_REPORTS
            if issue_type.lower() in ("bug", "incident", "defect"):
                try:
                    _execute(conn, bug_upsert, (
                        key,                                 # MERGE key
                        key,                                 # bug_id
                        "jira",                              # source
                        fields.get("summary", "")[:1000],   # title
                        desc_text[:4000],                    # description
                        _text(fields.get("priority"), "name"),
                        _text(fields.get("status"), "name"),
                        key,                                 # ticket_ref
                        created_at,                          # reported_at
                    ))
                    bugs_inserted += 1
                except Exception as exc:
                    log.warning("  Skip bug_report %s: %s", key, exc)

        start_at += len(issues)
        next_page_token = data.get("nextPageToken")
        if data.get("isLast", not next_page_token) or start_at >= max_results:
            break

    log.info("  Upserted %d tickets, %d bug reports.", tickets_inserted, bugs_inserted)


def _adf_to_text(adf: dict) -> str:
    """Flatten Atlassian Document Format to plain text (best-effort)."""
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

def ingest_messages(conn, channel_id: str, limit_days: int = 90) -> None:
    """Fetch Slack messages from a channel and upsert into MESSAGES."""
    import time
    import requests

    token = _require("SLACK_BOT_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    # Resolve channel name
    channel_name = channel_id
    try:
        info = requests.get(
            "https://slack.com/api/conversations.info",
            params={"channel": channel_id}, headers=headers, timeout=15,
        ).json()
        channel_name = info.get("channel", {}).get("name", channel_id)
    except Exception:
        pass

    log.info("Fetching Slack messages from #%s (%s)...", channel_name, channel_id)

    oldest = str(time.time() - limit_days * 86400)

    upsert_sql = """
        MERGE INTO MESSAGES AS tgt
        USING (SELECT %s AS message_id) AS src ON tgt.message_id = src.message_id
        WHEN NOT MATCHED THEN INSERT (
            message_id, channel_id, channel_name, user_id, text,
            timestamp, thread_ts, reaction_count, reply_count
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor = None
    inserted = 0

    while True:
        params: dict[str, Any] = {
            "channel": channel_id, "limit": 200, "oldest": oldest,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            "https://slack.com/api/conversations.history",
            params=params, headers=headers, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("ok"):
            log.error("Slack API error: %s", data.get("error"))
            break

        for msg in data.get("messages", []):
            ts_float  = float(msg.get("ts", 0))
            ts        = datetime.fromtimestamp(ts_float, tz=timezone.utc)
            msg_id    = f"{channel_id}:{msg['ts']}"
            reactions = sum(r.get("count", 0) for r in msg.get("reactions", []))

            try:
                _execute(conn, upsert_sql, (
                    msg_id,                         # MERGE key
                    msg_id,                         # message_id
                    channel_id,                     # channel_id
                    channel_name,                   # channel_name
                    msg.get("user", ""),            # user_id
                    msg.get("text", "")[:4000],     # text
                    ts,                             # timestamp
                    msg.get("thread_ts"),           # thread_ts
                    reactions,                      # reaction_count
                    msg.get("reply_count", 0),      # reply_count
                ))
                inserted += 1
            except Exception as exc:
                log.warning("  Skip message %s: %s", msg_id, exc)

        meta = data.get("response_metadata", {})
        cursor = meta.get("next_cursor")
        if not cursor:
            break

    log.info("  Upserted %d messages from #%s.", inserted, channel_name)


# ---------------------------------------------------------------------------
# Step 5: GitHub ADR markdown files → ADR_RECORDS
# ---------------------------------------------------------------------------

def ingest_adrs(conn, repo: str, adr_path: str = "docs/adr") -> None:
    """Scan a repo directory for ADR markdown files and upsert into ADR_RECORDS."""
    import re
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    log.info("Scanning %s/%s for ADRs in '%s'...", owner, repo_name, adr_path)

    # List files in the ADR directory
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo_name}/contents/{adr_path}",
        headers=headers, timeout=15,
    )
    if not resp.ok:
        log.warning("  ADR path '%s' not found in %s (%s).", adr_path, repo, resp.status_code)
        return

    files = [f for f in resp.json() if f.get("name", "").endswith(".md")]
    log.info("  Found %d ADR files.", len(files))

    upsert_sql = """
        MERGE INTO ADR_RECORDS AS tgt
        USING (SELECT %s AS adr_id) AS src ON tgt.adr_id = src.adr_id
        WHEN NOT MATCHED THEN INSERT (
            adr_id, repo, file_path, title, status, context,
            decision, consequences, created_date, raw_markdown
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0
    for f in files:
        import base64
        content_resp = requests.get(f["url"], headers=headers, timeout=15)
        if not content_resp.ok:
            continue

        raw = base64.b64decode(content_resp.json().get("content", "")).decode("utf-8", errors="replace")
        title     = _extract_section(raw, r"^#\s+(.+)$")
        status    = _extract_section(raw, r"[Ss]tatus[:\s]+(.+)")
        context   = _extract_md_section(raw, "Context")
        decision  = _extract_md_section(raw, "Decision")
        conseq    = _extract_md_section(raw, "Consequences")

        # Try to parse a date from the filename (e.g. 0001-2024-01-15-use-postgres.md)
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
        created_date = date_match.group(1) if date_match else None

        adr_id = f"{repo}/{f['path']}"

        try:
            _execute(conn, upsert_sql, (
                adr_id,                 # MERGE key
                adr_id,                 # adr_id
                repo,                   # repo
                f["path"],              # file_path
                (title or f["name"])[:500],
                (status or "")[:50],
                (context or "")[:8000],
                (decision or "")[:8000],
                (conseq or "")[:8000],
                created_date,           # created_date (DATE)
                raw[:16000],            # raw_markdown
            ))
            inserted += 1
        except Exception as exc:
            log.warning("  Skip ADR %s: %s", adr_id, exc)

    log.info("  Upserted %d ADR records.", inserted)


def _extract_section(text: str, pattern: str) -> str:
    import re
    m = re.search(pattern, text, re.MULTILINE)
    return m.group(1).strip() if m else ""


def _extract_md_section(text: str, heading: str) -> str:
    import re
    m = re.search(
        rf"##\s+{heading}\s*\n(.*?)(?=\n##|\Z)",
        text, re.DOTALL | re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# Step 6: JSON feed → BUG_REPORTS / DECISIONS
# ---------------------------------------------------------------------------

def ingest_bugs_from_file(conn, path: str) -> None:
    """
    Load bugs from a JSON file. Expected format:
    [{"bug_id": "BUG-1", "title": "...", "description": "...",
      "severity": "high", "status": "open",
      "affected_repo": "...", "commit_ref": "...", "ticket_ref": "...",
      "reported_at": "2024-01-01T00:00:00Z", "resolved_at": null}]
    """
    with open(path) as f:
        bugs: list[dict] = json.load(f)

    upsert_sql = """
        MERGE INTO BUG_REPORTS AS tgt
        USING (SELECT %s AS bug_id) AS src ON tgt.bug_id = src.bug_id
        WHEN NOT MATCHED THEN INSERT (
            bug_id, source, title, description, severity, status,
            affected_repo, commit_ref, ticket_ref, reported_at, resolved_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0
    for bug in bugs:
        def _ts(raw):
            if not raw:
                return None
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))

        try:
            _execute(conn, upsert_sql, (
                bug["bug_id"],
                bug["bug_id"],
                bug.get("source", "manual"),
                bug.get("title", "")[:1000],
                bug.get("description", "")[:4000],
                bug.get("severity", ""),
                bug.get("status", ""),
                bug.get("affected_repo", ""),
                bug.get("commit_ref", ""),
                bug.get("ticket_ref", ""),
                _ts(bug.get("reported_at")) or datetime.now(timezone.utc),
                _ts(bug.get("resolved_at")),
            ))
            inserted += 1
        except Exception as exc:
            log.warning("  Skip bug %s: %s", bug.get("bug_id"), exc)

    log.info("Upserted %d bug reports from %s.", inserted, path)


def ingest_decisions_from_file(conn, path: str) -> None:
    """
    Load decisions from a JSON file. Expected format:
    [{"decision_id": "DEC-1", "title": "...", "rationale": "...",
      "outcome": "...", "owner": "...", "related_ticket": "...",
      "related_commit": "...", "timestamp": "2024-01-01T00:00:00Z"}]
    """
    with open(path) as f:
        decisions: list[dict] = json.load(f)

    upsert_sql = """
        MERGE INTO DECISIONS AS tgt
        USING (SELECT %s AS decision_id) AS src ON tgt.decision_id = src.decision_id
        WHEN NOT MATCHED THEN INSERT (
            decision_id, source, title, rationale, outcome, owner,
            related_ticket, related_commit, timestamp
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0
    for dec in decisions:
        def _ts(raw):
            if not raw:
                return None
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))

        try:
            _execute(conn, upsert_sql, (
                dec["decision_id"],
                dec["decision_id"],
                dec.get("source", "manual"),
                dec.get("title", "")[:1000],
                dec.get("rationale", "")[:4000],
                dec.get("outcome", "")[:2000],
                dec.get("owner", ""),
                dec.get("related_ticket", ""),
                dec.get("related_commit", ""),
                _ts(dec.get("timestamp")) or datetime.now(timezone.utc),
            ))
            inserted += 1
        except Exception as exc:
            log.warning("  Skip decision %s: %s", dec.get("decision_id"), exc)

    log.info("Upserted %d decisions from %s.", inserted, path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GitMind Snowflake ETL")
    parser.add_argument("--step", default="all",
        choices=["all", "ddl", "commits", "tickets", "messages", "adrs", "bugs", "decisions"])
    parser.add_argument("--repo",    help="owner/repo for commits and ADRs")
    parser.add_argument("--branch",  default="main")
    parser.add_argument("--channel", help="Slack channel ID for messages")
    parser.add_argument("--adr-path", default="docs/adr")
    parser.add_argument("--project", help="Jira project key")
    parser.add_argument("--input",   help="JSON file path for bugs/decisions step")
    args = parser.parse_args(argv)

    try:
        conn = _get_connection()
    except Exception as exc:
        log.error("Cannot connect to Snowflake: %s", exc)
        return 1

    try:
        step = args.step
        if step in ("all", "ddl"):
            run_ddl(conn)
        if step in ("all", "commits"):
            if not args.repo:
                log.error("--repo required for commits step")
                return 1
            ingest_commits(conn, args.repo, branch=args.branch)
        if step in ("all", "tickets"):
            ingest_tickets(conn, project=args.project)
        if step in ("all", "messages"):
            if not args.channel:
                log.error("--channel required for messages step")
                return 1
            ingest_messages(conn, args.channel)
        if step in ("all", "adrs"):
            if not args.repo:
                log.error("--repo required for adrs step")
                return 1
            ingest_adrs(conn, args.repo, adr_path=args.adr_path)
        if step == "bugs":
            if not args.input:
                log.error("--input required for bugs step")
                return 1
            ingest_bugs_from_file(conn, args.input)
        if step == "decisions":
            if not args.input:
                log.error("--input required for decisions step")
                return 1
            ingest_decisions_from_file(conn, args.input)
    finally:
        conn.close()

    log.info("ETL done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
