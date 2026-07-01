"""
neo4j_etl.py — Graph ingestion pipeline for GitMind's Neo4j store
==================================================================
Creates nodes and causal edges consumed by Neo4jCausalGraph.trace() in
causal_graph.py. Mirrors the same six entity types as snowflake_etl.py,
then wires them together with the relationship types already hard-coded
in causal_graph.py:

  CAUSED_BY | INFLUENCED_BY | REFERENCES | SHAPES | DISCUSSED_IN | GOVERNED_BY

Node labels created:
  Commit  · Ticket  · SlackMessage  · ADR  · BugReport  · Decision

USAGE
-----
  # Full pipeline (pull from APIs → ingest into Neo4j)
  python neo4j_etl.py

  # Individual steps
  python neo4j_etl.py --step constraints
  python neo4j_etl.py --step commits   --repo owner/repo
  python neo4j_etl.py --step tickets
  python neo4j_etl.py --step messages  --channel C0XXXXXX
  python neo4j_etl.py --step adrs      --repo owner/repo
  python neo4j_etl.py --step bugs      --input bugs.json
  python neo4j_etl.py --step decisions --input decisions.json
  python neo4j_etl.py --step edges
  python neo4j_etl.py --step reset      # wipe all nodes/edges

ENVIRONMENT VARIABLES
---------------------
  NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE
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

log = logging.getLogger("gitmind.etl.neo4j")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_driver():
    from neo4j import GraphDatabase
    uri      = _require("NEO4J_URI")
    user     = _require("NEO4J_USER")
    password = _require("NEO4J_PASSWORD")
    driver   = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    log.info("Connected to Neo4j at %s", uri)
    return driver


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise EnvironmentError(f"Required env var '{name}' is not set.")
    return val


def _db(driver) -> str:
    return os.getenv("NEO4J_DATABASE", "neo4j")


def _run(session, cypher: str, **params) -> list[dict]:
    result = session.run(cypher, **params)
    return [dict(r) for r in result]


# ---------------------------------------------------------------------------
# Step 0: Reset — wipe every node + relationship. Called on frontend refresh
# / repo switch so a new repo's graph never overlaps the previous one. Runs
# in batches via apoc-free `CALL { } IN TRANSACTIONS` (Neo4j 5+) so it won't
# OOM on large graphs; falls back to a plain DETACH DELETE if that syntax
# isn't supported on the connected server version.
# ---------------------------------------------------------------------------

def reset_all(driver=None) -> None:
    own_driver = driver is None
    driver = driver or _get_driver()
    try:
        with driver.session(database=_db(driver)) as session:
            try:
                result = session.run("""
                    MATCH (n)
                    CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS
                """)
            except Exception:
                session.run("MATCH (n) DETACH DELETE n")
        log.info("Neo4j reset complete — all nodes and relationships wiped.")
    finally:
        if own_driver:
            driver.close()


# ---------------------------------------------------------------------------
# Step 1: Constraints & indexes
# ---------------------------------------------------------------------------

CONSTRAINTS = [
    ("Commit",       "id"),
    ("Ticket",       "id"),
    ("SlackMessage", "id"),
    ("ADR",          "id"),
    ("BugReport",    "id"),
    ("Decision",     "id"),
]

def run_constraints(driver) -> None:
    log.info("Creating uniqueness constraints...")
    with driver.session(database=_db(driver)) as session:
        for label, prop in CONSTRAINTS:
            cypher = (
                f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) "
                f"REQUIRE n.{prop} IS UNIQUE"
            )
            session.run(cypher)
        # Extra indexes for common traversal properties
        session.run("CREATE INDEX IF NOT EXISTS FOR (n:Commit)       ON (n.repo)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (n:Ticket)       ON (n.project)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (n:BugReport)    ON (n.ticket_ref)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (n:Decision)     ON (n.related_ticket)")
    log.info("Constraints + indexes done.")


# ---------------------------------------------------------------------------
# Helper: MERGE a node
# ---------------------------------------------------------------------------

def _merge_node(session, label: str, node_id: str, props: dict) -> None:
    """MERGE on (label {id: node_id}), SET all other props."""
    set_clause = ", ".join(f"n.{k} = ${k}" for k in props)
    cypher = f"""
        MERGE (n:{label} {{id: $node_id}})
        SET n.node_id = $node_id,
            n.source_type = '{label}',
            n.source_id = $node_id
            {", " + set_clause if set_clause else ""}
    """
    session.run(cypher, node_id=node_id, **props)


def _merge_rel(session, from_id: str, from_label: str,
               rel: str, to_id: str, to_label: str, props: dict | None = None) -> None:
    """MERGE a directed relationship between two already-existing nodes."""
    prop_str = ""
    if props:
        prop_str = " {" + ", ".join(f"{k}: ${k}" for k in props) + "}"
    cypher = f"""
        MATCH (a:{from_label} {{id: $from_id}})
        MATCH (b:{to_label}   {{id: $to_id}})
        MERGE (a)-[r:{rel}{prop_str}]->(b)
    """
    session.run(cypher, from_id=from_id, to_id=to_id, **(props or {}))


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


def ingest_commits(driver, repo: str, branch: str = "main", max_pages: int = 10) -> None:
    # max_pages=10 * per_page=100 = 1000 commits cap PER BRANCH (rolled back
    # from 50/5000). `branch` arg is kept only for CLI backward-compat and is
    # ignored below -- commits are now ingested for EVERY branch so the
    # causal graph isn't limited to main, and so commit-to-commit edges
    # (added below) can connect history across all branches, not just one.
    import requests

    token   = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)

    pinned = os.getenv("GITMIND_INGEST_BRANCH", "")
    branches = _list_branches(owner, repo_name, headers)
    if not branches:
        branches = [branch]
    if pinned:
        if pinned in branches:
            branches = [pinned]
            log.info("Branch pinned to '%s' via GITMIND_INGEST_BRANCH", pinned)
        else:
            # See ingest/snowflake_etl.py's ingest_commits for why this is
            # validated rather than trusted outright: a deployment-wide pin
            # left over from a previous repo would otherwise 404 every
            # commits/adrs call for any repo that doesn't happen to share
            # that branch name.
            log.warning(
                "GITMIND_INGEST_BRANCH='%s' does not exist on %s -- ignoring "
                "the pin and using all %d branch(es) found instead.",
                pinned, repo, len(branches),
            )

    log.info("Fetching commits from %s across %d branch(es) (max %d pages / %d commits per branch)...",
              repo, len(branches), max_pages, max_pages * 100)

    # sha -> (commit_dict, branch_name_first_seen, parents) -- dedupe commits
    # that appear on multiple branches; MERGE on sha already makes the Neo4j
    # side idempotent, but collecting parents once avoids redundant edge
    # MERGE calls for every branch a commit happens to be reachable from.
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

    log.info("  Merging %d Commit nodes...", len(commits_by_sha))
    with driver.session(database=_db(driver)) as session:
        for sha, entry in commits_by_sha.items():
            c           = entry["commit"]
            commit_data = c.get("commit", {})
            author_data = commit_data.get("author") or {}
            message     = commit_data.get("message", "")
            ts_raw      = author_data.get("date", "")

            _merge_node(session, "Commit", sha, {
                "repo":      repo,
                "branch":    entry["branch"],
                "author":    (c.get("author") or {}).get("login") or author_data.get("name", ""),
                "message":   message[:2000],
                "summary":   message.split("\n")[0][:500],
                "timestamp": ts_raw,
                "url":       c.get("html_url", ""),
            })

        log.info("  Done — %d Commit nodes merged.", len(commits_by_sha))

        # Commit-to-commit causal edges: every parent SHA, including all
        # parents of merge commits and across every branch. Lets trace()
        # walk real commit history even when no Jira/Slack/ADR/bug data
        # exists to link things together.
        edge_count = 0
        for sha, entry in commits_by_sha.items():
            for parent in entry["commit"].get("parents", []):
                parent_sha = parent.get("sha")
                if not parent_sha or parent_sha not in commits_by_sha:
                    continue
                _merge_rel(session, sha, "Commit", "CAUSED_BY", parent_sha, "Commit")
                edge_count += 1

        log.info("  Done — %d Commit -[CAUSED_BY]-> Commit edges merged.", edge_count)


# ---------------------------------------------------------------------------
# Step 3: Tickets
# ---------------------------------------------------------------------------

def _fetch_repo_local_tickets(repo: str, path: str = "jira/issues.json") -> dict | None:
    """See snowflake_etl._fetch_repo_local_tickets — identical detection logic,
    kept duplicated rather than shared to avoid a cross-module import between
    the two independent ETL CLIs.
    """
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo_name}/contents/{path}",
        headers=headers, timeout=15,
    )
    if not resp.ok:
        return None
    try:
        raw = base64.b64decode(resp.json().get("content", "")).decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception as exc:
        log.warning("Found %s in %s but couldn't parse it as JSON: %s", path, repo, exc)
        return None
    if not isinstance(data, dict) or not isinstance(data.get("issues"), list):
        log.warning("%s in %s doesn't match expected {project, issues:[...]} shape — ignoring.", path, repo)
        return None
    return data


def ingest_tickets(
    driver,
    repo: str | None = None,
    project: str | None = None,
    max_results: int = 5000,
    local_tickets_path: str = "jira/issues.json",
) -> None:
    import requests
    from requests.auth import HTTPBasicAuth

    if repo:
        local = _fetch_repo_local_tickets(repo, local_tickets_path)
        if local is not None:
            local_project = (local.get("project") or {}).get("key") or project or repo
            log.info("Found local ticket file '%s' in %s (project=%s) — using it instead of live Jira.",
                      local_tickets_path, repo, local_project)
            with driver.session(database=_db(driver)) as session:
                for issue in local["issues"]:
                    key = issue.get("id")
                    if not key:
                        continue
                    issue_type = issue.get("type", "")
                    is_bug = issue_type.lower() in ("bug", "incident", "defect")

                    _merge_node(session, "Ticket", key, {
                        "project": local_project,
                        "summary": str(issue.get("title", ""))[:1000],
                        "text": str(issue.get("description", ""))[:4000],
                        "status": issue.get("status", ""),
                        "priority": issue.get("priority", ""),
                        "issue_type": issue_type,
                        "assignee": "",
                        "created_at": "",
                        "url": f"https://github.com/{repo}/blob/HEAD/{local_tickets_path}#{key}",
                        "is_bug": is_bug,
                    })

                    if is_bug:
                        _merge_node(session, "BugReport", f"bug:{key}", {
                            "ticket_ref": key,
                            "title": str(issue.get("title", ""))[:500],
                            "summary": str(issue.get("description", ""))[:2000],
                            "severity": issue.get("priority", ""),
                            "status": issue.get("status", ""),
                            "reported_at": "",
                        })
                        _merge_rel(session, f"bug:{key}", "BugReport", "REFERENCES", key, "Ticket")

                    # The local ticket file states exactly which commit SHAs
                    # belong to this issue. That's a direct, reliable signal —
                    # use it instead of relying on build_edges()'s regex match
                    # against commit messages, which silently produces zero
                    # edges for any repo whose commit messages don't happen to
                    # contain the ticket key as a literal substring (true for
                    # most repos that don't follow Conventional Commits-style
                    # "PROJ-123: ..." prefixes).
                    # jira/issues.json stores 7-char short shas (GitHub's
                    # display convention), but ingest_commits() MERGEs
                    # Commit nodes with the full 40-char sha as `id`. A
                    # plain _merge_rel({id: $sha}) MATCH therefore finds
                    # zero rows and Cypher silently skips the downstream
                    # MERGE -- no error, no log, the edge just never
                    # exists. STARTS WITH matches the short sha as a
                    # prefix of the real id instead.
                    ticket_commit_edges = 0
                    for sha in issue.get("commits", []) or []:
                        result = session.run(
                            """
                            MATCH (a:Commit) WHERE a.id STARTS WITH $sha
                            MATCH (b:Ticket {id: $key})
                            MERGE (a)-[r:REFERENCES]->(b)
                            RETURN count(r) AS merged
                            """,
                            sha=sha, key=key,
                        )
                        ticket_commit_edges += result.single()["merged"]
                    if ticket_commit_edges:
                        log.info("  Ticket %s -[REFERENCES]<- %d Commit(s) linked via jira/issues.json commits[].",
                                  key, ticket_commit_edges)
            log.info("ETL done.")
            return

    jira_url   = os.getenv("JIRA_URL", "").rstrip("/")
    jira_user  = os.getenv("JIRA_USER", "")
    jira_token = os.getenv("JIRA_API_TOKEN", "")
    project    = project or os.getenv("JIRA_DEFAULT_PROJECT", "")

    if not jira_url:
        log.warning("JIRA_URL not set — skipping ticket ingestion.")
        return
    if not project:
        log.warning("JIRA_DEFAULT_PROJECT not set — skipping ticket ingestion.")
        return

    # Auth is optional — Apache Jira (issues.apache.org) is public, no auth needed.
    # Atlassian Cloud requires JIRA_USER + JIRA_API_TOKEN.
    auth = HTTPBasicAuth(jira_user, jira_token) if jira_user and jira_token else None

    # Apache uses REST API v2; Atlassian Cloud uses v3/search/jql.
    # Detect by whether the URL contains "atlassian.net".
    is_atlassian_cloud = "atlassian.net" in jira_url
    search_url = (
        f"{jira_url}/rest/api/3/search/jql" if is_atlassian_cloud
        else f"{jira_url}/rest/api/2/search"
    )

    log.info("Fetching Jira tickets for project %s...", project)

    start_at   = 0
    batch_size = 100
    inserted   = 0
    next_page_token = None

    while True:
        params = {
            "jql":        f"project={project} ORDER BY created DESC",
            "maxResults": batch_size,
            "fields":     "summary,description,status,priority,issuetype,"
                          "assignee,reporter,created,updated,resolutiondate,labels",
        }
        if is_atlassian_cloud and next_page_token:
            params["nextPageToken"] = next_page_token
        elif not is_atlassian_cloud:
            params["startAt"] = start_at

        resp = requests.get(search_url, params=params, auth=auth, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        issues = data.get("issues", [])
        if not issues:
            break

        with driver.session(database=_db(driver)) as session:
            for issue in issues:
                key    = issue["key"]
                fields = issue.get("fields", {})

                def _text(obj, *keys):
                    for k in keys:
                        obj = (obj or {}).get(k)
                    return obj or ""

                desc_raw  = fields.get("description")
                desc_text = _adf_to_text(desc_raw) if isinstance(desc_raw, dict) else (desc_raw or "")
                issue_type = _text(fields.get("issuetype"), "name")
                label     = "BugReport" if issue_type.lower() in ("bug", "incident", "defect") else "Ticket"

                _merge_node(session, "Ticket", key, {
                    "project":    project,
                    "summary":    fields.get("summary", "")[:1000],
                    "text":       desc_text[:4000],
                    "status":     _text(fields.get("status"), "name"),
                    "priority":   _text(fields.get("priority"), "name"),
                    "issue_type": issue_type,
                    "assignee":   _text(fields.get("assignee"), "displayName"),
                    "created_at": fields.get("created", ""),
                    "url":        f"{jira_url}/browse/{key}",
                    "is_bug":     label == "BugReport",
                })

                # Also create a BugReport node for bug-type issues
                if label == "BugReport":
                    _merge_node(session, "BugReport", f"bug:{key}", {
                        "ticket_ref":  key,
                        "title":       fields.get("summary", "")[:500],
                        "summary":     desc_text[:2000],
                        "severity":    _text(fields.get("priority"), "name"),
                        "status":      _text(fields.get("status"), "name"),
                        "reported_at": fields.get("created", ""),
                    })
                    # BugReport -[REFERENCES]-> Ticket
                    _merge_rel(session, f"bug:{key}", "BugReport", "REFERENCES", key, "Ticket")

                inserted += 1

        start_at += len(issues)
        next_page_token = data.get("nextPageToken")
        if data.get("isLast", not next_page_token) or start_at >= max_results:
            break

    log.info("  Merged %d Ticket nodes.", inserted)


def _adf_to_text(adf: dict) -> str:
    parts = []
    for node in adf.get("content", []):
        if node.get("type") == "paragraph":
            for inline in node.get("content", []):
                if inline.get("type") == "text":
                    parts.append(inline.get("text", ""))
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Step 4: Slack → SlackMessage nodes
# ---------------------------------------------------------------------------

def _fetch_repo_local_messages(repo: str, path: str = "slack/messages.json") -> dict | None:
    """Same detection pattern as _fetch_repo_local_tickets: a repo can ship
    slack/messages.json as a static export instead of requiring a live
    SLACK_BOT_TOKEN + --channel. Kept duplicated (not shared) for the same
    reason as the ticket fallback -- no cross-module import between the two
    independent ETL CLIs.
    """
    import requests

    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo_name = repo.split("/", 1)
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo_name}/contents/{path}",
        headers=headers, timeout=15,
    )
    if not resp.ok:
        return None
    try:
        raw = base64.b64decode(resp.json().get("content", "")).decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception as exc:
        log.warning("Found %s in %s but couldn't parse it as JSON: %s", path, repo, exc)
        return None
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        log.warning("%s in %s doesn't match expected {channel, messages:[...]} shape — ignoring.", path, repo)
        return None
    return data


def ingest_messages(
    driver,
    channel_id: str | None = None,
    limit_days: int = 90,
    repo: str | None = None,
    local_messages_path: str = "slack/messages.json",
) -> None:
    import requests

    if repo:
        local = _fetch_repo_local_messages(repo, local_messages_path)
        if local is not None:
            local_channel_id   = local.get("channel_id") or channel_id or "local"
            local_channel_name = local.get("channel") or local_channel_id
            log.info("Found local message file '%s' in %s (channel=%s) — using it instead of live Slack.",
                      local_messages_path, repo, local_channel_name)

            inserted = 0
            with driver.session(database=_db(driver)) as session:
                for msg in local["messages"]:
                    ts = msg.get("ts")
                    if not ts:
                        continue
                    msg_id = f"{local_channel_id}:{ts}"
                    try:
                        ts_dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                        timestamp = ts_dt.isoformat()
                    except (TypeError, ValueError):
                        # slack/messages.json may store ISO timestamps directly
                        # instead of Slack's raw epoch `ts` string.
                        timestamp = str(ts)

                    _merge_node(session, "SlackMessage", msg_id, {
                        "channel_id":   local_channel_id,
                        "channel_name": local_channel_name,
                        "user_id":      msg.get("user", ""),
                        "text":         str(msg.get("text", ""))[:2000],
                        "summary":      str(msg.get("text", ""))[:300],
                        "timestamp":    timestamp,
                        "thread_ts":    msg.get("thread_ts", ""),
                    })
                    inserted += 1

                    # Same directness trick as ingest_tickets' local path: if
                    # the file states exactly which commits/tickets a message
                    # discusses, wire DISCUSSED_IN here instead of leaving it
                    # to build_edges()'s regex/keyword match, which silently
                    # produces zero edges for messages that don't happen to
                    # contain a literal ticket key or full commit sha.
                    for sha in msg.get("commits", []) or []:
                        result = session.run(
                            """
                            MATCH (a:SlackMessage {id: $msg_id})
                            MATCH (b:Commit) WHERE b.id STARTS WITH $sha
                            MERGE (a)-[r:DISCUSSED_IN]->(b)
                            RETURN count(r) AS merged
                            """,
                            msg_id=msg_id, sha=sha,
                        )
                        if result.single()["merged"]:
                            log.info("  SlackMessage %s -[DISCUSSED_IN]-> Commit (sha=%s) via local file.", msg_id, sha)
                    for key in msg.get("tickets", []) or []:
                        result = session.run(
                            """
                            MATCH (a:SlackMessage {id: $msg_id})
                            MATCH (b:Ticket {id: $key})
                            MERGE (a)-[r:DISCUSSED_IN]->(b)
                            RETURN count(r) AS merged
                            """,
                            msg_id=msg_id, key=key,
                        )
                        if result.single()["merged"]:
                            log.info("  SlackMessage %s -[DISCUSSED_IN]-> Ticket %s via local file.", msg_id, key)

            log.info("  Merged %d SlackMessage nodes from local file (channel=%s).", inserted, local_channel_name)
            return

    if not channel_id:
        log.warning("No slack/messages.json found and no --channel given — skipping message ingestion.")
        return

    token   = _require("SLACK_BOT_TOKEN")
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
    inserted = 0

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

        with driver.session(database=_db(driver)) as session:
            for msg in data.get("messages", []):
                msg_id = f"{channel_id}:{msg['ts']}"
                ts_dt  = datetime.fromtimestamp(float(msg.get("ts", 0)), tz=timezone.utc)

                _merge_node(session, "SlackMessage", msg_id, {
                    "channel_id":   channel_id,
                    "channel_name": channel_name,
                    "user_id":      msg.get("user", ""),
                    "text":         msg.get("text", "")[:2000],
                    "summary":      msg.get("text", "")[:300],
                    "timestamp":    ts_dt.isoformat(),
                    "thread_ts":    msg.get("thread_ts", ""),
                })
                inserted += 1

        meta   = data.get("response_metadata", {})
        cursor = meta.get("next_cursor")
        if not cursor:
            break

    log.info("  Merged %d SlackMessage nodes from #%s.", inserted, channel_name)


# ---------------------------------------------------------------------------
# Step 5: ADRs → ADR nodes
# ---------------------------------------------------------------------------

def ingest_adrs(driver, repo: str, adr_path: str = "docs/adr") -> None:
    import re, base64, requests

    token   = os.getenv("GITHUB_TOKEN")
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

    inserted = 0
    with driver.session(database=_db(driver)) as session:
        for f in files:
            content_resp = requests.get(f["url"], headers=headers, timeout=15)
            if not content_resp.ok:
                continue
            raw   = base64.b64decode(content_resp.json().get("content", "")).decode("utf-8", errors="replace")
            title = _extract_section(raw, r"^#\s+(.+)$") or f["name"]
            status = _extract_section(raw, r"[Ss]tatus[:\s]+(.+)")

            date_match   = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
            created_date = date_match.group(1) if date_match else ""

            adr_id = f"{repo}/{f['path']}"
            _merge_node(session, "ADR", adr_id, {
                "repo":         repo,
                "file_path":    f["path"],
                "title":        title[:500],
                "summary":      title[:500],
                "status":       (status or "")[:50],
                "created_date": created_date,
                "text":         raw[:4000],
            })
            inserted += 1

    log.info("  Merged %d ADR nodes.", inserted)


def _extract_section(text: str, pattern: str) -> str:
    import re
    m = re.search(pattern, text, re.MULTILINE)
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# Step 5.5: Synthesize Decision nodes from ADRs — generic, any repo.
#
# build_edges() already wires Decision -[GOVERNED_BY]-> Ticket and
# Decision -[SHAPES]-> Commit, but those queries only fire if Decision
# nodes exist with related_ticket / related_commit populated. The only
# path that creates Decision nodes is ingest_decisions_from_file(), which
# is manual-JSON-only and never called by run_ingest.py's automatic
# pipeline. Result: every repo ingested through /ingest/repo has zero
# Decision nodes, so SHAPES/GOVERNED_BY are permanently empty regardless
# of repo. This derives one Decision per ADR automatically, using the
# same best-effort regex/keyword matching already used elsewhere in this
# file (ticket-key shape, ADR-title-in-commit-message) so it generalizes
# to any public repo without per-repo configuration.
# ---------------------------------------------------------------------------

_TICKET_KEY_RE = re.compile(r"\b[A-Z][A-Z0-9]+-\d{1,5}\b")


def synthesize_decisions_from_adrs(driver, repo: str) -> None:
    log.info("Synthesizing Decision nodes from ADRs for %s...", repo)

    inserted = 0
    with driver.session(database=_db(driver)) as session:
        adrs = session.run(
            "MATCH (a:ADR {repo: $repo}) RETURN a.id AS id, a.title AS title, "
            "a.text AS text, a.status AS status",
            repo=repo,
        ).data()

        for adr in adrs:
            adr_id = adr["id"]
            title = adr.get("title") or ""
            text = adr.get("text") or ""
            decision_id = f"decision:{adr_id}"

            # related_ticket: any ticket-key-shaped token mentioned in the
            # ADR body — same shape regex used for commit/query ticket
            # extraction elsewhere in this codebase. Generic, no per-repo
            # config required.
            #
            # BUG (found via live debugging): ADR titles/IDs are themselves
            # LETTERS-DIGITS shaped ("ADR-001"), matching this same regex.
            # `.search()` returns the first hit, so on an ADR whose title
            # starts with "ADR-001: ..." the match was always the ADR's own
            # ID, never the real Jira key mentioned further down in the
            # body (e.g. "RRW-011") — related_ticket silently got set to
            # "ADR-001", which matches no Ticket node, so GOVERNED_BY was
            # 0 edges on every ingest regardless of the cartesian-query
            # fix. Exclude ADR-shaped tokens from candidate matches so the
            # first real ticket key in the body wins instead.
            candidates = [
                m for m in _TICKET_KEY_RE.finditer(text)
                if not m.group(0).startswith("ADR-")
            ]
            if not candidates:
                candidates = [
                    m for m in _TICKET_KEY_RE.finditer(title)
                    if not m.group(0).startswith("ADR-")
                ]
            related_ticket = candidates[0].group(0) if candidates else ""

            # related_commit: the commit whose message references this
            # ADR's title keyword — same best-effort match build_edges()
            # already uses for ADR -[REFERENCES]-> Commit, reused here so
            # the Decision node anchors to the same commit.
            commit_row = session.run(
                """
                MATCH (c:Commit {repo: $repo})
                WHERE toLower(c.message) CONTAINS toLower(split($title, ':')[0])
                RETURN c.id AS id ORDER BY c.timestamp ASC LIMIT 1
                """,
                repo=repo, title=title,
            ).single()
            related_commit = commit_row["id"] if commit_row else ""

            _merge_node(session, "Decision", decision_id, {
                "title": title[:500],
                "summary": (text[:500] or title[:500]),
                "outcome": adr.get("status") or "",
                "owner": "",
                "related_ticket": related_ticket,
                "related_commit": related_commit,
                "timestamp": "",
            })
            inserted += 1

    log.info("  Synthesized %d Decision node(s) from ADRs.", inserted)


# ---------------------------------------------------------------------------
# Step 6: JSON feeds → BugReport / Decision nodes
# ---------------------------------------------------------------------------

def ingest_bugs_from_file(driver, path: str) -> None:
    with open(path) as f:
        bugs: list[dict] = json.load(f)

    inserted = 0
    with driver.session(database=_db(driver)) as session:
        for bug in bugs:
            bid = bug["bug_id"]
            _merge_node(session, "BugReport", bid, {
                "title":       bug.get("title", "")[:500],
                "summary":     bug.get("description", "")[:1000],
                "severity":    bug.get("severity", ""),
                "status":      bug.get("status", ""),
                "ticket_ref":  bug.get("ticket_ref", ""),
                "commit_ref":  bug.get("commit_ref", ""),
                "reported_at": bug.get("reported_at", ""),
            })
            inserted += 1

    log.info("Merged %d BugReport nodes from %s.", inserted, path)


def ingest_decisions_from_file(driver, path: str) -> None:
    with open(path) as f:
        decisions: list[dict] = json.load(f)

    inserted = 0
    with driver.session(database=_db(driver)) as session:
        for dec in decisions:
            did = dec["decision_id"]
            _merge_node(session, "Decision", did, {
                "title":          dec.get("title", "")[:500],
                "summary":        (dec.get("rationale") or dec.get("title", ""))[:1000],
                "outcome":        dec.get("outcome", "")[:500],
                "owner":          dec.get("owner", ""),
                "related_ticket": dec.get("related_ticket", ""),
                "related_commit": dec.get("related_commit", ""),
                "timestamp":      dec.get("timestamp", ""),
            })
            inserted += 1

    log.info("Merged %d Decision nodes from %s.", inserted, path)


# ---------------------------------------------------------------------------
# Step 7: Wire edges
# ---------------------------------------------------------------------------

def build_edges(driver) -> None:
    """
    Infer relationships between existing nodes from shared identifiers
    (ticket refs in commit messages, commit SHAs in bug reports, etc.)
    and create the causal edges that Neo4jCausalGraph.trace() traverses.
    """
    log.info("Building edges between nodes...")
    with driver.session(database=_db(driver)) as session:

        # Commit message references a Jira ticket  → Commit -[REFERENCES]-> Ticket
        # Regex match can't use an index seek like the equality joins above,
        # but correlating Commit inside a scoped subquery per Ticket row
        # (rather than an open `MATCH (c),(t)`) still avoids the cartesian
        # product notification, since Neo4j can plan it as a per-row nested
        # loop against a bound `t` instead of a full cross join.
        result = session.run("""
            MATCH (t:Ticket)
            CALL (t) {
                MATCH (c:Commit)
                WHERE c.message =~ ('(?i).*' + t.id + '.*')
                  AND NOT (c)-[:REFERENCES]->(t)
                MERGE (c)-[:REFERENCES]->(t)
            }
        """)
        merged = result.consume().counters.relationships_created
        log.info("  Commit -[REFERENCES]-> Ticket: %d edge(s) created", merged)

        # BugReport references a commit  → BugReport -[CAUSED_BY]-> Commit
        # Rewritten from `MATCH (b),(c) WHERE b.commit_ref = c.id` (a true
        # cartesian product — every BugReport paired with every Commit
        # before filtering) to a per-row index lookup: for each BugReport,
        # look up the one Commit whose id matches via the range index on
        # Commit.id. Neo4j plans the second MATCH as an indexed seek, not
        # a scan-and-join, so this no longer triggers (or deserves) the
        # cartesian product notification.
        result = session.run("""
            MATCH (b:BugReport)
            WHERE b.commit_ref <> ''
            MATCH (c:Commit {id: b.commit_ref})
            WHERE NOT (b)-[:CAUSED_BY]->(c)
            MERGE (b)-[:CAUSED_BY]->(c)
        """)
        merged = result.consume().counters.relationships_created
        log.info("  BugReport -[CAUSED_BY]-> Commit: %d edge(s) created", merged)

        # BugReport references a Ticket  → already created inline; ensure symmetric link
        result = session.run("""
            MATCH (b:BugReport)
            WHERE b.ticket_ref <> ''
            MATCH (t:Ticket {id: b.ticket_ref})
            WHERE NOT (b)-[:REFERENCES]->(t)
            MERGE (b)-[:REFERENCES]->(t)
        """)
        merged = result.consume().counters.relationships_created
        log.info("  BugReport -[REFERENCES]-> Ticket: %d edge(s) created", merged)

        # Decision governs a Ticket  → Decision -[GOVERNED_BY]-> Ticket
        result = session.run("""
            MATCH (d:Decision)
            WHERE d.related_ticket <> ''
            MATCH (t:Ticket {id: d.related_ticket})
            WHERE NOT (d)-[:GOVERNED_BY]->(t)
            MERGE (d)-[:GOVERNED_BY]->(t)
        """)
        merged = result.consume().counters.relationships_created
        log.info("  Decision -[GOVERNED_BY]-> Ticket: %d edge(s) created", merged)

        # Decision shaped a Commit  → Decision -[SHAPES]-> Commit
        result = session.run("""
            MATCH (d:Decision)
            WHERE d.related_commit <> ''
            MATCH (c:Commit {id: d.related_commit})
            WHERE NOT (d)-[:SHAPES]->(c)
            MERGE (d)-[:SHAPES]->(c)
        """)
        merged = result.consume().counters.relationships_created
        log.info("  Decision -[SHAPES]-> Commit: %d edge(s) created", merged)

        # ADR governs Decisions (link by project/title keyword overlap — best effort)
        result = session.run("""
            MATCH (a:ADR)
            WHERE a.repo IS NOT NULL
            CALL (a) {
                MATCH (d:Decision)
                WHERE d.title IS NOT NULL
                  AND toLower(d.title) CONTAINS toLower(split(a.title, ':')[0])
                  AND NOT (a)-[:GOVERNED_BY]->(d)
                MERGE (a)-[:GOVERNED_BY]->(d)
            }
        """)
        merged = result.consume().counters.relationships_created
        log.info("  ADR -[GOVERNED_BY]-> Decision: %d edge(s) created (keyword match)", merged)

        # ADR -[GOVERNED_BY]-> Decision above only ever fires if Decision nodes
        # exist, and Decision nodes only come from ingest_decisions_from_file(),
        # which run_ingest.py's automatic pipeline never calls (it's a manual,
        # --input-file-only step). So for every repo ingested through
        # /ingest/repo, that edge is permanently zero rows and ADR nodes are
        # disconnected islands. Give ADR a second, automatic path into the
        # graph: link it to commits in the same repo whose message mentions
        # the ADR's title (same best-effort keyword approach as above).
        result = session.run("""
            MATCH (a:ADR)
            CALL (a) {
                MATCH (c:Commit)
                WHERE a.repo = c.repo
                  AND toLower(c.message) CONTAINS toLower(split(a.title, ':')[0])
                  AND NOT (a)-[:REFERENCES]->(c)
                MERGE (a)-[:REFERENCES]->(c)
            }
        """)
        merged = result.consume().counters.relationships_created
        log.info("  ADR -[REFERENCES]-> Commit: %d edge(s) created (keyword match)", merged)

        # SlackMessage discusses a Ticket  → SlackMessage -[DISCUSSED_IN]-> Ticket
        # Slack has no connection in this deployment (no SLACK_BOT_TOKEN),
        # so SlackMessage will have 0 rows and this MATCH is a no-op — but
        # it's kept correlated/indexable rather than an open cartesian join
        # so it's cheap now and correct the moment Slack is connected.
        result = session.run("""
            MATCH (t:Ticket)
            CALL (t) {
                MATCH (m:SlackMessage)
                WHERE m.text =~ ('(?i).*' + t.id + '.*')
                  AND NOT (m)-[:DISCUSSED_IN]->(t)
                MERGE (m)-[:DISCUSSED_IN]->(t)
            }
        """)
        merged = result.consume().counters.relationships_created
        log.info("  SlackMessage -[DISCUSSED_IN]-> Ticket: %d edge(s) created", merged)

        # SlackMessage discusses a Commit (SHA mention)
        result = session.run("""
            MATCH (c:Commit)
            CALL (c) {
                MATCH (m:SlackMessage)
                WHERE m.text =~ ('(?i).*' + left(c.id, 7) + '.*')
                  AND NOT (m)-[:DISCUSSED_IN]->(c)
                MERGE (m)-[:DISCUSSED_IN]->(c)
            }
        """)
        merged = result.consume().counters.relationships_created
        log.info("  SlackMessage -[DISCUSSED_IN]-> Commit: %d edge(s) created", merged)

    log.info("Edge building complete.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GitMind Neo4j ETL")
    parser.add_argument("--step", default="all",
        choices=["all", "constraints", "commits", "tickets", "messages",
                 "adrs", "decisions_synth", "bugs", "decisions", "edges", "reset"])
    parser.add_argument("--repo",     help="owner/repo for commits and ADRs")
    parser.add_argument("--branch",   default="main")
    parser.add_argument("--channel",  help="Slack channel ID")
    parser.add_argument("--adr-path", default="docs/adr")
    parser.add_argument("--project",  help="Jira project key")
    parser.add_argument("--input",    help="JSON file for bugs/decisions step")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="GitHub commit pages to fetch (10 pages * 100/page = 1000 commits)")
    args = parser.parse_args(argv)

    if args.step == "reset":
        try:
            reset_all()
        except Exception as exc:
            log.error("Cannot reset Neo4j: %s", exc)
            return 1
        log.info("ETL done.")
        return 0

    try:
        driver = _get_driver()
    except Exception as exc:
        log.error("Cannot connect to Neo4j: %s", exc)
        return 1

    try:
        step = args.step
        if step in ("all", "constraints"):
            run_constraints(driver)
        if step in ("all", "commits"):
            if not args.repo:
                log.error("--repo required for commits step")
                return 1
            ingest_commits(driver, args.repo, branch=args.branch, max_pages=args.max_pages)
        if step in ("all", "tickets"):
            ingest_tickets(driver, repo=args.repo or None, project=args.project)
        if step in ("all", "messages"):
            if not args.channel and not args.repo:
                log.error("--channel or --repo (with slack/messages.json) required for messages step")
                return 1
            ingest_messages(driver, args.channel, repo=args.repo or None)
        if step in ("all", "adrs"):
            if not args.repo:
                log.error("--repo required for adrs step")
                return 1
            ingest_adrs(driver, args.repo, adr_path=args.adr_path)
            synthesize_decisions_from_adrs(driver, args.repo)
        if step == "decisions_synth":
            if not args.repo:
                log.error("--repo required for decisions_synth step")
                return 1
            synthesize_decisions_from_adrs(driver, args.repo)
        if step == "bugs":
            if not args.input:
                log.error("--input required for bugs step")
                return 1
            ingest_bugs_from_file(driver, args.input)
        if step == "decisions":
            if not args.input:
                log.error("--input required for decisions step")
                return 1
            ingest_decisions_from_file(driver, args.input)
        if step in ("all", "edges"):
            build_edges(driver)
    finally:
        driver.close()

    log.info("ETL done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
