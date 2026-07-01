from __future__ import annotations

import logging
import re
import uuid
from enum import Enum
from typing import Any
from datetime import datetime

from fastapi import FastAPI, APIRouter, HTTPException, Request, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.config import GitMindConfig, GitMindConfigError, Neo4jConfig, SnowflakeConfig, LLMConfig
from backend.snowflake_client import SnowflakeClient
from backend.graph.causal_graph import GraphPathNode, Neo4jCausalGraph
from backend.agent.gitmind_agent import (
    GitMindRuntime,
    set_runtime,
    debug as agent_debug,
)
from backend.harsh_engine.core.regression_guard import check_chain_for_regressions
from ingest.snowflake_etl import validate_schema
import os as _os

log = logging.getLogger("gitmind.api")

# --- Models ---

class QueryIntent(str, Enum):
    causal = "causal"
    factual = "factual"


class GitMindQuery(BaseModel):
    query: str = Field(..., min_length=3)
    entity_id: str = ""
    function_name: str = ""
    ticket_id: str = ""
    adr_ref: str = ""
    table: str = ""
    limit: int = Field(default=25, ge=1, le=100)


class GitMindQueryResponse(BaseModel):
    intent: QueryIntent
    route: list[str]
    causal_chain: list[dict[str, Any]] = Field(default_factory=list)
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    answer: str
    regression_check: dict[str, Any] | None = None


class WaitlistEntry(BaseModel):
    email: str = Field(..., pattern=r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
    ts: str = Field(default_factory=lambda: datetime.now().isoformat())


class WaitlistResponse(BaseModel):
    status: str
    message: str
    email: str


class GitHubFetchRequest(BaseModel):
    url: str = Field(..., min_length=10)
    per_branch_limit: int = Field(default=20, ge=1, le=200)
    include_file_previews: bool = False


class RepoIngestRequest(BaseModel):
    """Body for POST /ingest/repo — any public GitHub repo, on demand.

    jira_project / adr_path are optional per-request overrides. Without
    them, ingestion falls back to JIRA_DEFAULT_PROJECT / GITMIND_ADR_PATH
    from the environment — which only matches the ONE repo a deployment
    happened to be set up for. Passing them explicitly lets a single
    deployment ingest a different repo, with a different Jira project and
    a different ADR location, on every call.
    """
    repo: str = Field(..., min_length=3, description="owner/repo, or a full GitHub URL")
    branch: str = Field(default="main")
    jira_project: str = Field(default="", description="Jira project key for this repo, e.g. 'PROJ'. Falls back to JIRA_DEFAULT_PROJECT if omitted.")
    adr_path: str = Field(default="", description="Path to this repo's ADR folder, e.g. 'docs/adr'. Falls back to GITMIND_ADR_PATH (default 'docs/adr') if omitted.")


class InsightOverviewRequest(BaseModel):
    """Body for POST /insight/overview.

    motive_prompt is free text on what the project is supposed to do --
    optional. When given, the report includes an alignment_note comparing
    observed activity (tickets/ADRs/commits) against the stated purpose.
    """
    repo: str = Field(..., min_length=3, description="owner/repo, or a full GitHub URL")
    motive_prompt: str = Field(default="", max_length=2000)


class InsightOverviewResponse(BaseModel):
    report_id: str
    pdf_url: str


class SnowflakeDetails:
    """Whitelisted detail queries against the Snowflake ledger."""

    _TABLES = {
        "COMMITS": ("commit_id", "timestamp"),
        "TICKETS": ("ticket_id", "created_at"),
        "MESSAGES": ("message_id", "timestamp"),
        "ADR_RECORDS": ("adr_id", "created_date"),
        "BUG_REPORTS": ("bug_id", "reported_at"),
        "DECISIONS": ("decision_id", "timestamp"),
    }

    _SOURCE_TO_TABLE = {
        "Commit": "COMMITS",
        "COMMIT": "COMMITS",
        "JiraTicket": "TICKETS",
        "Ticket": "TICKETS",
        "TICKET": "TICKETS",
        "SlackMessage": "MESSAGES",
        "Message": "MESSAGES",
        "MESSAGE": "MESSAGES",
        "ADR": "ADR_RECORDS",
        "BUG_REPORT": "BUG_REPORTS",
        "Decision": "DECISIONS",
    }

    def __init__(self, client) -> None:
        self._client = client

    def fetch_for_chain(self, chain: list[GraphPathNode]) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for node in chain:
            table = self._SOURCE_TO_TABLE.get(node.source_type) or self._SOURCE_TO_TABLE.get(node.label)
            if not table or not node.source_id:
                continue
            rows = self.fetch_by_id(table, node.source_id, limit=1)
            evidence.extend(rows)
        return evidence

    def fetch_by_id(self, table: str, source_id: str, *, limit: int = 25) -> list[dict[str, Any]]:
        table = table.upper()
        if table not in self._TABLES:
            raise ValueError(f"Unsupported Snowflake table: {table}")
        id_column, _ = self._TABLES[table]
        sql = f"SELECT * FROM {table} WHERE {id_column} = %s LIMIT {limit}"
        return self._client.execute(sql, (source_id,))

    def fetch_summary(self, table: str, *, limit: int = 25) -> list[dict[str, Any]]:
        table = table.upper()
        if table not in self._TABLES:
            raise ValueError(f"Unsupported Snowflake table: {table}")
        _, order_column = self._TABLES[table]
        sql = f"SELECT * FROM {table} ORDER BY {order_column} DESC LIMIT {limit}"
        return self._client.execute(sql)

    def count(self, table: str) -> list[dict[str, Any]]:
        table = table.upper()
        if table not in self._TABLES:
            raise ValueError(f"Unsupported Snowflake table: {table}")
        return self._client.execute(f"SELECT COUNT(*) AS COUNT FROM {table}")


# --- Constants & Helpers ---

CAUSAL_WORDS = ("why", "how", "trace", "influence", "influenced", "caused", "root cause")
FACT_WORDS = ("list", "count", "summarize the text of", "summarise the text of", "show logs", "show")

# owner/repo, optionally as a full GitHub URL — used to validate /ingest/repo
# input before it ever reaches GitHub/Snowflake/Neo4j calls.
#
# GitHub username rules: alphanumeric + single hyphens, no leading/trailing/
# doubled hyphen, max 39 chars. Repo name rules: alnum, ., _, - (GitHub
# itself is looser than this in practice, but this is a validation
# boundary, not a mirror of GitHub's own rules — tighter is safer here).
_OWNER_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")
_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,100}$")
_REPO_SLUG_RE = re.compile(r"^([A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38})/([A-Za-z0-9._-]{1,100})$")

# Accepted GitHub hosts — exact match only, checked against urlparse().netloc,
# never as a substring of the raw string. "evilgithub.com" or
# "github.com.attacker.net" must never satisfy this.
_GITHUB_HOSTS = {"github.com", "www.github.com"}


def normalize_repo_slug(raw: str) -> str:
    """Accepts 'owner/repo' or a genuine https://github.com/owner/repo URL,
    returns 'owner/repo'. Rejects (with a logged warning) anything else,
    including strings that merely *contain* 'github.com' as a substring —
    e.g. 'evilgithub.com/owner/repo' or 'github.com.attacker.net/owner/repo'
    used to look like a match under a naive substring/regex .search().
    """
    from urllib.parse import urlparse

    original = raw
    raw = raw.strip()

    looks_like_url = "://" in raw or raw.lower().startswith("github.com") or raw.lower().startswith("www.github.com")

    if looks_like_url:
        # Force a scheme so urlparse reliably splits netloc from path even
        # for inputs like "github.com/owner/repo" with no "https://".
        parse_target = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(parse_target)
        host = parsed.netloc.lower().split("@")[-1].split(":")[0]  # strip userinfo@ and :port

        if host not in _GITHUB_HOSTS:
            log.warning("Rejected /ingest/repo input — host %r is not github.com: %r", host, original)
            raise ValueError(
                f"Expected a github.com repository URL or 'owner/repo', got a URL with host {host!r}: {original!r}"
            )

        segments = [s for s in parsed.path.split("/") if s]
        if len(segments) < 2:
            log.warning("Rejected /ingest/repo input — no owner/repo path on github.com URL: %r", original)
            raise ValueError(f"Could not find owner/repo in GitHub URL path: {original!r}")

        owner, repo_name = segments[0], segments[1]
        repo_name = re.sub(r"\.git$", "", repo_name)

        if len(segments) > 2:
            log.warning(
                "Ingest input had extra path segments beyond owner/repo — using only %s/%s, ignoring %r",
                owner, repo_name, segments[2:],
            )

        if not _OWNER_RE.match(owner) or not _REPO_NAME_RE.match(repo_name):
            log.warning("Rejected /ingest/repo input — owner/repo failed identifier rules: %r", original)
            raise ValueError(f"Owner or repo name contains disallowed characters: {owner!r}/{repo_name!r}")

        return f"{owner}/{repo_name}"

    if _REPO_SLUG_RE.match(raw):
        return raw

    log.warning("Rejected /ingest/repo input — not a github.com URL or valid owner/repo slug: %r", original)
    raise ValueError(f"Could not parse a GitHub 'owner/repo' from: {original!r}")


def classify_intent(query: str) -> QueryIntent:
    lowered = query.lower()
    if any(word in lowered for word in CAUSAL_WORDS):
        return QueryIntent.causal
    if any(word in lowered for word in FACT_WORDS):
        return QueryIntent.factual
    return QueryIntent.factual


# "ADR-001", "ADR 1", "adr-12" — matched BEFORE the ticket regex below,
# because "ADR-001" also syntactically matches a Jira-ticket-key shape
# ([A-Z][A-Z0-9]+-\d+). Without this check running first, every ADR
# reference in a query was being misread as a ticket id, then sent through
# an exact Cypher {id: $entity_id} match against the literal string
# "ADR-001" -- which can never hit, since the real Neo4j node id is the
# full namespaced path ingest_adrs() builds (e.g.
# "owner/repo/docs/adr/ADR-001-neo4j.md"). That's the root cause of the
# "ADR trace not found" fallback.
_ADR_RE = re.compile(r"\bADR[-\s]?(\d{1,4})\b", re.IGNORECASE)


def enrich_identifiers(payload: GitMindQuery) -> GitMindQuery:
    data = payload.model_copy()
    text = payload.query

    if not data.adr_ref:
        adr = _ADR_RE.search(text)
        if adr:
            data.adr_ref = adr.group(1)

    if not data.ticket_id and not data.adr_ref:
        ticket = re.search(r"\b[A-Z][A-Z0-9]+-\d+\b", text)
        if ticket:
            data.ticket_id = ticket.group(0)

    if not data.entity_id and data.ticket_id:
        # Ticket nodes are MERGEd in neo4j_etl.py with `id` = the raw Jira
        # key (e.g. "PLAT-123"), not a "ticket:"-prefixed value, so the
        # entity_id used for the `{id: $entity_id}` Cypher match must match
        # that exactly or the trace silently returns nothing.
        data.entity_id = data.ticket_id

    if not data.entity_id and not data.adr_ref:
        commit = re.search(r"\b[0-9a-f]{7,40}\b", text, flags=re.IGNORECASE)
        if commit:
            data.entity_id = commit.group(0)

    if not data.function_name:
        fn = re.search(r"\bfunction\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b", text, flags=re.IGNORECASE)
        if fn:
            data.function_name = fn.group(1)

    return data


def infer_table(query: str, explicit_table: str = "") -> str:
    if explicit_table:
        return explicit_table.upper()

    lowered = query.lower()
    if "commit" in lowered:
        return "COMMITS"
    if "ticket" in lowered or "jira" in lowered:
        return "TICKETS"
    if "message" in lowered or "slack" in lowered:
        return "MESSAGES"
    if "adr" in lowered or "architecture decision" in lowered:
        return "ADR_RECORDS"
    if "bug" in lowered or "error" in lowered or "logs" in lowered:
        return "BUG_REPORTS"
    if "decision" in lowered:
        return "DECISIONS"
    # BUG FIX: this used to fall back to "DECISIONS". DECISIONS is only
    # populated from ADR files / a manually-fed JSON decision log (see
    # ingest/snowflake_etl.py Step 6) -- it is NOT populated by the normal
    # GitHub commit/ticket/message ingestion. Every other repo-level query
    # (e.g. the frontend's auto-generated "analyze repository <url>" probe,
    # which contains none of the keywords above) was silently routed to
    # this near-always-empty table, producing "Returned 0 row(s) from
    # DECISIONS" even when COMMITS/TICKETS/MESSAGES were fully ingested.
    # COMMITS is the one table guaranteed to have rows for any ingested
    # repo, so it's the correct default for an unrecognised query.
    return "COMMITS"


def summarize_chain(chain: list[GraphPathNode], evidence: list[dict[str, Any]]) -> str:
    if not chain:
        return "No causal path was found in Neo4j for the supplied identifier."

    root = chain[-1]
    start = chain[0]
    return (
        f"Neo4j traced {start.node_id} through {len(chain)} node(s); "
        f"the deepest observed causal node is {root.node_id}. "
        f"Snowflake returned {len(evidence)} raw evidence row(s)."
    )


# --- Slack Handlers ---

def verify_slack_signature(body: bytes, timestamp: str, signature: str, secret: str) -> bool:
    import time
    import hmac
    import hashlib
    if not secret:
        return True
    try:
        if abs(time.time() - float(timestamp)) > 300:
            return False
        sig_basestring = f"v0:{timestamp}:".encode() + body
        computed_sig = "v0=" + hmac.new(
            secret.encode(),
            sig_basestring,
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(computed_sig, signature)
    except Exception:
        return False


def process_slack_event(event: dict, slack_cfg: Any) -> None:
    import logging
    import re
    from backend.agent.gitmind_agent import GitMindRuntime, debug, GitMindConfigError
    from slack_sdk import WebClient

    log = logging.getLogger("gitmind.api.slack")
    event_type = event.get("type")
    if event_type not in ("app_mention", "message"):
        return

    if event.get("bot_id") or event.get("subtype") == "bot_message":
        return

    channel = event.get("channel")
    text = event.get("text", "")
    ts = event.get("ts")
    thread_ts = event.get("thread_ts") or ts

    cleaned_text = re.sub(r"<@[A-Z0-9]+>", "", text).strip()
    if not cleaned_text:
        return

    client = WebClient(token=slack_cfg.bot_token)

    try:
        working_msg = client.chat_postMessage(
            channel=channel,
            text="🔍 *GitMind Causal Agent* is analyzing the incident... Please wait.",
            thread_ts=thread_ts
        )
        working_ts = working_msg.get("ts")
    except Exception as exc:
        log.error("Failed to post message to Slack: %s", exc)
        return

    try:
        try:
            runtime = GitMindRuntime.from_env()
        except GitMindConfigError:
            log.warning("Live configuration unavailable. Running Slack bot in DEMO mode.")
            runtime = GitMindRuntime.demo()

        report = debug(cleaned_text, runtime=runtime)
        runtime.close()

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 GitMind Causal Debugging Report"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Root Cause Identified:*\n{report.get('root_cause', 'Unknown')}"
                }
            }
        ]

        evidence = report.get("evidence_chain", [])
        if evidence:
            evidence_text = "\n".join([f"• {item}" for item in evidence])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Evidence Chain:*\n{evidence_text}"
                }
            })

        patch = report.get("patch", "")
        if patch:
            patch_preview = patch[:1500] + "\n..." if len(patch) > 1500 else patch
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested Patch:*\n```diff\n{patch_preview}\n```"
                }
            })
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Regression Safe: *{'Yes' if report.get('regression_safe') else 'No'}*"
                    }
                ]
            })

        client.chat_update(
            channel=channel,
            ts=working_ts,
            text="Analysis completed.",
            blocks=blocks
        )

    except Exception as exc:
        log.error("Failed to run GitMind debug agent: %s", exc)
        try:
            client.chat_update(
                channel=channel,
                ts=working_ts,
                text=f"❌ *Analysis Failed:* {exc}"
            )
        except Exception:
            pass


# --- Router & Endpoints ---

router = APIRouter()


@router.get("/health")
def health(request: Request) -> dict[str, Any]:
    neo4j_ready = bool(getattr(request.app.state, "neo4j_graph", None))
    snowflake_ready = bool(getattr(request.app.state, "snowflake_details", None))
    return {
        "status": "ok",
        "runtime_ready": neo4j_ready and snowflake_ready,
        "neo4j_ready": neo4j_ready,
        "snowflake_ready": snowflake_ready,
    }


@router.post("/waitlist", response_model=WaitlistResponse)
def add_to_waitlist(payload: WaitlistEntry) -> WaitlistResponse:
    """Add an email to the early access waitlist."""
    log.info("Waitlist signup: %s at %s", payload.email, payload.ts)
    return WaitlistResponse(
        status="success",
        message="Thank you for your interest! You've been added to the waitlist.",
        email=payload.email,
    )


_ingest_state: dict[str, Any] = {"running": False, "last_result": None}


def _run_ingest_job() -> None:
    """Runs in a BackgroundTask on the free web service — no paid Shell or
    One-Off Jobs needed. Same env vars this service already has.
    """
    import io
    import contextlib

    _ingest_state["running"] = True
    buf = io.StringIO()
    try:
        from ingest.run_ingest import main as ingest_main

        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        buf_handler = logging.StreamHandler(buf)
        buf_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()  # -> stdout, visible in Render Logs tab
        console_handler.setFormatter(formatter)
        ingest_logger = logging.getLogger("gitmind.ingest")
        ingest_logger.addHandler(buf_handler)
        ingest_logger.addHandler(console_handler)
        ingest_logger.setLevel(logging.INFO)
        try:
            rc = ingest_main()
        finally:
            ingest_logger.removeHandler(buf_handler)
            ingest_logger.removeHandler(console_handler)
        _ingest_state["last_result"] = {"exit_code": rc, "log": buf.getvalue()[-8000:]}
    except Exception as exc:  # noqa: BLE001
        log.error("Ingest run failed: %s", exc, exc_info=True)
        _ingest_state["last_result"] = {"exit_code": 1, "log": buf.getvalue()[-8000:] + f"\nFATAL: {exc}"}
    finally:
        _ingest_state["running"] = False


@router.post("/admin/ingest")
def trigger_ingest(background_tasks: BackgroundTasks, x_admin_token: str = Header(None)) -> dict[str, Any]:
    """Trigger ingest/run_ingest.py over plain HTTP — workaround for not
    having paid Render Shell/Jobs access. Protect with ADMIN_TOKEN env var.
    """
    expected = _os.environ.get("ADMIN_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN not configured on this service")
    if x_admin_token != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Admin-Token header")
    if _ingest_state["running"]:
        return {"status": "already_running"}
    background_tasks.add_task(_run_ingest_job)
    return {"status": "started"}


@router.get("/admin/ingest/status")
def ingest_status(x_admin_token: str = Header(None)) -> dict[str, Any]:
    expected = _os.environ.get("ADMIN_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Admin-Token header")
    return {"running": _ingest_state["running"], "last_result": _ingest_state["last_result"]}


# ---------------------------------------------------------------------------
# On-demand ingest for ANY public repo, called by the frontend itself
# (no admin token — this is a user-facing action, not an admin op).
#
# Every call wipes Snowflake + Neo4j first (TRUNCATE / DETACH DELETE — see
# reset_all() in both ETL modules) before pulling the new repo. This is the
# "wipe old data" behaviour: since the only thing that previously held
# state was whatever repo got ingested into these two stores, clearing them
# right before a new repo's ingest guarantees the graph never mixes data
# from two different repos, and a stale repo's data never lingers after the
# frontend is pointed at something new.
# ---------------------------------------------------------------------------

_repo_ingest_state: dict[str, Any] = {
    "running": False,
    "current_repo": None,
    "last_result": None,
}


def _run_repo_ingest_job(
    repo: str,
    branch: str,
    jira_project: str = "",
    adr_path: str = "",
) -> None:
    import io

    _repo_ingest_state["running"] = True
    _repo_ingest_state["current_repo"] = repo
    buf = io.StringIO()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    buf_handler = logging.StreamHandler(buf)
    buf_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    loggers = [logging.getLogger("gitmind.ingest"),
               logging.getLogger("gitmind.etl.snowflake"),
               logging.getLogger("gitmind.etl.neo4j")]
    for lg in loggers:
        lg.addHandler(buf_handler)
        lg.addHandler(console_handler)
        lg.setLevel(logging.INFO)

    try:
        from ingest import snowflake_etl, neo4j_etl

        log.info("Wiping previous repo's data before ingesting %s...", repo)
        try:
            snowflake_etl.reset_all()
        except Exception as exc:
            log.warning("Snowflake reset failed (continuing): %s", exc)
        try:
            neo4j_etl.reset_all()
        except Exception as exc:
            log.warning("Neo4j reset failed (continuing): %s", exc)

        # Call the ingest pipeline directly with THIS request's values —
        # repo, branch, Jira project, and ADR path are all per-call here,
        # not read back off fixed env vars. That's what makes this work for
        # any public repo: a repo whose Jira project or ADR folder differs
        # from whatever the deployment's env vars happen to be set to still
        # gets ingested correctly, because those values are explicit
        # request fields, not assumed constants.
        from ingest.run_ingest import run as ingest_run

        # Only pass a channel through if Slack is actually connected (a bot
        # token is set). A channel name alone isn't a connection — without
        # a token the messages step still fires and dies on `missing_scope`,
        # which is noise in the log and wasted API calls, not a real ETL
        # step. No token => Slack is treated as not-connected => the step
        # is skipped entirely (see run_ingest.run()'s `if channel:` guard).
        slack_bot_token = _os.environ.get("SLACK_BOT_TOKEN", "")
        channel = (
            (_os.environ.get("GITMIND_INGEST_CHANNEL") or _first_csv_env("SLACK_DEFAULT_CHANNELS"))
            if slack_bot_token else ""
        )
        if not slack_bot_token:
            log.info("SLACK_BOT_TOKEN not set - Slack has no connection, skipping messages step for this ingest.")

        # Jira is resolved and passed independently of Slack's channel gate -
        # ticket ingestion never depends on whether Slack is connected.
        rc = ingest_run(
            repo=repo,
            branch=branch,
            channel=channel,
            project=jira_project or _os.environ.get("JIRA_DEFAULT_PROJECT", ""),
            adr_path=adr_path or _os.environ.get("GITMIND_ADR_PATH", "docs/adr"),
            max_pages=_os.environ.get("GITMIND_MAX_PAGES", "2"),
        )

        _repo_ingest_state["last_result"] = {
            "repo": repo, "exit_code": rc, "log": buf.getvalue()[-8000:]
        }
    except Exception as exc:  # noqa: BLE001
        log.error("Repo ingest failed for %s: %s", repo, exc, exc_info=True)
        _repo_ingest_state["last_result"] = {
            "repo": repo, "exit_code": 1, "log": buf.getvalue()[-8000:] + f"\nFATAL: {exc}"
        }
    finally:
        for lg in loggers:
            lg.removeHandler(buf_handler)
            lg.removeHandler(console_handler)
        _repo_ingest_state["running"] = False


def _first_csv_env(csv_env: str) -> str:
    raw = _os.environ.get(csv_env, "")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts[0] if parts else ""


@router.post("/ingest/repo")
def ingest_repo(payload: RepoIngestRequest, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Ingest any public GitHub repo on demand. Wipes prior repo's data first.

    repo is the only required field. jira_project / adr_path are optional —
    set them when the repo being ingested doesn't use the deployment's
    default Jira project or ADR folder.
    """
    try:
        slug = normalize_repo_slug(payload.repo)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if _repo_ingest_state["running"]:
        return {"status": "already_running", "current_repo": _repo_ingest_state["current_repo"]}

    background_tasks.add_task(
        _run_repo_ingest_job, slug, payload.branch, payload.jira_project, payload.adr_path
    )
    return {"status": "started", "repo": slug, "branch": payload.branch}


@router.get("/ingest/repo/status")
def ingest_repo_status() -> dict[str, Any]:
    return {
        "running": _repo_ingest_state["running"],
        "current_repo": _repo_ingest_state["current_repo"],
        "last_result": _repo_ingest_state["last_result"],
    }


@router.post("/insight/overview", response_model=InsightOverviewResponse)
def insight_overview(payload: InsightOverviewRequest, request: Request) -> InsightOverviewResponse:
    """Generate a Project Overview Report PDF: repo stats (already-ingested
    Snowflake data) + one live GitHub branch lookup + one bounded LLM
    prompt. Synchronous -- no chunking, no file-content analysis, no
    BackgroundTasks; this is meant to complete in a few seconds.
    """
    from backend.insight_report import (
        gather_repo_stats, fetch_branch_info, generate_overview, render_pdf, register_report,
    )

    try:
        slug = normalize_repo_slug(payload.repo)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    snowflake_client: SnowflakeClient | None = getattr(request.app.state, "snowflake_client", None)
    if snowflake_client is None:
        raise HTTPException(
            status_code=503,
            detail="Snowflake is not connected on this backend. Ingest a repo first via /ingest/repo, "
                   "then retry -- overview stats are read from already-ingested Snowflake data.",
        )

    try:
        stats = gather_repo_stats(snowflake_client, slug)
    except Exception as exc:
        log.error("Overview stats query failed for %s: %s", slug, exc)
        raise HTTPException(status_code=503, detail=f"Snowflake is unreachable or the stats query failed: {exc}") from exc

    if stats.commit_count == 0 and stats.ticket_count == 0 and stats.adr_count == 0:
        raise HTTPException(
            status_code=422,
            detail=f"No ingested data found for {slug}. Run POST /ingest/repo for this repo first.",
        )

    stats.branch_count, stats.branch_names = fetch_branch_info(slug)

    try:
        llm_result = generate_overview(stats, slug, payload.motive_prompt)
        pdf_path = render_pdf(slug, stats, llm_result)
    except Exception as exc:
        log.error("Overview report generation failed for %s: %s", slug, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate overview report: {exc}") from exc

    report_id = str(uuid.uuid4())
    register_report(report_id, pdf_path, slug)

    return InsightOverviewResponse(report_id=report_id, pdf_url=f"/insight/overview/{report_id}/download")


@router.get("/insight/overview/{report_id}/download")
def insight_overview_download(report_id: str):
    from fastapi.responses import FileResponse
    from backend.insight_report import lookup_report

    entry = lookup_report(report_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail="Report not found -- it may not exist, or this backend instance restarted since it "
                   "was generated (PDF storage is ephemeral on Render). Generate a new report.",
        )

    repo_part = re.sub(r"[^A-Za-z0-9_.-]", "_", entry["repo"])
    return FileResponse(
        entry["path"],
        media_type="application/pdf",
        filename=f"gitmind_overview_{repo_part}.pdf",
    )


@router.post("/query", response_model=GitMindQueryResponse)
def query_gitmind(payload: GitMindQuery, request: Request) -> GitMindQueryResponse:
    graph: Neo4jCausalGraph | None = getattr(request.app.state, "neo4j_graph", None)
    snowflake: SnowflakeDetails | None = getattr(request.app.state, "snowflake_details", None)

    if graph is None and snowflake is None:
        # Full demo mode — let agent run with placeholders, skip DB calls
        try:
            agent_result = agent_debug(
                payload.query,
                entity_id=payload.entity_id,
                function_name=payload.function_name,
                ticket_id=payload.ticket_id,
                adr_ref=payload.adr_ref,
            )
            return GitMindQueryResponse(
                intent=classify_intent(payload.query),
                route=["GitMind_Agent_Demo"],
                causal_chain=[],
                evidence=[],
                answer=agent_result.get("root_cause", "Demo mode: no live data sources connected."),
            )
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"GitMind runtime is not connected. Configure Neo4j and Snowflake environment variables. Agent error: {exc}")

    intent = classify_intent(payload.query)
    enriched = enrich_identifiers(payload)

    # A query that names a specific node (ADR-001, RRW-011, a commit sha, a
    # function name) is an explicit signal to run the graph trace, even if
    # classify_intent()'s keyword heuristic guessed "factual" because the
    # sentence didn't contain "why"/"how"/"caused" etc. (e.g. "What is
    # ADR-001 about?" has no causal keyword but clearly wants that ADR's
    # place in the graph, not a blind table dump). Without this, any named
    # entity falls through to the Snowflake-only factual branch below,
    # which never calls graph.trace() and always returns an empty
    # causal_chain -- which is why ADR/ticket/commit nodes never appeared
    # in the frontend's graph view for anything but "why"-phrased queries.
    has_named_entity = bool(
        enriched.entity_id or enriched.function_name or enriched.ticket_id or enriched.adr_ref
    )

    if intent == QueryIntent.causal or has_named_entity:
        if not (enriched.entity_id or enriched.function_name or enriched.ticket_id or enriched.adr_ref):
            raise HTTPException(
                status_code=422,
                detail="Clarify the specific Function name, Ticket ID, ADR reference, or graph node ID before running causal traversal.",
            )

        if graph is None:
            # Partial-connector state: snowflake connected but neo4j didn't
            # (or never finished initialising). Without this guard the code
            # below would call .trace() on None and surface a confusing
            # "'NoneType' object has no attribute 'trace'" message instead
            # of a clear, actionable one.
            raise HTTPException(
                status_code=503,
                detail="Neo4j is not connected on this backend (NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD missing, or the connection failed at startup). Causal traversal is unavailable until this is fixed.",
            )

        # Primary path: run the full LLM agent
        try:
            agent_result = agent_debug(
                enriched.query,
                entity_id=enriched.entity_id,
                function_name=enriched.function_name,
                ticket_id=enriched.ticket_id,
                adr_ref=enriched.adr_ref,
            )
            chain = agent_result.get("causal_chain") or graph.trace(
                entity_id=enriched.entity_id,
                function_name=enriched.function_name,
                ticket_id=enriched.ticket_id,
                adr_ref=enriched.adr_ref,
            )
            evidence = snowflake.fetch_for_chain(chain) if snowflake else []
            answer = agent_result.get("root_cause") or summarize_chain(chain, evidence)
            chain_dicts = [node.__dict__ for node in chain]
            regression_check = check_chain_for_regressions(chain_dicts, enriched.query)
            return GitMindQueryResponse(
                intent=intent,
                route=["GitMind_Agent", "Neo4j_Causal_Graph", "Snowflake_Details"],
                causal_chain=chain_dicts,
                evidence=evidence,
                answer=answer,
                regression_check=regression_check,
            )
        except Exception as exc:
            log.warning("Agent failed, falling back to direct graph trace: %s", exc)
            # Fallback: direct Neo4j trace without agent. This needs its own
            # try/except -- if the *fallback itself* hits a genuine Neo4j or
            # Snowflake outage (not just an agent/LLM failure), that must
            # degrade to a clean 503 instead of an unhandled 500.
            try:
                chain = graph.trace(
                    entity_id=enriched.entity_id,
                    function_name=enriched.function_name,
                    ticket_id=enriched.ticket_id,
                    adr_ref=enriched.adr_ref,
                )
                evidence = snowflake.fetch_for_chain(chain) if snowflake else []
                chain_dicts = [node.__dict__ for node in chain]
                regression_check = check_chain_for_regressions(chain_dicts, enriched.query)
                return GitMindQueryResponse(
                    intent=intent,
                    route=["Neo4j_Causal_Graph", "Snowflake_Details"],
                    causal_chain=chain_dicts,
                    evidence=evidence,
                    answer=summarize_chain(chain, evidence),
                    regression_check=regression_check,
                )
            except Exception as fallback_exc:
                log.error("Fallback graph trace also failed: %s", fallback_exc)
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "GitMind could not complete this causal trace -- the agent failed and the "
                        f"direct Neo4j/Snowflake fallback also failed: {fallback_exc}"
                    ),
                ) from fallback_exc

    if snowflake is None:
        # Partial-connector state: neo4j connected but snowflake didn't.
        # Without this guard, the calls below would hit .fetch_by_id()/
        # .count()/.fetch_summary() on None and raise a confusing
        # NoneType AttributeError instead of a clear, actionable 503.
        raise HTTPException(
            status_code=503,
            detail="Snowflake is not connected on this backend (SNOWFLAKE_* environment variables missing, or the connection failed at startup). Evidence lookups are unavailable until this is fixed.",
        )

    table = infer_table(enriched.query, enriched.table)
    try:
        if enriched.entity_id:
            evidence = snowflake.fetch_by_id(table, enriched.entity_id, limit=enriched.limit)
        elif "count" in enriched.query.lower():
            evidence = snowflake.count(table)
        else:
            evidence = snowflake.fetch_summary(table, limit=enriched.limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        log.error("Snowflake query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail=f"Snowflake is unreachable or the query failed: {exc}",
        ) from exc

    return GitMindQueryResponse(
        intent=intent,
        route=["Snowflake_Details"],
        evidence=evidence,
        answer=f"Returned {len(evidence)} row(s) from {table}.",
    )


@router.post("/github/fetch")
def fetch_github_repo(payload: GitHubFetchRequest) -> dict[str, Any]:
    """Fetch branches, commits, and diffs for a public GitHub repo URL.

    Returns a JSON structure containing `default_branch`, `branches`, and `commits` per branch.
    Commits include per-commit file diffs (patches). Results are limited by `per_branch_limit`.
    """
    from backend.utils import github_api

    try:
        owner, repo, path, ref = github_api.parse_github_url(payload.url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid GitHub URL: {exc}")

    token = _os.getenv("GITHUB_TOKEN") or None

    try:
        repo_info = github_api.get_repo_info(owner, repo, token=token)
        default_branch = repo_info.get("default_branch")
    except Exception as exc:
        exc_str = str(exc)
        if "403" in exc_str or "rate limit" in exc_str.lower():
            raise HTTPException(status_code=429, detail="GitHub API rate limit reached. Set GITHUB_TOKEN on the backend to increase the limit to 5000 req/hr.")
        raise HTTPException(status_code=502, detail=f"Failed to fetch repo info: {exc}")

    try:
        branches = github_api.list_branches(owner, repo, token=token)
    except Exception as exc:
        exc_str = str(exc)
        if "403" in exc_str or "rate limit" in exc_str.lower():
            raise HTTPException(status_code=429, detail="GitHub API rate limit reached. Set GITHUB_TOKEN on the backend.")
        raise HTTPException(status_code=502, detail=f"Failed to list branches: {exc}")

    branch_names = [b["name"] if isinstance(b, dict) else b for b in branches]

    # Same cap as /github/all_commits below — without this, a repo with
    # many branches (some repos have 90+) makes this loop one GitHub API
    # call per branch at minimum, which at the throttled 0.75s/call gap
    # alone exceeds the frontend's 60s timeout for this endpoint before
    # GitHub rate limiting even becomes a factor. When that timeout fires,
    # the frontend sets dm_github = null and the commit-list/timeline UI
    # panels go empty -- even though the causal graph (a separate pipeline,
    # /ingest/repo + /query) may have populated correctly. Two independent
    # pipelines, one fixed here, the other previously wasn't; this brings
    # /github/fetch in line with /github/all_commits's existing fix.
    pinned_branch = _os.getenv("GITMIND_INGEST_BRANCH", "")
    if pinned_branch and pinned_branch in branch_names:
        branch_names = [pinned_branch]

    result: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "default_branch": default_branch,
        "branches": branch_names,
        "commits": {},
    }

    # For each branch, fetch ALL commits (paginated) and their diffs
    for b_name in branch_names:
        try:
            if payload.include_file_previews:
                commits = github_api.list_all_commits(owner, repo, b_name, token=token)
            else:
                commits = github_api.list_commits(owner, repo, b_name, per_page=payload.per_branch_limit, token=token)
        except Exception as exc:
            exc_str = str(exc)
            if "403" in exc_str or "rate limit" in exc_str.lower():
                # Previously this raised HTTPException(429) immediately,
                # which discarded every branch already fetched before this
                # one in the loop -- one rate-limited branch out of e.g. 92
                # nuked the entire response, including branches 1 through
                # (n-1) that had already succeeded. Now: log it, return []
                # for THIS branch only, and let every other branch's data
                # (already gathered, or gathered after this one) survive.
                log.warning("GitHub rate limit hit fetching branch %s for %s/%s -- returning [] for this branch only", b_name, owner, repo)
                commits = []
            else:
                commits = []

        # Return lightweight commit list only — no per-commit detail calls.
        # Detail (files/patches) fetched on demand via GET /github/commits/{sha}.
        result["commits"][b_name] = [
            {
                "sha": c.get("sha", ""),
                "message": (c.get("commit") or {}).get("message", ""),
                "author": ((c.get("commit") or {}).get("author") or {}).get("name", ""),
                "date": ((c.get("commit") or {}).get("author") or {}).get("date", ""),
                "url": c.get("html_url", ""),
            }
            for c in commits
        ]

    return result


@router.post("/github/list_files")
def github_list_files(payload: GitHubFetchRequest) -> dict[str, Any]:
    from backend.utils import github_api

    try:
        owner, repo, path, ref = github_api.parse_github_url(payload.url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid GitHub URL: {exc}")

    token = _os.getenv("GITHUB_TOKEN") or None
    try:
        files = github_api.list_repo_files(owner, repo, ref=ref or None, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to list repo files: {exc}")

    return {"owner": owner, "repo": repo, "path": path, "ref": ref, "files": files}


class GitHubFileRequest(BaseModel):
    url: str = Field(..., min_length=10)
    path: str = Field(..., min_length=1)
    ref: str | None = None


@router.post("/github/file_content")
def github_file_content(payload: GitHubFileRequest) -> dict[str, Any]:
    from backend.utils import github_api

    try:
        owner, repo, _, ref_from_url = github_api.parse_github_url(payload.url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid GitHub URL: {exc}")

    ref = payload.ref or ref_from_url
    token = _os.getenv("GITHUB_TOKEN") or None
    try:
        raw, mime = github_api.get_file_content(owner, repo, payload.path, ref=ref, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch file content: {exc}")

    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    return {"owner": owner, "repo": repo, "path": payload.path, "ref": ref, "content": text}


@router.get("/github/commit/{owner}/{repo}/{sha}")
def github_commit_detail(owner: str, repo: str, sha: str) -> dict[str, Any]:
    """Fetch full diff/files for a single commit on demand."""
    from backend.utils import github_api
    token = _os.getenv("GITHUB_TOKEN") or None
    try:
        detail = github_api.get_commit_detail(owner, repo, sha, token=token)
        for f in detail.get("files", []):
            if f.get("patch") and len(f["patch"]) > 20000:
                f["patch"] = f["patch"][:20000] + "\n...truncated..."
        return detail
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch commit detail: {exc}")


@router.get("/github/commits")
def github_commits(owner: str, repo: str, branch: str, per_page: int = 30, page: int = 1) -> dict[str, Any]:
    from backend.utils import github_api
    token = _os.getenv("GITHUB_TOKEN") or None
    try:
        commits = github_api.list_commits(owner, repo, branch, per_page=per_page, page=page, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to list commits: {exc}")
    return {"owner": owner, "repo": repo, "branch": branch, "page": page, "per_page": per_page, "commits": commits}


@router.post("/github/all_commits")
def github_all_commits(payload: GitHubFetchRequest) -> dict[str, Any]:
    """Fetch ALL commits across ALL branches of a repo.

    Returns a flat list per branch. Useful for whole-repo causal analysis.
    Use `per_branch_limit` to cap max commits per branch (0 = unlimited, default 100/page * 10 pages).
    """
    from backend.utils import github_api

    try:
        owner, repo, path, ref = github_api.parse_github_url(payload.url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid GitHub URL: {exc}")

    token = _os.getenv("GITHUB_TOKEN") or None

    try:
        branches_raw = github_api.list_branches(owner, repo, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to list branches: {exc}")

    branch_names = [b["name"] if isinstance(b, dict) else b for b in branches_raw]

    # Cap to GITMIND_MAX_PAGES (default 1) to avoid fetching all branches
    # of large repos that can have 90+ branches.
    max_pages = int(_os.getenv("GITMIND_MAX_PAGES", "1"))

    # Also cap to trunk/main branch only if GITMIND_INGEST_BRANCH is set
    pinned_branch = _os.getenv("GITMIND_INGEST_BRANCH", "")
    if pinned_branch and pinned_branch in branch_names:
        branch_names = [pinned_branch]

    all_commits: dict[str, Any] = {}
    for branch in branch_names:
        try:
            commits = github_api.list_all_commits(owner, repo, branch, token=token, max_pages=max_pages)
            all_commits[branch] = [
                {
                    "sha": c.get("sha", ""),
                    "message": (c.get("commit") or {}).get("message", ""),
                    "author": ((c.get("commit") or {}).get("author") or {}).get("name", ""),
                    "date": ((c.get("commit") or {}).get("author") or {}).get("date", ""),
                    "url": c.get("html_url", ""),
                }
                for c in commits
            ]
        except Exception as exc:
            log.warning("Failed fetching all commits for branch %s: %s", branch, exc)
            all_commits[branch] = []

    total = sum(len(v) for v in all_commits.values())
    return {
        "owner": owner,
        "repo": repo,
        "branches": branch_names,
        "total_commits": total,
        "commits_by_branch": all_commits,
    }


@router.post("/slack/events")
async def slack_events(
    request: Request,
    background_tasks: BackgroundTasks,
    x_slack_request_timestamp: str = Header(None),
    x_slack_signature: str = Header(None)
) -> Any:
    import json
    import os

    body = await request.body()
    try:
        payload = json.loads(body.decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if payload.get("type") == "url_verification":
        challenge = payload.get("challenge")
        if not challenge:
            raise HTTPException(status_code=400, detail="Challenge field missing")
        return {"challenge": challenge}

    try:
        cfg = GitMindConfig.from_env()
        slack_cfg = cfg.slack
    except Exception:
        class DemoSlackConfig:
            bot_token = os.getenv("SLACK_BOT_TOKEN", "")
            signing_secret = os.getenv("SLACK_SIGNING_SECRET", "")
        slack_cfg = DemoSlackConfig()

    if not slack_cfg.signing_secret:
        log.warning(
            "SLACK_SIGNING_SECRET not set — Slack event signature verification is DISABLED. "
            "Set SLACK_SIGNING_SECRET in your .env to enable it."
        )
    else:
        # Only attempt verification when header values are real strings. When
        # the handler is invoked directly in tests the Header(...) sentinel
        # objects are not strings and should not trigger verification.
        if (
            isinstance(x_slack_signature, str)
            and isinstance(x_slack_request_timestamp, str)
            and x_slack_signature
            and x_slack_request_timestamp
        ):
            if not verify_slack_signature(body, x_slack_request_timestamp, x_slack_signature, slack_cfg.signing_secret):
                raise HTTPException(status_code=401, detail="Invalid Slack signature")

    if payload.get("type") == "event_callback":
        event = payload.get("event")
        if event:
            background_tasks.add_task(process_slack_event, event, slack_cfg)

    return {"status": "accepted"}


# --- App Creation ---

def create_app() -> FastAPI:
    app = FastAPI(title="GitMind Causal Debugging API", version="0.1.0")
    
    # Add CORS middleware to allow frontend requests.
    # Frontend is now a static site (frontend/) — in production this means
    # your Vercel domain, set via CORS_ALLOWED_ORIGINS. The default below
    # only covers local dev: docker-compose's nginx-served static frontend
    # (port 8501) and a bare `python -m http.server` / similar (port 8080).
    _raw_origins = _os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:8501,http://frontend:8501,http://localhost:8080",
    )
    _CORS_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

    # If caller explicitly sets CORS_ALLOWED_ORIGINS=* allow all origins
    # (useful for demo / hackathon deployments where the Vercel URL isn't
    # known yet).  In that case allow_credentials must be False per the
    # CORS spec — browsers reject credentials + wildcard.
    _allow_all = _CORS_ORIGINS == ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if _allow_all else _CORS_ORIGINS,
        allow_credentials=not _allow_all,
        allow_methods=["GET", "POST"],
        allow_headers=[
            "Content-Type",
            "Authorization",
            "X-Slack-Request-Timestamp",
            "X-Slack-Signature",
        ],
    )
    
    app.include_router(router)

    @app.on_event("startup")
    def startup() -> None:
        # NOTE: this used to load everything through GitMindConfig.from_env(),
        # which ALSO requires SLACK_BOT_TOKEN, JIRA_URL/JIRA_USER/JIRA_API_TOKEN,
        # and GITHUB_TOKEN to be set -- none of which /query or the causal
        # graph actually need. Because that loader collects every missing
        # var and raises ONE combined error, a single absent Slack/Jira/
        # GitHub variable was enough to skip the Neo4j connection attempt
        # entirely and force full demo mode, even with perfectly valid
        # Neo4j + Snowflake credentials. Load each piece independently so
        # they can succeed or fail on their own.
        try:
            neo4j_cfg: Neo4jConfig | None = Neo4jConfig.from_env()
        except GitMindConfigError as exc:
            log.warning("Neo4j not configured: %s", exc)
            neo4j_cfg = None

        try:
            snowflake_cfg: SnowflakeConfig | None = SnowflakeConfig.from_env()
            snowflake_cfg.validate()
        except GitMindConfigError as exc:
            log.warning("Snowflake not configured: %s", exc)
            snowflake_cfg = None

        try:
            llm_cfg: LLMConfig | None = LLMConfig.from_env()
        except GitMindConfigError as exc:
            log.warning("LLM (GOOGLE_API_KEY) not configured: %s", exc)
            llm_cfg = None

        if neo4j_cfg is None and snowflake_cfg is None:
            log.warning("GitMind API started without live connectors: neither Neo4j nor Snowflake configured.")
            try:
                set_runtime(GitMindRuntime.demo())
            except Exception:
                pass
            return

        from neo4j import GraphDatabase

        neo4j_driver = None
        if neo4j_cfg is not None:
            try:
                neo4j_driver = GraphDatabase.driver(
                    neo4j_cfg.uri,
                    auth=(neo4j_cfg.user, neo4j_cfg.password),
                    connection_timeout=10,
                    connection_acquisition_timeout=10,
                )
                neo4j_driver.verify_connectivity()
                app.state.neo4j_driver = neo4j_driver
                app.state.neo4j_graph = Neo4jCausalGraph(neo4j_driver, database=neo4j_cfg.database)
            except Exception as exc:
                log.warning("Neo4j connection failed: %s", exc)
                neo4j_driver = None

        if snowflake_cfg is not None:
            try:
                app.state.snowflake_client = SnowflakeClient(snowflake_cfg)
                app.state.snowflake_details = SnowflakeDetails(app.state.snowflake_client)
                try:
                    validate_schema(app.state.snowflake_client.execute)
                except Exception as exc:  # noqa: BLE001 - check is informational, never block boot
                    log.warning("Schema drift check failed to run: %s", exc)
            except Exception as exc:
                log.warning("Snowflake connection failed: %s", exc)

        if not getattr(app.state, "neo4j_graph", None) and not getattr(app.state, "snowflake_details", None):
            log.warning("GitMind API started without live connectors: both connections failed.")
            try:
                set_runtime(GitMindRuntime.demo())
            except Exception:
                pass
            return

        # Wire the agent runtime using whatever live connections we got.
        if llm_cfg is not None and neo4j_driver is not None:
            try:
                runtime = GitMindRuntime(
                    llm_config=llm_cfg,
                    neo4j_driver=neo4j_driver,
                    sf_client=getattr(app.state, "snowflake_client", None),
                    use_placeholders=False,
                    neo4j_database=neo4j_cfg.database if neo4j_cfg else "neo4j",
                )
                set_runtime(runtime)
                app.state.gitmind_runtime = runtime
                log.info("GitMind agent runtime initialised.")
            except Exception as exc:
                log.warning("Agent runtime init failed (non-fatal): %s", exc)
                set_runtime(GitMindRuntime.demo())
        else:
            log.warning(
                "Agent runtime not initialised (missing GOOGLE_API_KEY and/or Neo4j driver); "
                "direct Neo4j/Snowflake queries may still work via /query's fallback path."
            )
            set_runtime(GitMindRuntime.demo())

    @app.on_event("shutdown")
    def shutdown() -> None:
        if getattr(app.state, "neo4j_driver", None):
            app.state.neo4j_driver.close()
        if getattr(app.state, "snowflake_client", None):
            app.state.snowflake_client.close()

    return app


app = create_app()
