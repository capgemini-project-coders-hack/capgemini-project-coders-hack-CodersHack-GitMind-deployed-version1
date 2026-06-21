from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Any
from datetime import datetime

from fastapi import FastAPI, APIRouter, HTTPException, Request, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.config import GitMindConfig, GitMindConfigError
from backend.snowflake_client import SnowflakeClient
from backend.graph.causal_graph import GraphPathNode, Neo4jCausalGraph
from backend.agent.gitmind_agent import (
    GitMindRuntime,
    set_runtime,
    debug as agent_debug,
)
from backend.harsh_engine.core.regression_guard import check_chain_for_regressions
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


def classify_intent(query: str) -> QueryIntent:
    lowered = query.lower()
    if any(word in lowered for word in CAUSAL_WORDS):
        return QueryIntent.causal
    if any(word in lowered for word in FACT_WORDS):
        return QueryIntent.factual
    return QueryIntent.factual


def enrich_identifiers(payload: GitMindQuery) -> GitMindQuery:
    data = payload.model_copy()
    text = payload.query

    if not data.ticket_id:
        ticket = re.search(r"\b[A-Z][A-Z0-9]+-\d+\b", text)
        if ticket:
            data.ticket_id = ticket.group(0)

    if not data.entity_id and data.ticket_id:
        data.entity_id = f"ticket:{data.ticket_id}"

    if not data.entity_id:
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
    return "DECISIONS"


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
    return {
        "status": "ok",
        "runtime_ready": bool(getattr(request.app.state, "neo4j_graph", None) and getattr(request.app.state, "snowflake_details", None)),
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


@router.post("/query", response_model=GitMindQueryResponse)
def query_gitmind(payload: GitMindQuery, request: Request) -> GitMindQueryResponse:
    graph: Neo4jCausalGraph | None = getattr(request.app.state, "neo4j_graph", None)
    snowflake: SnowflakeDetails | None = getattr(request.app.state, "snowflake_details", None)

    if graph is None and snowflake is None:
        # Full demo mode — let agent run with placeholders, skip DB calls
        try:
            agent_result = agent_debug(payload.query)
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

    if intent == QueryIntent.causal:
        if not (enriched.entity_id or enriched.function_name or enriched.ticket_id):
            raise HTTPException(
                status_code=422,
                detail="Clarify the specific Function name, Ticket ID, or graph node ID before running causal traversal.",
            )

        # Primary path: run the full LLM agent
        try:
            agent_result = agent_debug(enriched.query)
            chain = graph.trace(
                entity_id=enriched.entity_id,
                function_name=enriched.function_name,
                ticket_id=enriched.ticket_id,
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
        raise HTTPException(status_code=502, detail=f"Failed to fetch repo info: {exc}")

    try:
        branches = github_api.list_branches(owner, repo, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to list branches: {exc}")

    result: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "default_branch": default_branch,
        "branches": branches,
        "commits": {},
    }

    # For each branch, fetch a limited set of commits and their diffs
    for b in branches:
        try:
            commits = github_api.list_commits(owner, repo, b, per_page=payload.per_branch_limit, token=token)
        except Exception:
            commits = []

        detailed = []
        for c in commits:
            sha = c.get("sha")
            try:
                detail = github_api.get_commit_detail(owner, repo, sha, token=token)
                # Truncate large patches for safety
                for f in detail.get("files", []):
                    if f.get("patch") and len(f["patch"]) > 20000:
                        f["patch"] = f["patch"][:20000] + "\n...truncated..."
                detailed.append(detail)
            except Exception:
                detailed.append({"sha": sha, "error": "failed to fetch details"})

        result["commits"][b] = detailed

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


@router.get("/github/commits")
def github_commits(owner: str, repo: str, branch: str, per_page: int = 30, page: int = 1) -> dict[str, Any]:
    from backend.utils import github_api
    token = _os.getenv("GITHUB_TOKEN") or None
    try:
        commits = github_api.list_commits(owner, repo, branch, per_page=per_page, page=page, token=token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to list commits: {exc}")
    return {"owner": owner, "repo": repo, "branch": branch, "page": page, "per_page": per_page, "commits": commits}


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
    _CORS_ORIGINS = [
        o.strip()
        for o in _os.getenv(
            "CORS_ALLOWED_ORIGINS",
            "http://localhost:8501,http://frontend:8501,http://localhost:8080",
        ).split(",")
        if o.strip()
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Authorization"],
    )
    
    app.include_router(router)

    @app.on_event("startup")
    def startup() -> None:
        try:
            cfg = GitMindConfig.from_env()
            cfg.snowflake.validate()
        except GitMindConfigError as exc:
            log.warning("GitMind API started without live connectors: %s", exc)
            # No live credentials — set demo runtime so agent can still run in placeholder mode
            try:
                set_runtime(GitMindRuntime.demo())
            except Exception:
                pass
            return

        from neo4j import GraphDatabase

        try:
            neo4j_driver = GraphDatabase.driver(
                cfg.neo4j.uri,
                auth=(cfg.neo4j.user, cfg.neo4j.password),
                connection_timeout=10,
                connection_acquisition_timeout=10,
            )
            neo4j_driver.verify_connectivity()

            app.state.neo4j_driver = neo4j_driver
            app.state.snowflake_client = SnowflakeClient(cfg.snowflake)
            app.state.neo4j_graph = Neo4jCausalGraph(neo4j_driver, database=cfg.neo4j.database)
            app.state.snowflake_details = SnowflakeDetails(app.state.snowflake_client)
        except Exception as exc:
            # Live credentials were present but the connection itself failed
            # (wrong URI/creds, network/firewall block, instance paused, etc).
            # Don't crash the whole API — degrade to demo mode instead.
            log.warning("GitMind API started without live connectors: failed to connect — %s", exc)
            try:
                set_runtime(GitMindRuntime.demo())
            except Exception:
                pass
            return

        # Wire the agent runtime using the already-open connections
        try:
            runtime = GitMindRuntime(
                llm_config=cfg.llm,
                neo4j_driver=neo4j_driver,
                sf_client=app.state.snowflake_client,
                use_placeholders=False,
            )
            set_runtime(runtime)
            app.state.gitmind_runtime = runtime
            log.info("GitMind agent runtime initialised.")
        except Exception as exc:
            log.warning("Agent runtime init failed (non-fatal): %s", exc)
            # Fall back: set demo runtime so agent tools don't crash
            set_runtime(GitMindRuntime.demo())

    @app.on_event("shutdown")
    def shutdown() -> None:
        if getattr(app.state, "neo4j_driver", None):
            app.state.neo4j_driver.close()
        if getattr(app.state, "snowflake_client", None):
            app.state.snowflake_client.close()

    return app


app = create_app()
