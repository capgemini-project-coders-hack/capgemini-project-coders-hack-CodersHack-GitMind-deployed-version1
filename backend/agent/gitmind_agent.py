"""
gitmind_agent.py — LLM agent runtime for GitMind (Gemini + LangChain)
========================================================================
`backend/main.py` and `backend/harsh_engine/core/regression_guard.py`
(indirectly) expect from this module:

    GitMindRuntime
        .from_env() -> GitMindRuntime          (live config from env vars)
        .demo() -> GitMindRuntime               (no external calls — safe fallback)
        __init__(self, llm_config, neo4j_driver=None, sf_client=None,
                 use_placeholders: bool = True, neo4j_database: str = "neo4j")
        .close(self) -> None

    set_runtime(runtime: GitMindRuntime) -> None
        Stores a module-level "current" runtime so `debug()` can be
        called without explicitly threading the runtime through every
        call site (mirrors how `backend/main.py` calls `agent_debug(...)`
        with no runtime argument from inside request handlers).

    debug(query: str, runtime: GitMindRuntime | None = None) -> dict
        Returns a dict with (at minimum) the keys main.py reads:
            root_cause: str
            evidence_chain: list[str]
            patch: str
            regression_safe: bool
        Also returns `causal_chain: list[GraphPathNode]` so callers that
        already need the raw chain (e.g. /query) don't have to re-trace
        Neo4j a second time.

`debug()` does real tool selection + retrieval before calling the LLM:
  1. Figure out which identifier (entity_id / function_name / ticket_id)
     the query is about — either supplied by the caller or extracted
     from the raw text.
  2. If a Neo4j driver is attached to the runtime, trace the causal chain
     for that identifier (backend/graph/causal_graph.py).
  3. If a Snowflake client is attached, pull the raw evidence row for
     each node in that chain.
  4. Feed the traced chain + evidence to the LLM as grounding context and
     ask it to synthesize a root-cause explanation and (optionally) a
     patch, instead of just answering the bare query from parametric
     knowledge.
  5. Run the regression guard against any Decision/ADR nodes in the chain.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from backend.config import GitMindConfigError, GitMindConfig, LLMConfig
from backend.graph.causal_graph import GraphPathNode, Neo4jCausalGraph
from backend.harsh_engine.core.regression_guard import check_chain_for_regressions

log = logging.getLogger("gitmind.agent")

_CURRENT_RUNTIME: "GitMindRuntime | None" = None


def set_runtime(runtime: "GitMindRuntime") -> None:
    global _CURRENT_RUNTIME
    _CURRENT_RUNTIME = runtime


def get_runtime() -> "GitMindRuntime | None":
    return _CURRENT_RUNTIME


class GitMindRuntime:
    """Holds the LLM client plus whatever live connections the agent
    is allowed to call as tools (Neo4j driver, Snowflake client, etc).
    """

    def __init__(
        self,
        llm_config: LLMConfig,
        neo4j_driver: Any = None,
        sf_client: Any = None,
        use_placeholders: bool = True,
        neo4j_database: str = "neo4j",
    ) -> None:
        self.llm_config = llm_config
        self.neo4j_driver = neo4j_driver
        self.sf_client = sf_client
        self.use_placeholders = use_placeholders
        self.neo4j_database = neo4j_database
        self._llm = None  # lazy-built on first use

    @classmethod
    def from_env(cls) -> "GitMindRuntime":
        """Build a live runtime straight from environment variables.

        Used by the Slack bot path (backend/main.py:process_slack_event),
        which doesn't share the FastAPI app's already-open Neo4j driver /
        Snowflake client, so it opens its own here. Neo4j/Snowflake
        connection failures are logged and degrade to a tool-less runtime
        (LLM-only) rather than raising, so a transient DB outage doesn't
        take down the whole Slack integration.
        """
        cfg = GitMindConfig.from_env()

        neo4j_driver = None
        try:
            from neo4j import GraphDatabase

            neo4j_driver = GraphDatabase.driver(
                cfg.neo4j.uri,
                auth=(cfg.neo4j.user, cfg.neo4j.password),
                connection_timeout=10,
                connection_acquisition_timeout=10,
            )
            neo4j_driver.verify_connectivity()
        except Exception as exc:  # noqa: BLE001 - degrade, don't crash the bot
            log.warning("GitMindRuntime.from_env(): Neo4j connection failed: %s", exc)
            neo4j_driver = None

        sf_client = None
        try:
            from backend.snowflake_client import SnowflakeClient

            sf_client = SnowflakeClient(cfg.snowflake)
        except Exception as exc:  # noqa: BLE001
            log.warning("GitMindRuntime.from_env(): Snowflake client init failed: %s", exc)
            sf_client = None

        return cls(
            llm_config=cfg.llm,
            neo4j_driver=neo4j_driver,
            sf_client=sf_client,
            use_placeholders=False,
            neo4j_database=cfg.neo4j.database,
        )

    @classmethod
    def demo(cls) -> "GitMindRuntime":
        """A runtime with no live LLM/DB calls — used whenever required
        env vars are missing or a live connection fails, so the API
        and Slack bot degrade gracefully instead of crashing.
        """
        return cls(llm_config=LLMConfig(api_key="", model="demo"), use_placeholders=True)

    def _get_llm(self):
        if self._llm is not None:
            return self._llm
        if self.use_placeholders or not self.llm_config.api_key:
            return None
        from langchain_google_genai import ChatGoogleGenerativeAI

        self._llm = ChatGoogleGenerativeAI(
            model=self.llm_config.model,
            google_api_key=self.llm_config.api_key,
            temperature=self.llm_config.temperature,
            max_output_tokens=self.llm_config.max_tokens,
        )
        return self._llm

    def close(self) -> None:
        # Only closes connections this runtime instance opened itself
        # (e.g. via from_env()). The FastAPI app's long-lived runtime is
        # built from app.state's already-open driver/client, which
        # backend/main.py's own shutdown handler closes directly — calling
        # close() here a second time on those is harmless (idempotent)
        # but kept narrow in scope just in case.
        if self.neo4j_driver is not None:
            try:
                self.neo4j_driver.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                log.warning("Error closing Neo4j driver", exc_info=True)
        if self.sf_client is not None:
            try:
                self.sf_client.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                log.warning("Error closing Snowflake client", exc_info=True)


# ---------------------------------------------------------------------------
# Tool selection: pull an identifier out of free text when the caller
# hasn't already supplied one. Mirrors backend/main.py's enrich_identifiers
# so the Slack path gets the same graph grounding the HTTP /query path does.
# ---------------------------------------------------------------------------

_TICKET_RE = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")
_COMMIT_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.IGNORECASE)
_FUNCTION_RE = re.compile(
    r"\bfunction\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b",
    re.IGNORECASE,
)


def _extract_identifiers(
    query: str, entity_id: str = "", function_name: str = "", ticket_id: str = ""
) -> tuple[str, str, str]:
    if not ticket_id:
        m = _TICKET_RE.search(query)
        if m:
            ticket_id = m.group(0)

    if not entity_id:
        if ticket_id:
            # Ticket nodes are MERGEd (ingest/neo4j_etl.py) with `id` set
            # to the raw Jira key itself — that's also what entity_id needs
            # to be for the `{id: $entity_id}` Cypher match to hit.
            entity_id = ticket_id
        else:
            m = _COMMIT_RE.search(query)
            if m:
                entity_id = m.group(0)

    if not function_name:
        m = _FUNCTION_RE.search(query)
        if m:
            function_name = m.group(1)

    return entity_id, function_name, ticket_id


# ---------------------------------------------------------------------------
# Snowflake evidence lookup for a traced chain. Kept local (rather than
# importing backend.main.SnowflakeDetails) to avoid a circular import —
# backend/main.py imports this module, so this module can't import back.
# ---------------------------------------------------------------------------

_SF_TABLES = {
    "COMMITS": "commit_id",
    "TICKETS": "ticket_id",
    "MESSAGES": "message_id",
    "ADR_RECORDS": "adr_id",
    "BUG_REPORTS": "bug_id",
    "DECISIONS": "decision_id",
}

_SF_SOURCE_TO_TABLE = {
    "Commit": "COMMITS", "COMMIT": "COMMITS",
    "JiraTicket": "TICKETS", "Ticket": "TICKETS", "TICKET": "TICKETS",
    "SlackMessage": "MESSAGES", "Message": "MESSAGES", "MESSAGE": "MESSAGES",
    "ADR": "ADR_RECORDS",
    "BUG_REPORT": "BUG_REPORTS", "BugReport": "BUG_REPORTS",
    "Decision": "DECISIONS",
}


def _fetch_evidence(sf_client: Any, chain: list[GraphPathNode]) -> list[dict[str, Any]]:
    if sf_client is None or not chain:
        return []
    evidence: list[dict[str, Any]] = []
    for node in chain:
        table = _SF_SOURCE_TO_TABLE.get(node.source_type) or _SF_SOURCE_TO_TABLE.get(node.label)
        if not table or not node.source_id:
            continue
        id_column = _SF_TABLES[table]
        try:
            rows = sf_client.execute(
                f"SELECT * FROM {table} WHERE {id_column} = %s LIMIT 1", (node.source_id,)
            )
            evidence.extend(rows)
        except Exception as exc:  # noqa: BLE001 - one bad lookup shouldn't sink the rest
            log.warning("Evidence lookup failed for %s/%s: %s", table, node.source_id, exc)
    return evidence


# ---------------------------------------------------------------------------
# LLM synthesis
# ---------------------------------------------------------------------------

_AGENT_PROMPT = """You are GitMind, a causal-debugging assistant for a software team.

You are given a causal chain traced from a Neo4j graph (commits, tickets, \
Slack messages, ADRs, decisions, bug reports) plus raw supporting evidence \
pulled from Snowflake. Base your answer ONLY on this context and the user's \
question below — do not invent commit hashes, ticket IDs, or facts that \
aren't present in the context. If the context is empty, say so explicitly \
rather than guessing.

=== CONTEXT ===
{context}
=== END CONTEXT ===

User question: {query}

Respond in exactly this format:
ROOT_CAUSE: <one or two sentence plain-English root-cause explanation>
PATCH: <a short suggested patch/diff if the context warrants one, otherwise NONE>
"""


def _build_context_block(chain: list[GraphPathNode], evidence: list[dict[str, Any]]) -> str:
    if not chain and not evidence:
        return "(no causal chain or evidence was found for this query)"

    lines: list[str] = []
    if chain:
        lines.append("Causal chain (start -> root cause):")
        for i, node in enumerate(chain, start=1):
            lines.append(
                f"  {i}. [{node.label}] id={node.node_id} "
                f"source={node.source_type}:{node.source_id} -- {node.summary}"
            )
    if evidence:
        lines.append("Raw Snowflake evidence rows:")
        for row in evidence:
            lines.append(f"  - {row}")
    return "\n".join(lines)


def _parse_agent_response(text: str) -> tuple[str, str]:
    root_cause = text.strip()
    patch = ""
    m = re.search(r"ROOT_CAUSE:\s*(.*?)(?:\n+PATCH:|\Z)", text, re.DOTALL | re.IGNORECASE)
    if m:
        root_cause = m.group(1).strip()
    p = re.search(r"PATCH:\s*(.*)\Z", text, re.DOTALL | re.IGNORECASE)
    if p:
        patch_raw = p.group(1).strip()
        patch = "" if patch_raw[:4].upper() == "NONE" else patch_raw
    return root_cause, patch


def debug(
    query: str,
    runtime: GitMindRuntime | None = None,
    *,
    entity_id: str = "",
    function_name: str = "",
    ticket_id: str = "",
) -> dict[str, Any]:
    """Run the causal-debugging agent for a natural-language query.

    Does real tool selection: traces Neo4j and pulls Snowflake evidence
    for whatever identifier the query (or caller) supplies, feeds that
    into the LLM as grounding context, and parses out a root-cause +
    patch instead of just forwarding the raw query to the model.
    """
    rt = runtime or get_runtime() or GitMindRuntime.demo()

    if rt.use_placeholders:
        return {
            "root_cause": "Demo mode: no live LLM configured. Set GOOGLE_API_KEY to enable real analysis.",
            "evidence_chain": [],
            "patch": "",
            "regression_safe": True,
            "causal_chain": [],
        }

    llm = rt._get_llm()
    if llm is None:
        return {
            "root_cause": "LLM unavailable — check GOOGLE_API_KEY and network connectivity.",
            "evidence_chain": [],
            "patch": "",
            "regression_safe": True,
            "causal_chain": [],
        }

    entity_id, function_name, ticket_id = _extract_identifiers(
        query, entity_id, function_name, ticket_id
    )

    chain: list[GraphPathNode] = []
    if rt.neo4j_driver is not None and (entity_id or function_name or ticket_id):
        try:
            graph = Neo4jCausalGraph(rt.neo4j_driver, database=rt.neo4j_database)
            chain = graph.trace(
                entity_id=entity_id,
                function_name=function_name,
                ticket_id=ticket_id,
            )
        except Exception as exc:  # noqa: BLE001 - fall through to LLM-only answer
            log.warning("Neo4j trace failed inside agent: %s", exc)

    evidence: list[dict[str, Any]] = []
    if rt.sf_client is not None and chain:
        evidence = _fetch_evidence(rt.sf_client, chain)

    context = _build_context_block(chain, evidence)
    prompt = _AGENT_PROMPT.format(context=context, query=query)

    try:
        response = llm.invoke(prompt)
        text = getattr(response, "content", str(response))
    except Exception as exc:  # noqa: BLE001 - surface as a soft failure, not a 500
        log.warning("LLM call failed: %s", exc)
        return {
            "root_cause": f"LLM call failed: {exc}",
            "evidence_chain": [f"[{n.label}] {n.node_id}: {n.summary}" for n in chain],
            "patch": "",
            "regression_safe": True,
            "causal_chain": chain,
        }

    root_cause, patch = _parse_agent_response(text)

    regression_safe = True
    if chain:
        try:
            result = check_chain_for_regressions([n.__dict__ for n in chain], query)
            if result.get("ran"):
                regression_safe = bool(result.get("safe"))
        except Exception as exc:  # noqa: BLE001 - never let this block the answer
            log.warning("Regression check failed inside agent: %s", exc)

    return {
        "root_cause": root_cause,
        "evidence_chain": [f"[{n.label}] {n.node_id}: {n.summary}" for n in chain],
        "patch": patch,
        "regression_safe": regression_safe,
        "causal_chain": chain,
    }
