"""
gitmind_agent.py — LLM agent runtime for GitMind (Gemini + LangChain)
========================================================================
STATUS: PLACEHOLDER — this file was not present in the uploaded project
and has been stubbed out so the rest of the codebase imports and runs.

`backend/main.py` and `backend/harsh_engine/core/regression_guard.py`
(indirectly) expect from this module:

    GitMindRuntime
        .from_env() -> GitMindRuntime          (live config from env vars)
        .demo() -> GitMindRuntime               (no external calls — safe fallback)
        __init__(self, llm_config, neo4j_driver=None, sf_client=None,
                 use_placeholders: bool = True)
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

This stub wires up a LangChain + `langchain-google-genai` ChatGoogleGenerativeAI
call using GITMIND_MODEL / GITMIND_TEMPERATURE / GITMIND_MAX_TOKENS from
backend/config.py's LLMConfig, but the actual *tool selection* /
RAG-over-the-causal-graph logic (deciding which of Neo4j, Snowflake,
GitHub, Jira to query for a given question) is the part you need to
fill in — that's the real "agent" behavior this project is named for.
See README-LLM-RAG.md for the design this stub assumes.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.config import GitMindConfigError, GitMindConfig, LLMConfig

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
    ) -> None:
        self.llm_config = llm_config
        self.neo4j_driver = neo4j_driver
        self.sf_client = sf_client
        self.use_placeholders = use_placeholders
        self._llm = None  # lazy-built on first use

    @classmethod
    def from_env(cls) -> "GitMindRuntime":
        cfg = GitMindConfig.from_env()
        return cls(llm_config=cfg.llm, use_placeholders=False)

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
        # No persistent resources of our own to close — the Neo4j driver
        # and Snowflake client are owned and closed by backend/main.py.
        pass


def debug(query: str, runtime: GitMindRuntime | None = None) -> dict[str, Any]:
    """Run the causal-debugging agent for a natural-language query.

    TODO: this is the core "LLM RAG tool selection" logic referenced in
    the project's intended design (see README-LLM-RAG.md) — it should:
      1. Ask the LLM which tool(s) to use (Neo4j trace / Snowflake
         lookup / GitHub diff / Jira ticket) given the query.
      2. Call those tools, feed results back into the LLM as context.
      3. Synthesize a root-cause explanation + suggested patch.

    As written, this placeholder only calls the LLM directly with no
    tool use, so `root_cause` will be a plain LLM answer rather than a
    graph-grounded one.
    """
    rt = runtime or get_runtime() or GitMindRuntime.demo()

    if rt.use_placeholders:
        return {
            "root_cause": "Demo mode: no live LLM configured. Set GOOGLE_API_KEY to enable real analysis.",
            "evidence_chain": [],
            "patch": "",
            "regression_safe": True,
        }

    llm = rt._get_llm()
    if llm is None:
        return {
            "root_cause": "LLM unavailable — check GOOGLE_API_KEY and network connectivity.",
            "evidence_chain": [],
            "patch": "",
            "regression_safe": True,
        }

    try:
        response = llm.invoke(query)
        text = getattr(response, "content", str(response))
    except Exception as exc:  # noqa: BLE001 - surface as a soft failure, not a 500
        log.warning("LLM call failed: %s", exc)
        return {
            "root_cause": f"LLM call failed: {exc}",
            "evidence_chain": [],
            "patch": "",
            "regression_safe": True,
        }

    return {
        "root_cause": text,
        "evidence_chain": [],
        "patch": "",
        "regression_safe": True,
    }
