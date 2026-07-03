"""
run_ingest.py — single entrypoint that drives snowflake_etl.py + neo4j_etl.py
================================================================================
Why this exists: both ETL scripts require --repo (commits/adrs step) and
--channel (messages step) on the CLI, and `--step all` aborts on the very
first missing one (see their `main()` — it returns 1 the moment a required
arg is absent for the step it's currently on). Nothing in render.yaml was
ever calling them with those args, so neither pipeline ever actually ran —
that's why Snowflake/Neo4j stayed empty even though the tables/credentials
were fine.

This module works for ANY public GitHub repo, not just whatever repo the
deployment happens to be configured for:

  - `main()` is the CLI / scheduled-Job entrypoint. It pulls repo/channel/
    project/adr_path from env vars (GITHUB_DEFAULT_REPOS, SLACK_DEFAULT_CHANNELS,
    JIRA_DEFAULT_PROJECT, GITMIND_ADR_PATH) — this is the "one repo per
    deployment" batch job, unchanged in behaviour.
  - `run(**overrides)` is the actual worker and accepts explicit per-call
    overrides for repo/branch/project/adr_path/channel/max_pages. The
    backend's on-demand `/ingest/repo` endpoint calls this directly with
    whatever the caller asked for, so a single deployment can ingest any
    repo + any Jira project + any ADR path at request time, without
    touching env vars or restarting anything.

Run manually:    python -m ingest.run_ingest
Run via Render:  configured as the gitmind-ingest Job's dockerCommand.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gitmind.ingest")


class _LockedCache:
    """Thin dict-like proxy that serializes check-then-act cache access.

    OPT#6: snowflake_etl.py / neo4j_etl.py both do
    `if key in cache: ... else: fetch(); cache[key] = data` at ~10 call
    sites. That pattern is check-then-act, not atomic. CPython's GIL makes
    each individual `in` / `[]` / `[]=` call atomic in isolation, so a race
    here can never corrupt the underlying dict -- the only real effect of
    two threads missing the same key at once is a redundant network fetch,
    immediately followed by a harmless overwrite with equivalent data
    (both threads are fetching the *same* GitHub/Jira/Slack resource for
    the same repo/branch/channel).

    So this lock is not required for correctness. It exists to close that
    redundant-fetch window: Snowflake ETL and Neo4j ETL now start at the
    same instant (see run(), below), which is exactly the moment they're
    most likely to race on a cold cache -- and every avoided duplicate
    call is one less hit against GitHub's rate limit. Held only across a
    single get/contains/set, never across the fetch+set span, so it never
    serializes the actual network I/O -- both pipelines can still fetch
    different resources fully in parallel.
    """

    __slots__ = ("_d", "_lock")

    def __init__(self) -> None:
        self._d: dict = {}
        self._lock = threading.Lock()

    def __contains__(self, key) -> bool:
        with self._lock:
            return key in self._d

    def __getitem__(self, key):
        with self._lock:
            return self._d[key]

    def __setitem__(self, key, value) -> None:
        with self._lock:
            self._d[key] = value


def _first(csv_env: str) -> str:
    raw = os.environ.get(csv_env, "")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts[0] if parts else ""


def _run_step(label: str, fn, argv: list[str], cache: dict | None = None) -> int:
    """Run one ETL CLI invocation, catching anything it raises.

    Neither snowflake_etl.main() nor neo4j_etl.main() catch exceptions
    raised inside their own step logic — they only have a `finally` to
    close the connection. Left uncaught here, one bad step (e.g. GitHub
    rate-limited, a malformed repo string) would kill this whole script
    and silently skip every step after it — including the entire Neo4j
    half, which is exactly the "no data, no graph" failure this is meant
    to fix. Catching per-step keeps one failure from taking out the rest.
    """
    log.info("=== %s ===", label)
    try:
        rc = fn(argv, cache=cache)
        if rc:
            log.error("%s exited with code %s", label, rc)
        return rc or 0
    except Exception as exc:  # noqa: BLE001 - isolate, log, keep going
        log.error("%s raised an uncaught exception: %s", label, exc, exc_info=True)
        return 1


def run(
    *,
    repo: str,
    branch: str = "main",
    channel: str = "",
    project: str = "",
    adr_path: str = "docs/adr",
    max_pages: str | int = "2",
) -> int:
    """Run the full Snowflake + Neo4j ingest pipeline for one repo.

    This is repo-agnostic: every value that differs between repos (the repo
    itself, its default branch, its Jira project key, where its ADRs live)
    is a parameter here, not a constant. Any public GitHub repo can be
    passed in, along with whichever Jira project and ADR folder actually
    apply to *that* repo — they don't have to match what a previous repo
    used.
    """
    if not repo:
        log.error("No repo to ingest — pass repo='owner/repo'. Aborting before either ETL runs.")
        return 1
    if not channel:
        log.warning(
            "No Slack channel set — skipping the 'messages' step for both "
            "pipelines, running everything else."
        )

    base_args = [
        "--repo", repo,
        "--branch", branch,
        "--adr-path", adr_path,
        "--max-pages", str(max_pages),
    ]
    if project:
        base_args += ["--project", project]

    cache = _LockedCache()

    def _snowflake_pipeline() -> int:
        from ingest import snowflake_etl

        rc = 0
        rc |= _run_step("Snowflake ETL: ddl", snowflake_etl.main, ["--step", "ddl"], cache=cache)
        rc |= _run_step("Snowflake ETL: commits", snowflake_etl.main, ["--step", "commits", *base_args], cache=cache)
        rc |= _run_step("Snowflake ETL: tickets", snowflake_etl.main, ["--step", "tickets", *base_args], cache=cache)
        if channel:
            rc |= _run_step("Snowflake ETL: messages", snowflake_etl.main, ["--step", "messages", "--channel", channel], cache=cache)
        rc |= _run_step("Snowflake ETL: adrs", snowflake_etl.main, ["--step", "adrs", *base_args], cache=cache)
        return rc

    def _neo4j_pipeline() -> int:
        from ingest import neo4j_etl

        rc = 0
        rc |= _run_step("Neo4j ETL: constraints", neo4j_etl.main, ["--step", "constraints"], cache=cache)
        rc |= _run_step("Neo4j ETL: commits", neo4j_etl.main, ["--step", "commits", *base_args], cache=cache)
        rc |= _run_step("Neo4j ETL: tickets", neo4j_etl.main, ["--step", "tickets", *base_args], cache=cache)
        if channel:
            rc |= _run_step("Neo4j ETL: messages", neo4j_etl.main, ["--step", "messages", "--channel", channel], cache=cache)
        rc |= _run_step("Neo4j ETL: adrs", neo4j_etl.main, ["--step", "adrs", *base_args], cache=cache)
        rc |= _run_step("Neo4j ETL: edges", neo4j_etl.main, ["--step", "edges"], cache=cache)
        return rc

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="gitmind-ingest") as pool:
        snowflake_future = pool.submit(_snowflake_pipeline)
        neo4j_future = pool.submit(_neo4j_pipeline)

        try:
            snowflake_rc = snowflake_future.result()
        except Exception as exc:  # noqa: BLE001 - isolate, log, keep going
            log.error("Snowflake pipeline raised an uncaught exception: %s", exc, exc_info=True)
            snowflake_rc = 1

        try:
            neo4j_rc = neo4j_future.result()
        except Exception as exc:  # noqa: BLE001 - isolate, log, keep going
            log.error("Neo4j pipeline raised an uncaught exception: %s", exc, exc_info=True)
            neo4j_rc = 1

    overall_rc = snowflake_rc | neo4j_rc

    if overall_rc:
        log.error("One or more ETL steps failed — see logs above for which ones. Exiting non-zero.")
    else:
        log.info("Ingest complete: Snowflake + Neo4j both populated for %s.", repo)
    return overall_rc


def main() -> int:
    """CLI / scheduled-Job entrypoint — resolves everything from env vars.

    Used by the standalone `gitmind-ingest` Job, which only ever targets the
    one repo/project the deployment was configured with. The on-demand API
    path (backend/main.py's /ingest/repo) does NOT go through this function
    — it calls run() directly with per-request values instead, so it isn't
    limited to whatever repo happens to be set here.
    """
    repo = os.environ.get("GITMIND_INGEST_REPO") or _first("GITHUB_DEFAULT_REPOS")
    branch = os.environ.get("GITMIND_INGEST_BRANCH", "main")
    channel = os.environ.get("GITMIND_INGEST_CHANNEL") or _first("SLACK_DEFAULT_CHANNELS")
    project = os.environ.get("JIRA_DEFAULT_PROJECT", "")
    adr_path = os.environ.get("GITMIND_ADR_PATH", "docs/adr")

    if not repo:
        log.error(
            "No repo to ingest — set GITHUB_DEFAULT_REPOS (or GITMIND_INGEST_REPO) "
            "to 'owner/repo'. Aborting before either ETL runs."
        )
        return 1

    # GITMIND_MAX_PAGES caps commits per branch (100 commits/page).
    # Default 2 = 200 commits — enough for a demo; safe even for repos with
    # very long commit histories. Set higher on Render env for more nodes.
    max_pages = os.environ.get("GITMIND_MAX_PAGES", "2")

    return run(
        repo=repo,
        branch=branch,
        channel=channel,
        project=project,
        adr_path=adr_path,
        max_pages=max_pages,
    )


if __name__ == "__main__":
    sys.exit(main())
