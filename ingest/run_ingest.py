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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gitmind.ingest")


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

    overall_rc = 0

    # PERF: single run-scoped cache shared by both pipelines below. Both
    # snowflake_etl and neo4j_etl fetch the exact same GitHub branch list,
    # commit pages, ADR listing/content, jira/issues.json probe, and (when
    # applicable) the same live Jira/Slack pages for this repo/branch/
    # project/channel -- passing the same dict into both `main()` calls
    # means whichever pipeline runs second reads already-fetched payloads
    # back out instead of hitting the network again. Created fresh here on
    # every run() call and discarded when it returns -- not persisted.
    from ingest.fetch_cache import new_cache
    cache = new_cache()

    # --- Snowflake -----------------------------------------------------
    from ingest import snowflake_etl

    overall_rc |= _run_step("Snowflake ETL: ddl", snowflake_etl.main, ["--step", "ddl"], cache=cache)
    overall_rc |= _run_step("Snowflake ETL: commits", snowflake_etl.main, ["--step", "commits", *base_args], cache=cache)
    overall_rc |= _run_step("Snowflake ETL: tickets", snowflake_etl.main, ["--step", "tickets", *base_args], cache=cache)
    if channel:
        overall_rc |= _run_step("Snowflake ETL: messages", snowflake_etl.main, ["--step", "messages", "--channel", channel], cache=cache)
    overall_rc |= _run_step("Snowflake ETL: adrs", snowflake_etl.main, ["--step", "adrs", *base_args], cache=cache)

    # --- Neo4j -----------------------------------------------------------
    from ingest import neo4j_etl

    overall_rc |= _run_step("Neo4j ETL: constraints", neo4j_etl.main, ["--step", "constraints"], cache=cache)
    overall_rc |= _run_step("Neo4j ETL: commits", neo4j_etl.main, ["--step", "commits", *base_args], cache=cache)
    overall_rc |= _run_step("Neo4j ETL: tickets", neo4j_etl.main, ["--step", "tickets", *base_args], cache=cache)
    if channel:
        overall_rc |= _run_step("Neo4j ETL: messages", neo4j_etl.main, ["--step", "messages", "--channel", channel], cache=cache)
    overall_rc |= _run_step("Neo4j ETL: adrs", neo4j_etl.main, ["--step", "adrs", *base_args], cache=cache)
    overall_rc |= _run_step("Neo4j ETL: edges", neo4j_etl.main, ["--step", "edges"], cache=cache)

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
