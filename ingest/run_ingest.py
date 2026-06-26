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

This script pulls the same values from env vars already defined in
render.yaml (GITHUB_DEFAULT_REPOS, SLACK_DEFAULT_CHANNELS,
JIRA_DEFAULT_PROJECT) and calls both ETL mains with them, so a single
`python -m ingest.run_ingest` populates both stores end-to-end.

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


def _run_step(label: str, fn, argv: list[str]) -> int:
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
        rc = fn(argv)
        if rc:
            log.error("%s exited with code %s", label, rc)
        return rc or 0
    except Exception as exc:  # noqa: BLE001 - isolate, log, keep going
        log.error("%s raised an uncaught exception: %s", label, exc, exc_info=True)
        return 1


def main() -> int:
    repo = os.environ.get("GITMIND_INGEST_REPO") or _first("GITHUB_DEFAULT_REPOS")
    channel = os.environ.get("GITMIND_INGEST_CHANNEL") or _first("SLACK_DEFAULT_CHANNELS")
    project = os.environ.get("JIRA_DEFAULT_PROJECT", "")
    adr_path = os.environ.get("GITMIND_ADR_PATH", "docs/adr")

    if not repo:
        log.error(
            "No repo to ingest — set GITHUB_DEFAULT_REPOS (or GITMIND_INGEST_REPO) "
            "to 'owner/repo'. Aborting before either ETL runs."
        )
        return 1
    if not channel:
        log.warning(
            "No Slack channel set (SLACK_DEFAULT_CHANNELS / GITMIND_INGEST_CHANNEL) — "
            "skipping the 'messages' step for both pipelines, running everything else."
        )

    # GITMIND_MAX_PAGES caps commits per branch (100 commits/page).
    # Default 2 = 200 commits — enough for demo, safe for large repos like Kafka.
    # Set higher on Render env if you want more nodes in the graph.
    max_pages = os.environ.get("GITMIND_MAX_PAGES", "2")

    base_args = ["--repo", repo, "--adr-path", adr_path, "--max-pages", max_pages]
    if project:
        base_args += ["--project", project]

    overall_rc = 0

    # --- Snowflake -----------------------------------------------------
    from ingest import snowflake_etl

    overall_rc |= _run_step("Snowflake ETL: ddl", snowflake_etl.main, ["--step", "ddl"])
    overall_rc |= _run_step("Snowflake ETL: commits", snowflake_etl.main, ["--step", "commits", *base_args])
    overall_rc |= _run_step("Snowflake ETL: tickets", snowflake_etl.main, ["--step", "tickets", *base_args])
    if channel:
        overall_rc |= _run_step("Snowflake ETL: messages", snowflake_etl.main, ["--step", "messages", "--channel", channel])
    overall_rc |= _run_step("Snowflake ETL: adrs", snowflake_etl.main, ["--step", "adrs", *base_args])

    # --- Neo4j -----------------------------------------------------------
    from ingest import neo4j_etl

    overall_rc |= _run_step("Neo4j ETL: constraints", neo4j_etl.main, ["--step", "constraints"])
    overall_rc |= _run_step("Neo4j ETL: commits", neo4j_etl.main, ["--step", "commits", *base_args])
    overall_rc |= _run_step("Neo4j ETL: tickets", neo4j_etl.main, ["--step", "tickets", *base_args])
    if channel:
        overall_rc |= _run_step("Neo4j ETL: messages", neo4j_etl.main, ["--step", "messages", "--channel", channel])
    overall_rc |= _run_step("Neo4j ETL: adrs", neo4j_etl.main, ["--step", "adrs", *base_args])
    overall_rc |= _run_step("Neo4j ETL: edges", neo4j_etl.main, ["--step", "edges"])

    if overall_rc:
        log.error("One or more ETL steps failed — see logs above for which ones. Exiting non-zero.")
    else:
        log.info("Ingest complete: Snowflake + Neo4j both populated.")
    return overall_rc


if __name__ == "__main__":
    sys.exit(main())
