"""insight_report.py — Project Overview Report (POST /insight/overview)
========================================================================
Lightweight PDF: repo stats (already-ingested Snowflake data) + one
branch-count live GitHub call + ONE bounded Gemini prompt for a short
overview / next-steps. Deliberately NOT full-file-content analysis --
see CONSTRAINTS in the feature spec this implements.

Holds an in-memory report_id -> filepath map. Render's disk is ephemeral
(wiped on restart/redeploy), so this map -- and the PDFs themselves --
do not survive a process restart. /insight/overview/{id}/download
returns 404 for a report_id from before the most recent restart, which
is the correct behavior given that constraint (not a bug to "fix" by
adding fake persistence).
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from backend.config import LLMConfig, GitMindConfigError
from backend.snowflake_client import SnowflakeClient

log = logging.getLogger("gitmind.insight_report")

REPORTS_DIR = os.environ.get("GITMIND_REPORTS_DIR", "/tmp/reports")

# report_id -> {"path": str, "repo": str, "created_at": str}
# In-memory only -- see module docstring on ephemeral storage.
_REPORTS: dict[str, dict[str, str]] = {}


def register_report(report_id: str, path: str, repo: str) -> None:
    _REPORTS[report_id] = {
        "path": path,
        "repo": repo,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def lookup_report(report_id: str) -> dict[str, str] | None:
    entry = _REPORTS.get(report_id)
    if entry is None:
        return None
    if not os.path.exists(entry["path"]):
        # File gone (e.g. ephemeral disk wiped) but map entry survived --
        # treat as not-found rather than serving a broken FileResponse.
        return None
    return entry


# ---------------------------------------------------------------------------
# Step 1 — Snowflake stats (already-ingested data only, no new ingestion)
# ---------------------------------------------------------------------------

@dataclass
class RepoStats:
    commit_count: int = 0
    distinct_authors: int = 0
    first_commit: str | None = None
    last_commit: str | None = None

    ticket_count: int = 0
    tickets_by_status: dict[str, int] = field(default_factory=dict)
    tickets_by_type: dict[str, int] = field(default_factory=dict)

    adr_count: int = 0
    adrs: list[dict[str, str]] = field(default_factory=list)  # [{title, status}]

    branch_count: int = 0
    branch_names: list[str] = field(default_factory=list)


def _safe_execute(client: SnowflakeClient, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
    try:
        return client.execute(sql, params)
    except Exception as exc:  # noqa: BLE001 - one bad query shouldn't kill the whole report
        log.warning("Stats query failed (%s): %s", sql[:60], exc)
        return []


def gather_repo_stats(client: SnowflakeClient, repo_slug: str) -> RepoStats:
    stats = RepoStats()

    # --- Commits ---
    # COMMITS.repo is populated on ingest; filter by it, but the table is
    # also fully TRUNCATEd+re-ingested per repo (see ingest/snowflake_etl.py
    # reset_all()), so an empty filtered result still falls back to an
    # unfiltered count rather than reporting zero for a repo that was, in
    # fact, just ingested under a slightly different slug string.
    rows = _safe_execute(
        client,
        'SELECT COUNT(*) AS COUNT, COUNT(DISTINCT author) AS AUTHORS, '
        'MIN(timestamp) AS FIRST_TS, MAX(timestamp) AS LAST_TS '
        "FROM COMMITS WHERE repo = %s",
        (repo_slug,),
    )
    if not rows or not rows[0].get("COUNT"):
        rows = _safe_execute(
            client,
            'SELECT COUNT(*) AS COUNT, COUNT(DISTINCT author) AS AUTHORS, '
            'MIN(timestamp) AS FIRST_TS, MAX(timestamp) AS LAST_TS FROM COMMITS',
        )
    if rows:
        r = rows[0]
        stats.commit_count = int(r.get("COUNT") or 0)
        stats.distinct_authors = int(r.get("AUTHORS") or 0)
        stats.first_commit = r.get("FIRST_TS")
        stats.last_commit = r.get("LAST_TS")

    # --- Tickets ---
    # TICKETS has no repo column (deployments are single-repo-at-a-time,
    # wiped on each /ingest/repo) -- so no WHERE clause to scope it by.
    rows = _safe_execute(client, "SELECT COUNT(*) AS COUNT FROM TICKETS")
    if rows:
        stats.ticket_count = int(rows[0].get("COUNT") or 0)

    rows = _safe_execute(
        client, "SELECT status AS STATUS, COUNT(*) AS COUNT FROM TICKETS GROUP BY status"
    )
    stats.tickets_by_status = {
        (r.get("STATUS") or "Unknown"): int(r.get("COUNT") or 0) for r in rows
    }

    rows = _safe_execute(
        client, "SELECT issue_type AS ISSUE_TYPE, COUNT(*) AS COUNT FROM TICKETS GROUP BY issue_type"
    )
    stats.tickets_by_type = {
        (r.get("ISSUE_TYPE") or "Unknown"): int(r.get("COUNT") or 0) for r in rows
    }

    # --- ADRs ---
    rows = _safe_execute(
        client, "SELECT title AS TITLE, status AS STATUS FROM ADR_RECORDS WHERE repo = %s", (repo_slug,)
    )
    if not rows:
        rows = _safe_execute(client, "SELECT title AS TITLE, status AS STATUS FROM ADR_RECORDS")
    stats.adrs = [{"title": r.get("TITLE") or "(untitled)", "status": r.get("STATUS") or "Unknown"} for r in rows]
    stats.adr_count = len(stats.adrs)

    return stats


# ---------------------------------------------------------------------------
# Step 2 — one live GitHub call: branch count + names
# ---------------------------------------------------------------------------

def fetch_branch_info(repo_slug: str) -> tuple[int, list[str]]:
    from backend.utils import github_api

    owner, _, repo = repo_slug.partition("/")
    if not repo:
        return 0, []
    token = os.environ.get("GITHUB_TOKEN") or None
    try:
        branches = github_api.list_branches(owner, repo, token=token)
    except Exception as exc:  # noqa: BLE001 - degrade gracefully, this is a "nice to have" stat
        log.warning("Branch fetch failed for %s: %s", repo_slug, exc)
        return 0, []
    names = [b["name"] if isinstance(b, dict) else b for b in branches]
    return len(names), names


# ---------------------------------------------------------------------------
# Step 3 — one bounded Gemini prompt (fixed size, no chunking)
# ---------------------------------------------------------------------------

_OVERVIEW_PROMPT = """You are summarizing a software project from its already-collected \
activity stats. You do NOT have access to source code -- only the numbers and \
records below. Do not claim or imply deeper code-level analysis than what is given.

Repository: {repo}

Commit activity:
- {commit_count} commits by {distinct_authors} distinct author(s)
- First commit: {first_commit}
- Last commit: {last_commit}
- Branches ({branch_count}): {branch_names}

Tickets ({ticket_count} total):
- By status: {tickets_by_status}
- By type: {tickets_by_type}

Architecture Decision Records ({adr_count} total):
{adr_list}
{motive_block}
Respond with ONLY a JSON object (no markdown fences, no preamble), with exactly
these keys:
  "overview": 2-4 sentence plain-language summary of the project's activity
    and state based purely on the stats above.
  "alignment_note": {alignment_instruction}
  "next_steps": a JSON array of 3-5 short, concrete next-step strings, inferred
    only from ticket status breakdown and ADR consequences/status -- not from
    any code you don't have access to.
"""


def _build_prompt(stats: RepoStats, repo_slug: str, motive_prompt: str) -> str:
    adr_list = "\n".join(f"  - {a['title']} ({a['status']})" for a in stats.adrs) or "  (none recorded)"
    motive_block = f"\nProject's intended purpose (per the user): {motive_prompt}\n" if motive_prompt else ""
    alignment_instruction = (
        '1-2 sentence note on whether the observed activity looks aligned with '
        'the stated intended purpose -- omit this key entirely (do not include '
        'it in the JSON) if no intended purpose was given.'
        if motive_prompt
        else 'omit this key entirely from the JSON -- no intended purpose was given.'
    )
    return _OVERVIEW_PROMPT.format(
        repo=repo_slug,
        commit_count=stats.commit_count,
        distinct_authors=stats.distinct_authors,
        first_commit=stats.first_commit or "unknown",
        last_commit=stats.last_commit or "unknown",
        branch_count=stats.branch_count,
        branch_names=", ".join(stats.branch_names) or "none found",
        ticket_count=stats.ticket_count,
        tickets_by_status=json.dumps(stats.tickets_by_status),
        tickets_by_type=json.dumps(stats.tickets_by_type),
        adr_count=stats.adr_count,
        adr_list=adr_list,
        motive_block=motive_block,
        alignment_instruction=alignment_instruction,
    )


def _get_llm():
    """Builds a Gemini client the same way GitMindRuntime._get_llm() does,
    independent of the neo4j-gated global runtime -- this feature only
    needs Snowflake + an LLM key, not a Neo4j connection."""
    try:
        llm_cfg = LLMConfig.from_env()
    except GitMindConfigError:
        return None
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except Exception:  # noqa: BLE001
        return None
    return ChatGoogleGenerativeAI(
        model=llm_cfg.model,
        google_api_key=llm_cfg.api_key,
        temperature=llm_cfg.temperature,
        max_output_tokens=llm_cfg.max_tokens,
    )


def _parse_llm_json(text: str) -> dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(cleaned)
    except Exception:  # noqa: BLE001
        m = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:  # noqa: BLE001
                pass
        return {}


def generate_overview(stats: RepoStats, repo_slug: str, motive_prompt: str) -> dict[str, Any]:
    """Returns {overview, alignment_note?, next_steps}. Degrades to a
    stats-only placeholder if no LLM key is configured or the call fails --
    this never blocks the PDF from being produced."""
    llm = _get_llm()
    if llm is None:
        return {
            "overview": (
                f"{stats.commit_count} commits, {stats.ticket_count} tickets, and "
                f"{stats.adr_count} ADR(s) recorded for {repo_slug}. "
                "(No GOOGLE_API_KEY configured -- narrative summary unavailable.)"
            ),
            "next_steps": [],
        }

    prompt = _build_prompt(stats, repo_slug, motive_prompt)
    try:
        response = llm.invoke(prompt)
        text = getattr(response, "content", str(response))
    except Exception as exc:  # noqa: BLE001
        log.warning("Overview LLM call failed: %s", exc)
        return {
            "overview": f"LLM call failed ({exc}); see stats table above for raw activity data.",
            "next_steps": [],
        }

    parsed = _parse_llm_json(text)
    if not parsed.get("overview"):
        parsed["overview"] = text.strip()[:1000]
    parsed.setdefault("next_steps", [])
    if not isinstance(parsed.get("next_steps"), list):
        parsed["next_steps"] = []
    return parsed


# ---------------------------------------------------------------------------
# Step 4-5 — render PDF, save to ephemeral disk
# ---------------------------------------------------------------------------

def render_pdf(repo_slug: str, stats: RepoStats, llm_result: dict[str, Any]) -> str:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, ListFlowable, ListItem
    from reportlab.lib import colors

    os.makedirs(REPORTS_DIR, exist_ok=True)
    slug_safe = re.sub(r"[^A-Za-z0-9_.-]", "_", repo_slug)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(REPORTS_DIR, f"{slug_safe}_{timestamp}.pdf")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("GMTitle", parent=styles["Title"], fontSize=18)
    h2 = ParagraphStyle("GMH2", parent=styles["Heading2"], spaceBefore=14)
    body = styles["BodyText"]

    doc = SimpleDocTemplate(path, pagesize=LETTER, title=f"GitMind Overview — {repo_slug}")
    elements: list[Any] = []

    elements.append(Paragraph("GitMind Project Overview Report", title_style))
    elements.append(Paragraph(repo_slug, styles["Heading3"]))
    elements.append(Paragraph(
        "Generated from already-ingested commit, ticket, and ADR data plus a live "
        "branch lookup. This is a stats-and-summary report, not a code-level analysis.",
        styles["Italic"],
    ))
    elements.append(Spacer(1, 0.2 * inch))

    elements.append(Paragraph("Overview", h2))
    elements.append(Paragraph(llm_result.get("overview", ""), body))

    if llm_result.get("alignment_note"):
        elements.append(Paragraph("Alignment with Stated Purpose", h2))
        elements.append(Paragraph(llm_result["alignment_note"], body))

    elements.append(Paragraph("Repository Stats", h2))
    stats_table_data = [
        ["Metric", "Value"],
        ["Commits", str(stats.commit_count)],
        ["Distinct authors", str(stats.distinct_authors)],
        ["First commit", stats.first_commit or "unknown"],
        ["Last commit", stats.last_commit or "unknown"],
        ["Branches", f"{stats.branch_count} ({', '.join(stats.branch_names) or 'none found'})"],
        ["Tickets", str(stats.ticket_count)],
        ["ADRs", str(stats.adr_count)],
    ]
    t = Table(stats_table_data, colWidths=[2 * inch, 4 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0C1A2E")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.15 * inch))

    if stats.tickets_by_status:
        elements.append(Paragraph("Tickets by Status", h2))
        elements.append(Paragraph(
            ", ".join(f"{k}: {v}" for k, v in stats.tickets_by_status.items()), body
        ))
    if stats.tickets_by_type:
        elements.append(Paragraph("Tickets by Type", h2))
        elements.append(Paragraph(
            ", ".join(f"{k}: {v}" for k, v in stats.tickets_by_type.items()), body
        ))

    if stats.adrs:
        elements.append(Paragraph("ADR Records", h2))
        elements.append(ListFlowable(
            [ListItem(Paragraph(f"{a['title']} — {a['status']}", body)) for a in stats.adrs],
            bulletType="bullet",
        ))

    next_steps = llm_result.get("next_steps") or []
    elements.append(Paragraph("Suggested Next Steps", h2))
    if next_steps:
        elements.append(ListFlowable(
            [ListItem(Paragraph(str(s), body)) for s in next_steps],
            bulletType="bullet",
        ))
    else:
        elements.append(Paragraph(
            "No next steps inferred (no LLM configured, or insufficient ticket/ADR data).", body
        ))

    doc.build(elements)
    return path
