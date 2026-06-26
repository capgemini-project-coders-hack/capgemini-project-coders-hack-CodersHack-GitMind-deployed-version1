"""
github_api.py — Thin wrapper around the GitHub REST API for GitMind
=======================================================================
Includes:
  - _throttle(): enforces minimum gap between API calls (default 0.75s)
  - _get(): single retry wrapper that honours 429/403 Retry-After headers
  - GITHUB_API_MIN_GAP env var to tune throttle (default 0.75s)

With a GITHUB_TOKEN set: 5000 req/hr = safe at 0.75s gap.
Without token: 60 req/hr = set GITHUB_TOKEN or expect rate limiting.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any
from urllib.parse import urlparse

import requests

GITHUB_API = "https://api.github.com"
log = logging.getLogger("gitmind.etl.snowflake")

# Minimum seconds between ANY GitHub API call.
# Authenticated = 5000 req/hr ≈ 1.38/s → 0.75s gap is safe.
_MIN_CALL_GAP = float(os.getenv("GITHUB_API_MIN_GAP", "0.75"))
_last_call_time: float = 0.0


def _throttle() -> None:
    global _last_call_time
    elapsed = time.monotonic() - _last_call_time
    wait = _MIN_CALL_GAP - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_call_time = time.monotonic()


def _get(url: str, token: str | None, params: dict | None = None, timeout: int = 15) -> requests.Response:
    """GitHub GET with throttle + smart retry on rate limit responses."""
    max_retries = 3
    resp = None
    for attempt in range(1, max_retries + 1):
        _throttle()
        resp = requests.get(url, headers=_headers(token), params=params, timeout=timeout)

        is_rate_limited = resp.status_code == 429 or (
            resp.status_code == 403 and "rate limit" in resp.text.lower()
        )
        if not is_rate_limited:
            return resp

        reset_at = int(resp.headers.get("X-RateLimit-Reset", 0))
        retry_after = int(resp.headers.get("Retry-After", 0))
        if reset_at:
            wait = max(1, reset_at - int(time.time()) + 1)
        elif retry_after:
            wait = retry_after
        else:
            wait = 60 * attempt  # 60s, 120s, 180s

        log.warning(
            "GitHub rate limited (attempt %d/%d) — waiting %ds",
            attempt, max_retries, wait,
        )
        if attempt < max_retries:
            time.sleep(wait)

    resp.raise_for_status()
    return resp


def _headers(token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def parse_github_url(url: str) -> tuple[str, str, str, str]:
    parsed = urlparse(url)
    if "github.com" not in parsed.netloc:
        raise ValueError("URL is not a github.com URL")
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("URL must include at least an owner and repo")
    owner, repo = parts[0], parts[1]
    repo = re.sub(r"\.git$", "", repo)
    path = ""
    ref = ""
    if len(parts) > 3 and parts[2] in ("tree", "blob"):
        ref = parts[3]
        path = "/".join(parts[4:])
    return owner, repo, path, ref


def get_repo_info(owner: str, repo: str, token: str | None = None) -> dict[str, Any]:
    resp = _get(f"{GITHUB_API}/repos/{owner}/{repo}", token, timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_branches(owner: str, repo: str, token: str | None = None) -> list[dict[str, Any]]:
    # Previously a single unpaginated GET -- GitHub defaults to per_page=30
    # when it's not specified, so repos with more branches than that (e.g.
    # apache/kafka has 90+) silently lost every branch past page 1. For
    # Kafka specifically this cut "trunk" (the actual default branch) out
    # of the list entirely, which broke GITMIND_INGEST_BRANCH pinning
    # downstream in /github/fetch: the pin check is `pinned_branch in
    # branch_names`, and trunk was never in branch_names to begin with --
    # not because the pin was misconfigured, but because this call never
    # fetched it. Paginating fixes the pin and gives an accurate full
    # branch list for any repo, large or small.
    all_branches: list[dict[str, Any]] = []
    page = 1
    while True:
        resp = _get(
            f"{GITHUB_API}/repos/{owner}/{repo}/branches",
            token,
            params={"per_page": 100, "page": page},
            timeout=15,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_branches.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return all_branches


def list_commits(
    owner: str,
    repo: str,
    branch: str,
    per_page: int = 30,
    page: int = 1,
    token: str | None = None,
) -> list[dict[str, Any]]:
    resp = _get(
        f"{GITHUB_API}/repos/{owner}/{repo}/commits",
        token,
        params={"sha": branch, "per_page": per_page, "page": page},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_commit_detail(owner: str, repo: str, sha: str, token: str | None = None) -> dict[str, Any]:
    resp = _get(f"{GITHUB_API}/repos/{owner}/{repo}/commits/{sha}", token, timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_all_commits(
    owner: str,
    repo: str,
    branch: str,
    token: str | None = None,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    all_commits: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        resp = _get(
            f"{GITHUB_API}/repos/{owner}/{repo}/commits",
            token,
            params={"sha": branch, "per_page": 100, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_commits.extend(batch)
        if len(batch) < 100:
            break
    return all_commits


def list_repo_files(owner: str, repo: str, ref: str | None = None, token: str | None = None) -> list[str]:
    if ref:
        branch_resp = _get(f"{GITHUB_API}/repos/{owner}/{repo}/branches/{ref}", token, timeout=15)
        if branch_resp.ok:
            sha = branch_resp.json()["commit"]["sha"]
        else:
            sha = ref
    else:
        repo_info = get_repo_info(owner, repo, token=token)
        default_branch = repo_info.get("default_branch", "main")
        branch_resp = _get(f"{GITHUB_API}/repos/{owner}/{repo}/branches/{default_branch}", token, timeout=15)
        branch_resp.raise_for_status()
        sha = branch_resp.json()["commit"]["sha"]

    tree_resp = _get(
        f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/{sha}",
        token,
        params={"recursive": "1"},
        timeout=30,
    )
    tree_resp.raise_for_status()
    data = tree_resp.json()
    return [item["path"] for item in data.get("tree", []) if item.get("type") == "blob"]


def get_file_content(
    owner: str,
    repo: str,
    path: str,
    ref: str | None = None,
    token: str | None = None,
) -> tuple[bytes, str]:
    import base64
    params = {"ref": ref} if ref else {}
    resp = _get(f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}", token, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    content = base64.b64decode(data.get("content", ""))
    return content, data.get("type", "file")
