"""
github_api.py — Thin wrapper around the GitHub REST API for GitMind
=======================================================================
STATUS: PLACEHOLDER — this file was not present in the uploaded project
and has been stubbed out so the rest of the codebase imports and runs.

`backend/main.py` expects these module-level functions:

    parse_github_url(url: str) -> tuple[owner, repo, path, ref]
    get_repo_info(owner, repo, token=None) -> dict          # needs "default_branch"
    list_branches(owner, repo, token=None) -> list[dict]    # needs "name"
    list_commits(owner, repo, branch, per_page=30, page=1, token=None) -> list[dict]
    get_commit_detail(owner, repo, sha, token=None) -> dict # needs "files": [{"patch": ...}]
    list_repo_files(owner, repo, ref=None, token=None) -> list[dict]
    get_file_content(owner, repo, path, ref=None, token=None) -> tuple[bytes, str]  # (raw, mime)

All functions call the public GitHub REST API directly via `requests`.
GITHUB_TOKEN (read from backend/config.py's GitHubConfig, or passed in
directly by main.py via `_os.getenv("GITHUB_TOKEN")`) is optional —
without it you get the public unauthenticated rate limit (60 req/hr).
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

import requests

GITHUB_API = "https://api.github.com"


def _headers(token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def parse_github_url(url: str) -> tuple[str, str, str, str]:
    """Parse a GitHub URL into (owner, repo, path, ref).

    Supports:
        https://github.com/owner/repo
        https://github.com/owner/repo/tree/<ref>/<path>
        https://github.com/owner/repo/blob/<ref>/<path>
    """
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
    resp = requests.get(f"{GITHUB_API}/repos/{owner}/{repo}", headers=_headers(token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_branches(owner: str, repo: str, token: str | None = None) -> list[dict[str, Any]]:
    resp = requests.get(f"{GITHUB_API}/repos/{owner}/{repo}/branches", headers=_headers(token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_commits(
    owner: str,
    repo: str,
    branch: str,
    per_page: int = 30,
    page: int = 1,
    token: str | None = None,
) -> list[dict[str, Any]]:
    resp = requests.get(
        f"{GITHUB_API}/repos/{owner}/{repo}/commits",
        params={"sha": branch, "per_page": per_page, "page": page},
        headers=_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_commit_detail(owner: str, repo: str, sha: str, token: str | None = None) -> dict[str, Any]:
    resp = requests.get(f"{GITHUB_API}/repos/{owner}/{repo}/commits/{sha}", headers=_headers(token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_all_commits(
    owner: str,
    repo: str,
    branch: str,
    token: str | None = None,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    """Fetch ALL commits on a branch (up to max_pages * 100)."""
    all_commits: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        resp = requests.get(
            f"{GITHUB_API}/repos/{owner}/{repo}/commits",
            params={"sha": branch, "per_page": 100, "page": page},
            headers=_headers(token),
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
    """Return ALL file paths in the repo recursively using the git trees API."""
    # First resolve the SHA for the given ref (or default branch)
    if ref:
        branch_resp = requests.get(
            f"{GITHUB_API}/repos/{owner}/{repo}/branches/{ref}",
            headers=_headers(token),
            timeout=15,
        )
        if branch_resp.ok:
            sha = branch_resp.json()["commit"]["sha"]
        else:
            # ref might be a commit SHA already
            sha = ref
    else:
        repo_info = get_repo_info(owner, repo, token=token)
        default_branch = repo_info.get("default_branch", "main")
        branch_resp = requests.get(
            f"{GITHUB_API}/repos/{owner}/{repo}/branches/{default_branch}",
            headers=_headers(token),
            timeout=15,
        )
        branch_resp.raise_for_status()
        sha = branch_resp.json()["commit"]["sha"]

    # Use git trees API with recursive=1 to get ALL files in one call
    tree_resp = requests.get(
        f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/{sha}",
        params={"recursive": "1"},
        headers=_headers(token),
        timeout=30,
    )
    tree_resp.raise_for_status()
    data = tree_resp.json()
    # Return only blob (file) paths, not trees (dirs)
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
    resp = requests.get(
        f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}",
        params=params,
        headers=_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    content = base64.b64decode(data.get("content", ""))
    return content, data.get("type", "file")
