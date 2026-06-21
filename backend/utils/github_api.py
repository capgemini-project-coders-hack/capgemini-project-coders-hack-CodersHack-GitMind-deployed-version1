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


def list_repo_files(owner: str, repo: str, ref: str | None = None, token: str | None = None) -> list[dict[str, Any]]:
    params = {"ref": ref} if ref else {}
    resp = requests.get(
        f"{GITHUB_API}/repos/{owner}/{repo}/contents/",
        params=params,
        headers=_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


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
