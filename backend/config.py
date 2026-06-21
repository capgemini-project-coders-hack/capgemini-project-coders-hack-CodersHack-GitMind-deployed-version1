"""
config.py — Centralised configuration for GitMind
===================================================
All connection settings are resolved from environment variables.
Missing required variables raise GitMindConfigError immediately so the
caller gets a precise, actionable error message instead of a cryptic
AttributeError or NoneType failure buried deep in a library.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv:
    load_dotenv()


class GitMindConfigError(Exception):
    """Raised when a required environment variable is absent or invalid."""


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise GitMindConfigError(
            f"Required environment variable '{name}' is not set or is empty. "
            f"Set it before starting GitMind."
        )
    return value


def _optional_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str
    database: str = "neo4j"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        return cls(
            uri=_require_env("NEO4J_URI"),
            user=_require_env("NEO4J_USER"),
            password=_require_env("NEO4J_PASSWORD"),
            database=_optional_env("NEO4J_DATABASE", "neo4j"),
        )


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str
    # Optional private-key auth (takes precedence over password when set)
    private_key_path: str = ""
    private_key_passphrase: str = ""

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        return cls(
            account=_require_env("SNOWFLAKE_ACCOUNT"),
            user=_require_env("SNOWFLAKE_USER"),
            password=_optional_env("SNOWFLAKE_PASSWORD"),          # optional if using key-pair
            warehouse=_require_env("SNOWFLAKE_WAREHOUSE"),
            database=_require_env("SNOWFLAKE_DATABASE"),
            schema=_require_env("SNOWFLAKE_SCHEMA"),
            role=_optional_env("SNOWFLAKE_ROLE", "PUBLIC"),
            private_key_path=_optional_env("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=_optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )

    def validate(self) -> None:
        """Raise GitMindConfigError if neither password nor private key is configured."""
        if not self.password and not self.private_key_path:
            raise GitMindConfigError(
                "Snowflake auth is incomplete: set either SNOWFLAKE_PASSWORD "
                "or SNOWFLAKE_PRIVATE_KEY_PATH."
            )


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SlackConfig:
    bot_token: str
    signing_secret: str
    default_channels: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "SlackConfig":
        raw_channels = _optional_env("SLACK_DEFAULT_CHANNELS", "#incidents,#engineering")
        return cls(
            bot_token=_require_env("SLACK_BOT_TOKEN"),
            signing_secret=_optional_env("SLACK_SIGNING_SECRET"),
            default_channels=[c.strip() for c in raw_channels.split(",") if c.strip()],
        )


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JiraConfig:
    url: str
    user: str
    api_token: str
    default_project: str

    @classmethod
    def from_env(cls) -> "JiraConfig":
        return cls(
            url=_require_env("JIRA_URL"),
            user=_require_env("JIRA_USER"),
            api_token=_require_env("JIRA_API_TOKEN"),
            default_project=_optional_env("JIRA_DEFAULT_PROJECT", "PLAT"),
        )


# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GitHubConfig:
    token: str
    org: str
    default_repos: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "GitHubConfig":
        raw_repos = _optional_env("GITHUB_DEFAULT_REPOS", "")
        return cls(
            token=_require_env("GITHUB_TOKEN"),
            org=_optional_env("GITHUB_ORG"),
            default_repos=[r.strip() for r in raw_repos.split(",") if r.strip()],
        )


# ---------------------------------------------------------------------------
# Google AI Studio / LLM
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    model: str = "gemini-1.5-pro"
    max_tokens: int = 4096
    temperature: float = 0.0
    max_agent_iterations: int = 10

    @classmethod
    def from_env(cls) -> "LLMConfig":
        return cls(
            api_key=_require_env("GOOGLE_API_KEY"),
            model=_optional_env("GITMIND_MODEL", "gemini-1.5-pro"),
            max_tokens=int(_optional_env("GITMIND_MAX_TOKENS", "4096")),
            temperature=float(_optional_env("GITMIND_TEMPERATURE", "0")),
            max_agent_iterations=int(_optional_env("GITMIND_MAX_ITERATIONS", "10")),
        )


# ---------------------------------------------------------------------------
# Top-level config bundle
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GitMindConfig:
    neo4j: Neo4jConfig
    snowflake: SnowflakeConfig
    slack: SlackConfig
    jira: JiraConfig
    github: GitHubConfig
    llm: LLMConfig

    @classmethod
    def from_env(cls) -> "GitMindConfig":
        """
        Build the full config from environment variables.
        Raises GitMindConfigError with a clear message for every missing variable.
        Collect ALL errors before raising so the user can fix everything at once.
        """
        errors: list[str] = []
        results: dict = {}

        for key, loader in [
            ("neo4j", Neo4jConfig.from_env),
            ("snowflake", SnowflakeConfig.from_env),
            ("slack", SlackConfig.from_env),
            ("jira", JiraConfig.from_env),
            ("github", GitHubConfig.from_env),
            ("llm", LLMConfig.from_env),
        ]:
            try:
                results[key] = loader()
            except GitMindConfigError as exc:
                errors.append(str(exc))

        if errors:
            bullet_list = "\n  • ".join(errors)
            raise GitMindConfigError(
                f"GitMind configuration is incomplete. Fix the following:\n  • {bullet_list}"
            )

        # Extra cross-field validation
        try:
            results["snowflake"].validate()
        except GitMindConfigError as exc:
            raise GitMindConfigError(str(exc)) from exc

        return cls(**results)
