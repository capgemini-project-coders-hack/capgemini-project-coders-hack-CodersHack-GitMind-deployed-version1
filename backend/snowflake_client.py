"""
snowflake_client.py — Snowflake connection wrapper for GitMind
================================================================
STATUS: PLACEHOLDER — this file was not present in the uploaded project
and has been stubbed out so the rest of the codebase imports and runs.

`backend/main.py` expects a `SnowflakeClient` class with:
    - __init__(self, config: backend.config.SnowflakeConfig)
    - execute(self, sql: str, params: tuple | None = None) -> list[dict]
    - close(self) -> None

Fill in the real connection + query logic below (or replace this file
entirely with your original implementation). The shape of `execute()`
must keep returning a list of plain dicts (one per row) because
`backend/main.py`'s `SnowflakeDetails` class consumes it directly.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.config import SnowflakeConfig

log = logging.getLogger("gitmind.snowflake")


class SnowflakeClient:
    """Thin wrapper around `snowflake-connector-python`.

    NOTE: connection is lazy — it only opens on first `execute()` call,
    so importing/instantiating this class never fails even if Snowflake
    is unreachable at startup (main.py relies on this to degrade
    gracefully into demo mode).
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        self._config = config
        self._conn = None

    def _connect(self):
        if self._conn is not None:
            return self._conn

        import snowflake.connector

        connect_kwargs: dict[str, Any] = {
            "account": self._config.account,
            "user": self._config.user,
            "warehouse": self._config.warehouse,
            "database": self._config.database,
            "schema": self._config.schema,
            "role": self._config.role,
            # Without explicit timeouts, snowflake-connector-python's defaults
            # are login_timeout=120s and network_timeout/socket_timeout=None
            # (no timeout at all). A firewalled/paused/unreachable account
            # would otherwise hang the first request for minutes instead of
            # degrading gracefully like the rest of this codebase does.
            "login_timeout": 10,
            "network_timeout": 10,
            "socket_timeout": 10,
        }

        if self._config.private_key_path:
            # Key-pair auth takes precedence over password, per config.py
            with open(self._config.private_key_path, "rb") as key_file:
                from cryptography.hazmat.primitives import serialization

                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=(self._config.private_key_passphrase or None).encode()
                    if self._config.private_key_passphrase
                    else None,
                )
                der_key = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            connect_kwargs["private_key"] = der_key
        else:
            connect_kwargs["password"] = self._config.password

        self._conn = snowflake.connector.connect(**connect_kwargs)
        return self._conn

    def execute(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """Run a query and return rows as a list of dicts."""
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                log.warning("Error closing Snowflake connection", exc_info=True)
            finally:
                self._conn = None
