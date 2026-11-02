"""dbt-duckdb plugin: hand the PII hashing salt to DuckDB without it ever
appearing in SQL.

The salt (secret key) comes from the HMIS_HASH_SALT environment variable
and is stored in a DuckDB session variable on every connection and cursor
dbt opens. Models read it with ``getvariable('hmis_hash_salt')`` through the
``hash_pii`` macro, so compiled SQL, logs and the dbt docs site only ever
show the variable name, never the secret.

Enabled in profiles.yml (``plugins: [{module: hmis_pii}]``,
``module_paths: [plugins]``).
"""

from __future__ import annotations

import os

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    def initialize(self, plugin_config):
        self.salt = os.environ.get("HMIS_HASH_SALT", "")
        if len(self.salt) < 16:
            raise RuntimeError(
                "HMIS_HASH_SALT must be set to a secret of at least 16 characters "
                "(see .env.example). It keys the hashing of personal data in staging."
            )

    def _set(self, conn) -> None:
        escaped = self.salt.replace("'", "''")
        conn.execute(f"SET VARIABLE hmis_hash_salt = '{escaped}'")

    def configure_connection(self, conn):
        self._set(conn)

    def configure_cursor(self, cursor):
        # Session variables are not inherited by cursors, so set them again.
        self._set(cursor)
