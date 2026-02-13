"""PostgreSQL connector using DuckDB native postgres scanner"""

import duckdb
import logging
from typing import Any, Dict
from urllib.parse import quote
from .duckdb_base import DuckDBBaseConnector, _make_duckdb_config
import anyio

logger = logging.getLogger(__name__)


class PostgresDuckDBConnector(DuckDBBaseConnector):
    """PostgreSQL connector via DuckDB postgres extension"""

    def __init__(self, credentials: Dict[str, Any]) -> None:
        super().__init__(credentials)
        encoded_username = quote(credentials["username"], safe="")
        encoded_password = quote(credentials["password"], safe="")
        self._conn_str = (
            f"postgresql://{encoded_username}:{encoded_password}"
            f"@{credentials['host']}:{credentials.get('port', 5432)}"
            f"/{credentials['database']}"
        )

    def _get_db_alias(self) -> str:
        return "pg"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        conn.execute("LOAD postgres")
        conn.execute(f"ATTACH '{self._conn_str}' AS pg (TYPE postgres)")
        logger.info("Attached PostgreSQL database as 'pg'")
        return "pg"

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def _test() -> int:
                conn = duckdb.connect(":memory:", config=_make_duckdb_config())
                try:
                    conn.execute("SET autoinstall_known_extensions = false")
                    conn.execute("SET autoload_known_extensions = true")
                    conn.execute("LOAD postgres")
                    conn.execute(f"ATTACH '{self._conn_str}' AS pg (TYPE postgres)")
                    row = conn.execute(
                        "SELECT COUNT(*) FROM pg.information_schema.tables"
                    ).fetchone()
                    return row[0] if row else 0
                finally:
                    conn.close()

            table_count = await anyio.to_thread.run_sync(_test)
            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "PostgreSQL",
                    "engine": "duckdb-postgres-scanner",
                    "table_count": table_count,
                },
            }
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            return {"success": False, "error": "connection_failed", "message": str(e)}
