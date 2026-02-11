"""SQLite connector using DuckDB native support"""

import duckdb
import logging
from typing import Any, Dict
from .duckdb_base import DuckDBBaseConnector, _make_duckdb_config
import anyio

logger = logging.getLogger(__name__)


class SQLiteDuckDBConnector(DuckDBBaseConnector):
    """SQLite connector via DuckDB's built-in SQLite support"""

    def __init__(self, credentials: Dict[str, Any]) -> None:
        super().__init__(credentials)
        self._db_path = credentials.get("database") or credentials.get("file_path")
        if not self._db_path:
            raise ValueError("SQLite requires 'database' or 'file_path' in credentials")

    def _get_db_alias(self) -> str:
        return "sqlite"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        conn.execute(f"ATTACH '{self._db_path}' AS sqlite (TYPE sqlite)")
        logger.info(f"Attached SQLite database from '{self._db_path}' as 'sqlite'")
        return "sqlite"

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def _test() -> int:
                conn = duckdb.connect(":memory:", config=_make_duckdb_config())
                try:
                    conn.execute("SET autoinstall_known_extensions = false")
                    conn.execute("SET autoload_known_extensions = true")
                    conn.execute(f"ATTACH '{self._db_path}' AS sqlite (TYPE sqlite)")
                    row = conn.execute(
                        "SELECT COUNT(*) FROM sqlite.sqlite_master WHERE type='table'"
                    ).fetchone()
                    return row[0] if row else 0
                finally:
                    conn.close()

            table_count = await anyio.to_thread.run_sync(_test)
            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "SQLite",
                    "engine": "duckdb-native",
                    "table_count": table_count,
                    "database_path": self._db_path,
                },
            }
        except Exception as e:
            logger.error(f"SQLite connection test failed: {e}")
            return {"success": False, "error": "connection_failed", "message": str(e)}
