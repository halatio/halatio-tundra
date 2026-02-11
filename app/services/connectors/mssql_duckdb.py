"""MS SQL Server connector using DuckDB ADBC driver"""

import duckdb
import logging
from typing import Any, Dict
from .duckdb_base import DuckDBBaseConnector, _make_duckdb_config
import anyio

logger = logging.getLogger(__name__)


class MSSQLDuckDBConnector(DuckDBBaseConnector):
    """MS SQL Server connector via DuckDB ADBC integration"""

    def __init__(self, credentials: Dict[str, Any]) -> None:
        super().__init__(credentials)
        self._host = credentials["host"]
        self._database = credentials["database"]
        self._username = credentials["username"]
        self._password = credentials["password"]
        self._port = credentials.get("port", 1433)

    def _get_db_alias(self) -> str:
        return "mssql"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        conn.execute(f"""
            CREATE SECRET mssql_secret (
                TYPE adbc,
                driver 'adbc_driver_mssql',
                host '{self._host}',
                port '{self._port}',
                database '{self._database}',
                username '{self._username}',
                password '{self._password}'
            )
        """)
        logger.info(f"Created ADBC secret for MS SQL Server at {self._host}:{self._port}/{self._database}")
        return "mssql"

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def _test() -> bool:
                conn = duckdb.connect(":memory:", config=_make_duckdb_config())
                try:
                    conn.execute("SET autoinstall_known_extensions = false")
                    conn.execute("SET autoload_known_extensions = true")
                    conn.execute(f"""
                        CREATE SECRET mssql_secret (
                            TYPE adbc,
                            driver 'adbc_driver_mssql',
                            host '{self._host}',
                            port '{self._port}',
                            database '{self._database}',
                            username '{self._username}',
                            password '{self._password}'
                        )
                    """)
                    conn.execute("SELECT 1")
                    return True
                finally:
                    conn.close()

            await anyio.to_thread.run_sync(_test)
            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "MS SQL Server",
                    "engine": "duckdb-adbc-mssql",
                    "host": self._host,
                    "database": self._database,
                },
            }
        except Exception as e:
            logger.error(f"MS SQL Server connection test failed: {e}")
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e),
                "note": "Ensure ADBC driver is installed: pip install adbc-driver-mssql",
            }
