"""
Database Connection Manager
============================
Thread-safe PostgreSQL connection management with connection pooling.
Provides separate pools for the warehouse and observability databases
matching the Deliverable 1 schema (elt_warehouse + observability).

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
import psycopg2.extras
import psycopg2.pool

from .config import DatabaseConfig
from .exceptions import PipelineError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages PostgreSQL connections for both the data warehouse
    and the observability database.

    Usage::

        db = DatabaseManager(config.db)
        with db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

        with db.observability_conn() as conn:
            ...

        db.close()
    """

    def __init__(self, config: DatabaseConfig, min_conn: int = 1, max_conn: int = 5):
        self._config = config
        self._warehouse_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            dbname=config.warehouse_db,
        )
        self._observability_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            dbname=config.observability_db,
        )
        logger.info(
            "Database pools initialised",
            extra={
                "warehouse_db": config.warehouse_db,
                "observability_db": config.observability_db,
                "host": config.host,
            },
        )

    # ── Context-managed connections ──────────────────────────────────────

    @contextmanager
    def warehouse_conn(self) -> Generator[Any, None, None]:
        """Yield a warehouse database connection (auto-commit on success)."""
        conn = self._warehouse_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._warehouse_pool.putconn(conn)

    @contextmanager
    def observability_conn(self) -> Generator[Any, None, None]:
        """Yield an observability database connection (auto-commit on success)."""
        conn = self._observability_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._observability_pool.putconn(conn)

    # ── Convenience helpers ──────────────────────────────────────────────

    def execute_warehouse(self, sql: str, params: tuple | None = None) -> list[dict]:
        """Execute a query against the warehouse and return rows as dicts."""
        with self.warehouse_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                if cur.description:
                    return [dict(row) for row in cur.fetchall()]
                return []

    def execute_observability(self, sql: str, params: tuple | None = None) -> list[dict]:
        """Execute a query against the observability DB and return rows as dicts."""
        with self.observability_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                if cur.description:
                    return [dict(row) for row in cur.fetchall()]
                return []

    def test_connections(self) -> dict[str, bool]:
        """
        Test connectivity to both databases.

        Returns:
            Dict with 'warehouse' and 'observability' boolean results.
        """
        results = {}
        for name, pool in [
            ("warehouse", self._warehouse_pool),
            ("observability", self._observability_pool),
        ]:
            try:
                conn = pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                pool.putconn(conn)
                results[name] = True
                logger.info(f"Connection test passed: {name}")
            except Exception as exc:
                results[name] = False
                logger.error(f"Connection test failed: {name}", exc_info=exc)
        return results

    def close(self) -> None:
        """Close all connection pools."""
        self._warehouse_pool.closeall()
        self._observability_pool.closeall()
        logger.info("All database connection pools closed")
