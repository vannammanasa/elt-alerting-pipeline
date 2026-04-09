"""
PostgreSQL Raw Loader (AI-2)
=============================
Loads pandas DataFrames into the ``raw`` schema of the elt_warehouse
database.  Appends metadata columns required by the ELT pipeline
(ingestion_timestamp, source_id, batch_id) so that every record can
be traced back to its extraction run.

The loader auto-creates the target table on first run using the
DataFrame's dtypes — no manual DDL needed.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from ..core.database import DatabaseManager
from ..core.exceptions import LoadError

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Type mapping: pandas / numpy → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════════

_PG_TYPE_MAP: dict[str, str] = {
    "object": "TEXT",
    "string": "TEXT",
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP WITH TIME ZONE",
    "datetime64[ns, UTC]": "TIMESTAMP WITH TIME ZONE",
}


def _pg_type(dtype: np.dtype | pd.api.types.CategoricalDtype) -> str:
    """Map a pandas dtype to PostgreSQL column type."""
    key = str(dtype)
    return _PG_TYPE_MAP.get(key, "TEXT")


# ═══════════════════════════════════════════════════════════════════════════
# Loader
# ═══════════════════════════════════════════════════════════════════════════

class RawLoader:
    """
    Loads extracted data into ``raw.<table_name>`` in the warehouse.

    Usage::

        loader = RawLoader(db_manager, source_id="synthetic_v1")
        stats  = loader.load(df, table_name="raw_transactions")
        print(stats)
        # {'rows_loaded': 10000, 'batch_id': 'batch-abc123', ...}

    The loader:

    1. Auto-creates the target table if it does not exist.
    2. Appends three metadata columns to every record:
       - ``_ingestion_timestamp`` (UTC)
       - ``_source_id`` (identifies data origin)
       - ``_batch_id`` (unique per load invocation)
    3. Uses ``psycopg2.extras.execute_values`` for high-throughput
       bulk insertion (~50 k rows/sec on commodity hardware).
    4. Returns a statistics dict for the observability layer.
    """

    METADATA_COLUMNS: list[tuple[str, str]] = [
        ("_ingestion_timestamp", "TIMESTAMP WITH TIME ZONE"),
        ("_source_id", "TEXT"),
        ("_batch_id", "TEXT"),
    ]

    def __init__(self, db: DatabaseManager, source_id: str = "synthetic"):
        """
        Args:
            db:        Initialised ``DatabaseManager`` instance.
            source_id: Label identifying the data source (e.g. "synthetic_v1").
        """
        self._db = db
        self._source_id = source_id

    # ── Public API ───────────────────────────────────────────────────────

    def load(
        self,
        df: pd.DataFrame,
        table_name: str = "raw_transactions",
        schema: str = "raw",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Load a DataFrame into the raw warehouse schema.

        Args:
            df:         Source DataFrame (from SyntheticDataGenerator).
            table_name: Target table name (without schema prefix).
            schema:     Target schema (default ``raw``).
            batch_id:   Optional batch identifier; auto-generated if omitted.

        Returns:
            Dict with load statistics:
                rows_loaded, batch_id, table, ingestion_timestamp,
                source_id, duration_ms

        Raises:
            LoadError: If table creation or data insertion fails.
        """
        batch_id = batch_id or f"batch-{uuid.uuid4().hex[:12]}"
        ingestion_ts = datetime.now(timezone.utc)
        qualified = f"{schema}.{table_name}"
        start = datetime.now(timezone.utc)

        logger.info(
            "Starting raw load",
            extra={
                "table": qualified,
                "rows": len(df),
                "batch_id": batch_id,
                "source_id": self._source_id,
            },
        )

        try:
            # 1. Ensure table exists
            self._ensure_table(df, table_name, schema)

            # 2. Append metadata columns
            load_df = df.copy()
            load_df["_ingestion_timestamp"] = ingestion_ts
            load_df["_source_id"] = self._source_id
            load_df["_batch_id"] = batch_id

            # 3. Bulk insert
            rows_loaded = self._bulk_insert(load_df, table_name, schema)

            duration_ms = int(
                (datetime.now(timezone.utc) - start).total_seconds() * 1000
            )

            stats = {
                "rows_loaded": rows_loaded,
                "batch_id": batch_id,
                "table": qualified,
                "schema": schema,
                "source_id": self._source_id,
                "ingestion_timestamp": ingestion_ts.isoformat(),
                "duration_ms": duration_ms,
                "columns": list(load_df.columns),
            }

            logger.info(
                "Raw load completed",
                extra=stats,
            )
            return stats

        except Exception as exc:
            raise LoadError(
                f"Failed to load data into {qualified}: {exc}",
                details={"batch_id": batch_id, "rows_attempted": len(df)},
            ) from exc

    # ── Table management ─────────────────────────────────────────────────

    def _ensure_table(
        self, df: pd.DataFrame, table_name: str, schema: str
    ) -> None:
        """Create the target table if it does not already exist."""
        columns_sql = []

        # Data columns (from DataFrame)
        for col_name in df.columns:
            pg_type = _pg_type(df[col_name].dtype)
            columns_sql.append(f'    "{col_name}" {pg_type}')

        # Metadata columns
        for meta_name, meta_type in self.METADATA_COLUMNS:
            columns_sql.append(f'    "{meta_name}" {meta_type}')

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n"
            + ",\n".join(columns_sql)
            + "\n);"
        )

        with self._db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

        logger.debug("Table ensured: %s.%s", schema, table_name)

    # ── Bulk insertion ───────────────────────────────────────────────────

    def _bulk_insert(
        self, df: pd.DataFrame, table_name: str, schema: str
    ) -> int:
        """Insert all rows using execute_values for speed."""
        columns = list(df.columns)
        col_list = ", ".join(f'"{c}"' for c in columns)
        template = "(" + ", ".join(["%s"] * len(columns)) + ")"
        insert_sql = f"INSERT INTO {schema}.{table_name} ({col_list}) VALUES %s"

        # Convert DataFrame to list of tuples, handling NaN → None
        records = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False, name=None)
        ]

        with self._db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, insert_sql, records, template=template, page_size=1000
                )
                return len(records)

    # ── Utility ──────────────────────────────────────────────────────────

    def get_row_count(self, table_name: str, schema: str = "raw") -> int:
        """Return current row count for the target table."""
        rows = self._db.execute_warehouse(
            f"SELECT COUNT(*) AS cnt FROM {schema}.{table_name}"
        )
        return rows[0]["cnt"] if rows else 0

    def truncate(self, table_name: str, schema: str = "raw") -> None:
        """Truncate the target table (useful for pipeline reset)."""
        with self._db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
        logger.info("Table truncated: %s.%s", schema, table_name)
