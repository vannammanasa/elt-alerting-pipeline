"""
Pipeline Reset Mechanism (AI-9a)
=================================
Restores the pipeline to a clean state between experimental runs.
Chapter 3 §3.6.5 requires "pipeline reset between runs to eliminate
state carryover."  This module:

    1. Truncates raw, staging, and transformed schema tables
    2. Clears run-specific observability records
    3. Reloads clean baseline data using the synthetic generator
    4. Reverses any schema drift injections

Without this reset, residual failures from the previous run would
contaminate the next experimental condition, invalidating results.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


class PipelineReset:
    """
    Restores the ELT pipeline to a known clean state.

    Usage::

        reset = PipelineReset(db)
        reset.full_reset(reload_data=True, record_count=10000, seed=42)
    """

    # Tables to truncate in each schema (order matters for FK constraints)
    WAREHOUSE_TABLES: dict[str, list[str]] = {
        "transformed": [
            "mart_transactions",
            "mart_daily_summary",
            "mart_customer_summary",
        ],
        "raw": [
            "raw_transactions",
        ],
    }

    # Observability tables to clear per run (not the full history)
    OBSERVABILITY_TABLES: list[str] = [
        "monitoring.quality_check_results",
        "monitoring.alerts",
        "monitoring.injected_failures",
        "monitoring.pipeline_executions",
    ]

    # Known schema drift columns that may have been injected
    DRIFT_COLUMNS: list[dict[str, str]] = [
        {"table": "raw.raw_transactions", "column": "unexpected_column"},
    ]

    # Original columns that may have been dropped by schema drift
    ORIGINAL_COLUMNS: list[dict[str, str]] = [
        {
            "table": "raw.raw_transactions",
            "column": "shipping_country",
            "type": "TEXT",
        },
    ]

    def __init__(self, db: DatabaseManager):
        self._db = db

    def full_reset(
        self,
        reload_data: bool = True,
        record_count: int = 10_000,
        seed: int = 42,
        clear_observability: bool = True,
    ) -> dict[str, Any]:
        """
        Execute a complete pipeline reset.

        Args:
            reload_data:        If True, regenerate and load baseline data.
            record_count:       Number of synthetic records to reload.
            seed:               Random seed for reproducibility.
            clear_observability: If True, clear observability tables.

        Returns:
            Dict with reset statistics.
        """
        start = datetime.now(timezone.utc)
        stats: dict[str, Any] = {"reset_at": start.isoformat()}

        logger.info("=" * 60)
        logger.info("PIPELINE RESET — Starting full reset")
        logger.info("=" * 60)

        # Step 1: Reverse schema drift
        logger.info("Step 1/4: Reversing schema drift injections...")
        drift_stats = self._reverse_schema_drift()
        stats["schema_drift_reversed"] = drift_stats

        # Step 2: Truncate warehouse tables
        logger.info("Step 2/4: Truncating warehouse tables...")
        trunc_stats = self._truncate_warehouse()
        stats["tables_truncated"] = trunc_stats

        # Step 3: Clear observability records
        if clear_observability:
            logger.info("Step 3/4: Clearing observability records...")
            obs_stats = self._clear_observability()
            stats["observability_cleared"] = obs_stats
        else:
            logger.info("Step 3/4: Skipping observability clear (preserved)")
            stats["observability_cleared"] = "skipped"

        # Step 4: Reload baseline data
        if reload_data:
            logger.info("Step 4/4: Reloading baseline data (%d rows, seed=%d)...",
                        record_count, seed)
            load_stats = self._reload_baseline(record_count, seed)
            stats["data_reloaded"] = load_stats
        else:
            logger.info("Step 4/4: Skipping data reload")
            stats["data_reloaded"] = "skipped"

        duration_ms = int(
            (datetime.now(timezone.utc) - start).total_seconds() * 1000
        )
        stats["duration_ms"] = duration_ms

        logger.info("PIPELINE RESET — Complete (%d ms)", duration_ms)
        return stats

    # ── Internal steps ───────────────────────────────────────────────────

    def _reverse_schema_drift(self) -> list[str]:
        """Drop injected columns and restore dropped columns."""
        actions = []

        # Drop any columns that were added by schema drift injection
        for drift in self.DRIFT_COLUMNS:
            try:
                sql = f'ALTER TABLE {drift["table"]} DROP COLUMN IF EXISTS "{drift["column"]}"'
                with self._db.warehouse_conn() as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    conn.autocommit = False
                actions.append(f"dropped {drift['table']}.{drift['column']}")
            except Exception as exc:
                logger.warning("Failed to drop drift column: %s", exc)

        # Restore any columns that were removed by schema drift injection
        for orig in self.ORIGINAL_COLUMNS:
            try:
                sql = (
                    f'ALTER TABLE {orig["table"]} '
                    f'ADD COLUMN IF NOT EXISTS "{orig["column"]}" {orig["type"]}'
                )
                with self._db.warehouse_conn() as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    conn.autocommit = False
                actions.append(f"restored {orig['table']}.{orig['column']}")
            except Exception as exc:
                logger.warning("Failed to restore column: %s", exc)

        return actions

    def _truncate_warehouse(self) -> dict[str, int]:
        """Truncate all warehouse tables and return row counts cleared."""
        stats = {}
        with self._db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                for schema, tables in self.WAREHOUSE_TABLES.items():
                    for table in tables:
                        qualified = f"{schema}.{table}"
                        try:
                            cur.execute(f"SELECT count(*) FROM {qualified}")
                            count = cur.fetchone()[0]
                            cur.execute(f"TRUNCATE TABLE {qualified} CASCADE")
                            stats[qualified] = count
                            logger.debug("Truncated %s (%d rows)", qualified, count)
                        except Exception as exc:
                            # Table may not exist yet on first run
                            logger.debug("Skip truncate %s: %s", qualified, exc)
                            conn.rollback()
                            stats[qualified] = 0
        return stats

    def _clear_observability(self) -> dict[str, int]:
        """Clear observability tables for a fresh experimental run."""
        stats = {}
        with self._db.observability_conn() as conn:
            with conn.cursor() as cur:
                for table in self.OBSERVABILITY_TABLES:
                    try:
                        cur.execute(f"SELECT count(*) FROM {table}")
                        count = cur.fetchone()[0]
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                        stats[table] = count
                        logger.debug("Cleared %s (%d rows)", table, count)
                    except Exception as exc:
                        logger.debug("Skip clear %s: %s", table, exc)
                        conn.rollback()
                        stats[table] = 0
        return stats

    def _reload_baseline(self, record_count: int, seed: int) -> dict[str, Any]:
        """Regenerate synthetic data and load into raw schema."""
        from ..extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig
        from ..loaders.raw_loader import RawLoader

        config = SyntheticDataConfig(record_count=record_count, seed=seed)
        generator = SyntheticDataGenerator(config)
        df = generator.generate()

        loader = RawLoader(self._db, source_id="synthetic_v1")
        return loader.load(df, table_name="raw_transactions")
