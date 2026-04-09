"""
Failure Injection Engine (AI-8)
================================
Introduces controlled failures into the ELT pipeline for experimental
evaluation of alerting strategies.  Each injected failure is recorded
in the observability database with a precise timestamp, enabling
accurate MTTD computation in Phase 7.

Supported failure types (Chapter 3, Table 3.4):

    1. null_pk         — NULL injection on primary key columns     → CRITICAL
    2. null_minor      — NULL injection on non-essential columns   → WARNING
    3. schema_drift    — Add/remove/rename columns                 → CRITICAL
    4. volume_anomaly  — Drop rows below threshold                 → CRITICAL
    5. freshness_delay — Backdate _ingestion_timestamp             → WARNING
    6. duplicate       — Insert duplicate records                  → WARNING

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import psycopg2.extras

from ..core.database import DatabaseManager
from ..core.exceptions import FailureInjectionError

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Enums
# ═══════════════════════════════════════════════════════════════════════════

class FailureType(str, Enum):
    """The six failure types defined in Chapter 3, Table 3.4."""
    NULL_PK         = "null_pk"
    NULL_MINOR      = "null_minor"
    SCHEMA_DRIFT    = "schema_drift"
    VOLUME_ANOMALY  = "volume_anomaly"
    FRESHNESS_DELAY = "freshness_delay"
    DUPLICATE       = "duplicate"


class ExpectedSeverity(str, Enum):
    """Expected severity classification per failure type."""
    CRITICAL = "critical"
    WARNING  = "warning"


# Mapping from failure type → expected severity (ground truth for validation)
FAILURE_SEVERITY_MAP: dict[FailureType, ExpectedSeverity] = {
    FailureType.NULL_PK:         ExpectedSeverity.CRITICAL,
    FailureType.NULL_MINOR:      ExpectedSeverity.WARNING,
    FailureType.SCHEMA_DRIFT:    ExpectedSeverity.CRITICAL,
    FailureType.VOLUME_ANOMALY:  ExpectedSeverity.CRITICAL,
    FailureType.FRESHNESS_DELAY: ExpectedSeverity.WARNING,
    FailureType.DUPLICATE:       ExpectedSeverity.WARNING,
}


# ═══════════════════════════════════════════════════════════════════════════
# Injection record
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class InjectionRecord:
    """Captures details of an injected failure for observability."""
    injection_id: str
    failure_type: FailureType
    expected_severity: ExpectedSeverity
    target_table: str
    target_column: str | None
    rows_affected: int
    injected_at: datetime
    details: dict[str, Any]


# ═══════════════════════════════════════════════════════════════════════════
# Engine
# ═══════════════════════════════════════════════════════════════════════════

class FailureInjectionEngine:
    """
    Injects controlled failures into the PostgreSQL raw schema and
    records each injection in the observability database.

    Usage::

        engine = FailureInjectionEngine(db)
        record = engine.inject(
            failure_type=FailureType.NULL_PK,
            table="raw_transactions",
            column="transaction_id",
            row_count=50,
        )
        print(record.injection_id, record.rows_affected)
    """

    def __init__(self, db: DatabaseManager, schema: str = "raw"):
        self._db = db
        self._schema = schema

    # ── Public API ───────────────────────────────────────────────────────

    def inject(
        self,
        failure_type: FailureType,
        table: str = "raw_transactions",
        column: str | None = None,
        row_count: int = 50,
        **kwargs: Any,
    ) -> InjectionRecord:
        """
        Inject a single controlled failure.

        Args:
            failure_type: One of the six FailureType enums.
            table:        Target table in raw schema.
            column:       Target column (required for null injections).
            row_count:    Number of rows to affect.
            **kwargs:     Type-specific parameters.

        Returns:
            InjectionRecord with full details.

        Raises:
            FailureInjectionError: If injection fails.
        """
        injection_id = f"inj-{uuid.uuid4().hex[:12]}"
        injected_at = datetime.now(timezone.utc)

        logger.info(
            "Injecting failure",
            extra={
                "injection_id": injection_id,
                "failure_type": failure_type.value,
                "table": table,
                "column": column,
                "row_count": row_count,
            },
        )

        dispatch = {
            FailureType.NULL_PK:         self._inject_null_pk,
            FailureType.NULL_MINOR:      self._inject_null_minor,
            FailureType.SCHEMA_DRIFT:    self._inject_schema_drift,
            FailureType.VOLUME_ANOMALY:  self._inject_volume_anomaly,
            FailureType.FRESHNESS_DELAY: self._inject_freshness_delay,
            FailureType.DUPLICATE:       self._inject_duplicate,
        }

        handler = dispatch.get(failure_type)
        if handler is None:
            raise FailureInjectionError(f"Unknown failure type: {failure_type}")

        try:
            affected, details = handler(
                table=table, column=column, row_count=row_count, **kwargs
            )
        except Exception as exc:
            raise FailureInjectionError(
                f"Injection failed: {failure_type.value} on {table}.{column}: {exc}",
                details={"injection_id": injection_id},
            ) from exc

        record = InjectionRecord(
            injection_id=injection_id,
            failure_type=failure_type,
            expected_severity=FAILURE_SEVERITY_MAP[failure_type],
            target_table=f"{self._schema}.{table}",
            target_column=column,
            rows_affected=affected,
            injected_at=injected_at,
            details=details,
        )

        # Persist to observability database
        self._record_injection(record)

        logger.info(
            "Failure injected successfully",
            extra={
                "injection_id": injection_id,
                "rows_affected": affected,
                "expected_severity": record.expected_severity.value,
            },
        )

        return record

    # ── Injection handlers ───────────────────────────────────────────────

    def _inject_null_pk(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Set primary key column to NULL on random rows → CRITICAL."""
        col = column or "transaction_id"
        sql = f"""
            UPDATE {self._schema}.{table}
            SET "{col}" = NULL
            WHERE ctid IN (
                SELECT ctid FROM {self._schema}.{table}
                WHERE "{col}" IS NOT NULL
                ORDER BY random()
                LIMIT %s
            )
        """
        affected = self._execute_update(sql, (row_count,))
        return affected, {"column": col, "action": "set_null_on_pk"}

    def _inject_null_minor(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Set non-essential column to NULL on random rows → WARNING."""
        col = column or "shipping_country"
        sql = f"""
            UPDATE {self._schema}.{table}
            SET "{col}" = NULL
            WHERE ctid IN (
                SELECT ctid FROM {self._schema}.{table}
                WHERE "{col}" IS NOT NULL
                ORDER BY random()
                LIMIT %s
            )
        """
        affected = self._execute_update(sql, (row_count,))
        return affected, {"column": col, "action": "set_null_on_minor"}

    def _inject_schema_drift(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Add, remove, or rename a column → CRITICAL."""
        action = kwargs.get("drift_action", "add")
        col = column or "unexpected_column"

        if action == "add":
            sql = f'ALTER TABLE {self._schema}.{table} ADD COLUMN IF NOT EXISTS "{col}" TEXT'
            self._execute_ddl(sql)
            return 0, {"column": col, "action": "column_added"}

        elif action == "drop":
            sql = f'ALTER TABLE {self._schema}.{table} DROP COLUMN IF EXISTS "{col}"'
            self._execute_ddl(sql)
            return 0, {"column": col, "action": "column_dropped"}

        elif action == "rename":
            new_name = kwargs.get("new_name", f"{col}_renamed")
            sql = f'ALTER TABLE {self._schema}.{table} RENAME COLUMN "{col}" TO "{new_name}"'
            self._execute_ddl(sql)
            return 0, {"column": col, "new_name": new_name, "action": "column_renamed"}

        else:
            raise FailureInjectionError(f"Unknown drift action: {action}")

    def _inject_volume_anomaly(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Delete rows to simulate >50% volume drop → CRITICAL."""
        drop_pct = kwargs.get("drop_percentage", 60)

        # Get current count
        result = self._db.execute_warehouse(
            f"SELECT count(*) AS cnt FROM {self._schema}.{table}"
        )
        current = result[0]["cnt"] if result else 0
        rows_to_delete = int(current * drop_pct / 100)

        sql = f"""
            DELETE FROM {self._schema}.{table}
            WHERE ctid IN (
                SELECT ctid FROM {self._schema}.{table}
                ORDER BY random()
                LIMIT %s
            )
        """
        affected = self._execute_update(sql, (rows_to_delete,))
        return affected, {
            "original_count": current,
            "deleted": affected,
            "remaining": current - affected,
            "drop_percentage": drop_pct,
            "action": "volume_drop",
        }

    def _inject_freshness_delay(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Backdate _ingestion_timestamp to simulate stale data → WARNING."""
        delay_hours = kwargs.get("delay_hours", 48)

        sql = f"""
            UPDATE {self._schema}.{table}
            SET "_ingestion_timestamp" = "_ingestion_timestamp" - interval '%s hours'
        """
        affected = self._execute_update(sql, (delay_hours,))
        return affected, {
            "delay_hours": delay_hours,
            "action": "backdate_ingestion_timestamp",
        }

    def _inject_duplicate(
        self, table: str, column: str | None, row_count: int, **kwargs
    ) -> tuple[int, dict]:
        """Copy random rows back into the table → WARNING."""
        sql = f"""
            INSERT INTO {self._schema}.{table}
            SELECT * FROM {self._schema}.{table}
            ORDER BY random()
            LIMIT %s
        """
        affected = self._execute_update(sql, (row_count,))
        return affected, {"duplicates_inserted": affected, "action": "duplicate_rows"}

    # ── Database helpers ─────────────────────────────────────────────────

    def _execute_update(self, sql: str, params: tuple) -> int:
        """Execute an UPDATE/DELETE/INSERT and return rows affected."""
        with self._db.warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount

    def _execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (ALTER TABLE, etc.)."""
        with self._db.warehouse_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.autocommit = False

    # ── Observability recording ──────────────────────────────────────────

    def _record_injection(self, record: InjectionRecord) -> None:
        """Persist injection record to the observability database."""
        import json

        sql = """
            INSERT INTO monitoring.injected_failures
                (injection_id, failure_type, expected_severity,
                 target_table, target_column, rows_affected,
                 injected_at, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        record.injection_id,
                        record.failure_type.value,
                        record.expected_severity.value,
                        record.target_table,
                        record.target_column,
                        record.rows_affected,
                        record.injected_at,
                        json.dumps(record.details, default=str),
                    ))
            logger.debug("Injection recorded: %s", record.injection_id)
        except Exception as exc:
            # Don't fail the injection if observability write fails
            logger.warning(
                "Failed to record injection in observability DB: %s", exc
            )
