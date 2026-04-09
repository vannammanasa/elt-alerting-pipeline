"""
Observability Writer (AI-12b)
==============================
Persists every alert event — whether delivered via email (critical)
or logged (warning) — to the PostgreSQL observability database.
This is the bridge between the alerting engine and the metrics
calculator (Phase 7, AI-15).

Records are written to two tables:
    monitoring.alerts            — individual alert records
    monitoring.quality_check_results — dbt test outcomes

Without this module, Phase 7 has no data to analyse.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class AlertRecord:
    """A single alert event to be persisted."""
    alert_id: str = ""
    execution_id: str = ""
    test_name: str = ""
    severity: str = "warning"              # "critical" or "warning"
    channel: str = "log"                   # "email" or "log"
    table_name: str = ""
    column_name: str = ""
    message: str = ""
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    delivered_at: datetime | None = None
    is_actionable: bool = False
    injection_id: str | None = None        # links to injected_failures
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.alert_id:
            self.alert_id = f"alert-{uuid.uuid4().hex[:12]}"
        self.is_actionable = self.severity == "critical"


@dataclass
class QualityCheckRecord:
    """A dbt test result to be persisted."""
    check_id: str = ""
    execution_id: str = ""
    test_name: str = ""
    test_type: str = ""                    # "not_null", "unique", etc.
    model_name: str = ""
    column_name: str = ""
    status: str = "pass"                   # "pass", "fail", "warn", "error"
    severity_config: str = "warn"          # dbt config: "error" or "warn"
    rows_affected: int = 0
    execution_time_ms: int = 0
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.check_id:
            self.check_id = f"chk-{uuid.uuid4().hex[:12]}"


class ObservabilityWriter:
    """
    Writes alert records and quality check results to the
    observability database.

    Usage::

        writer = ObservabilityWriter(db)
        writer.write_alert(AlertRecord(
            test_name="not_null_transaction_id",
            severity="critical",
            channel="email",
            ...
        ))
        writer.write_quality_check(QualityCheckRecord(
            test_name="not_null_transaction_id",
            status="fail",
            ...
        ))
    """

    def __init__(self, db: DatabaseManager):
        self._db = db

    # ── Alert records ────────────────────────────────────────────────────

    def write_alert(self, record: AlertRecord) -> bool:
        """
        Persist a single alert record to monitoring.alerts.

        Returns:
            True if write succeeded, False otherwise.
        """
        sql = """
            INSERT INTO monitoring.alerts
                (alert_id, execution_id, test_name, severity, channel,
                 table_name, column_name, message, emitted_at,
                 delivered_at, is_actionable, injection_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        record.alert_id,
                        record.execution_id,
                        record.test_name,
                        record.severity,
                        record.channel,
                        record.table_name,
                        record.column_name,
                        record.message,
                        record.emitted_at,
                        record.delivered_at,
                        record.is_actionable,
                        record.injection_id,
                        json.dumps(record.metadata, default=str),
                    ))
            logger.debug(
                "Alert recorded: %s [%s/%s]",
                record.alert_id, record.severity, record.channel,
            )
            return True

        except Exception as exc:
            logger.error("Failed to write alert record: %s", exc)
            return False

    def write_alerts_batch(self, records: list[AlertRecord]) -> int:
        """
        Persist multiple alert records in a single transaction.

        Returns:
            Number of records successfully written.
        """
        if not records:
            return 0

        sql = """
            INSERT INTO monitoring.alerts
                (alert_id, execution_id, test_name, severity, channel,
                 table_name, column_name, message, emitted_at,
                 delivered_at, is_actionable, injection_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    for r in records:
                        cur.execute(sql, (
                            r.alert_id, r.execution_id, r.test_name,
                            r.severity, r.channel, r.table_name,
                            r.column_name, r.message, r.emitted_at,
                            r.delivered_at, r.is_actionable,
                            r.injection_id,
                            json.dumps(r.metadata, default=str),
                        ))
            logger.info("Batch wrote %d alert records", len(records))
            return len(records)

        except Exception as exc:
            logger.error("Batch alert write failed: %s", exc)
            return 0

    # ── Quality check records ────────────────────────────────────────────

    def write_quality_check(self, record: QualityCheckRecord) -> bool:
        """
        Persist a dbt test result to monitoring.quality_check_results.

        Returns:
            True if write succeeded.
        """
        sql = """
            INSERT INTO monitoring.quality_check_results
                (check_id, execution_id, test_name, test_type,
                 model_name, column_name, status, severity_config,
                 rows_affected, execution_time_ms, executed_at, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        record.check_id,
                        record.execution_id,
                        record.test_name,
                        record.test_type,
                        record.model_name,
                        record.column_name,
                        record.status,
                        record.severity_config,
                        record.rows_affected,
                        record.execution_time_ms,
                        record.executed_at,
                        json.dumps(record.details, default=str),
                    ))
            logger.debug("Quality check recorded: %s [%s]", record.check_id, record.status)
            return True

        except Exception as exc:
            logger.error("Failed to write quality check record: %s", exc)
            return False

    # ── Pipeline execution records ───────────────────────────────────────

    def write_execution_start(
        self,
        execution_id: str,
        pipeline_name: str = "elt_pipeline",
        alerting_mode: str = "baseline",
    ) -> bool:
        """Record the start of a pipeline execution."""
        sql = """
            INSERT INTO monitoring.pipeline_executions
                (execution_id, pipeline_name, alerting_mode, status, started_at)
            VALUES (%s, %s, %s, 'running', %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        execution_id,
                        pipeline_name,
                        alerting_mode,
                        datetime.now(timezone.utc),
                    ))
            return True
        except Exception as exc:
            logger.error("Failed to write execution start: %s", exc)
            return False

    def write_execution_end(
        self,
        execution_id: str,
        status: str = "completed",
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: str | None = None,
    ) -> bool:
        """Record the end of a pipeline execution."""
        sql = """
            UPDATE monitoring.pipeline_executions
            SET status = %s,
                completed_at = %s,
                records_processed = %s,
                records_failed = %s,
                error_message = %s
            WHERE execution_id = %s
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        status,
                        datetime.now(timezone.utc),
                        records_processed,
                        records_failed,
                        error_message,
                        execution_id,
                    ))
            return True
        except Exception as exc:
            logger.error("Failed to write execution end: %s", exc)
            return False
