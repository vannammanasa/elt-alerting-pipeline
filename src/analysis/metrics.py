"""
Metrics Calculator (AI-15)
===========================
Computes the four evaluation metrics defined in Chapter 3, Table 3.5
from the observability database:

    1. Alert Volume        — COUNT of email notifications per run
    2. Mean Time to Detect — AVG(alert_timestamp – injection_timestamp) for criticals
    3. Actionable Ratio    — critical_alerts / total_alerts
    4. False Positive Rate — warning_emails / total_emails (baseline only)

Results are stored in monitoring.experiment_metrics for statistical
analysis (AI-16) and report generation (AI-16a).

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
class RunMetrics:
    """Computed metrics for a single scenario × strategy run."""
    metrics_id: str = ""
    execution_id: str = ""
    scenario_id: str = ""
    alerting_mode: str = ""

    # Chapter 3 Table 3.5 metrics
    alert_volume: int = 0                  # Total email notifications
    total_alerts: int = 0                  # All alerts (email + log)
    critical_alerts: int = 0
    warning_alerts: int = 0
    mttd_ms: float | None = None           # Mean Time to Detect (milliseconds)
    mttd_seconds: float | None = None      # MTTD in seconds (human-readable)
    actionable_ratio: float | None = None  # critical / total
    false_positive_rate: float | None = None  # warning_emails / total_emails

    # Supporting detail
    email_count: int = 0
    log_count: int = 0
    injection_count: int = 0
    detections: int = 0                    # Injections that triggered alerts
    missed_detections: int = 0             # Injections without matching alerts

    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.metrics_id:
            self.metrics_id = f"met-{uuid.uuid4().hex[:12]}"


class MetricsCalculator:
    """
    Computes evaluation metrics from the observability database.

    Usage::

        calc = MetricsCalculator(db)
        metrics = calc.compute_for_run(execution_id="run-abc123")
        print(f"Alert volume: {metrics.alert_volume}")
        print(f"MTTD: {metrics.mttd_seconds:.1f}s")
        print(f"Actionable ratio: {metrics.actionable_ratio:.2%}")

        # Or compute for all runs in an experiment
        all_metrics = calc.compute_all()
    """

    def __init__(self, db: DatabaseManager):
        self._db = db

    # ── Per-run computation ──────────────────────────────────────────────

    def compute_for_run(
        self,
        execution_id: str,
        scenario_id: str = "",
        alerting_mode: str = "",
    ) -> RunMetrics:
        """
        Compute all four metrics for a single pipeline execution.

        Args:
            execution_id: The pipeline execution ID.
            scenario_id:  Scenario identifier (for labelling).
            alerting_mode: "baseline" or "smart".

        Returns:
            RunMetrics with all computed values.
        """
        metrics = RunMetrics(
            execution_id=execution_id,
            scenario_id=scenario_id,
            alerting_mode=alerting_mode,
        )

        # ── 1. Alert Volume (email count) ────────────────────────────
        alert_rows = self._db.execute_observability("""
            SELECT
                count(*)                                          AS total_alerts,
                count(*) FILTER (WHERE channel = 'email')         AS email_count,
                count(*) FILTER (WHERE channel = 'log')           AS log_count,
                count(*) FILTER (WHERE severity = 'critical')     AS critical_alerts,
                count(*) FILTER (WHERE severity = 'warning')      AS warning_alerts,
                count(*) FILTER (WHERE severity = 'warning' AND channel = 'email')
                                                                  AS warning_emails
            FROM monitoring.alerts
            WHERE execution_id = %s
        """, (execution_id,))

        if alert_rows:
            r = alert_rows[0]
            metrics.total_alerts = r["total_alerts"] or 0
            metrics.email_count = r["email_count"] or 0
            metrics.log_count = r["log_count"] or 0
            metrics.critical_alerts = r["critical_alerts"] or 0
            metrics.warning_alerts = r["warning_alerts"] or 0
            metrics.alert_volume = metrics.email_count

            # ── 3. Actionable Alert Ratio ────────────────────────────
            if metrics.total_alerts > 0:
                metrics.actionable_ratio = metrics.critical_alerts / metrics.total_alerts
            else:
                metrics.actionable_ratio = None

            # ── 4. False Positive Rate ───────────────────────────────
            if metrics.email_count > 0:
                warning_emails = r["warning_emails"] or 0
                metrics.false_positive_rate = warning_emails / metrics.email_count
            else:
                metrics.false_positive_rate = None

        # ── 2. Mean Time to Detect (MTTD) ────────────────────────────
        mttd_rows = self._db.execute_observability("""
            SELECT
                avg(
                    EXTRACT(EPOCH FROM (a.emitted_at - f.injected_at)) * 1000
                ) AS mttd_ms
            FROM monitoring.alerts a
            JOIN monitoring.injected_failures f
                ON a.execution_id = %s
               AND f.expected_severity = 'critical'
               AND a.severity = 'critical'
            WHERE a.execution_id = %s
              AND a.severity = 'critical'
        """, (execution_id, execution_id))

        if mttd_rows and mttd_rows[0]["mttd_ms"] is not None:
            metrics.mttd_ms = float(mttd_rows[0]["mttd_ms"])
            metrics.mttd_seconds = metrics.mttd_ms / 1000.0

        # ── Injection count ──────────────────────────────────────────
        inj_rows = self._db.execute_observability("""
            SELECT count(*) AS cnt
            FROM monitoring.injected_failures
        """)
        if inj_rows:
            metrics.injection_count = inj_rows[0]["cnt"] or 0

        logger.info(
            "Metrics computed for %s: volume=%d, MTTD=%.1fms, actionable=%.2f",
            execution_id,
            metrics.alert_volume,
            metrics.mttd_ms or 0,
            metrics.actionable_ratio or 0,
        )

        return metrics

    # ── Batch computation ────────────────────────────────────────────────

    def compute_all(self) -> list[RunMetrics]:
        """
        Compute metrics for all completed pipeline executions.

        Returns:
            List of RunMetrics, one per execution.
        """
        rows = self._db.execute_observability("""
            SELECT execution_id, alerting_mode
            FROM monitoring.pipeline_executions
            WHERE status = 'completed'
            ORDER BY started_at
        """)

        all_metrics = []
        for row in rows:
            m = self.compute_for_run(
                execution_id=row["execution_id"],
                alerting_mode=row.get("alerting_mode", ""),
            )
            all_metrics.append(m)

        logger.info("Computed metrics for %d runs", len(all_metrics))
        return all_metrics

    # ── Persistence ──────────────────────────────────────────────────────

    def save_metrics(self, metrics: RunMetrics) -> bool:
        """Persist computed metrics to monitoring.experiment_metrics."""
        sql = """
            INSERT INTO monitoring.experiment_metrics
                (metrics_id, execution_id, scenario_id, alerting_mode,
                 alert_volume, total_alerts, critical_alerts, warning_alerts,
                 mttd_ms, actionable_ratio, false_positive_rate,
                 email_count, log_count, injection_count,
                 computed_at, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._db.observability_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        metrics.metrics_id,
                        metrics.execution_id,
                        metrics.scenario_id,
                        metrics.alerting_mode,
                        metrics.alert_volume,
                        metrics.total_alerts,
                        metrics.critical_alerts,
                        metrics.warning_alerts,
                        metrics.mttd_ms,
                        metrics.actionable_ratio,
                        metrics.false_positive_rate,
                        metrics.email_count,
                        metrics.log_count,
                        metrics.injection_count,
                        metrics.computed_at,
                        json.dumps(metrics.details, default=str),
                    ))
            return True
        except Exception as exc:
            logger.error("Failed to save metrics: %s", exc)
            return False

    def save_all(self, metrics_list: list[RunMetrics]) -> int:
        """Persist a batch of metrics. Returns count saved."""
        saved = sum(1 for m in metrics_list if self.save_metrics(m))
        logger.info("Saved %d/%d metrics records", saved, len(metrics_list))
        return saved
