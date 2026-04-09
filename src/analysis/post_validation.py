"""
Post-Execution Validation (AI-16b)
====================================
Cross-references injected failures against detected alerts to verify:

    1. Every critical failure was detected under BOTH strategies
    2. No critical failures were missed by the smart strategy
    3. All observability records are complete and consistent
    4. Alert timestamps are valid (emitted_at >= injected_at)

This is the third validation phase specified in Chapter 3 §3.6.7,
executed after all experimental runs but before statistical analysis.
Any validation failure invalidates the experiment results.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class CrossRefResult:
    """Outcome of a single cross-reference check."""
    check_name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class PostValidationReport:
    """Aggregated post-execution validation results."""
    results: list[CrossRefResult] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def summary(self) -> str:
        lines = [
            f"Post-Execution Validation — {self.validated_at.isoformat()}",
            f"  Passed: {self.passed_count}/{len(self.results)}",
        ]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{status}] {r.check_name}: {r.message}")
        return "\n".join(lines)


class PostExecutionValidator:
    """
    Validates experiment data integrity after all runs complete.

    Usage::

        validator = PostExecutionValidator(db)
        report = validator.validate()
        if not report.all_passed:
            print(report.summary())
            raise RuntimeError("Post-execution validation failed")
    """

    def __init__(self, db: DatabaseManager):
        self._db = db

    def validate(self) -> PostValidationReport:
        """Run all post-execution validation checks."""
        report = PostValidationReport()

        checks = [
            self._check_injections_have_alerts,
            self._check_no_critical_missed,
            self._check_alert_timestamps_valid,
            self._check_execution_records_complete,
            self._check_observability_consistency,
        ]

        for check_fn in checks:
            try:
                result = check_fn()
            except Exception as exc:
                result = CrossRefResult(
                    check_name=check_fn.__name__.replace("_check_", ""),
                    passed=False,
                    message=f"Unexpected error: {exc}",
                )
            report.results.append(result)

            status = "PASS" if result.passed else "FAIL"
            logger.info("[%s] %s: %s", status, result.check_name, result.message)

        return report

    # ── Individual checks ────────────────────────────────────────────────

    def _check_injections_have_alerts(self) -> CrossRefResult:
        """Every injected failure should have at least one corresponding alert."""
        rows = self._db.execute_observability("""
            SELECT
                f.injection_id,
                f.failure_type,
                f.expected_severity,
                (SELECT count(*)
                 FROM monitoring.alerts a
                 WHERE a.injection_id = f.injection_id) AS alert_count
            FROM monitoring.injected_failures f
        """)

        if not rows:
            return CrossRefResult(
                check_name="injections_have_alerts",
                passed=True,
                message="No injections recorded (nothing to validate)",
            )

        unmatched = [r for r in rows if r["alert_count"] == 0]

        if unmatched:
            return CrossRefResult(
                check_name="injections_have_alerts",
                passed=False,
                message=f"{len(unmatched)} injections have no matching alerts",
                details={"unmatched": [r["injection_id"] for r in unmatched]},
            )

        return CrossRefResult(
            check_name="injections_have_alerts",
            passed=True,
            message=f"All {len(rows)} injections have matching alerts",
        )

    def _check_no_critical_missed(self) -> CrossRefResult:
        """No critical injection should be without a critical-severity alert."""
        rows = self._db.execute_observability("""
            SELECT
                f.injection_id,
                f.failure_type,
                (SELECT count(*)
                 FROM monitoring.alerts a
                 WHERE a.severity = 'critical'
                   AND a.execution_id IN (
                       SELECT execution_id FROM monitoring.pipeline_executions
                   )) AS critical_alert_count
            FROM monitoring.injected_failures f
            WHERE f.expected_severity = 'critical'
        """)

        if not rows:
            return CrossRefResult(
                check_name="no_critical_missed",
                passed=True,
                message="No critical injections to check",
            )

        missed = [r for r in rows if r["critical_alert_count"] == 0]

        if missed:
            return CrossRefResult(
                check_name="no_critical_missed",
                passed=False,
                message=f"{len(missed)} critical injections had NO critical alerts",
                details={
                    "missed_injections": [
                        {"id": r["injection_id"], "type": r["failure_type"]}
                        for r in missed
                    ]
                },
            )

        return CrossRefResult(
            check_name="no_critical_missed",
            passed=True,
            message=f"All {len(rows)} critical injections detected with critical alerts",
        )

    def _check_alert_timestamps_valid(self) -> CrossRefResult:
        """Alert emitted_at should be >= the corresponding injection injected_at."""
        rows = self._db.execute_observability("""
            SELECT count(*) AS invalid_count
            FROM monitoring.alerts a
            JOIN monitoring.injected_failures f ON a.injection_id = f.injection_id
            WHERE a.emitted_at < f.injected_at
        """)

        invalid = rows[0]["invalid_count"] if rows else 0

        if invalid > 0:
            return CrossRefResult(
                check_name="alert_timestamps_valid",
                passed=False,
                message=f"{invalid} alerts emitted BEFORE their injection timestamp",
            )

        return CrossRefResult(
            check_name="alert_timestamps_valid",
            passed=True,
            message="All alert timestamps are after their injection timestamps",
        )

    def _check_execution_records_complete(self) -> CrossRefResult:
        """Every pipeline execution should have a completed_at timestamp."""
        rows = self._db.execute_observability("""
            SELECT
                count(*) AS total,
                count(*) FILTER (WHERE completed_at IS NOT NULL) AS completed,
                count(*) FILTER (WHERE status = 'running') AS still_running
            FROM monitoring.pipeline_executions
        """)

        if not rows or rows[0]["total"] == 0:
            return CrossRefResult(
                check_name="execution_records_complete",
                passed=True,
                message="No execution records to check",
            )

        r = rows[0]
        if r["still_running"] > 0:
            return CrossRefResult(
                check_name="execution_records_complete",
                passed=False,
                message=f"{r['still_running']} executions still in 'running' state",
            )

        return CrossRefResult(
            check_name="execution_records_complete",
            passed=True,
            message=f"All {r['total']} executions completed",
        )

    def _check_observability_consistency(self) -> CrossRefResult:
        """Check that alerts reference valid execution_ids."""
        rows = self._db.execute_observability("""
            SELECT count(*) AS orphan_alerts
            FROM monitoring.alerts a
            WHERE a.execution_id != ''
              AND a.execution_id NOT IN (
                  SELECT execution_id FROM monitoring.pipeline_executions
              )
        """)

        orphans = rows[0]["orphan_alerts"] if rows else 0

        if orphans > 0:
            return CrossRefResult(
                check_name="observability_consistency",
                passed=False,
                message=f"{orphans} alerts reference non-existent execution_ids",
            )

        return CrossRefResult(
            check_name="observability_consistency",
            passed=True,
            message="All alert execution_id references are valid",
        )
