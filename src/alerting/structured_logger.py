"""
Structured Logging Channel (AI-12a)
====================================
The "log only" path for warning-level alerts under the smart alerting
strategy.  Captures the same metadata as email alerts (timestamp,
severity, test name, failure details) in JSON-structured log entries,
enabling accurate measurement of warning volume without generating
email notifications.

This is the other half of the smart strategy's bifurcation:
    Critical → SMTPSender (AI-12)
    Warning  → StructuredLogger (this module)

Both channels feed their records to the ObservabilityWriter (AI-12b)
so that all alerts — regardless of delivery channel — are persisted
in the observability database for Phase 7 metrics computation.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("elt_pipeline.alerts")


@dataclass
class LogResult:
    """Captures the outcome of a structured log entry."""
    success: bool
    logged_at: datetime
    channel: str = "log"


class StructuredLogger:
    """
    Writes warning-level alerts to structured logs.

    Usage::

        log_channel = StructuredLogger()
        result = log_channel.log_alert(
            test_name="not_null_shipping_country",
            severity="warning",
            message="15% null rate on shipping_country exceeds 10% threshold",
            metadata={"table": "stg_transactions", "null_rate": 0.15}
        )
    """

    def __init__(self, logger_name: str = "elt_pipeline.alerts"):
        self._logger = logging.getLogger(logger_name)

    def log_alert(
        self,
        test_name: str,
        severity: str,
        message: str,
        table_name: str = "",
        column_name: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> LogResult:
        """
        Write a structured alert log entry.

        Args:
            test_name:   Name of the dbt test that triggered the alert.
            severity:    Alert severity ("warning" or "critical").
            message:     Human-readable failure description.
            table_name:  Affected table.
            column_name: Affected column (if applicable).
            metadata:    Additional context.

        Returns:
            LogResult with timestamp.
        """
        logged_at = datetime.now(timezone.utc)

        log_entry = {
            "alert_type": "pipeline_quality",
            "test_name": test_name,
            "severity": severity,
            "message": message,
            "table_name": table_name,
            "column_name": column_name,
            "channel": "log",
            "logged_at": logged_at.isoformat(),
            "metadata": metadata or {},
        }

        # Write at appropriate log level
        if severity == "critical":
            self._logger.critical(json.dumps(log_entry, default=str))
        elif severity == "warning":
            self._logger.warning(json.dumps(log_entry, default=str))
        else:
            self._logger.info(json.dumps(log_entry, default=str))

        return LogResult(success=True, logged_at=logged_at)
