"""
Alerting Strategies (AI-10 + AI-11)
====================================
Implements the two alerting strategies compared in the experiment:

    Baseline (AI-10): Every dbt test failure → email alert.
                      No filtering, no classification.

    Smart (AI-11):    Each failure is classified by severity:
                        Critical → email alert via SMTPSender
                        Warning  → structured log via StructuredLogger
                      This is the core experimental intervention.

Both strategies record all events through the ObservabilityWriter
(AI-12b), ensuring that the metrics calculator (AI-15) has complete
data regardless of which strategy was active.

The strategy is selected at runtime via PipelineConfig.alerting_mode
("baseline" or "smart"), controlled by the configuration manager
(AI-14a) and toggled by the experiment runner (AI-14).

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..core.config import PipelineConfig
from ..core.database import DatabaseManager
from .smtp_sender import SMTPSender, EmailResult
from .structured_logger import StructuredLogger, LogResult
from .observability_writer import ObservabilityWriter, AlertRecord

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Alert data from dbt test results
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TestFailure:
    """Represents a single dbt test failure to be processed by a strategy."""
    test_name: str
    severity_config: str       # "error" (CRITICAL) or "warn" (WARNING)
    model_name: str = ""
    column_name: str = ""
    message: str = ""
    rows_affected: int = 0
    execution_id: str = ""
    injection_id: str | None = None
    metadata: dict[str, Any] | None = None

    @property
    def is_critical(self) -> bool:
        """True if dbt test was configured with severity: error."""
        return self.severity_config == "error"

    @property
    def severity_label(self) -> str:
        return "critical" if self.is_critical else "warning"


@dataclass
class AlertOutcome:
    """Result of processing a test failure through a strategy."""
    test_name: str
    severity: str              # "critical" or "warning"
    channel: str               # "email" or "log"
    delivered: bool
    emitted_at: datetime
    delivered_at: datetime | None = None
    alert_id: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# Base strategy
# ═══════════════════════════════════════════════════════════════════════════

class AlertingStrategy(ABC):
    """Abstract base for alerting strategies."""

    def __init__(
        self,
        smtp: SMTPSender,
        log_channel: StructuredLogger,
        obs_writer: ObservabilityWriter,
    ):
        self._smtp = smtp
        self._log = log_channel
        self._obs = obs_writer

    @abstractmethod
    def process_failure(self, failure: TestFailure) -> AlertOutcome:
        """Process a single test failure and return the outcome."""
        ...

    def process_failures(self, failures: list[TestFailure]) -> list[AlertOutcome]:
        """Process a batch of test failures."""
        outcomes = []
        for failure in failures:
            outcome = self.process_failure(failure)
            outcomes.append(outcome)
        return outcomes

    @property
    @abstractmethod
    def mode_name(self) -> str:
        """Return the strategy name for observability."""
        ...

    def _record_alert(
        self, failure: TestFailure, channel: str,
        emitted_at: datetime, delivered_at: datetime | None,
    ) -> str:
        """Persist alert to observability DB and return alert_id."""
        record = AlertRecord(
            execution_id=failure.execution_id,
            test_name=failure.test_name,
            severity=failure.severity_label,
            channel=channel,
            table_name=failure.model_name,
            column_name=failure.column_name,
            message=failure.message,
            emitted_at=emitted_at,
            delivered_at=delivered_at,
            injection_id=failure.injection_id,
            metadata=failure.metadata or {},
        )
        self._obs.write_alert(record)
        return record.alert_id


# ═══════════════════════════════════════════════════════════════════════════
# AI-10: Baseline Strategy — Uniform Alerting
# ═══════════════════════════════════════════════════════════════════════════

class BaselineStrategy(AlertingStrategy):
    """
    Every test failure triggers an email alert, regardless of severity.

    This mirrors the common industry practice where teams configure
    comprehensive monitoring that treats all failures equally,
    contributing to notification overload and alert fatigue
    (Foidl et al., 2024).
    """

    @property
    def mode_name(self) -> str:
        return "baseline"

    def process_failure(self, failure: TestFailure) -> AlertOutcome:
        """Send email for every failure — no filtering."""
        emitted_at = datetime.now(timezone.utc)

        # Always send email, regardless of severity
        result: EmailResult = self._smtp.send_alert(
            subject=f"{failure.test_name} failed on {failure.model_name}",
            body=(
                f"Test: {failure.test_name}\n"
                f"Model: {failure.model_name}\n"
                f"Column: {failure.column_name}\n"
                f"Severity config: {failure.severity_config}\n"
                f"Rows affected: {failure.rows_affected}\n"
                f"\n{failure.message}"
            ),
            severity=failure.severity_label,
            metadata=failure.metadata,
        )

        # Record in observability DB
        alert_id = self._record_alert(
            failure, channel="email",
            emitted_at=emitted_at,
            delivered_at=result.delivered_at,
        )

        logger.info(
            "BASELINE: Email sent for %s [%s] — delivered=%s",
            failure.test_name, failure.severity_label, result.success,
        )

        return AlertOutcome(
            test_name=failure.test_name,
            severity=failure.severity_label,
            channel="email",
            delivered=result.success,
            emitted_at=emitted_at,
            delivered_at=result.delivered_at,
            alert_id=alert_id,
        )


# ═══════════════════════════════════════════════════════════════════════════
# AI-11: Smart Strategy — Severity-Based Alerting
# ═══════════════════════════════════════════════════════════════════════════

class SmartStrategy(AlertingStrategy):
    """
    Routes test failures based on dbt severity configuration:

        severity: error  → Critical → Email via SMTP
        severity: warn   → Warning  → Structured log only

    This is the core experimental intervention.  The hypothesis is
    that this classification reduces alert volume (email count) without
    increasing MTTD for critical failures.
    """

    @property
    def mode_name(self) -> str:
        return "smart"

    def process_failure(self, failure: TestFailure) -> AlertOutcome:
        """Route failure based on severity classification."""
        emitted_at = datetime.now(timezone.utc)

        if failure.is_critical:
            # ── CRITICAL path: email alert ───────────────────────────
            result: EmailResult = self._smtp.send_alert(
                subject=f"{failure.test_name} failed on {failure.model_name}",
                body=(
                    f"Test: {failure.test_name}\n"
                    f"Model: {failure.model_name}\n"
                    f"Column: {failure.column_name}\n"
                    f"Severity: CRITICAL\n"
                    f"Rows affected: {failure.rows_affected}\n"
                    f"\n{failure.message}"
                ),
                severity="critical",
                metadata=failure.metadata,
            )

            alert_id = self._record_alert(
                failure, channel="email",
                emitted_at=emitted_at,
                delivered_at=result.delivered_at,
            )

            logger.info(
                "SMART: CRITICAL → email for %s — delivered=%s",
                failure.test_name, result.success,
            )

            return AlertOutcome(
                test_name=failure.test_name,
                severity="critical",
                channel="email",
                delivered=result.success,
                emitted_at=emitted_at,
                delivered_at=result.delivered_at,
                alert_id=alert_id,
            )

        else:
            # ── WARNING path: structured log only ────────────────────
            log_result: LogResult = self._log.log_alert(
                test_name=failure.test_name,
                severity="warning",
                message=failure.message,
                table_name=failure.model_name,
                column_name=failure.column_name,
                metadata=failure.metadata,
            )

            alert_id = self._record_alert(
                failure, channel="log",
                emitted_at=emitted_at,
                delivered_at=log_result.logged_at,
            )

            logger.info(
                "SMART: WARNING → log only for %s",
                failure.test_name,
            )

            return AlertOutcome(
                test_name=failure.test_name,
                severity="warning",
                channel="log",
                delivered=True,
                emitted_at=emitted_at,
                delivered_at=log_result.logged_at,
                alert_id=alert_id,
            )


# ═══════════════════════════════════════════════════════════════════════════
# Strategy factory
# ═══════════════════════════════════════════════════════════════════════════

def create_strategy(
    config: PipelineConfig,
    db: DatabaseManager,
) -> AlertingStrategy:
    """
    Factory: build the appropriate strategy from configuration.

    Args:
        config: Pipeline configuration with alerting_mode set.
        db:     Database manager for observability writes.

    Returns:
        BaselineStrategy or SmartStrategy instance.

    Raises:
        ValueError: If alerting_mode is not "baseline" or "smart".
    """
    smtp = SMTPSender(config.smtp)
    log_channel = StructuredLogger()
    obs_writer = ObservabilityWriter(db)

    if config.alerting_mode == "baseline":
        logger.info("Alerting strategy: BASELINE (uniform — all failures → email)")
        return BaselineStrategy(smtp, log_channel, obs_writer)

    elif config.alerting_mode == "smart":
        logger.info("Alerting strategy: SMART (severity-based — critical → email, warning → log)")
        return SmartStrategy(smtp, log_channel, obs_writer)

    else:
        raise ValueError(
            f"Unknown alerting_mode: '{config.alerting_mode}'. "
            f"Expected 'baseline' or 'smart'."
        )
