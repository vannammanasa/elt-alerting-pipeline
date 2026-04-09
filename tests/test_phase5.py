"""
Phase 5 Unit Tests
===================
Tests for the alerting engine.  Uses mocks for SMTP and database
so no external services are needed.

Run with::

    pytest tests/test_phase5.py -v

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.alerting.strategies import (
    BaselineStrategy, SmartStrategy, TestFailure, AlertOutcome,
    create_strategy,
)
from src.alerting.smtp_sender import SMTPSender, EmailResult
from src.alerting.structured_logger import StructuredLogger, LogResult
from src.alerting.observability_writer import (
    ObservabilityWriter, AlertRecord, QualityCheckRecord,
)


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def mock_smtp():
    smtp = MagicMock(spec=SMTPSender)
    smtp.send_alert.return_value = EmailResult(
        success=True,
        sent_at=datetime.now(timezone.utc),
        delivered_at=datetime.now(timezone.utc),
    )
    return smtp


@pytest.fixture
def mock_logger():
    log = MagicMock(spec=StructuredLogger)
    log.log_alert.return_value = LogResult(
        success=True,
        logged_at=datetime.now(timezone.utc),
    )
    return log


@pytest.fixture
def mock_obs_writer():
    writer = MagicMock(spec=ObservabilityWriter)
    writer.write_alert.return_value = True
    return writer


@pytest.fixture
def critical_failure() -> TestFailure:
    return TestFailure(
        test_name="not_null_stg_transactions_transaction_id",
        severity_config="error",
        model_name="stg_transactions",
        column_name="transaction_id",
        message="50 rows with NULL transaction_id",
        rows_affected=50,
        execution_id="exec-001",
    )


@pytest.fixture
def warning_failure() -> TestFailure:
    return TestFailure(
        test_name="not_null_stg_transactions_shipping_country",
        severity_config="warn",
        model_name="stg_transactions",
        column_name="shipping_country",
        message="15% null rate exceeds 10% threshold",
        rows_affected=1500,
        execution_id="exec-001",
    )


# ═══════════════════════════════════════════════════════════════════════════
# TestFailure dataclass
# ═══════════════════════════════════════════════════════════════════════════

class TestTestFailureDataclass:
    """Verify TestFailure properties."""

    def test_critical_from_error_severity(self, critical_failure):
        assert critical_failure.is_critical is True
        assert critical_failure.severity_label == "critical"

    def test_warning_from_warn_severity(self, warning_failure):
        assert warning_failure.is_critical is False
        assert warning_failure.severity_label == "warning"


# ═══════════════════════════════════════════════════════════════════════════
# AI-10: Baseline Strategy
# ═══════════════════════════════════════════════════════════════════════════

class TestBaselineStrategy:
    """Baseline sends email for EVERY failure."""

    def test_critical_sends_email(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure):
        strategy = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcome = strategy.process_failure(critical_failure)

        assert outcome.channel == "email"
        assert outcome.severity == "critical"
        assert outcome.delivered is True
        mock_smtp.send_alert.assert_called_once()
        mock_obs_writer.write_alert.assert_called_once()

    def test_warning_also_sends_email(self, mock_smtp, mock_logger, mock_obs_writer, warning_failure):
        """Key baseline behaviour: warnings get email too."""
        strategy = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcome = strategy.process_failure(warning_failure)

        assert outcome.channel == "email"
        assert outcome.severity == "warning"
        mock_smtp.send_alert.assert_called_once()

    def test_mode_name(self, mock_smtp, mock_logger, mock_obs_writer):
        strategy = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        assert strategy.mode_name == "baseline"

    def test_batch_processing(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure, warning_failure):
        strategy = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcomes = strategy.process_failures([critical_failure, warning_failure])

        assert len(outcomes) == 2
        assert all(o.channel == "email" for o in outcomes)
        assert mock_smtp.send_alert.call_count == 2

    def test_failed_delivery_recorded(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure):
        mock_smtp.send_alert.return_value = EmailResult(
            success=False, sent_at=datetime.now(timezone.utc),
            delivered_at=None, error="Connection refused",
        )
        strategy = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcome = strategy.process_failure(critical_failure)

        assert outcome.delivered is False
        assert outcome.delivered_at is None
        mock_obs_writer.write_alert.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
# AI-11: Smart Strategy
# ═══════════════════════════════════════════════════════════════════════════

class TestSmartStrategy:
    """Smart routes critical→email, warning→log."""

    def test_critical_sends_email(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure):
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcome = strategy.process_failure(critical_failure)

        assert outcome.channel == "email"
        assert outcome.severity == "critical"
        mock_smtp.send_alert.assert_called_once()
        mock_logger.log_alert.assert_not_called()

    def test_warning_logs_only(self, mock_smtp, mock_logger, mock_obs_writer, warning_failure):
        """Key smart behaviour: warnings go to log, NOT email."""
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcome = strategy.process_failure(warning_failure)

        assert outcome.channel == "log"
        assert outcome.severity == "warning"
        mock_logger.log_alert.assert_called_once()
        mock_smtp.send_alert.assert_not_called()

    def test_mode_name(self, mock_smtp, mock_logger, mock_obs_writer):
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        assert strategy.mode_name == "smart"

    def test_mixed_batch_routes_correctly(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure, warning_failure):
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcomes = strategy.process_failures([critical_failure, warning_failure])

        assert len(outcomes) == 2
        assert outcomes[0].channel == "email"   # critical → email
        assert outcomes[1].channel == "log"     # warning → log
        assert mock_smtp.send_alert.call_count == 1
        assert mock_logger.log_alert.call_count == 1

    def test_all_events_recorded_in_observability(self, mock_smtp, mock_logger, mock_obs_writer, critical_failure, warning_failure):
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        strategy.process_failures([critical_failure, warning_failure])

        # Both events should be written to observability regardless of channel
        assert mock_obs_writer.write_alert.call_count == 2

    def test_warning_never_triggers_email(self, mock_smtp, mock_logger, mock_obs_writer):
        """Process 5 warnings — zero emails should be sent."""
        strategy = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        warnings = [
            TestFailure(test_name=f"warn_test_{i}", severity_config="warn",
                        execution_id="exec-001")
            for i in range(5)
        ]
        outcomes = strategy.process_failures(warnings)

        assert all(o.channel == "log" for o in outcomes)
        mock_smtp.send_alert.assert_not_called()
        assert mock_logger.log_alert.call_count == 5


# ═══════════════════════════════════════════════════════════════════════════
# Strategy comparison (the core experiment logic)
# ═══════════════════════════════════════════════════════════════════════════

class TestStrategyComparison:
    """Verify the key experimental hypothesis at the unit level."""

    def test_smart_reduces_email_count(self, mock_smtp, mock_logger, mock_obs_writer):
        """Smart strategy produces fewer emails than baseline for same failures."""
        failures = [
            TestFailure(test_name="critical_1", severity_config="error", execution_id="e1"),
            TestFailure(test_name="warning_1", severity_config="warn", execution_id="e1"),
            TestFailure(test_name="warning_2", severity_config="warn", execution_id="e1"),
            TestFailure(test_name="warning_3", severity_config="warn", execution_id="e1"),
        ]

        # Baseline: all 4 → email
        baseline = BaselineStrategy(mock_smtp, mock_logger, mock_obs_writer)
        baseline_outcomes = baseline.process_failures(failures)
        baseline_emails = sum(1 for o in baseline_outcomes if o.channel == "email")

        mock_smtp.reset_mock()

        # Smart: 1 critical → email, 3 warnings → log
        smart = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        smart_outcomes = smart.process_failures(failures)
        smart_emails = sum(1 for o in smart_outcomes if o.channel == "email")

        assert baseline_emails == 4
        assert smart_emails == 1
        assert smart_emails < baseline_emails

    def test_smart_does_not_miss_critical(self, mock_smtp, mock_logger, mock_obs_writer):
        """Smart strategy detects ALL critical failures (MTTD not compromised)."""
        failures = [
            TestFailure(test_name="critical_1", severity_config="error", execution_id="e1"),
            TestFailure(test_name="critical_2", severity_config="error", execution_id="e1"),
            TestFailure(test_name="warning_1", severity_config="warn", execution_id="e1"),
        ]

        smart = SmartStrategy(mock_smtp, mock_logger, mock_obs_writer)
        outcomes = smart.process_failures(failures)

        critical_emails = [o for o in outcomes if o.severity == "critical"]
        assert len(critical_emails) == 2
        assert all(o.channel == "email" for o in critical_emails)


# ═══════════════════════════════════════════════════════════════════════════
# AlertRecord / QualityCheckRecord dataclasses
# ═══════════════════════════════════════════════════════════════════════════

class TestAlertRecord:
    def test_auto_generates_id(self):
        record = AlertRecord(test_name="test_1")
        assert record.alert_id.startswith("alert-")

    def test_critical_is_actionable(self):
        record = AlertRecord(severity="critical")
        assert record.is_actionable is True

    def test_warning_not_actionable(self):
        record = AlertRecord(severity="warning")
        assert record.is_actionable is False


class TestQualityCheckRecord:
    def test_auto_generates_id(self):
        record = QualityCheckRecord(test_name="test_1")
        assert record.check_id.startswith("chk-")


# ═══════════════════════════════════════════════════════════════════════════
# Factory
# ═══════════════════════════════════════════════════════════════════════════

class TestCreateStrategy:
    def test_baseline_mode(self):
        from src.core.config import PipelineConfig, DatabaseConfig, SMTPConfig
        config = PipelineConfig(alerting_mode="baseline")
        db = MagicMock()
        strategy = create_strategy(config, db)
        assert isinstance(strategy, BaselineStrategy)

    def test_smart_mode(self):
        from src.core.config import PipelineConfig
        config = PipelineConfig(alerting_mode="smart")
        db = MagicMock()
        strategy = create_strategy(config, db)
        assert isinstance(strategy, SmartStrategy)

    def test_invalid_mode_raises(self):
        from src.core.config import PipelineConfig
        config = PipelineConfig(alerting_mode="invalid")
        db = MagicMock()
        with pytest.raises(ValueError, match="Unknown alerting_mode"):
            create_strategy(config, db)
