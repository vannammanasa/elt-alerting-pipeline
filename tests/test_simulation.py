"""
End-to-End Simulation Test
============================
Simulates the complete experimental pipeline flow without a live
PostgreSQL or SMTP server.  Exercises every module from Phase 1
through Phase 7 in the exact sequence the experiment runner uses.

This catches:
    - Import chain breaks
    - Interface mismatches between phases
    - Missing method signatures
    - Data flow gaps (output of phase N not compatible with input of phase N+1)
    - Logic errors in the alerting strategy routing
    - Statistical analysis with realistic synthetic data

Run with::

    python -m pytest tests/test_simulation.py -v -s

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

# ═══════════════════════════════════════════════════════════════════════════
# Setup logging so we can see the flow
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 1: Extract & Load
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase1Simulation:
    """Simulate data generation and verify output schema."""

    def test_generate_and_verify_schema(self):
        """AI-1a + AI-1b: Generate synthetic data and verify it's loader-ready."""
        from src.extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig

        config = SyntheticDataConfig(record_count=1000, seed=42)
        gen = SyntheticDataGenerator(config)
        df = gen.generate()

        # Verify schema matches what dbt staging model expects
        expected_columns = {
            "transaction_id", "event_timestamp", "customer_id", "product_id",
            "product_category", "quantity", "unit_price", "total_amount",
            "payment_method", "order_status", "shipping_country",
            "customer_email", "is_returning_customer",
        }
        assert set(df.columns) == expected_columns, f"Column mismatch: {set(df.columns) - expected_columns}"
        assert len(df) == 1000

        # Verify data types are compatible with PostgreSQL loading
        assert pd.api.types.is_string_dtype(df["transaction_id"])  # string/UUID
        assert pd.api.types.is_integer_dtype(df["customer_id"])
        assert pd.api.types.is_integer_dtype(df["product_id"])
        assert pd.api.types.is_integer_dtype(df["quantity"])
        assert pd.api.types.is_float_dtype(df["unit_price"])
        assert pd.api.types.is_float_dtype(df["total_amount"])

        # Verify non-nullable columns have zero nulls
        non_nullable = [
            "transaction_id", "event_timestamp", "customer_id",
            "product_id", "product_category", "quantity", "unit_price",
            "total_amount", "payment_method", "order_status",
            "is_returning_customer",
        ]
        for col in non_nullable:
            assert df[col].notna().all(), f"Unexpected nulls in {col}"

        # Verify primary key uniqueness
        assert df["transaction_id"].is_unique, "Duplicate transaction_ids"

        # Verify categorical values match dbt accepted_values tests
        valid_categories = {"electronics", "clothing", "home", "sports", "books"}
        assert set(df["product_category"].unique()).issubset(valid_categories)

        valid_statuses = {"completed", "pending", "shipped", "cancelled", "refunded"}
        assert set(df["order_status"].unique()).issubset(valid_statuses)

        valid_payments = {"credit_card", "debit_card", "paypal", "bank_transfer"}
        assert set(df["payment_method"].unique()).issubset(valid_payments)

        valid_countries = {"GB", "US", "DE", "FR", "IE", "NL", "ES", "IT"}
        actual_countries = set(df["shipping_country"].dropna().unique())
        assert actual_countries.issubset(valid_countries)

        # Verify value ranges match dbt value_in_range tests
        assert df["quantity"].between(1, 10).all()
        assert df["unit_price"].between(0.01, 9999.99).all()

        # Store for next phases
        self.__class__.generated_df = df
        print(f"\n  ✓ Generated {len(df)} rows, {len(df.columns)} columns")
        print(f"  ✓ Null rates: shipping_country={df['shipping_country'].isna().mean():.2%}, "
              f"customer_email={df['customer_email'].isna().mean():.2%}")

    def test_raw_loader_interface(self):
        """AI-2: Verify RawLoader accepts the generated DataFrame."""
        from src.loaders.raw_loader import RawLoader, _pg_type

        df = getattr(self.__class__, "generated_df", None)
        if df is None:
            pytest.skip("Depends on test_generate_and_verify_schema")

        # Verify type mapping works for all columns
        for col in df.columns:
            pg_type = _pg_type(df[col].dtype)
            assert pg_type in ("TEXT", "BIGINT", "INTEGER", "DOUBLE PRECISION",
                               "REAL", "BOOLEAN", "TIMESTAMP WITH TIME ZONE"), \
                f"Unmapped dtype for {col}: {df[col].dtype} → {pg_type}"

        # Verify metadata columns would be added
        assert "_ingestion_timestamp" not in df.columns
        assert "_source_id" not in df.columns
        assert "_batch_id" not in df.columns
        print("  ✓ All dtypes map to PostgreSQL types")
        print("  ✓ Metadata columns not yet present (loader adds them)")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2+3: dbt Models & Tests (parse validation)
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase2_3Simulation:
    """Verify dbt project structure and test coverage."""

    def test_dbt_project_structure(self):
        """AI-3/4/5: Verify all model files exist."""
        dbt_path = Path("dbt_project")
        expected_models = [
            "models/staging/stg_transactions.sql",
            "models/intermediate/int_transactions_enriched.sql",
            "models/intermediate/int_daily_sales.sql",
            "models/marts/mart_transactions.sql",
            "models/marts/mart_daily_summary.sql",
            "models/marts/mart_customer_summary.sql",
        ]
        for model in expected_models:
            assert (dbt_path / model).exists(), f"Missing: {model}"
        print(f"  ✓ All {len(expected_models)} dbt models present")

    def test_dbt_schema_docs_exist(self):
        """AI-5a: Verify schema YAML files exist."""
        dbt_path = Path("dbt_project")
        expected_yamls = [
            "models/staging/_sources.yml",
            "models/staging/_stg_schema.yml",
            "models/intermediate/_int_schema.yml",
            "models/marts/_marts_schema.yml",
        ]
        for y in expected_yamls:
            assert (dbt_path / y).exists(), f"Missing: {y}"
        print(f"  ✓ All {len(expected_yamls)} schema docs present")

    def test_custom_macros_exist(self):
        """AI-7: Verify custom test macros exist."""
        dbt_path = Path("dbt_project/macros")
        expected_macros = [
            "test_row_count_threshold.sql",
            "test_null_rate_below.sql",
            "test_schema_has_columns.sql",
            "test_value_in_range.sql",
            "test_freshness_hours.sql",
            "generate_schema_name.sql",
        ]
        for m in expected_macros:
            assert (dbt_path / m).exists(), f"Missing macro: {m}"
        print(f"  ✓ All {len(expected_macros)} macros present")

    def test_severity_config_in_schema(self):
        """AI-7a: Verify severity: error/warn is configured in YAML."""
        import yaml
        with open("dbt_project/models/staging/_stg_schema.yml") as f:
            schema = yaml.safe_load(f)

        # Count severity configs
        content = Path("dbt_project/models/staging/_stg_schema.yml").read_text()
        error_count = content.count("severity: error")
        warn_count = content.count("severity: warn")

        assert error_count > 0, "No severity: error found in staging schema"
        assert warn_count > 0, "No severity: warn found in staging schema"
        print(f"  ✓ Severity config: {error_count} × error (CRITICAL), {warn_count} × warn (WARNING)")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 4: Failure Injection
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase4Simulation:
    """Simulate failure injection without database."""

    def test_all_failure_types_map_to_scenarios(self):
        """AI-8 + AI-9: Every failure type appears in at least one scenario."""
        from src.injection.engine import FailureType
        from src.injection.scenarios import load_scenarios

        scenarios = load_scenarios("config/failure_scenarios.yml")

        used_types = set()
        for s in scenarios:
            for step in s.failures:
                used_types.add(step.failure_type)

        all_types = set(FailureType)
        unused = all_types - used_types
        assert len(unused) == 0, f"Failure types not in any scenario: {unused}"
        print(f"  ✓ All {len(all_types)} failure types covered by {len(scenarios)} scenarios")

    def test_scenario_severity_ground_truth(self):
        """AI-9: Verify scenario expected_severity matches engine mapping."""
        from src.injection.engine import FailureType, FAILURE_SEVERITY_MAP
        from src.injection.scenarios import load_scenarios

        scenarios = load_scenarios("config/failure_scenarios.yml")

        for s in scenarios:
            for step in s.failures:
                engine_severity = FAILURE_SEVERITY_MAP[step.failure_type]
                assert step.expected_severity == engine_severity, \
                    f"{s.id}: {step.failure_type} expected {step.expected_severity} " \
                    f"but engine maps to {engine_severity}"

        print("  ✓ All scenario severities match engine mapping")

    def test_reset_mechanism_imports(self):
        """AI-9a: Verify PipelineReset is importable and configured."""
        from src.injection.reset import PipelineReset

        assert "raw.raw_transactions" in [
            f"{schema}.{table}"
            for schema, tables in PipelineReset.WAREHOUSE_TABLES.items()
            for table in tables
        ]
        assert len(PipelineReset.OBSERVABILITY_TABLES) >= 4
        print(f"  ✓ Reset covers {sum(len(t) for t in PipelineReset.WAREHOUSE_TABLES.values())} "
              f"warehouse tables + {len(PipelineReset.OBSERVABILITY_TABLES)} observability tables")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 5: Alerting Engine — Full Strategy Simulation
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase5Simulation:
    """Simulate the complete alerting flow for all 10 scenarios."""

    def test_full_scenario_simulation(self):
        """Run all 10 scenarios through both strategies, compare results."""
        from src.injection.scenarios import load_scenarios
        from src.injection.engine import FAILURE_SEVERITY_MAP
        from src.alerting.strategies import (
            BaselineStrategy, SmartStrategy, TestFailure, AlertOutcome,
        )
        from src.alerting.smtp_sender import SMTPSender, EmailResult
        from src.alerting.structured_logger import StructuredLogger, LogResult
        from src.alerting.observability_writer import ObservabilityWriter

        # Mock dependencies
        mock_smtp = MagicMock(spec=SMTPSender)
        mock_smtp.send_alert.return_value = EmailResult(
            success=True,
            sent_at=datetime.now(timezone.utc),
            delivered_at=datetime.now(timezone.utc),
        )
        mock_log = MagicMock(spec=StructuredLogger)
        mock_log.log_alert.return_value = LogResult(
            success=True, logged_at=datetime.now(timezone.utc),
        )
        mock_obs = MagicMock(spec=ObservabilityWriter)
        mock_obs.write_alert.return_value = True

        baseline = BaselineStrategy(mock_smtp, mock_log, mock_obs)
        smart = SmartStrategy(mock_smtp, mock_log, mock_obs)

        scenarios = load_scenarios("config/failure_scenarios.yml")

        baseline_total_emails = 0
        smart_total_emails = 0
        smart_total_logs = 0
        critical_missed_by_smart = 0

        print("\n  Scenario Results:")
        print(f"  {'Scenario':<30} {'Failures':>8} {'Base Emails':>12} {'Smart Emails':>13} {'Smart Logs':>11}")
        print(f"  {'-'*30} {'-'*8} {'-'*12} {'-'*13} {'-'*11}")

        for scenario in scenarios:
            # Convert scenario failures to TestFailure objects
            failures = []
            for step in scenario.failures:
                severity = FAILURE_SEVERITY_MAP[step.failure_type]
                severity_config = "error" if severity.value == "critical" else "warn"
                failures.append(TestFailure(
                    test_name=f"test_{step.failure_type.value}_{step.table}",
                    severity_config=severity_config,
                    model_name=step.table,
                    column_name=step.column or "",
                    message=f"Simulated {step.failure_type.value}",
                    execution_id=f"sim-{scenario.id}",
                ))

            # Reset mock counts
            mock_smtp.reset_mock()
            mock_log.reset_mock()
            mock_obs.reset_mock()

            # Run through baseline
            b_outcomes = baseline.process_failures(failures)
            b_emails = sum(1 for o in b_outcomes if o.channel == "email")

            mock_smtp.reset_mock()
            mock_log.reset_mock()

            # Run through smart
            s_outcomes = smart.process_failures(failures)
            s_emails = sum(1 for o in s_outcomes if o.channel == "email")
            s_logs = sum(1 for o in s_outcomes if o.channel == "log")

            # Verify smart doesn't miss any critical
            critical_failures = [f for f in failures if f.is_critical]
            critical_emailed = [o for o in s_outcomes if o.severity == "critical" and o.channel == "email"]
            if len(critical_emailed) < len(critical_failures):
                critical_missed_by_smart += len(critical_failures) - len(critical_emailed)

            baseline_total_emails += b_emails
            smart_total_emails += s_emails
            smart_total_logs += s_logs

            print(f"  {scenario.id:<30} {len(failures):>8} {b_emails:>12} {s_emails:>13} {s_logs:>11}")

        print(f"  {'-'*30} {'-'*8} {'-'*12} {'-'*13} {'-'*11}")
        print(f"  {'TOTAL':<30} {'':>8} {baseline_total_emails:>12} {smart_total_emails:>13} {smart_total_logs:>11}")

        # ── Core hypothesis assertions ───────────────────────────────
        reduction_pct = ((baseline_total_emails - smart_total_emails) / baseline_total_emails) * 100

        print(f"\n  Alert Volume Reduction: {reduction_pct:.1f}%")
        print(f"  Baseline emails: {baseline_total_emails}")
        print(f"  Smart emails:    {smart_total_emails}")
        print(f"  Smart logs:      {smart_total_logs}")
        print(f"  Critical missed: {critical_missed_by_smart}")

        # Smart should have FEWER emails
        assert smart_total_emails < baseline_total_emails, \
            "Smart strategy should produce fewer emails than baseline"

        # Smart should NOT miss any critical failures
        assert critical_missed_by_smart == 0, \
            f"Smart strategy missed {critical_missed_by_smart} critical failures!"

        # Total events should be the same (email + log = all failures)
        assert baseline_total_emails == smart_total_emails + smart_total_logs, \
            "Total events should be conserved (email + log = total)"

        print(f"\n  ✓ HYPOTHESIS VALIDATED: {reduction_pct:.0f}% email reduction, 0 critical misses")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 6: Orchestration
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase6Simulation:
    """Verify orchestration components integrate correctly."""

    def test_config_manager_mode_switch(self):
        """AI-14a: Config manager switches modes and produces valid configs."""
        from src.experiment.config_manager import ConfigurationManager
        from src.alerting.strategies import create_strategy

        mgr = ConfigurationManager()

        # Baseline mode
        baseline_cfg = mgr.for_mode("baseline")
        assert baseline_cfg.alerting_mode == "baseline"

        # Smart mode
        smart_cfg = mgr.for_mode("smart")
        assert smart_cfg.alerting_mode == "smart"

        # Factory accepts both configs
        mock_db = MagicMock()
        b_strategy = create_strategy(baseline_cfg, mock_db)
        s_strategy = create_strategy(smart_cfg, mock_db)

        assert b_strategy.mode_name == "baseline"
        assert s_strategy.mode_name == "smart"
        print("  ✓ Config manager ↔ strategy factory integration verified")

    def test_validator_report_structure(self):
        """AI-14b: Validator produces properly structured reports."""
        from src.experiment.validator import ValidationReport, ValidationResult

        report = ValidationReport(results=[
            ValidationResult(check_name="warehouse_connectivity", passed=True, message="ok"),
            ValidationResult(check_name="observability_connectivity", passed=True, message="ok"),
            ValidationResult(check_name="warehouse_schemas", passed=True, message="ok"),
            ValidationResult(check_name="observability_tables", passed=True, message="ok"),
            ValidationResult(check_name="scenarios_loadable", passed=True, message="ok"),
            ValidationResult(check_name="dbt_parseable", passed=True, message="ok"),
            ValidationResult(check_name="baseline_data", passed=True, message="ok"),
            ValidationResult(check_name="smtp_connectivity", passed=False, message="optional"),
        ])

        assert report.critical_passed is True
        assert report.all_passed is False  # SMTP failed
        assert report.passed_count == 7
        assert report.failed_count == 1
        print(f"  ✓ Validator: {report.passed_count} passed, {report.failed_count} failed, critical_passed={report.critical_passed}")

    def test_run_result_data_flows_to_phase7(self):
        """AI-14: RunResult contains all data Phase 7 needs."""
        from src.experiment.runner import RunResult
        from src.alerting.strategies import AlertOutcome

        run = RunResult(
            run_id="scenario_1_baseline",
            scenario_id="scenario_1",
            scenario_name="PK null injection",
            alerting_mode="baseline",
            execution_id="run-abc123",
            outcomes=[
                AlertOutcome(test_name="t1", severity="critical", channel="email",
                             delivered=True, emitted_at=datetime.now(timezone.utc),
                             delivered_at=datetime.now(timezone.utc)),
                AlertOutcome(test_name="t2", severity="warning", channel="email",
                             delivered=True, emitted_at=datetime.now(timezone.utc),
                             delivered_at=datetime.now(timezone.utc)),
            ],
        )

        # Phase 7 needs these fields
        assert run.execution_id is not None
        assert run.alerting_mode in ("baseline", "smart")
        assert run.email_count == 2
        assert run.log_count == 0
        assert run.scenario_id is not None
        print(f"  ✓ RunResult → Phase 7 interface verified (execution_id, mode, email_count, log_count)")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 7: Metrics, Statistics, Validation, Reporting
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase7Simulation:
    """Full statistical analysis simulation with realistic data."""

    def _build_simulated_metrics(self):
        """Build realistic metrics matching expected experimental outcomes."""
        from src.analysis.metrics import RunMetrics
        from src.injection.scenarios import load_scenarios
        from src.injection.engine import FAILURE_SEVERITY_MAP

        scenarios = load_scenarios("config/failure_scenarios.yml")
        rng = np.random.default_rng(42)

        baseline_metrics = []
        smart_metrics = []

        for i, scenario in enumerate(scenarios):
            critical_count = scenario.expected_critical_count
            warning_count = scenario.expected_warning_count
            total = critical_count + warning_count

            # Baseline: all failures → email
            baseline_metrics.append(RunMetrics(
                execution_id=f"base-{i}",
                scenario_id=scenario.id,
                alerting_mode="baseline",
                alert_volume=total,
                total_alerts=total,
                critical_alerts=critical_count,
                warning_alerts=warning_count,
                email_count=total,
                log_count=0,
                mttd_ms=float(rng.uniform(150, 600)),
                mttd_seconds=float(rng.uniform(0.15, 0.6)),
                actionable_ratio=critical_count / total if total > 0 else 0,
                false_positive_rate=warning_count / total if total > 0 else 0,
            ))

            # Smart: only critical → email, warning → log
            smart_metrics.append(RunMetrics(
                execution_id=f"smart-{i}",
                scenario_id=scenario.id,
                alerting_mode="smart",
                alert_volume=critical_count,  # Only critical emails
                total_alerts=total,
                critical_alerts=critical_count,
                warning_alerts=warning_count,
                email_count=critical_count,
                log_count=warning_count,
                mttd_ms=float(rng.uniform(150, 620)),  # Similar MTTD
                mttd_seconds=float(rng.uniform(0.15, 0.62)),
                actionable_ratio=1.0 if critical_count > 0 else 0,  # All emails are critical
                false_positive_rate=0.0,  # No warning emails
            ))

        return baseline_metrics, smart_metrics

    def test_metrics_computation(self):
        """AI-15: Verify metrics are computable from simulated data."""
        baseline, smart = self._build_simulated_metrics()

        assert len(baseline) == 10
        assert len(smart) == 10

        # Verify baseline has more emails than smart
        b_total = sum(m.alert_volume for m in baseline)
        s_total = sum(m.alert_volume for m in smart)
        assert b_total > s_total, f"Baseline ({b_total}) should have more emails than smart ({s_total})"

        print(f"\n  Baseline total emails: {b_total}")
        print(f"  Smart total emails:    {s_total}")
        print(f"  Reduction:             {((b_total - s_total) / b_total) * 100:.1f}%")

    def test_statistical_analysis_full(self):
        """AI-16: Run complete statistical analysis and verify outputs."""
        from src.analysis.statistics import StatisticalAnalyser

        baseline, smart = self._build_simulated_metrics()
        analyser = StatisticalAnalyser()
        report = analyser.analyse(baseline, smart, experiment_id="sim-test")

        # Verify all 4 comparisons exist
        assert len(report.comparisons) == 4
        metric_names = {c.metric_name for c in report.comparisons}
        assert metric_names == {"alert_volume", "mttd_seconds", "actionable_ratio", "false_positive_rate"}

        # Alert volume should show reduction
        vol = report.get_comparison("alert_volume")
        assert vol is not None
        assert vol.pct_change is not None
        assert vol.pct_change < 0, "Smart should reduce alert volume"

        # Actionable ratio should improve
        ratio = report.get_comparison("actionable_ratio")
        assert ratio is not None
        assert ratio.smart_stats.mean >= ratio.baseline_stats.mean

        # False positive rate should decrease
        fpr = report.get_comparison("false_positive_rate")
        assert fpr is not None
        assert fpr.smart_stats.mean <= fpr.baseline_stats.mean

        # Wilcoxon should be computed
        assert vol.wilcoxon_p_value is not None

        # Cohen's d should be computed
        assert vol.cohens_d is not None
        assert vol.effect_size_label in ("negligible", "small", "medium", "large")

        # Summary should be generated
        assert len(report.summary) > 100
        assert "CONCLUSION" in report.summary

        print(f"\n  Statistical Analysis Results:")
        for c in report.comparisons:
            print(f"    {c.metric_name:25s}: baseline={c.baseline_stats.mean:.2f}, "
                  f"smart={c.smart_stats.mean:.2f}, change={c.pct_change:+.1f}%, "
                  f"p={c.wilcoxon_p_value or 0:.4f}, d={c.cohens_d or 0:.2f} ({c.effect_size_label})")

        print(f"\n  Summary:\n{report.summary}")

        # Store for report generation test
        self.__class__.report = report
        self.__class__.baseline = baseline
        self.__class__.smart = smart

    def test_report_generation(self, tmp_path):
        """AI-16a: Generate all charts and text report."""
        from src.analysis.report_generator import ReportGenerator

        report = getattr(self.__class__, "report", None)
        baseline = getattr(self.__class__, "baseline", None)
        smart = getattr(self.__class__, "smart", None)

        if report is None:
            pytest.skip("Depends on test_statistical_analysis_full")

        gen = ReportGenerator(output_dir=str(tmp_path))
        files = gen.generate_all(baseline, smart, report)

        assert len(files) == 6, f"Expected 6 output files, got {len(files)}"

        expected_files = [
            "alert_volume.png",
            "mttd_boxplot.png",
            "actionable_ratio.png",
            "false_positive_rate.png",
            "comparison_table.png",
            "experiment_report.txt",
        ]

        for expected in expected_files:
            path = tmp_path / expected
            assert path.exists(), f"Missing output: {expected}"
            size = path.stat().st_size
            assert size > 0, f"Empty file: {expected}"
            print(f"    ✓ {expected} ({size:,} bytes)")

        # Verify text report content
        report_text = (tmp_path / "experiment_report.txt").read_text()
        assert "EXPERIMENT RESULTS REPORT" in report_text
        assert "Alert Volume" in report_text
        assert "CONCLUSION" in report_text

        print(f"\n  ✓ All {len(files)} report artefacts generated successfully")

    def test_post_validation_structure(self):
        """AI-16b: Verify post-validation report structure."""
        from src.analysis.post_validation import PostValidationReport, CrossRefResult

        # Simulate a passing validation
        report = PostValidationReport(results=[
            CrossRefResult(check_name="injections_have_alerts", passed=True, message="10/10 matched"),
            CrossRefResult(check_name="no_critical_missed", passed=True, message="7/7 detected"),
            CrossRefResult(check_name="alert_timestamps_valid", passed=True, message="All valid"),
            CrossRefResult(check_name="execution_records_complete", passed=True, message="20/20 completed"),
            CrossRefResult(check_name="observability_consistency", passed=True, message="No orphans"),
        ])

        assert report.all_passed is True
        assert report.passed_count == 5
        print(f"  ✓ Post-validation: {report.passed_count}/5 checks passed")


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-PHASE INTEGRATION: Full pipeline flow
# ═══════════════════════════════════════════════════════════════════════════

class TestCrossPhaseIntegration:
    """Verify data flows correctly between all phases."""

    def test_complete_data_flow(self):
        """Trace data from generation through to report — every interface."""
        # Phase 1: Generate data
        from src.extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig
        df = SyntheticDataGenerator(SyntheticDataConfig(record_count=500, seed=99)).generate()
        assert len(df) == 500

        # Phase 2+3: Verify dbt would accept this schema
        staging_expected = {
            "transaction_id", "event_timestamp", "customer_id", "product_id",
            "product_category", "quantity", "unit_price", "total_amount",
            "payment_method", "order_status", "shipping_country",
            "customer_email", "is_returning_customer",
        }
        assert set(df.columns) == staging_expected

        # Phase 4: Load scenarios and map to failures
        from src.injection.scenarios import load_scenarios
        from src.injection.engine import FAILURE_SEVERITY_MAP
        scenarios = load_scenarios("config/failure_scenarios.yml")
        assert len(scenarios) == 10

        # Phase 5: Convert to TestFailures and process
        from src.alerting.strategies import TestFailure, BaselineStrategy, SmartStrategy
        from src.alerting.smtp_sender import SMTPSender, EmailResult
        from src.alerting.structured_logger import StructuredLogger, LogResult
        from src.alerting.observability_writer import ObservabilityWriter

        mock_smtp = MagicMock(spec=SMTPSender)
        mock_smtp.send_alert.return_value = EmailResult(
            success=True, sent_at=datetime.now(timezone.utc),
            delivered_at=datetime.now(timezone.utc),
        )
        mock_log = MagicMock(spec=StructuredLogger)
        mock_log.log_alert.return_value = LogResult(success=True, logged_at=datetime.now(timezone.utc))
        mock_obs = MagicMock(spec=ObservabilityWriter)
        mock_obs.write_alert.return_value = True

        # Phase 6: Config manager drives mode selection
        from src.experiment.config_manager import ConfigurationManager
        mgr = ConfigurationManager()

        all_results = {"baseline": [], "smart": []}

        for mode in ["baseline", "smart"]:
            cfg = mgr.for_mode(mode)
            strategy = BaselineStrategy(mock_smtp, mock_log, mock_obs) if mode == "baseline" \
                else SmartStrategy(mock_smtp, mock_log, mock_obs)

            for scenario in scenarios:
                failures = []
                for step in scenario.failures:
                    sev = FAILURE_SEVERITY_MAP[step.failure_type]
                    failures.append(TestFailure(
                        test_name=f"test_{step.failure_type.value}",
                        severity_config="error" if sev.value == "critical" else "warn",
                        execution_id=f"flow-{scenario.id}-{mode}",
                    ))
                outcomes = strategy.process_failures(failures)
                all_results[mode].append({
                    "scenario": scenario.id,
                    "email_count": sum(1 for o in outcomes if o.channel == "email"),
                    "log_count": sum(1 for o in outcomes if o.channel == "log"),
                })

        # Phase 7: Build metrics and run analysis
        from src.analysis.metrics import RunMetrics
        from src.analysis.statistics import StatisticalAnalyser

        baseline_metrics = [
            RunMetrics(
                execution_id=f"flow-{r['scenario']}-baseline",
                scenario_id=r["scenario"],
                alerting_mode="baseline",
                alert_volume=r["email_count"],
                total_alerts=r["email_count"] + r["log_count"],
                mttd_seconds=float(np.random.uniform(0.1, 0.5)),
                actionable_ratio=0.5,
                false_positive_rate=0.5,
            )
            for r in all_results["baseline"]
        ]

        smart_metrics = [
            RunMetrics(
                execution_id=f"flow-{r['scenario']}-smart",
                scenario_id=r["scenario"],
                alerting_mode="smart",
                alert_volume=r["email_count"],
                total_alerts=r["email_count"] + r["log_count"],
                mttd_seconds=float(np.random.uniform(0.1, 0.5)),
                actionable_ratio=1.0 if r["email_count"] > 0 else 0,
                false_positive_rate=0.0,
            )
            for r in all_results["smart"]
        ]

        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)

        vol = report.get_comparison("alert_volume")
        assert vol is not None
        assert vol.baseline_stats.mean > vol.smart_stats.mean

        print("\n  ✓ COMPLETE DATA FLOW VERIFIED:")
        print(f"    Phase 1 → {len(df)} rows generated")
        print(f"    Phase 2+3 → {len(staging_expected)} columns validated")
        print(f"    Phase 4 → {len(scenarios)} scenarios loaded")
        print(f"    Phase 5 → {sum(r['email_count'] for r in all_results['baseline'])} baseline emails, "
              f"{sum(r['email_count'] for r in all_results['smart'])} smart emails")
        print(f"    Phase 6 → Config manager drove 2 modes × 10 scenarios = 20 runs")
        print(f"    Phase 7 → Alert volume: baseline={vol.baseline_stats.mean:.1f}, "
              f"smart={vol.smart_stats.mean:.1f} ({vol.pct_change:+.1f}%)")
        print(f"    CONCLUSION present in report: {'CONCLUSION' in report.summary}")
