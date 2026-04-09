"""
Phase 7 Unit Tests
===================
Tests for metrics computation, statistical analysis, post-execution
validation, and report generation.  All tests run without a database
using synthetic RunMetrics objects.

Run with::

    pytest tests/test_phase7.py -v

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from src.analysis.metrics import RunMetrics
from src.analysis.statistics import (
    StatisticalAnalyser, AnalysisReport, ComparisonResult, DescriptiveStats,
)
from src.analysis.post_validation import (
    PostValidationReport, CrossRefResult,
)
from src.analysis.report_generator import ReportGenerator


# ═══════════════════════════════════════════════════════════════════════════
# Test fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def baseline_metrics() -> list[RunMetrics]:
    """Simulate 10 baseline runs — every failure triggers email."""
    np.random.seed(42)
    return [
        RunMetrics(
            execution_id=f"base-{i}",
            scenario_id=f"scenario_{i+1}",
            alerting_mode="baseline",
            alert_volume=np.random.randint(2, 6),        # 2-5 emails per run
            total_alerts=np.random.randint(2, 6),
            critical_alerts=np.random.randint(1, 3),
            warning_alerts=np.random.randint(1, 4),
            email_count=np.random.randint(2, 6),
            log_count=0,
            mttd_ms=float(np.random.uniform(200, 800)),
            mttd_seconds=float(np.random.uniform(0.2, 0.8)),
            actionable_ratio=float(np.random.uniform(0.25, 0.55)),
            false_positive_rate=float(np.random.uniform(0.40, 0.75)),
        )
        for i in range(10)
    ]


@pytest.fixture
def smart_metrics() -> list[RunMetrics]:
    """Simulate 10 smart runs — only critical → email, warning → log."""
    np.random.seed(42)
    return [
        RunMetrics(
            execution_id=f"smart-{i}",
            scenario_id=f"scenario_{i+1}",
            alerting_mode="smart",
            alert_volume=np.random.randint(0, 3),         # 0-2 emails (fewer)
            total_alerts=np.random.randint(2, 6),
            critical_alerts=np.random.randint(1, 3),
            warning_alerts=np.random.randint(1, 4),
            email_count=np.random.randint(0, 3),
            log_count=np.random.randint(1, 4),
            mttd_ms=float(np.random.uniform(200, 850)),    # Similar MTTD
            mttd_seconds=float(np.random.uniform(0.2, 0.85)),
            actionable_ratio=float(np.random.uniform(0.70, 1.0)),  # Higher
            false_positive_rate=float(np.random.uniform(0.0, 0.10)), # Lower
        )
        for i in range(10)
    ]


@pytest.fixture
def tmp_reports(tmp_path) -> Path:
    return tmp_path / "reports"


# ═══════════════════════════════════════════════════════════════════════════
# AI-15: RunMetrics dataclass
# ═══════════════════════════════════════════════════════════════════════════

class TestRunMetrics:
    def test_auto_id(self):
        m = RunMetrics()
        assert m.metrics_id.startswith("met-")

    def test_fields_populated(self):
        m = RunMetrics(
            alert_volume=5, mttd_seconds=0.5,
            actionable_ratio=0.4, false_positive_rate=0.6,
        )
        assert m.alert_volume == 5
        assert m.mttd_seconds == 0.5
        assert m.actionable_ratio == 0.4
        assert m.false_positive_rate == 0.6

    def test_defaults_are_zero(self):
        m = RunMetrics()
        assert m.alert_volume == 0
        assert m.total_alerts == 0
        assert m.mttd_ms is None


# ═══════════════════════════════════════════════════════════════════════════
# AI-16: Statistical Analyser
# ═══════════════════════════════════════════════════════════════════════════

class TestStatisticalAnalyser:
    def test_analyse_returns_report(self, baseline_metrics, smart_metrics):
        analyser = StatisticalAnalyser()
        report = analyser.analyse(baseline_metrics, smart_metrics, "exp-test")
        assert isinstance(report, AnalysisReport)
        assert report.experiment_id == "exp-test"

    def test_four_comparisons_generated(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        assert len(report.comparisons) == 4
        names = {c.metric_name for c in report.comparisons}
        assert names == {"alert_volume", "mttd_seconds", "actionable_ratio", "false_positive_rate"}

    def test_alert_volume_decreases(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        vol = report.get_comparison("alert_volume")
        assert vol is not None
        # Smart should have fewer emails on average
        assert vol.smart_stats.mean <= vol.baseline_stats.mean

    def test_actionable_ratio_improves(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        ratio = report.get_comparison("actionable_ratio")
        assert ratio is not None
        # Smart should have higher actionable ratio
        assert ratio.smart_stats.mean >= ratio.baseline_stats.mean

    def test_false_positive_rate_decreases(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        fpr = report.get_comparison("false_positive_rate")
        assert fpr is not None
        assert fpr.smart_stats.mean <= fpr.baseline_stats.mean

    def test_cohens_d_computed(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        for c in report.comparisons:
            assert c.cohens_d is not None
            assert c.effect_size_label in ("negligible", "small", "medium", "large")

    def test_wilcoxon_computed_with_enough_data(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        vol = report.get_comparison("alert_volume")
        # 10 paired samples should be enough
        assert vol.wilcoxon_p_value is not None
        assert 0 <= vol.wilcoxon_p_value <= 1

    def test_summary_generated(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        assert len(report.summary) > 50
        assert "Alert Volume" in report.summary

    def test_interpretation_not_empty(self, baseline_metrics, smart_metrics):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        for c in report.comparisons:
            assert len(c.interpretation) > 20

    def test_empty_inputs_handled(self):
        report = StatisticalAnalyser().analyse([], [])
        assert len(report.comparisons) == 4
        for c in report.comparisons:
            assert "Insufficient" in c.interpretation

    def test_descriptive_stats_correct(self):
        analyser = StatisticalAnalyser()
        stats = analyser._descriptive("test", [1.0, 2.0, 3.0, 4.0, 5.0])
        assert stats.n == 5
        assert stats.mean == 3.0
        assert stats.median == 3.0
        assert stats.min == 1.0
        assert stats.max == 5.0

    def test_cohens_d_known_values(self):
        """Two identical groups → d = 0."""
        d = StatisticalAnalyser._cohens_d([1, 2, 3], [1, 2, 3])
        assert d is not None
        assert abs(d) < 0.01

    def test_cohens_d_large_difference(self):
        """Very different groups with zero variance → infinite d."""
        d = StatisticalAnalyser._cohens_d([1, 1, 1, 1], [10, 10, 10, 10])
        assert d is not None
        assert abs(d) == float("inf")

    def test_effect_size_labels(self):
        assert StatisticalAnalyser._effect_size_label(0.1) == "negligible"
        assert StatisticalAnalyser._effect_size_label(0.3) == "small"
        assert StatisticalAnalyser._effect_size_label(0.6) == "medium"
        assert StatisticalAnalyser._effect_size_label(1.0) == "large"


# ═══════════════════════════════════════════════════════════════════════════
# AI-16a: Report Generator
# ═══════════════════════════════════════════════════════════════════════════

class TestReportGenerator:
    def test_all_charts_generated(self, baseline_metrics, smart_metrics, tmp_reports):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        gen = ReportGenerator(output_dir=str(tmp_reports))
        files = gen.generate_all(baseline_metrics, smart_metrics, report)

        assert len(files) == 6
        for f in files:
            assert Path(f).exists(), f"{f} was not created"

    def test_alert_volume_chart_exists(self, baseline_metrics, smart_metrics, tmp_reports):
        gen = ReportGenerator(output_dir=str(tmp_reports))
        path = gen.chart_alert_volume(baseline_metrics, smart_metrics)
        assert Path(path).exists()
        assert path.endswith(".png")

    def test_mttd_boxplot_exists(self, baseline_metrics, smart_metrics, tmp_reports):
        gen = ReportGenerator(output_dir=str(tmp_reports))
        path = gen.chart_mttd_boxplot(baseline_metrics, smart_metrics)
        assert Path(path).exists()

    def test_text_report_exists(self, baseline_metrics, smart_metrics, tmp_reports):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        gen = ReportGenerator(output_dir=str(tmp_reports))
        path = gen.generate_text_report(report)

        assert Path(path).exists()
        content = Path(path).read_text()
        assert "EXPERIMENT RESULTS REPORT" in content
        assert "Alert Volume" in content

    def test_comparison_table_chart(self, baseline_metrics, smart_metrics, tmp_reports):
        report = StatisticalAnalyser().analyse(baseline_metrics, smart_metrics)
        gen = ReportGenerator(output_dir=str(tmp_reports))
        path = gen.chart_metric_comparison_table(report)
        assert Path(path).exists()


# ═══════════════════════════════════════════════════════════════════════════
# AI-16b: Post-Execution Validation (unit-level)
# ═══════════════════════════════════════════════════════════════════════════

class TestPostValidationReport:
    def test_all_passed(self):
        report = PostValidationReport(results=[
            CrossRefResult(check_name="a", passed=True, message="ok"),
            CrossRefResult(check_name="b", passed=True, message="ok"),
        ])
        assert report.all_passed is True

    def test_failure_detected(self):
        report = PostValidationReport(results=[
            CrossRefResult(check_name="a", passed=True, message="ok"),
            CrossRefResult(check_name="b", passed=False, message="2 missed"),
        ])
        assert report.all_passed is False
        assert report.failed_count == 1

    def test_summary_format(self):
        report = PostValidationReport(results=[
            CrossRefResult(check_name="critical_check", passed=False, message="missed 1"),
        ])
        summary = report.summary()
        assert "FAIL" in summary
        assert "critical_check" in summary

    def test_empty_report_passes(self):
        report = PostValidationReport()
        assert report.all_passed is True

    def test_cross_ref_result_with_details(self):
        result = CrossRefResult(
            check_name="test",
            passed=False,
            message="Issues found",
            details={"unmatched": ["inj-1", "inj-2"]},
        )
        assert len(result.details["unmatched"]) == 2
