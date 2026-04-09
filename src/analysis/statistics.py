"""
Statistical Analysis Module (AI-16)
=====================================
Performs the comparative statistical analysis defined in Chapter 3
§3.3.4 to evaluate whether the smart alerting strategy produces a
statistically significant reduction in alert volume without
increasing MTTD.

Methods:
    - Descriptive statistics (mean, median, std, IQR)
    - Wilcoxon signed-rank test (paired, non-parametric, p < 0.05)
    - Cohen's d effect size (small=0.2, medium=0.5, large=0.8)
    - Percentage reduction calculation

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
from scipy import stats as scipy_stats

from .metrics import RunMetrics

logger = logging.getLogger(__name__)


@dataclass
class DescriptiveStats:
    """Summary statistics for a single metric across runs."""
    metric_name: str
    n: int = 0
    mean: float = 0.0
    median: float = 0.0
    std: float = 0.0
    min: float = 0.0
    max: float = 0.0
    q1: float = 0.0
    q3: float = 0.0
    iqr: float = 0.0


@dataclass
class ComparisonResult:
    """Result of comparing a metric between baseline and smart."""
    metric_name: str
    baseline_stats: DescriptiveStats
    smart_stats: DescriptiveStats

    # Percentage change
    pct_change: float | None = None        # (smart - baseline) / baseline × 100
    absolute_change: float | None = None

    # Wilcoxon signed-rank test
    wilcoxon_statistic: float | None = None
    wilcoxon_p_value: float | None = None
    is_significant: bool = False           # p < 0.05

    # Cohen's d effect size
    cohens_d: float | None = None
    effect_size_label: str = ""            # "negligible", "small", "medium", "large"

    interpretation: str = ""


@dataclass
class AnalysisReport:
    """Complete statistical analysis report for the experiment."""
    experiment_id: str = ""
    comparisons: list[ComparisonResult] = field(default_factory=list)
    analysed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sample_size_baseline: int = 0
    sample_size_smart: int = 0
    summary: str = ""

    def get_comparison(self, metric_name: str) -> ComparisonResult | None:
        for c in self.comparisons:
            if c.metric_name == metric_name:
                return c
        return None


class StatisticalAnalyser:
    """
    Performs comparative analysis between baseline and smart strategies.

    Usage::

        analyser = StatisticalAnalyser()
        report = analyser.analyse(
            baseline_metrics=[...],
            smart_metrics=[...],
        )
        print(report.summary)
        for c in report.comparisons:
            print(f"{c.metric_name}: p={c.wilcoxon_p_value:.4f}, d={c.cohens_d:.2f}")
    """

    SIGNIFICANCE_LEVEL = 0.05

    def analyse(
        self,
        baseline_metrics: list[RunMetrics],
        smart_metrics: list[RunMetrics],
        experiment_id: str = "",
    ) -> AnalysisReport:
        """
        Run the full comparative analysis.

        Args:
            baseline_metrics: Metrics from baseline strategy runs.
            smart_metrics:    Metrics from smart strategy runs.
            experiment_id:    Experiment identifier.

        Returns:
            AnalysisReport with all comparisons and interpretations.
        """
        report = AnalysisReport(
            experiment_id=experiment_id,
            sample_size_baseline=len(baseline_metrics),
            sample_size_smart=len(smart_metrics),
        )

        # Extract metric arrays
        metric_pairs = [
            (
                "alert_volume",
                [m.alert_volume for m in baseline_metrics],
                [m.alert_volume for m in smart_metrics],
                "lower is better",
            ),
            (
                "mttd_seconds",
                [m.mttd_seconds for m in baseline_metrics if m.mttd_seconds is not None],
                [m.mttd_seconds for m in smart_metrics if m.mttd_seconds is not None],
                "lower is better",
            ),
            (
                "actionable_ratio",
                [m.actionable_ratio for m in baseline_metrics if m.actionable_ratio is not None],
                [m.actionable_ratio for m in smart_metrics if m.actionable_ratio is not None],
                "higher is better",
            ),
            (
                "false_positive_rate",
                [m.false_positive_rate for m in baseline_metrics if m.false_positive_rate is not None],
                [m.false_positive_rate for m in smart_metrics if m.false_positive_rate is not None],
                "lower is better",
            ),
        ]

        for metric_name, baseline_vals, smart_vals, direction in metric_pairs:
            comparison = self._compare_metric(
                metric_name, baseline_vals, smart_vals, direction
            )
            report.comparisons.append(comparison)

        # Generate summary narrative
        report.summary = self._generate_summary(report)

        logger.info("Statistical analysis complete: %d comparisons", len(report.comparisons))
        return report

    # ── Metric comparison ────────────────────────────────────────────────

    def _compare_metric(
        self,
        name: str,
        baseline: list[float],
        smart: list[float],
        direction: str,
    ) -> ComparisonResult:
        """Compare a single metric between strategies."""
        b_stats = self._descriptive(name, baseline)
        s_stats = self._descriptive(name, smart)

        result = ComparisonResult(
            metric_name=name,
            baseline_stats=b_stats,
            smart_stats=s_stats,
        )

        if b_stats.n == 0 or s_stats.n == 0:
            result.interpretation = f"Insufficient data for {name} comparison."
            return result

        # Percentage change
        if b_stats.mean != 0:
            result.absolute_change = s_stats.mean - b_stats.mean
            result.pct_change = (result.absolute_change / abs(b_stats.mean)) * 100

        # Wilcoxon signed-rank test (requires paired samples of equal length)
        min_n = min(b_stats.n, s_stats.n)
        if min_n >= 5:
            b_arr = np.array(baseline[:min_n])
            s_arr = np.array(smart[:min_n])

            # Only run if there is variance in the differences
            diffs = b_arr - s_arr
            if np.any(diffs != 0):
                try:
                    stat, p_val = scipy_stats.wilcoxon(b_arr, s_arr, alternative="two-sided")
                    result.wilcoxon_statistic = float(stat)
                    result.wilcoxon_p_value = float(p_val)
                    result.is_significant = p_val < self.SIGNIFICANCE_LEVEL
                except Exception as exc:
                    logger.warning("Wilcoxon test failed for %s: %s", name, exc)

        # Cohen's d effect size
        result.cohens_d = self._cohens_d(baseline, smart)
        if result.cohens_d is not None:
            result.effect_size_label = self._effect_size_label(abs(result.cohens_d))

        # Interpretation
        result.interpretation = self._interpret(result, direction)

        return result

    # ── Descriptive statistics ───────────────────────────────────────────

    @staticmethod
    def _descriptive(name: str, values: list[float]) -> DescriptiveStats:
        """Compute descriptive statistics for a list of values."""
        if not values:
            return DescriptiveStats(metric_name=name)

        arr = np.array(values, dtype=float)
        q1, q3 = float(np.percentile(arr, 25)), float(np.percentile(arr, 75))

        return DescriptiveStats(
            metric_name=name,
            n=len(arr),
            mean=float(np.mean(arr)),
            median=float(np.median(arr)),
            std=float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
            min=float(np.min(arr)),
            max=float(np.max(arr)),
            q1=q1,
            q3=q3,
            iqr=q3 - q1,
        )

    # ── Cohen's d ────────────────────────────────────────────────────────

    @staticmethod
    def _cohens_d(group1: list[float], group2: list[float]) -> float | None:
        """
        Compute Cohen's d effect size.

        d = (mean1 - mean2) / pooled_std

        Interpretation (Cohen, 1988):
            |d| < 0.2  → negligible
            0.2–0.5    → small
            0.5–0.8    → medium
            > 0.8      → large
        """
        if len(group1) < 2 or len(group2) < 2:
            return None

        a1, a2 = np.array(group1, dtype=float), np.array(group2, dtype=float)
        n1, n2 = len(a1), len(a2)
        var1, var2 = np.var(a1, ddof=1), np.var(a2, ddof=1)

        # Pooled standard deviation
        pooled_var = ((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2)
        pooled_std = math.sqrt(pooled_var)

        if pooled_std == 0:
            # Both groups have zero variance — if means differ, effect is infinite
            if np.mean(a1) != np.mean(a2):
                return float("inf") if np.mean(a1) > np.mean(a2) else float("-inf")
            return 0.0

        return float((np.mean(a1) - np.mean(a2)) / pooled_std)

    @staticmethod
    def _effect_size_label(d_abs: float) -> str:
        """Classify effect size magnitude."""
        if math.isinf(d_abs):
            return "large"
        if d_abs < 0.2:
            return "negligible"
        elif d_abs < 0.5:
            return "small"
        elif d_abs < 0.8:
            return "medium"
        else:
            return "large"

    # ── Interpretation ───────────────────────────────────────────────────

    def _interpret(self, result: ComparisonResult, direction: str) -> str:
        """Generate human-readable interpretation."""
        parts = []

        name = result.metric_name.replace("_", " ").title()
        b = result.baseline_stats
        s = result.smart_stats

        parts.append(
            f"{name}: baseline mean={b.mean:.2f} (sd={b.std:.2f}), "
            f"smart mean={s.mean:.2f} (sd={s.std:.2f})."
        )

        if result.pct_change is not None:
            change_dir = "decrease" if result.pct_change < 0 else "increase"
            parts.append(f"{abs(result.pct_change):.1f}% {change_dir} under smart strategy.")

        if result.wilcoxon_p_value is not None:
            sig = "statistically significant" if result.is_significant else "not statistically significant"
            parts.append(
                f"Wilcoxon p={result.wilcoxon_p_value:.4f} ({sig} at α=0.05)."
            )

        if result.cohens_d is not None:
            parts.append(
                f"Cohen's d={result.cohens_d:.2f} ({result.effect_size_label} effect)."
            )

        return " ".join(parts)

    # ── Summary ──────────────────────────────────────────────────────────

    def _generate_summary(self, report: AnalysisReport) -> str:
        """Generate overall experiment summary."""
        lines = [
            "EXPERIMENT ANALYSIS SUMMARY",
            "=" * 50,
            f"Baseline runs: {report.sample_size_baseline}",
            f"Smart runs:    {report.sample_size_smart}",
            "",
        ]

        vol = report.get_comparison("alert_volume")
        mttd = report.get_comparison("mttd_seconds")
        ratio = report.get_comparison("actionable_ratio")
        fpr = report.get_comparison("false_positive_rate")

        if vol and vol.pct_change is not None:
            lines.append(f"Alert Volume:      {vol.pct_change:+.1f}% change")
        if mttd and mttd.pct_change is not None:
            lines.append(f"MTTD:              {mttd.pct_change:+.1f}% change")
        if ratio and ratio.pct_change is not None:
            lines.append(f"Actionable Ratio:  {ratio.pct_change:+.1f}% change")
        if fpr and fpr.pct_change is not None:
            lines.append(f"False Positive Rate: {fpr.pct_change:+.1f}% change")

        lines.append("")

        # Hypothesis conclusion
        if vol and vol.pct_change is not None and vol.pct_change < 0:
            if mttd and (mttd.pct_change is None or mttd.pct_change <= 5):
                lines.append(
                    "CONCLUSION: Smart alerting REDUCED alert volume "
                    "without meaningfully increasing MTTD — hypothesis supported."
                )
            else:
                lines.append(
                    "CONCLUSION: Smart alerting reduced alert volume but "
                    "MTTD increased — further investigation needed."
                )
        else:
            lines.append("CONCLUSION: Insufficient evidence to support hypothesis.")

        return "\n".join(lines)
