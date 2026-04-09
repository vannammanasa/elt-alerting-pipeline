"""
Visualisation & Report Generator (AI-16a)
==========================================
Produces publication-quality charts and a summary report for
Chapter 4 of the dissertation:

    1. Bar chart:  Alert volume comparison (baseline vs smart)
    2. Box plot:   MTTD distribution comparison
    3. Bar chart:  Actionable alert ratio comparison
    4. Bar chart:  False positive rate comparison
    5. Summary table: All metrics side-by-side with statistical results

Output formats: PNG (for dissertation insertion) and a text report.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server/Docker
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from .metrics import RunMetrics
from .statistics import AnalysisReport, ComparisonResult

logger = logging.getLogger(__name__)

# ── Style constants ──────────────────────────────────────────────────────
BASELINE_COLOR = "#3498db"       # Blue
SMART_COLOR = "#2ecc71"          # Green
FIGSIZE = (10, 6)
DPI = 200
FONT_TITLE = 14
FONT_LABEL = 12
FONT_TICK = 10


class ReportGenerator:
    """
    Generates charts and a text report from experiment results.

    Usage::

        gen = ReportGenerator(output_dir="reports")
        gen.generate_all(
            baseline_metrics=[...],
            smart_metrics=[...],
            analysis_report=report,
        )
        # Outputs: reports/alert_volume.png, reports/mttd_boxplot.png, etc.
    """

    def __init__(self, output_dir: str = "reports"):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate_all(
        self,
        baseline_metrics: list[RunMetrics],
        smart_metrics: list[RunMetrics],
        analysis_report: AnalysisReport,
    ) -> list[str]:
        """
        Generate all charts and the text report.

        Returns:
            List of file paths for generated artefacts.
        """
        files = []

        files.append(self.chart_alert_volume(baseline_metrics, smart_metrics))
        files.append(self.chart_mttd_boxplot(baseline_metrics, smart_metrics))
        files.append(self.chart_actionable_ratio(baseline_metrics, smart_metrics))
        files.append(self.chart_false_positive_rate(baseline_metrics, smart_metrics))
        files.append(self.chart_metric_comparison_table(analysis_report))
        files.append(self.generate_text_report(analysis_report))

        logger.info("Generated %d report artefacts in %s", len(files), self._output_dir)
        return files

    # ── Chart 1: Alert Volume ────────────────────────────────────────────

    def chart_alert_volume(
        self,
        baseline: list[RunMetrics],
        smart: list[RunMetrics],
    ) -> str:
        """Bar chart comparing alert volume (email count) per scenario."""
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)

        b_vols = [m.alert_volume for m in baseline]
        s_vols = [m.alert_volume for m in smart]
        scenarios = [m.scenario_id or f"Run {i+1}" for i, m in enumerate(baseline)]

        x = np.arange(len(scenarios))
        width = 0.35

        bars_b = ax.bar(x - width/2, b_vols, width, label="Baseline (uniform)",
                        color=BASELINE_COLOR, edgecolor="white", linewidth=0.5)
        bars_s = ax.bar(x + width/2, s_vols, width, label="Smart (severity-based)",
                        color=SMART_COLOR, edgecolor="white", linewidth=0.5)

        # Value labels on bars
        for bar in bars_b:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f"{int(bar.get_height())}", ha="center", va="bottom", fontsize=9)
        for bar in bars_s:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f"{int(bar.get_height())}", ha="center", va="bottom", fontsize=9)

        ax.set_xlabel("Failure Scenario", fontsize=FONT_LABEL)
        ax.set_ylabel("Alert Volume (Email Count)", fontsize=FONT_LABEL)
        ax.set_title("Alert Volume: Baseline vs Smart Alerting", fontsize=FONT_TITLE, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(scenarios, rotation=45, ha="right", fontsize=FONT_TICK)
        ax.legend(fontsize=FONT_TICK)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", alpha=0.3)

        # Reduction annotation
        total_b = sum(b_vols)
        total_s = sum(s_vols)
        if total_b > 0:
            pct = ((total_b - total_s) / total_b) * 100
            ax.annotate(
                f"Total reduction: {pct:.0f}%\n({total_b} → {total_s} emails)",
                xy=(0.98, 0.95), xycoords="axes fraction",
                ha="right", va="top", fontsize=10,
                bbox=dict(boxstyle="round,pad=0.4", facecolor="#e8f8e8", alpha=0.8),
            )

        plt.tight_layout()
        path = str(self._output_dir / "alert_volume.png")
        fig.savefig(path)
        plt.close(fig)
        logger.info("Chart saved: %s", path)
        return path

    # ── Chart 2: MTTD Box Plot ───────────────────────────────────────────

    def chart_mttd_boxplot(
        self,
        baseline: list[RunMetrics],
        smart: list[RunMetrics],
    ) -> str:
        """Box plot comparing MTTD distribution between strategies."""
        fig, ax = plt.subplots(figsize=(8, 6), dpi=DPI)

        b_mttd = [m.mttd_seconds for m in baseline if m.mttd_seconds is not None]
        s_mttd = [m.mttd_seconds for m in smart if m.mttd_seconds is not None]

        data = [b_mttd, s_mttd] if b_mttd or s_mttd else [[0], [0]]
        bp = ax.boxplot(
            data,
            tick_labels=["Baseline", "Smart"],
            patch_artist=True,
            widths=0.5,
            medianprops=dict(color="black", linewidth=2),
        )

        bp["boxes"][0].set_facecolor(BASELINE_COLOR)
        bp["boxes"][0].set_alpha(0.6)
        if len(bp["boxes"]) > 1:
            bp["boxes"][1].set_facecolor(SMART_COLOR)
            bp["boxes"][1].set_alpha(0.6)

        # Overlay data points
        for i, vals in enumerate(data):
            jitter = np.random.default_rng(42).uniform(-0.1, 0.1, len(vals))
            ax.scatter(
                np.full(len(vals), i + 1) + jitter,
                vals, alpha=0.4, s=20, zorder=3,
                color=BASELINE_COLOR if i == 0 else SMART_COLOR,
            )

        ax.set_ylabel("Mean Time to Detect (seconds)", fontsize=FONT_LABEL)
        ax.set_title("MTTD Distribution: Baseline vs Smart Alerting",
                      fontsize=FONT_TITLE, fontweight="bold")
        ax.grid(axis="y", alpha=0.3)

        # Mean annotation
        if b_mttd and s_mttd:
            ax.annotate(
                f"Baseline mean: {np.mean(b_mttd):.2f}s\nSmart mean: {np.mean(s_mttd):.2f}s",
                xy=(0.98, 0.95), xycoords="axes fraction",
                ha="right", va="top", fontsize=10,
                bbox=dict(boxstyle="round,pad=0.4", facecolor="#f0f0f0", alpha=0.8),
            )

        plt.tight_layout()
        path = str(self._output_dir / "mttd_boxplot.png")
        fig.savefig(path)
        plt.close(fig)
        logger.info("Chart saved: %s", path)
        return path

    # ── Chart 3: Actionable Ratio ────────────────────────────────────────

    def chart_actionable_ratio(
        self,
        baseline: list[RunMetrics],
        smart: list[RunMetrics],
    ) -> str:
        """Grouped bar chart for actionable alert ratio."""
        fig, ax = plt.subplots(figsize=(8, 5), dpi=DPI)

        b_ratios = [m.actionable_ratio or 0 for m in baseline]
        s_ratios = [m.actionable_ratio or 0 for m in smart]

        b_mean = np.mean(b_ratios) if b_ratios else 0
        s_mean = np.mean(s_ratios) if s_ratios else 0

        bars = ax.bar(
            ["Baseline", "Smart"],
            [b_mean * 100, s_mean * 100],
            color=[BASELINE_COLOR, SMART_COLOR],
            edgecolor="white", linewidth=0.5, width=0.5,
        )

        for bar in bars:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=11)

        ax.set_ylabel("Actionable Alert Ratio (%)", fontsize=FONT_LABEL)
        ax.set_title("Actionable Alert Ratio: Baseline vs Smart",
                      fontsize=FONT_TITLE, fontweight="bold")
        ax.set_ylim(0, 110)
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = str(self._output_dir / "actionable_ratio.png")
        fig.savefig(path)
        plt.close(fig)
        logger.info("Chart saved: %s", path)
        return path

    # ── Chart 4: False Positive Rate ─────────────────────────────────────

    def chart_false_positive_rate(
        self,
        baseline: list[RunMetrics],
        smart: list[RunMetrics],
    ) -> str:
        """Bar chart for false positive rate comparison."""
        fig, ax = plt.subplots(figsize=(8, 5), dpi=DPI)

        b_fpr = [m.false_positive_rate or 0 for m in baseline]
        s_fpr = [m.false_positive_rate or 0 for m in smart]

        b_mean = np.mean(b_fpr) * 100 if b_fpr else 0
        s_mean = np.mean(s_fpr) * 100 if s_fpr else 0

        bars = ax.bar(
            ["Baseline", "Smart"],
            [b_mean, s_mean],
            color=[BASELINE_COLOR, SMART_COLOR],
            edgecolor="white", linewidth=0.5, width=0.5,
        )

        for bar in bars:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                    f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=11)

        ax.set_ylabel("False Positive Rate (%)", fontsize=FONT_LABEL)
        ax.set_title("False Positive Rate: Baseline vs Smart",
                      fontsize=FONT_TITLE, fontweight="bold")
        ax.set_ylim(0, max(b_mean, s_mean) * 1.3 + 5)
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = str(self._output_dir / "false_positive_rate.png")
        fig.savefig(path)
        plt.close(fig)
        logger.info("Chart saved: %s", path)
        return path

    # ── Chart 5: Summary comparison table ────────────────────────────────

    def chart_metric_comparison_table(self, report: AnalysisReport) -> str:
        """Render a comparison table as an image for the dissertation."""
        fig, ax = plt.subplots(figsize=(12, 4), dpi=DPI)
        ax.axis("off")

        headers = ["Metric", "Baseline (mean±sd)", "Smart (mean±sd)",
                    "Change", "p-value", "Cohen's d", "Significant"]

        rows = []
        for c in report.comparisons:
            b = c.baseline_stats
            s = c.smart_stats
            rows.append([
                c.metric_name.replace("_", " ").title(),
                f"{b.mean:.2f} ± {b.std:.2f}",
                f"{s.mean:.2f} ± {s.std:.2f}",
                f"{c.pct_change:+.1f}%" if c.pct_change is not None else "N/A",
                f"{c.wilcoxon_p_value:.4f}" if c.wilcoxon_p_value is not None else "N/A",
                f"{c.cohens_d:.2f} ({c.effect_size_label})" if c.cohens_d is not None else "N/A",
                "Yes" if c.is_significant else "No",
            ])

        table = ax.table(
            cellText=rows,
            colLabels=headers,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        # Style header
        for j, header in enumerate(headers):
            table[0, j].set_facecolor("#2C3E50")
            table[0, j].set_text_props(color="white", fontweight="bold")

        # Alternate row colors
        for i in range(len(rows)):
            color = "#f8f9fa" if i % 2 == 0 else "white"
            for j in range(len(headers)):
                table[i + 1, j].set_facecolor(color)

        ax.set_title(
            "Table: Comparative Analysis of Alerting Strategies",
            fontsize=FONT_TITLE, fontweight="bold", pad=20,
        )

        plt.tight_layout()
        path = str(self._output_dir / "comparison_table.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        logger.info("Chart saved: %s", path)
        return path

    # ── Text report ──────────────────────────────────────────────────────

    def generate_text_report(self, report: AnalysisReport) -> str:
        """Write a plain-text summary report."""
        lines = [
            "=" * 70,
            "EXPERIMENT RESULTS REPORT",
            f"Generated: {datetime.now(timezone.utc).isoformat()}",
            "=" * 70,
            "",
            report.summary,
            "",
            "-" * 70,
            "DETAILED COMPARISONS",
            "-" * 70,
        ]

        for c in report.comparisons:
            lines.append("")
            lines.append(c.interpretation)

        lines.append("")
        lines.append("=" * 70)
        lines.append("END OF REPORT")
        lines.append("=" * 70)

        text = "\n".join(lines)
        path = str(self._output_dir / "experiment_report.txt")
        with open(path, "w") as f:
            f.write(text)

        logger.info("Text report saved: %s", path)
        return path
