"""
Pre-Execution Validation Suite (AI-14b)
========================================
Confirms that the Docker environment, database schemas, dbt models,
and SMTP configuration are functioning correctly before any
experimental run begins.  Chapter 3 §3.6.7 specifies this as the
first of three validation phases.

Checks performed:
    1. PostgreSQL warehouse connectivity + schema existence
    2. PostgreSQL observability connectivity + table existence
    3. dbt project parseable (models compile)
    4. SMTP connectivity (optional — can be skipped for dry runs)
    5. Failure scenarios config loadable
    6. Baseline data present in raw schema

A failing check halts the experiment before any data is modified.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.config import PipelineConfig
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Outcome of a single validation check."""
    check_name: str
    passed: bool
    message: str
    duration_ms: int = 0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Aggregated results from all validation checks."""
    results: list[ValidationResult] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def critical_passed(self) -> bool:
        """True if all non-optional checks passed."""
        critical_checks = {"warehouse_connectivity", "observability_connectivity",
                           "warehouse_schemas", "observability_tables",
                           "scenarios_loadable"}
        return all(
            r.passed for r in self.results
            if r.check_name in critical_checks
        )

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def summary(self) -> str:
        lines = [f"Validation Report — {self.validated_at.isoformat()}"]
        lines.append(f"  Passed: {self.passed_count}/{len(self.results)}")
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{status}] {r.check_name}: {r.message}")
        return "\n".join(lines)


class PreExecutionValidator:
    """
    Runs all pre-execution checks and returns a ValidationReport.

    Usage::

        validator = PreExecutionValidator(config, db)
        report = validator.run_all()
        if not report.all_passed:
            print(report.summary())
            sys.exit(1)
    """

    EXPECTED_WAREHOUSE_SCHEMAS = ["raw", "staging", "transformed", "quality"]
    EXPECTED_OBSERVABILITY_TABLES = [
        "monitoring.pipeline_executions",
        "monitoring.injected_failures",
        "monitoring.alerts",
        "monitoring.experiment_metrics",
        "monitoring.quality_check_results",
    ]

    def __init__(
        self,
        config: PipelineConfig,
        db: DatabaseManager,
        dbt_project_path: str = "dbt_project",
        scenarios_path: str = "config/failure_scenarios.yml",
    ):
        self._config = config
        self._db = db
        self._dbt_path = Path(dbt_project_path)
        self._scenarios_path = Path(scenarios_path)

    def run_all(self, check_smtp: bool = True) -> ValidationReport:
        """
        Execute all validation checks.

        Args:
            check_smtp: If True, test SMTP connectivity (requires Gmail credentials).

        Returns:
            ValidationReport with all check results.
        """
        report = ValidationReport()

        checks = [
            self._check_warehouse_connectivity,
            self._check_observability_connectivity,
            self._check_warehouse_schemas,
            self._check_observability_tables,
            self._check_dbt_parseable,
            self._check_scenarios_loadable,
            self._check_baseline_data,
        ]

        if check_smtp:
            checks.append(self._check_smtp_connectivity)

        for check_fn in checks:
            start = datetime.now(timezone.utc)
            try:
                result = check_fn()
            except Exception as exc:
                result = ValidationResult(
                    check_name=check_fn.__name__.replace("_check_", ""),
                    passed=False,
                    message=f"Unexpected error: {exc}",
                )
            result.duration_ms = int(
                (datetime.now(timezone.utc) - start).total_seconds() * 1000
            )
            report.results.append(result)

            status = "PASS" if result.passed else "FAIL"
            logger.info("[%s] %s: %s", status, result.check_name, result.message)

        return report

    # ── Individual checks ────────────────────────────────────────────────

    def _check_warehouse_connectivity(self) -> ValidationResult:
        """Test PostgreSQL warehouse connection."""
        try:
            rows = self._db.execute_warehouse("SELECT 1 AS ok")
            return ValidationResult(
                check_name="warehouse_connectivity",
                passed=True,
                message=f"Connected to {self._config.db.warehouse_db}",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="warehouse_connectivity",
                passed=False,
                message=f"Cannot connect: {exc}",
            )

    def _check_observability_connectivity(self) -> ValidationResult:
        """Test PostgreSQL observability connection."""
        try:
            self._db.execute_observability("SELECT 1 AS ok")
            return ValidationResult(
                check_name="observability_connectivity",
                passed=True,
                message=f"Connected to {self._config.db.observability_db}",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="observability_connectivity",
                passed=False,
                message=f"Cannot connect: {exc}",
            )

    def _check_warehouse_schemas(self) -> ValidationResult:
        """Verify all expected warehouse schemas exist."""
        try:
            rows = self._db.execute_warehouse(
                "SELECT schema_name FROM information_schema.schemata"
            )
            existing = {r["schema_name"] for r in rows}
            missing = [s for s in self.EXPECTED_WAREHOUSE_SCHEMAS if s not in existing]

            if missing:
                return ValidationResult(
                    check_name="warehouse_schemas",
                    passed=False,
                    message=f"Missing schemas: {missing}",
                    details={"existing": list(existing), "missing": missing},
                )
            return ValidationResult(
                check_name="warehouse_schemas",
                passed=True,
                message=f"All {len(self.EXPECTED_WAREHOUSE_SCHEMAS)} schemas present",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="warehouse_schemas",
                passed=False,
                message=f"Schema check failed: {exc}",
            )

    def _check_observability_tables(self) -> ValidationResult:
        """Verify all expected observability tables exist."""
        try:
            rows = self._db.execute_observability("""
                SELECT table_schema || '.' || table_name AS qualified
                FROM information_schema.tables
                WHERE table_schema = 'monitoring'
            """)
            existing = {r["qualified"] for r in rows}
            missing = [t for t in self.EXPECTED_OBSERVABILITY_TABLES if t not in existing]

            if missing:
                return ValidationResult(
                    check_name="observability_tables",
                    passed=False,
                    message=f"Missing tables: {missing}",
                    details={"existing": list(existing), "missing": missing},
                )
            return ValidationResult(
                check_name="observability_tables",
                passed=True,
                message=f"All {len(self.EXPECTED_OBSERVABILITY_TABLES)} tables present",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="observability_tables",
                passed=False,
                message=f"Table check failed: {exc}",
            )

    def _check_dbt_parseable(self) -> ValidationResult:
        """Verify dbt project parses without errors."""
        if not self._dbt_path.exists():
            return ValidationResult(
                check_name="dbt_parseable",
                passed=False,
                message=f"dbt project not found at {self._dbt_path}",
            )
        try:
            result = subprocess.run(
                ["dbt", "parse", "--profiles-dir", str(self._dbt_path)],
                cwd=str(self._dbt_path),
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                return ValidationResult(
                    check_name="dbt_parseable",
                    passed=True,
                    message="dbt project parses successfully",
                )
            else:
                return ValidationResult(
                    check_name="dbt_parseable",
                    passed=False,
                    message=f"dbt parse failed: {result.stderr[:200]}",
                )
        except FileNotFoundError:
            return ValidationResult(
                check_name="dbt_parseable",
                passed=False,
                message="dbt command not found — is dbt installed?",
            )
        except subprocess.TimeoutExpired:
            return ValidationResult(
                check_name="dbt_parseable",
                passed=False,
                message="dbt parse timed out (>60s)",
            )

    def _check_scenarios_loadable(self) -> ValidationResult:
        """Verify failure scenarios YAML loads correctly."""
        try:
            from ..injection.scenarios import load_scenarios
            scenarios = load_scenarios(self._scenarios_path)
            return ValidationResult(
                check_name="scenarios_loadable",
                passed=True,
                message=f"Loaded {len(scenarios)} scenarios",
                details={"scenario_ids": [s.id for s in scenarios]},
            )
        except Exception as exc:
            return ValidationResult(
                check_name="scenarios_loadable",
                passed=False,
                message=f"Failed to load scenarios: {exc}",
            )

    def _check_baseline_data(self) -> ValidationResult:
        """Check if raw.raw_transactions has data loaded."""
        try:
            rows = self._db.execute_warehouse(
                "SELECT count(*) AS cnt FROM raw.raw_transactions"
            )
            count = rows[0]["cnt"] if rows else 0
            if count > 0:
                return ValidationResult(
                    check_name="baseline_data",
                    passed=True,
                    message=f"raw.raw_transactions has {count:,} rows",
                )
            return ValidationResult(
                check_name="baseline_data",
                passed=False,
                message="raw.raw_transactions is empty — run Phase 1 extract/load first",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="baseline_data",
                passed=False,
                message=f"Data check failed: {exc}",
            )

    def _check_smtp_connectivity(self) -> ValidationResult:
        """Test Gmail SMTP connection."""
        try:
            from ..alerting.smtp_sender import SMTPSender
            sender = SMTPSender(self._config.smtp)
            if sender.test_connection():
                return ValidationResult(
                    check_name="smtp_connectivity",
                    passed=True,
                    message=f"SMTP connected to {self._config.smtp.host}:{self._config.smtp.port}",
                )
            return ValidationResult(
                check_name="smtp_connectivity",
                passed=False,
                message="SMTP connection test returned False",
            )
        except Exception as exc:
            return ValidationResult(
                check_name="smtp_connectivity",
                passed=False,
                message=f"SMTP check failed: {exc}",
            )
