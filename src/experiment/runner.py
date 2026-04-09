"""
Experiment Runner (AI-14)
==========================
Orchestrates the full experimental evaluation by executing every
failure scenario under both alerting strategies (baseline and smart)
with pipeline reset between runs.

Execution protocol per scenario (Chapter 3 §3.6.5):
    1. Reset pipeline to clean state (AI-9a)
    2. Load fresh baseline data
    3. Inject failures per scenario config (AI-8)
    4. Run dbt transformations + tests
    5. Process test results through active alerting strategy
    6. Record all metrics to observability DB
    7. Repeat under the other strategy

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.config import PipelineConfig
from ..core.database import DatabaseManager
from ..alerting.strategies import (
    AlertingStrategy, TestFailure, AlertOutcome, create_strategy,
)
from ..alerting.observability_writer import ObservabilityWriter
from ..injection.engine import FailureInjectionEngine, InjectionRecord
from ..injection.reset import PipelineReset
from ..injection.scenarios import Scenario, FailureStep, load_scenarios
from .config_manager import ConfigurationManager

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Outcome of a single scenario × strategy execution."""
    run_id: str
    scenario_id: str
    scenario_name: str
    alerting_mode: str
    execution_id: str
    injections: list[InjectionRecord] = field(default_factory=list)
    outcomes: list[AlertOutcome] = field(default_factory=list)
    dbt_test_results: list[dict[str, Any]] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    duration_ms: int = 0
    error: str | None = None

    @property
    def email_count(self) -> int:
        return sum(1 for o in self.outcomes if o.channel == "email")

    @property
    def log_count(self) -> int:
        return sum(1 for o in self.outcomes if o.channel == "log")

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass
class ExperimentResult:
    """Aggregated results across all scenarios and strategies."""
    experiment_id: str
    runs: list[RunResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    @property
    def baseline_runs(self) -> list[RunResult]:
        return [r for r in self.runs if r.alerting_mode == "baseline"]

    @property
    def smart_runs(self) -> list[RunResult]:
        return [r for r in self.runs if r.alerting_mode == "smart"]


class ExperimentRunner:
    """
    Executes the full experimental evaluation.

    Usage::

        runner = ExperimentRunner(config_mgr, db)
        result = runner.run_all(
            scenarios_path="config/failure_scenarios.yml",
            record_count=10000,
            seed=42,
        )
        print(f"Total runs: {len(result.runs)}")
    """

    def __init__(
        self,
        config_mgr: ConfigurationManager,
        db: DatabaseManager,
        dbt_project_path: str = "dbt_project",
    ):
        self._config_mgr = config_mgr
        self._db = db
        self._dbt_path = Path(dbt_project_path)
        self._reset = PipelineReset(db)
        self._injector = FailureInjectionEngine(db)

    # ── Public API ───────────────────────────────────────────────────────

    def run_all(
        self,
        scenarios_path: str = "config/failure_scenarios.yml",
        record_count: int = 10_000,
        seed: int = 42,
        modes: list[str] | None = None,
    ) -> ExperimentResult:
        """
        Execute all scenarios under both strategies.

        Args:
            scenarios_path: Path to failure_scenarios.yml.
            record_count:   Baseline data row count.
            seed:           Random seed for reproducible baseline data.
            modes:          Strategies to test. Default: ["baseline", "smart"].

        Returns:
            ExperimentResult with all run outcomes.
        """
        modes = modes or ["baseline", "smart"]
        scenarios = load_scenarios(scenarios_path)
        experiment_id = f"exp-{uuid.uuid4().hex[:12]}"

        result = ExperimentResult(experiment_id=experiment_id)

        logger.info("=" * 70)
        logger.info("EXPERIMENT %s — Starting", experiment_id)
        logger.info("  Scenarios: %d", len(scenarios))
        logger.info("  Modes:     %s", modes)
        logger.info("  Baseline:  %d rows, seed=%d", record_count, seed)
        logger.info("=" * 70)

        total_runs = len(scenarios) * len(modes)
        run_num = 0

        for scenario in scenarios:
            for mode in modes:
                run_num += 1
                logger.info(
                    "\n--- Run %d/%d: %s × %s ---",
                    run_num, total_runs, scenario.id, mode,
                )

                run_result = self._execute_single_run(
                    scenario=scenario,
                    mode=mode,
                    record_count=record_count,
                    seed=seed,
                    experiment_id=experiment_id,
                )
                result.runs.append(run_result)

                if run_result.success:
                    logger.info(
                        "  Result: %d emails, %d logs, %d ms",
                        run_result.email_count,
                        run_result.log_count,
                        run_result.duration_ms,
                    )
                else:
                    logger.error("  FAILED: %s", run_result.error)

        result.completed_at = datetime.now(timezone.utc)

        logger.info("=" * 70)
        logger.info("EXPERIMENT %s — Complete", experiment_id)
        logger.info("  Total runs:     %d", len(result.runs))
        logger.info("  Successful:     %d", sum(1 for r in result.runs if r.success))
        logger.info("  Baseline emails: %d", sum(r.email_count for r in result.baseline_runs))
        logger.info("  Smart emails:    %d", sum(r.email_count for r in result.smart_runs))
        logger.info("=" * 70)

        return result

    def run_single_scenario(
        self,
        scenario_id: str,
        mode: str,
        scenarios_path: str = "config/failure_scenarios.yml",
        record_count: int = 10_000,
        seed: int = 42,
    ) -> RunResult:
        """Run a single scenario under a single strategy (for debugging)."""
        scenarios = load_scenarios(scenarios_path)
        scenario = next((s for s in scenarios if s.id == scenario_id), None)
        if scenario is None:
            raise ValueError(f"Scenario not found: {scenario_id}")

        return self._execute_single_run(
            scenario=scenario,
            mode=mode,
            record_count=record_count,
            seed=seed,
            experiment_id=f"single-{uuid.uuid4().hex[:8]}",
        )

    # ── Single run execution ─────────────────────────────────────────────

    def _execute_single_run(
        self,
        scenario: Scenario,
        mode: str,
        record_count: int,
        seed: int,
        experiment_id: str,
    ) -> RunResult:
        """Execute one scenario under one strategy."""
        execution_id = f"run-{uuid.uuid4().hex[:12]}"
        run_result = RunResult(
            run_id=f"{scenario.id}_{mode}",
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            alerting_mode=mode,
            execution_id=execution_id,
        )

        obs_writer = ObservabilityWriter(self._db)

        try:
            # Step 1: Reset pipeline
            logger.info("  [1/5] Resetting pipeline...")
            self._reset.full_reset(
                reload_data=True,
                record_count=record_count,
                seed=seed,
                clear_observability=True,
            )

            # Record execution start
            obs_writer.write_execution_start(
                execution_id=execution_id,
                alerting_mode=mode,
            )

            # Step 2: Inject failures
            logger.info("  [2/5] Injecting %d failures...", len(scenario.failures))
            for step in scenario.failures:
                record = self._injector.inject(
                    failure_type=step.failure_type,
                    table=step.table,
                    column=step.column,
                    row_count=step.row_count,
                    **step.params,
                )
                run_result.injections.append(record)

            # Step 3: Run dbt (transform + test)
            logger.info("  [3/5] Running dbt run + test...")
            dbt_results = self._run_dbt()
            run_result.dbt_test_results = dbt_results

            # Step 4: Process test failures through alerting strategy
            logger.info("  [4/5] Processing alerts (mode=%s)...", mode)
            config = self._config_mgr.for_mode(mode)
            strategy = create_strategy(config, self._db)

            failures = self._parse_dbt_results(dbt_results, execution_id)
            outcomes = strategy.process_failures(failures)
            run_result.outcomes = outcomes

            # Step 5: Record completion
            logger.info("  [5/5] Recording results...")
            run_result.completed_at = datetime.now(timezone.utc)
            run_result.duration_ms = int(
                (run_result.completed_at - run_result.started_at).total_seconds() * 1000
            )

            obs_writer.write_execution_end(
                execution_id=execution_id,
                status="completed",
                records_processed=record_count,
                records_failed=len(failures),
            )

        except Exception as exc:
            run_result.error = str(exc)
            run_result.completed_at = datetime.now(timezone.utc)
            run_result.duration_ms = int(
                (run_result.completed_at - run_result.started_at).total_seconds() * 1000
            )
            logger.error("Run failed: %s", exc, exc_info=True)

            obs_writer.write_execution_end(
                execution_id=execution_id,
                status="failed",
                error_message=str(exc),
            )

        return run_result

    # ── dbt execution ────────────────────────────────────────────────────

    def _run_dbt(self) -> list[dict[str, Any]]:
        """
        Execute dbt run + dbt test and parse results.

        Returns:
            List of test result dicts with name, status, severity, etc.
        """
        results = []

        # dbt run (transformations)
        run_cmd = subprocess.run(
            ["dbt", "run", "--profiles-dir", str(self._dbt_path)],
            cwd=str(self._dbt_path),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if run_cmd.returncode != 0:
            logger.warning("dbt run had issues: %s", run_cmd.stderr[:300])

        # dbt test (quality checks) — run even if dbt run had warnings
        test_cmd = subprocess.run(
            ["dbt", "test", "--profiles-dir", str(self._dbt_path),
             "--output", "json", "--output-path", "target"],
            cwd=str(self._dbt_path),
            capture_output=True,
            text=True,
            timeout=120,
        )

        # Parse JSON results from target/run_results.json
        results_path = self._dbt_path / "target" / "run_results.json"
        if results_path.exists():
            import json
            with open(results_path) as f:
                data = json.load(f)
            for r in data.get("results", []):
                results.append({
                    "unique_id": r.get("unique_id", ""),
                    "status": r.get("status", "unknown"),
                    "failures": r.get("failures", 0),
                    "message": r.get("message", ""),
                    "execution_time": r.get("execution_time", 0),
                    "adapter_response": r.get("adapter_response", {}),
                })

        logger.info("dbt test returned %d results", len(results))
        return results

    def _parse_dbt_results(
        self, dbt_results: list[dict], execution_id: str
    ) -> list[TestFailure]:
        """Convert dbt test results into TestFailure objects for the alerting strategy."""
        failures = []

        for r in dbt_results:
            status = r.get("status", "pass")
            if status in ("fail", "error", "warn"):
                unique_id = r.get("unique_id", "")
                # Extract severity from dbt config
                # Convention: tests with severity: error → "error", severity: warn → "warn"
                severity_config = "error" if status in ("fail", "error") else "warn"

                # Parse model/column from unique_id
                # e.g. "test.elt_alerting_pipeline.not_null_stg_transactions_transaction_id.abc123"
                parts = unique_id.split(".")
                test_name = parts[-2] if len(parts) >= 3 else unique_id

                failures.append(TestFailure(
                    test_name=test_name,
                    severity_config=severity_config,
                    model_name=self._extract_model_name(test_name),
                    column_name=self._extract_column_name(test_name),
                    message=r.get("message", ""),
                    rows_affected=r.get("failures", 0),
                    execution_id=execution_id,
                    metadata={"dbt_status": status, "unique_id": unique_id},
                ))

        return failures

    @staticmethod
    def _extract_model_name(test_name: str) -> str:
        """Extract model name from dbt test name convention."""
        # e.g. "not_null_stg_transactions_transaction_id" → "stg_transactions"
        known_models = [
            "stg_transactions", "int_transactions_enriched",
            "int_daily_sales", "mart_transactions",
            "mart_daily_summary", "mart_customer_summary",
        ]
        for model in known_models:
            if model in test_name:
                return model
        return ""

    @staticmethod
    def _extract_column_name(test_name: str) -> str:
        """Extract column name from dbt test name convention."""
        known_models = [
            "stg_transactions", "int_transactions_enriched",
            "int_daily_sales", "mart_transactions",
            "mart_daily_summary", "mart_customer_summary",
        ]
        for model in known_models:
            if model in test_name:
                remainder = test_name.split(model + "_", 1)
                if len(remainder) > 1:
                    return remainder[1]
        return ""
