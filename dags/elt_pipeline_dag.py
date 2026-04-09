"""
Airflow DAG: ELT Pipeline with Severity-Based Alerting (AI-13)
================================================================
Defines the Directed Acyclic Graph for orchestrating the full
ELT pipeline.  Apache Airflow manages task dependencies, scheduling,
retry policies, and execution metadata — all of which feed the
MTTD computation in Phase 7.

Task chain:
    validate → extract_load → dbt_run → dbt_test → process_alerts

The DAG supports both manual triggering (for experimental runs)
and scheduled execution (for simulating real-world operations).
The alerting_mode is passed via Airflow Variables or DAG run config.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ── DAG default args ─────────────────────────────────────────────────────
default_args = {
    "owner": "manasa_vannam",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Task functions ───────────────────────────────────────────────────────

def task_validate(**context):
    """AI-14b: Run pre-execution validation checks."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.core.config import load_config
    from src.core.database import DatabaseManager
    from src.experiment.validator import PreExecutionValidator

    config = load_config()
    db = DatabaseManager(config.db)

    validator = PreExecutionValidator(config, db)
    report = validator.run_all(check_smtp=True)

    print(report.summary())

    if not report.critical_passed:
        db.close()
        raise RuntimeError(
            f"Pre-execution validation failed: "
            f"{report.failed_count} checks failed\n{report.summary()}"
        )

    db.close()
    return {"validation": "passed", "checks": report.passed_count}


def task_extract_load(**context):
    """Phase 1: Generate synthetic data and load into raw schema."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.core.config import load_config
    from src.core.database import DatabaseManager
    from src.extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig
    from src.loaders.raw_loader import RawLoader

    config = load_config()
    db = DatabaseManager(config.db)

    # Get params from DAG run config or defaults
    dag_conf = context.get("dag_run", {})
    conf = dag_conf.conf if hasattr(dag_conf, "conf") and dag_conf.conf else {}
    record_count = conf.get("record_count", 10_000)
    seed = conf.get("seed", 42)

    generator = SyntheticDataGenerator(SyntheticDataConfig(
        record_count=record_count, seed=seed
    ))
    df = generator.generate()

    loader = RawLoader(db, source_id="synthetic_v1")
    stats = loader.load(df, table_name="raw_transactions")

    db.close()
    return stats


def task_process_alerts(**context):
    """Phase 5: Process dbt test results through the active alerting strategy."""
    import json
    import sys
    from pathlib import Path
    sys.path.insert(0, "/opt/airflow")

    from src.core.config import load_config
    from src.core.database import DatabaseManager
    from src.alerting.strategies import create_strategy, TestFailure

    config = load_config()
    db = DatabaseManager(config.db)
    strategy = create_strategy(config, db)

    # Parse dbt test results
    results_path = Path("/opt/airflow/dbt_project/target/run_results.json")
    failures = []

    if results_path.exists():
        with open(results_path) as f:
            data = json.load(f)

        for r in data.get("results", []):
            status = r.get("status", "pass")
            if status in ("fail", "error", "warn"):
                unique_id = r.get("unique_id", "")
                parts = unique_id.split(".")
                test_name = parts[-2] if len(parts) >= 3 else unique_id
                severity_config = "error" if status in ("fail", "error") else "warn"

                failures.append(TestFailure(
                    test_name=test_name,
                    severity_config=severity_config,
                    message=r.get("message", ""),
                    rows_affected=r.get("failures", 0),
                    execution_id=context.get("run_id", "airflow-run"),
                ))

    outcomes = strategy.process_failures(failures)

    db.close()

    return {
        "mode": config.alerting_mode,
        "total_failures": len(failures),
        "emails_sent": sum(1 for o in outcomes if o.channel == "email"),
        "logs_written": sum(1 for o in outcomes if o.channel == "log"),
    }


# ── DAG definition ───────────────────────────────────────────────────────

with DAG(
    dag_id="elt_alerting_pipeline",
    default_args=default_args,
    description="ELT Pipeline with Severity-Based Alerting for Dissertation Experiment",
    schedule_interval=None,        # Manual trigger only (experiment-driven)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,             # One experiment at a time
    tags=["dissertation", "elt", "alerting"],
    params={
        "record_count": 10000,
        "seed": 42,
        "alerting_mode": "baseline",
    },
) as dag:

    # Task 1: Pre-execution validation
    t_validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        doc_md="AI-14b: Validates DB, schemas, dbt, SMTP before experiment run.",
    )

    # Task 2: Extract & Load synthetic data
    t_extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=task_extract_load,
        doc_md="Phase 1: Generate synthetic data and load into raw.raw_transactions.",
    )

    # Task 3: dbt run (transformations)
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
        doc_md="Phase 2: Execute dbt models (staging → intermediate → marts).",
    )

    # Task 4: dbt test (quality checks)
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir . || true",
        doc_md="Phase 3: Run 45 dbt quality tests. Exit 0 even on failures (alerts handle them).",
    )

    # Task 5: Process alerts through strategy
    t_alerts = PythonOperator(
        task_id="process_alerts",
        python_callable=task_process_alerts,
        doc_md="Phase 5: Route test failures through baseline or smart alerting strategy.",
    )

    # ── Task dependencies (the DAG) ──────────────────────────────────
    t_validate >> t_extract_load >> t_dbt_run >> t_dbt_test >> t_alerts
