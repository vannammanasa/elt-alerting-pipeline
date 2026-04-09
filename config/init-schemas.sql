-- =============================================================================
-- 02-init-schemas.sql — Schema & Table Initialisation
-- Project: An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines
-- =============================================================================
-- Executed automatically on first container start via docker-entrypoint-initdb.d.
-- Sets up the layered warehouse schema and the observability tables used to
-- capture experiment metrics (alert volume, MTTD, actionable ratio, etc.).
-- =============================================================================

-- *****************************************************************************
-- ELT WAREHOUSE DATABASE
-- *****************************************************************************
\connect elt_warehouse;

-- ── Schema Layers (ELT convention) ──────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;          -- Landing zone for extracted data
CREATE SCHEMA IF NOT EXISTS staging;      -- Light cleansing & type casting
CREATE SCHEMA IF NOT EXISTS transformed;  -- Business-logic transformations (dbt models)
CREATE SCHEMA IF NOT EXISTS quality;      -- dbt test results & quality metrics

-- Grant usage to the pipeline role
GRANT USAGE  ON SCHEMA raw, staging, transformed, quality TO pipeline_user;
GRANT CREATE ON SCHEMA raw, staging, transformed, quality TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw          GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging      GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed  GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality      GRANT ALL ON TABLES TO pipeline_user;


-- *****************************************************************************
-- OBSERVABILITY DATABASE
-- *****************************************************************************
\connect observability;

CREATE SCHEMA IF NOT EXISTS monitoring;

GRANT USAGE  ON SCHEMA monitoring TO pipeline_user;
GRANT CREATE ON SCHEMA monitoring TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT ALL ON TABLES TO pipeline_user;

-- ── Pipeline Execution Log ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS monitoring.pipeline_executions (
    execution_id        SERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    pipeline_name       VARCHAR(128) NOT NULL,
    experiment_mode     VARCHAR(16) NOT NULL CHECK (experiment_mode IN ('baseline', 'smart')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(16) NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'success', 'failure')),
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Failure Injection Log ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS monitoring.injected_failures (
    failure_id          SERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    failure_type        VARCHAR(64) NOT NULL,
    failure_severity    VARCHAR(16) NOT NULL CHECK (failure_severity IN ('critical', 'warning', 'info')),
    description         TEXT,
    injected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Alert Emission Log ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    alert_id            SERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    experiment_mode     VARCHAR(16) NOT NULL CHECK (experiment_mode IN ('baseline', 'smart')),
    alert_severity      VARCHAR(16) NOT NULL CHECK (alert_severity IN ('critical', 'warning', 'info')),
    alert_channel       VARCHAR(32) NOT NULL CHECK (alert_channel IN ('email', 'log', 'both')),
    failure_type        VARCHAR(64),
    message             TEXT NOT NULL,
    emitted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detected_at         TIMESTAMPTZ,
    acknowledged_at     TIMESTAMPTZ
);

-- ── Experiment Metrics (aggregated per run) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS monitoring.experiment_metrics (
    metric_id           SERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    experiment_mode     VARCHAR(16) NOT NULL CHECK (experiment_mode IN ('baseline', 'smart')),
    total_alerts        INTEGER NOT NULL DEFAULT 0,
    critical_alerts     INTEGER NOT NULL DEFAULT 0,
    warning_alerts      INTEGER NOT NULL DEFAULT 0,
    true_positives      INTEGER NOT NULL DEFAULT 0,
    false_positives     INTEGER NOT NULL DEFAULT 0,
    mean_time_to_detect NUMERIC(10,4),       -- Seconds
    actionable_ratio    NUMERIC(5,4),         -- Range 0.0000 – 1.0000
    alert_volume_rate   NUMERIC(10,4),        -- Alerts per hour
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Quality Check Results (mirrors dbt test outcomes) ───────────────────────
CREATE TABLE IF NOT EXISTS monitoring.quality_check_results (
    check_id            SERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    check_name          VARCHAR(256) NOT NULL,
    check_type          VARCHAR(64) NOT NULL,
    table_name          VARCHAR(256),
    column_name         VARCHAR(256),
    severity            VARCHAR(16) NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
    status              VARCHAR(16) NOT NULL CHECK (status IN ('pass', 'fail', 'warn', 'error')),
    rows_failing        INTEGER DEFAULT 0,
    executed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Indexes for Experiment Queries ──────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_alerts_run_id         ON monitoring.alerts (run_id);
CREATE INDEX IF NOT EXISTS idx_alerts_experiment      ON monitoring.alerts (experiment_mode);
CREATE INDEX IF NOT EXISTS idx_alerts_severity        ON monitoring.alerts (alert_severity);
CREATE INDEX IF NOT EXISTS idx_metrics_run_id         ON monitoring.experiment_metrics (run_id);
CREATE INDEX IF NOT EXISTS idx_metrics_experiment     ON monitoring.experiment_metrics (experiment_mode);
CREATE INDEX IF NOT EXISTS idx_executions_run_id      ON monitoring.pipeline_executions (run_id);
CREATE INDEX IF NOT EXISTS idx_failures_run_id        ON monitoring.injected_failures (run_id);
CREATE INDEX IF NOT EXISTS idx_quality_run_id         ON monitoring.quality_check_results (run_id);
