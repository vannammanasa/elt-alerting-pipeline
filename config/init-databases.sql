-- =============================================================================
-- 01-init-databases.sql — Database & Role Initialisation
-- Project: An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines
-- =============================================================================
-- Creates three isolated databases and a shared application role:
--   1. elt_warehouse      — Primary data warehouse (raw + transformed layers)
--   2. observability       — Execution logs, alert records, experiment metrics
--   3. airflow_metadata    — Apache Airflow internal metadata store
-- =============================================================================

-- ── Application Role ────────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pipeline_user') THEN
        CREATE ROLE pipeline_user WITH LOGIN PASSWORD 'pipeline_pass';
    END IF;
END
$$;

-- ── Airflow Role ────────────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow_pass' CREATEDB;
    END IF;
END
$$;

-- ── Databases ───────────────────────────────────────────────────────────────

-- Data Warehouse
SELECT 'CREATE DATABASE elt_warehouse OWNER pipeline_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'elt_warehouse')\gexec

-- Observability Store
SELECT 'CREATE DATABASE observability OWNER pipeline_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'observability')\gexec

-- Airflow Metadata
SELECT 'CREATE DATABASE airflow_metadata OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_metadata')\gexec

-- ── Grant Privileges ────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON DATABASE elt_warehouse  TO pipeline_user;
GRANT ALL PRIVILEGES ON DATABASE observability   TO pipeline_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow;
