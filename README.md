## ELT Alerting Pipeline - Experimental Implementation and System Architecture

**Project:** An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines: Minimising Alert Fatigue Without Compromising Detection.

**Author:** Manasa Vannam (S2471596)  
**Programme:** MSc Computer Science - Glasgow Caledonian University(London)

---
## Project Overview
This repository contains the practical implementation of the dissertation project on severity-based alerting in ELT pipelines. The system was developed to support a controlled experimental comparison between two alerting strategies:

Baseline alerting - all detected failures generate email alerts
Severity-based alerting - only critical failures generate email alerts, while warning-level events are logged to the observability database

The implementation supports the empirical evaluation presented in the dissertation by enabling reproducible failure injection, structured monitoring, alert routing, and metric collection

---

## Dissertation Alignment

This practical implementation directly supports the methodology and results chapters of the dissertation. In particular, it enables:

- reproducible ELT pipeline execution in a containerised environment
- controlled injection of representative pipeline failures(9 scenarios)
- comparison of baseline and severity-based alerting strategies
- storage of execution logs and alert records in an observability database
- computation of the three core evaluation metrics:
   - **Alert volume** - total email notifications per run
   - **Mean Time to Detect (MTTD)** - time from failure to alert delivery
   - **Actionable alert ratio** - proportion of critical alerts among all alerts

**Key experimental result:** The severity-based strategy achieved a 28.9% 
reduction in email alert volume (45 → 32 emails) whilst maintaining 
immediate MTTD for all critical failures and improving the actionable 
alert ratio from 71.1% to 100%.

---

## Architecture Overview

This environment provisions the complete infrastructure required for the ELT alerting pipeline experiments. All services are orchestrated through `docker-compose.yml` and can be managed through the `Makefile`.

### Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| **postgres** | `postgres:16` | `5432` | Data warehouse, observability store, Airflow metadata |
| **airflow-web** | `apache/airflow:2.10.4` | `8080` | Airflow web UI for DAG management |
| **airflow-sched** | `apache/airflow:2.10.4` | - | Airflow scheduler for triggering DAG runs |
| **pipeline** | Custom (Dockerfile) | - | Core ELT pipeline logic, dbt execution, failure injection, and alerting |


### PostgreSQL Databases

| Database | Owner | Purpose |
|---|---|---|
| `elt_warehouse` | `pipeline_user` | Raw → Staging → Transformed data layers |
| `observability` | `pipeline_user` | Execution logs, alert records, experiment metrics |
| `airflow_metadata` | `airflow` | Airflow internal state |

---

### Warehouse Schema Layers

| Schema | Layer | Contents |
|---|---|---|
| `raw` | Ingestion | `raw_transactions` - 10,000 synthetic e-commerce records |
| `staging` | Staging | `stg_transactions` - cleaned and validated view |
| `transformed` | Mart | `mart_transactions`, `mart_customer_summary`, `mart_daily_summary` |
| `quality` | Validation | dbt test results and quality check outputs |

---

## Observability Tables

The observability database stores the experimental records used for analysis:

| Table | Purpose |
|---|---|
| `pipeline_executions` | Execution timestamps, status, and pipeline metadata |
| `injected_failures` | Failure type, severity, affected rows, injection timestamp |
| `alerts` | Alert timestamp, severity, delivery channel (email/log), delivery status |
| `quality_check_results` | dbt test outcomes (pass/fail/warn) and severity configuration |
| `experiment_metrics` | Computed metrics: alert volume, MTTD, actionable alert ratio |

---
## Synthetic Dataset

The pipeline ingests a programmatically generated synthetic e-commerce 
transaction dataset with the following characteristics:

| Attribute | Value |
|---|---|
| Total records | 10,000 |
| Columns | 13 |
| Random seed | 42 (fixed for reproducibility) |
| Primary key | `transaction_id` (UUID) |
| Timestamp field | `event_timestamp` (UTC) |
| Key fields | `customer_id`, `product_id`, `product_category`, `quantity`, `unit_price`, `total_amount`, `payment_method`, `order_status`, `shipping_country`, `customer_email`, `is_returning_customer` |
| Storage location | `raw.raw_transactions` |

The fixed random seed (42) ensures that every experimental run operates 
on an identical dataset, supporting full reproducibility.

---
## dbt Quality Testing Framework

45 dbt tests are executed across staging and mart layers, classified into 
two severity categories:

| Severity | Count | dbt Config | Alert Action |
|---|---|---|---|
| Critical | 32 | `severity: error` | Immediate email notification |
| Warning | 13 | `severity: warn` | Logged to observability DB only |

**Test categories covered:**
- `not_null` - null value validation on required fields
- `unique` - duplicate detection on primary key fields
- `accepted_values` - categorical field validation
- `row_count` - volume threshold checks
- `freshness` - data arrival timeliness checks
- Custom macros - schema integrity and distribution checks

---
## Failure Injection Scenarios

Nine controlled failure scenarios were executed (one planned scenario - 
schema column drop - was excluded due to downstream dependency constraints):

| Scenario | Failure Type | Severity | Target | Rows Affected |
|---|---|---|---|---|
| S1 | NULL_PK - null primary key | Critical | transaction_id | 50 |
| S2 | NULL_MINOR - null non-essential column | Warning | shipping_country | 500 |
| S3 | SCHEMA_DRIFT - unexpected column added | Critical | unexpected_col | 0 (col added) |
| S4 | VOLUME_ANOMALY - 60% row deletion | Critical | raw_transactions | ~6,000 |
| S5 | FRESHNESS_DELAY - 48hr timestamp backdating | Warning | all timestamps | 4,000 |
| S6 | DUPLICATE - duplicate row injection | Warning | raw_transactions | 200 |
| S7 | MIXED_CRITICAL - NULL PK + volume drop | Critical | Multiple | 50 + ~6,000 |
| S8 | MIXED_WARNING - nulls + duplicates + freshness | Warning | Multiple | 500 + 200 + 4,000 |
| S9 | MIXED_BOTH - critical + warning combined | Both | Multiple | Combined |

---

## Prerequisites

- **Docker Desktop** ≥ 4.x (includes Docker Engine and Docker Compose)
- **Docker Compose** ≥ 2.x (V2 syntax — bundled with Docker Desktop)
- **Git** ≥ 2.x for version control
- **Gmail Account** with 2-Step Verification enabled and App Password generated
- ~4 GB free RAM (Airflow + PostgreSQL)
- ~2 GB free disk space

---

### Gmail App Password Setup

1. Go to `myaccount.google.com` → Security → 2-Step Verification
2. Navigate to `myaccount.google.com/apppasswords`
3. Select Mail as the app, enter "ELT Pipeline" as the name
4. Copy the 16-character App Password for use in `.env`

> **WARNING:** Never use your regular Gmail password. Use only the App Password.

---

## Security Note

The .env file is intended for local development only. Before sharing this project, replace all real credentials with placeholder values. Do not distribute personal Gmail app passwords or other sensitive configuration values.

Example:

GMAIL_ADDRESS=your_email@gmail.com
GMAIL_APP_PASSWORD=your_app_password
ALERT_RECIPIENT_EMAIL=recipient_email@gmail.com

---

## Quick Start

```bash
# 1. Clone the repository & Enter the project folder(cd elt-alerting-pipeline)
git clone <repository-url>
cd elt-alerting-pipeline

# 2. Start all services
make up

# 3. Verify services are healthy
make status

# 4. Access the UIs
#    Airflow  → http://localhost:8080  (admin / admin)

# 5. Run dbt to verify database connectivity
make dbt-debug

# 6. Run the baseline experiment
make baseline

# 7. Run the smart alerting experiment
make smart

# 8. Generate the comparison report
make report
```

---

## Technology Mapping

| Dissertation Specification | Docker Implementation |
|---|---|
| Python - Core pipeline logic | `pipeline` container with Python 3.11 |
| PostgreSQL - Data layers & observability | `postgres` service (3 databases) |
| dbt - Transformation & quality validation | Installed in `pipeline` container via `dbt-core` + `dbt-postgres` |
| Apache Airflow - Scheduling & orchestration | `airflow-web` + `airflow-sched` services |
| SMTP + structured logging - Alerting | Gmail SMTP (`smtp.gmail.com:587` TLS) + `python-json-logger` / `structlog` |
| Git - Version control | `.gitignore` included; ready for repository initialisation |

## Experiment Modes
- Baseline Mode

All detected test failures generate email alerts. This mode represents uniform alerting and serves as the control condition for comparison.

- Smart Mode

Only critical failures generate email alerts. Warning-level failures are written to the observability database without sending email notifications. This mode represents the proposed severity-based alerting strategy.

---

## Key Commands

```bash
make up          # Start all services
make down        # Stop all services
make build       # Rebuild the pipeline image
make clean       # Remove all volumes and containers
make baseline    # Run baseline experiment
make smart       # Run smart alerting experiment
make dbt-run     # Execute dbt models
make dbt-test    # Execute dbt tests
make psql        # Open psql to the warehouse
make psql-obs    # Open psql to the observability DB
make test        # Run Python unit tests
make report      # Generate experiment comparison report
```

---

## Directory Structure

```
.
├── Dockerfile                  # Pipeline application image
├── docker-compose.yml          # Service orchestration
├── Makefile                    # Convenience commands
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (dev only)
├── README.md
├── .dockerignore
├── .gitignore
├── config/
│   ├── init-databases.sql      # PostgreSQL database initialisation
│   ├── init-schemas.sql        # Schema & table creation
│   └── msmtprc                 # SMTP client configuration
├── dags/                       # Airflow DAG definitions
├── plugins/
├── dbt_project/
│   ├── dbt_project.yml         # dbt project configuration
│   ├── profiles.yml            # dbt connection profile
│   ├── models/                 # dbt SQL models
│   ├── tests/                  # dbt data quality tests
│   ├── macros/                 # dbt macros
│   └── seeds/                  # dbt seed data
├── src/                        # Python application source code
├── tests/                      # Python unit & integration tests
├── logs/                       # Runtime logs (gitignored)
├── reports/                    # Generated experiment reports (gitignored)
└── data/                       # Raw data files (gitignored)
```
---
## Known Issues and Workarounds

| Issue | Impact | Workaround |
|---|---|---|
| Airflow DAG validation step fails due to environment path configuration | Full end-to-end automation not achieved | Run pipeline stages manually using `make` commands as documented in Quick Start |
| Observability schema alignment - `execution_id` and `alerting_mode` fields missing in some runs | Partial automated metrics tracking | Metrics computed manually from `alerts` and `injected_failures` tables |
| Schema column drop scenario (S4 original design) excluded due to downstream dependency failures | 9 of 10 planned scenarios executed | All 9 executed scenarios cover the full range of critical and warning failure types |

> **Note:** These discrepancies did not affect the core comparative results. Both alerting strategies were evaluated under identical conditions across all 9 successfully executed scenarios.

---

## Reproducibility


This project was designed to support controlled and repeatable 
experimentation. Reproducibility is ensured through:

- Containerised infrastructure using Docker Compose - identical runtime across all runs
- Fixed random seed (42) - deterministic synthetic dataset generation
- Explicit failure injection scenarios - consistent fault conditions
- Structured observability logging - full traceability of all events
- Deterministic severity classification - rule-based, not probabilistic
- Version-controlled configuration - all parameters committed to Git

## Expected Outputs

After a complete experimental run, the system produces:


| Output | Location | Description |
|---|---|---|
| Pipeline execution records | `observability.pipeline_executions` | Timestamps and status for each run |
| Injected failure records | `observability.injected_failures` | All 9 scenarios with metadata |
| Alert records | `observability.alerts` | All email and log alerts with timestamps |
| Quality test results | `observability.quality_check_results` | dbt test outcomes per run |
| Experiment metrics | `observability.experiment_metrics` | Alert volume, MTTD, actionable ratio |
| Email notifications | Gmail inbox | CRITICAL alerts with severity and metadata |
| Comparison report | `reports/` | Summary of baseline vs smart performance |
