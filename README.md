# 🚀 ELT Pipeline with Severity-Based Alerting

### 🔍 An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines: Minimising Alert Fatigue Without Compromising Detection

A reproducible ELT pipeline exploring how severity-based alerting can reduce unnecessary email notifications while ensuring critical data quality failures are detected promptly.

👩‍💻 **Author:** Manasa Vannam  
🎓 **Programme:** MSc Computer Science, Glasgow Caledonian University  
📌 **Project Type:** MSc Dissertation

---

## 📖 About the Project

This project evaluates two alerting approaches in an ELT pipeline:

- 📧 **Baseline Alerting:** All detected data quality failures generate email notifications.
- 🚨 **Severity-Based Alerting:** Critical failures generate email notifications, while warnings are recorded in an observability database.

The system combines synthetic e-commerce data, automated data quality testing, controlled failure injection and structured monitoring to compare both approaches under repeatable experimental conditions.

## 📊 Key Results

| Metric | Result |
|---|---:|
| 📧 Baseline email alerts | 45 |
| 🚨 Severity-based email alerts | 32 |
| 📉 Reduction in email alerts | **28.9%** |
| 🎯 Baseline actionable alert ratio | 71.1% |
| ✅ Severity-based actionable alert ratio | **100%** |
| 🔍 Critical failures detected | **All 32** |

**Key finding:** Severity-based alerting reduced email notifications by 28.9% while maintaining immediate detection of all critical failures in the experiments.

## 🏗️ System Architecture

The project uses a containerised architecture with the following components:

1. 🛒 **Synthetic Data Generation** - Creates 10,000 e-commerce transaction records.
2. 🗄️ **PostgreSQL** - Stores raw, staging, transformed and quality-related data.
3. 🔄 **dbt** - Runs SQL transformations and data quality tests.
4. ⚙️ **Apache Airflow** - Provides workflow orchestration.
5. 🚨 **Alerting Logic** - Routes notifications according to failure severity.
6. 📈 **Observability Database** - Stores pipeline executions, injected failures, alerts, quality results and experiment metrics.
7. 📑 **Reporting** - Supports comparison of baseline and severity-based alerting results.

## 🛠️ Technology Stack

- 🐍 **Programming:** Python 3.11, SQL
- 🗄️ **Database:** PostgreSQL 16
- 🔄 **Data Transformation:** dbt
- ⚙️ **Workflow Orchestration:** Apache Airflow 2.10.4
- 🐳 **Infrastructure:** Docker, Docker Compose
- 📧 **Alerting:** SMTP email and database logging
- 🧪 **Testing:** dbt tests and controlled failure injection

## 🛒 Dataset

The pipeline uses a synthetic e-commerce dataset containing **10,000 transaction records** and 13 columns.

Dataset fields include:

- Transaction identifiers and timestamps
- Customer and product information
- Quantities and prices
- Payment methods and order status
- Shipping details

🔁 A fixed random seed is used to support reproducibility.

## 🧪 Data Quality Testing

The project implements **45 dbt tests**:

- 🔴 32 critical-severity tests configured with `error`.
- 🟡 13 warning-severity tests configured with `warn`.

Test coverage includes:

- Not-null constraints
- Uniqueness
- Accepted values
- Row-count validation
- Data freshness
- Custom SQL-based checks

## 🔬 Experimental Evaluation

The project uses controlled failure injection to compare the two alerting strategies.

**Nine failure scenario types** were executed across the experimental evaluation. One additional planned schema-change scenario was excluded because of downstream dependency constraints.

The evaluation considers:

- 📊 **Alert Volume:** Number of email notifications generated.
- ⏱️ **Mean Time to Detect (MTTD):** Time taken to detect failures.
- 🎯 **Actionable Alert Ratio:** Proportion of notifications representing critical, actionable failures.

The same pipeline and failure conditions were used to support a consistent comparison between alerting approaches.

## 📁 Project Structure

```text
elt-alerting-pipeline/
│
├── config/
├── dags/
├── plugins/
├── dbt_project/
├── src/
├── tests/
├── data/                  # Generated data; not committed
├── logs/                  # Runtime logs; not committed
├── reports/               # Generated reports; not committed
│
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 🚀 Getting Started

### 📋 Prerequisites

- 🐳 Docker Desktop 4.x or later
- Docker Compose V2
- Git
- At least 4 GB available RAM
- At least 2 GB free disk space

### 1️⃣ Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd elt-alerting-pipeline
```

### 2️⃣ Configure Environment Variables

Create your local `.env` file using the example configuration:

```bash
cp .env.example .env
```

Update the required values in `.env` before running the pipeline.

🔐 **Security Note:** The `.env` file may contain credentials and must not be committed to GitHub. Use placeholder values in `.env.example` and keep real credentials private.

### 3️⃣ Start the Services

```bash
make up
```

Check the running containers:

```bash
docker compose ps
```

### 4️⃣ Access Airflow

Open the following address in your browser:

```text
http://localhost:8080
```

Use the local development credentials configured for your environment.

### 5️⃣ Run the Experiments

📧 Run baseline alerting:

```bash
make baseline
```

🚨 Run severity-based alerting:

```bash
make smart
```

📊 Generate the experiment report:

```bash
make report
```

### 6️⃣ Stop the Services

```bash
make down
```

## ⌨️ Useful Make Commands

| Command | Description |
|---|---|
| `make up` | 🚀 Start Docker services |
| `make down` | 🛑 Stop services |
| `make build` | 🏗️ Build project containers |
| `make clean` | 🧹 Clean generated resources |
| `make baseline` | 📧 Run baseline alerting |
| `make smart` | 🚨 Run severity-based alerting |
| `make dbt-run` | 🔄 Execute dbt transformations |
| `make dbt-test` | 🧪 Run data quality tests |
| `make psql` | 🗄️ Access warehouse database |
| `make psql-obs` | 📈 Access observability database |
| `make test` | ✅ Run project tests |
| `make report` | 📑 Generate experiment report |

## ⚠️ Implementation Notes and Limitations

- The full Airflow DAG automation was not achieved because of a DAG validation and path issue. The pipeline can be executed through the provided Makefile commands.
- Some observability records required manual handling because certain execution metadata fields were not consistently populated.
- One planned schema-change failure scenario was excluded due to downstream dependency constraints.

These limitations are documented to make the experimental implementation and its boundaries transparent.

## 🔁 Reproducibility

The project includes Docker configuration, dependency definitions, SQL transformations, test definitions and Makefile commands to support repeatable execution.

The synthetic dataset uses a fixed random seed, and controlled failure injection allows the alerting approaches to be evaluated under consistent conditions.

## 🔮 Future Improvements

- ⚙️ Complete end-to-end Airflow DAG orchestration.
- 📈 Improve automated experiment metadata collection.
- 🧪 Extend the range of schema-change failure scenarios.
- 📊 Add richer monitoring dashboards and experiment visualisations.

---

## 👩‍💻 Author

**Manasa Vannam**

🎓 MSc Computer Science  
💻 Software Development | Data & Analytics | Automation

