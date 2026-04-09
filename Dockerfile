# =============================================================================
# Dockerfile — ELT Alerting Pipeline Application
# Project: An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines
# Author:  Manasa Vannam (S2471596)
# =============================================================================
# This image packages the core Python application, dbt, and all supporting
# tooling required to run extraction, loading, transformation, quality
# validation, failure injection, statistical analysis, and alerting modules.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — Base Python runtime
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS base

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
# so that log output is immediately visible in Docker logs.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install OS-level dependencies required by psycopg2, dbt, and general tooling
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
        curl \
        wget \
        gnupg2 \
        lsb-release \
        ca-certificates \
        mailutils \
        msmtp \
        msmtp-mta \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Stage 2 — Python dependency installation
# ---------------------------------------------------------------------------
FROM base AS dependencies

WORKDIR /tmp

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 3 — Application image
# ---------------------------------------------------------------------------
FROM base AS application

# Create a non-root application user for security best practice
RUN groupadd --gid 1000 pipeline \
    && useradd --uid 1000 --gid pipeline --shell /bin/bash --create-home pipeline

# Copy installed Python packages from the dependency stage
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Set the working directory
WORKDIR /app

# Copy the full application source tree
COPY --chown=pipeline:pipeline . .

# Create required directories for logs, reports, and dbt artefacts
RUN mkdir -p \
        /app/logs \
        /app/reports \
        /app/data \
        /app/dbt_project/target \
        /app/dbt_project/logs \
    && chown -R pipeline:pipeline /app

# Configure MSMTP for SMTP-based critical alerting
# Stored as template; entrypoint.sh expands env vars at runtime.
COPY config/msmtprc /etc/msmtprc.template
RUN chmod 644 /etc/msmtprc.template

# Install envsubst (part of gettext-base) for runtime env-var expansion
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base \
    && rm -rf /var/lib/apt/lists/*

# Copy and set the entrypoint script
COPY config/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch to the non-root user
USER pipeline

# Entrypoint expands env vars, then hands off to the command
ENTRYPOINT ["entrypoint.sh"]
CMD ["python", "-m", "pipeline.cli", "--help"]
