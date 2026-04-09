"""
Configuration Management
========================
Centralised configuration for the ELT pipeline. Reads from environment
variables (Docker .env) with sensible defaults matching Deliverable 1.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL connection parameters."""
    host: str = "postgres"
    port: int = 5432
    user: str = "pipeline_user"
    password: str = "pipeline_pass"
    warehouse_db: str = "elt_warehouse"
    observability_db: str = "observability"

    @property
    def warehouse_url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.warehouse_db}"
        )

    @property
    def observability_url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.observability_db}"
        )


@dataclass(frozen=True)
class SMTPConfig:
    """Gmail SMTP settings."""
    host: str = "smtp.gmail.com"
    port: int = 587
    use_tls: bool = True
    username: str = ""
    password: str = ""
    sender: str = ""
    recipient: str = ""


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration."""
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    smtp: SMTPConfig = field(default_factory=SMTPConfig)
    alerting_mode: str = "baseline"          # "baseline" or "smart"
    batch_id_prefix: str = "batch"
    log_level: str = "INFO"
    log_format: str = "json"                 # "json" or "text"


def load_config() -> PipelineConfig:
    """
    Build configuration from environment variables.

    Environment variables match the .env file shipped with Deliverable 1:
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
        SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
        SMTP_SENDER, SMTP_RECIPIENT, ALERTING_MODE, LOG_LEVEL
    """
    db = DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
        warehouse_db=os.getenv("WAREHOUSE_DB", "elt_warehouse"),
        observability_db=os.getenv("OBSERVABILITY_DB", "observability"),
    )
    smtp = SMTPConfig(
        host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        port=int(os.getenv("SMTP_PORT", "587")),
        use_tls=os.getenv("SMTP_USE_TLS", "true").lower() == "true",
        username=os.getenv("SMTP_USERNAME", ""),
        password=os.getenv("SMTP_PASSWORD", ""),
        sender=os.getenv("SMTP_SENDER", ""),
        recipient=os.getenv("SMTP_RECIPIENT", ""),
    )
    return PipelineConfig(
        db=db,
        smtp=smtp,
        alerting_mode=os.getenv("ALERTING_MODE", "baseline"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_format=os.getenv("LOG_FORMAT", "json"),
    )
