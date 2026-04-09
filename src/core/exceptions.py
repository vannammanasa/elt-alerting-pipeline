"""
Custom Exception Hierarchy
==========================
Structured exceptions for every pipeline stage, enabling precise
error categorisation in the alerting and observability layers.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class ConfigurationError(PipelineError):
    """Raised when configuration is invalid or missing."""


class ExtractionError(PipelineError):
    """Raised during data extraction / generation."""


class LoadError(PipelineError):
    """Raised when loading data into PostgreSQL fails."""


class TransformationError(PipelineError):
    """Raised when dbt transformation encounters an error."""


class QualityCheckError(PipelineError):
    """Raised when a data quality test detects a violation."""

    def __init__(
        self,
        message: str,
        severity: str = "warning",
        check_name: str = "",
        table_name: str = "",
        column_name: str = "",
        details: dict | None = None,
    ):
        super().__init__(message, details)
        self.severity = severity        # "critical" or "warning"
        self.check_name = check_name
        self.table_name = table_name
        self.column_name = column_name


class AlertDeliveryError(PipelineError):
    """Raised when alert delivery (SMTP or log) fails."""


class FailureInjectionError(PipelineError):
    """Raised when the failure injection framework encounters an error."""
