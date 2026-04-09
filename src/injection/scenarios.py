"""
Scenario Configuration Loader (AI-9)
=====================================
Parses the failure_scenarios.yml configuration file into typed Python
objects that the experiment runner (AI-14) consumes.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .engine import FailureType, ExpectedSeverity

logger = logging.getLogger(__name__)


@dataclass
class FailureStep:
    """A single failure to inject within a scenario."""
    failure_type: FailureType
    table: str
    column: str | None = None
    row_count: int = 50
    expected_severity: ExpectedSeverity = ExpectedSeverity.WARNING
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class Scenario:
    """A named sequence of failure injection steps."""
    id: str
    name: str
    description: str
    failures: list[FailureStep]

    @property
    def expected_critical_count(self) -> int:
        return sum(
            1 for f in self.failures
            if f.expected_severity == ExpectedSeverity.CRITICAL
        )

    @property
    def expected_warning_count(self) -> int:
        return sum(
            1 for f in self.failures
            if f.expected_severity == ExpectedSeverity.WARNING
        )


def load_scenarios(
    config_path: str | Path = "config/failure_scenarios.yml",
) -> list[Scenario]:
    """
    Load all scenarios from the YAML configuration file.

    Args:
        config_path: Path to the failure_scenarios.yml file.

    Returns:
        List of Scenario objects sorted by ID.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError:        If the YAML structure is invalid.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Scenario config not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected YAML dict, got {type(raw)}")

    scenarios = []
    for scenario_id, scenario_data in raw.items():
        steps = []
        for step_data in scenario_data.get("failures", []):
            # Extract known params, pass rest as kwargs
            ft = FailureType(step_data["type"])
            severity = ExpectedSeverity(step_data.get("expected_severity", "warning"))
            extra_params = {
                k: v for k, v in step_data.items()
                if k not in ("type", "table", "column", "row_count", "expected_severity")
            }

            steps.append(FailureStep(
                failure_type=ft,
                table=step_data.get("table", "raw_transactions"),
                column=step_data.get("column"),
                row_count=step_data.get("row_count", 50),
                expected_severity=severity,
                params=extra_params,
            ))

        scenarios.append(Scenario(
            id=scenario_id,
            name=scenario_data.get("name", scenario_id),
            description=scenario_data.get("description", ""),
            failures=steps,
        ))

    scenarios.sort(key=lambda s: s.id)

    logger.info(
        "Loaded %d failure scenarios from %s",
        len(scenarios), path,
    )
    return scenarios
