"""
Phase 4 Unit Tests
===================
Tests for the failure injection framework.  Scenario loader tests
run without a database.  Engine tests are marked for integration
testing with a live PostgreSQL instance.

Run with::

    pytest tests/test_phase4.py -v            # scenario loader tests only
    pytest tests/test_phase4.py -v -m "not db" # same (explicit)

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from src.injection.engine import (
    FailureType,
    ExpectedSeverity,
    FAILURE_SEVERITY_MAP,
    InjectionRecord,
)
from src.injection.scenarios import load_scenarios, Scenario, FailureStep


# ═══════════════════════════════════════════════════════════════════════════
# FailureType and severity mapping tests
# ═══════════════════════════════════════════════════════════════════════════

class TestFailureTypes:
    """Verify enum values and severity mapping."""

    def test_six_failure_types_exist(self):
        """All 6 failure types from Chapter 3 Table 3.4 are defined."""
        expected = {
            "null_pk", "null_minor", "schema_drift",
            "volume_anomaly", "freshness_delay", "duplicate",
        }
        actual = {ft.value for ft in FailureType}
        assert actual == expected

    def test_severity_map_complete(self):
        """Every failure type has a severity mapping."""
        for ft in FailureType:
            assert ft in FAILURE_SEVERITY_MAP

    def test_critical_types(self):
        """null_pk, schema_drift, volume_anomaly → CRITICAL."""
        for ft in [FailureType.NULL_PK, FailureType.SCHEMA_DRIFT, FailureType.VOLUME_ANOMALY]:
            assert FAILURE_SEVERITY_MAP[ft] == ExpectedSeverity.CRITICAL

    def test_warning_types(self):
        """null_minor, freshness_delay, duplicate → WARNING."""
        for ft in [FailureType.NULL_MINOR, FailureType.FRESHNESS_DELAY, FailureType.DUPLICATE]:
            assert FAILURE_SEVERITY_MAP[ft] == ExpectedSeverity.WARNING


# ═══════════════════════════════════════════════════════════════════════════
# Scenario loader tests
# ═══════════════════════════════════════════════════════════════════════════

class TestScenarioLoader:
    """Test YAML scenario parsing without database."""

    @pytest.fixture
    def scenarios_path(self) -> Path:
        """Path to the test scenarios file."""
        return Path(__file__).parent.parent / "config" / "failure_scenarios.yml"

    @pytest.fixture
    def minimal_yaml(self, tmp_path) -> Path:
        """Create a minimal test scenarios file."""
        data = {
            "test_scenario": {
                "name": "Test",
                "description": "A test scenario",
                "failures": [
                    {
                        "type": "null_pk",
                        "table": "raw_transactions",
                        "column": "transaction_id",
                        "row_count": 10,
                        "expected_severity": "critical",
                    }
                ],
            }
        }
        path = tmp_path / "test_scenarios.yml"
        with open(path, "w") as f:
            yaml.dump(data, f)
        return path

    def test_load_real_scenarios(self, scenarios_path):
        """Loads all scenarios from the project config file."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        scenarios = load_scenarios(scenarios_path)
        assert len(scenarios) == 10

    def test_scenario_has_required_fields(self, scenarios_path):
        """Each scenario has id, name, description, and failures list."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        for s in load_scenarios(scenarios_path):
            assert s.id
            assert s.name
            assert isinstance(s.failures, list)
            assert len(s.failures) >= 1

    def test_failure_step_types_are_valid(self, scenarios_path):
        """Every failure step has a valid FailureType."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        for s in load_scenarios(scenarios_path):
            for step in s.failures:
                assert isinstance(step.failure_type, FailureType)

    def test_expected_severity_set(self, scenarios_path):
        """Every failure step has an expected severity."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        for s in load_scenarios(scenarios_path):
            for step in s.failures:
                assert isinstance(step.expected_severity, ExpectedSeverity)

    def test_minimal_yaml_parsing(self, minimal_yaml):
        """Parses a minimal single-scenario file correctly."""
        scenarios = load_scenarios(minimal_yaml)
        assert len(scenarios) == 1
        s = scenarios[0]
        assert s.id == "test_scenario"
        assert s.name == "Test"
        assert len(s.failures) == 1
        assert s.failures[0].failure_type == FailureType.NULL_PK
        assert s.failures[0].expected_severity == ExpectedSeverity.CRITICAL
        assert s.failures[0].row_count == 10

    def test_missing_file_raises(self):
        """FileNotFoundError when config path doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_scenarios("/nonexistent/path.yml")

    def test_critical_count_property(self, scenarios_path):
        """Scenario.expected_critical_count returns correct count."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        scenarios = load_scenarios(scenarios_path)
        # scenario_10_mixed_both has 1 critical + 2 warnings
        mixed = [s for s in scenarios if s.id == "scenario_10_mixed_both"]
        if mixed:
            assert mixed[0].expected_critical_count == 1
            assert mixed[0].expected_warning_count == 2

    def test_scenario_sorted_by_id(self, scenarios_path):
        """Scenarios are returned sorted by id."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        scenarios = load_scenarios(scenarios_path)
        ids = [s.id for s in scenarios]
        assert ids == sorted(ids)

    def test_mixed_scenario_has_multiple_failures(self, scenarios_path):
        """Mixed scenarios contain more than one failure step."""
        if not scenarios_path.exists():
            pytest.skip("scenarios file not found")
        scenarios = load_scenarios(scenarios_path)
        mixed = [s for s in scenarios if "mixed" in s.id]
        for s in mixed:
            assert len(s.failures) >= 2, f"{s.id} should have multiple failures"


# ═══════════════════════════════════════════════════════════════════════════
# InjectionRecord tests
# ═══════════════════════════════════════════════════════════════════════════

class TestInjectionRecord:
    """Verify dataclass construction."""

    def test_record_creation(self):
        from datetime import datetime, timezone

        record = InjectionRecord(
            injection_id="inj-test123",
            failure_type=FailureType.NULL_PK,
            expected_severity=ExpectedSeverity.CRITICAL,
            target_table="raw.raw_transactions",
            target_column="transaction_id",
            rows_affected=50,
            injected_at=datetime.now(timezone.utc),
            details={"action": "set_null_on_pk"},
        )
        assert record.injection_id == "inj-test123"
        assert record.failure_type == FailureType.NULL_PK
        assert record.rows_affected == 50
