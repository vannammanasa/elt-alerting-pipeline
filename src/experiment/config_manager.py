"""
Configuration Manager (AI-14a)
===============================
Provides runtime switching between baseline and smart alerting modes
without code changes.  The experiment runner (AI-14) uses this to
toggle the pipeline between experimental conditions.

Supports three configuration sources (in priority order):
    1. Explicit function argument (highest)
    2. Environment variable ALERTING_MODE
    3. Default: "baseline"

This fulfils Spec §5(i): "configurable alerting logic."

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from typing import Literal

from ..core.config import PipelineConfig, load_config

logger = logging.getLogger(__name__)

AlertingMode = Literal["baseline", "smart"]
VALID_MODES: set[str] = {"baseline", "smart"}


class ConfigurationManager:
    """
    Manages pipeline configuration with mode-switching capability.

    Usage::

        mgr = ConfigurationManager()
        baseline_cfg = mgr.for_mode("baseline")
        smart_cfg    = mgr.for_mode("smart")

        # Or switch in place
        mgr.set_mode("smart")
        current = mgr.current_config
    """

    def __init__(self, base_config: PipelineConfig | None = None):
        self._base = base_config or load_config()
        self._current_mode: AlertingMode = self._base.alerting_mode  # type: ignore
        logger.info("ConfigurationManager initialised (mode=%s)", self._current_mode)

    # ── Mode switching ───────────────────────────────────────────────────

    @property
    def current_mode(self) -> str:
        return self._current_mode

    @property
    def current_config(self) -> PipelineConfig:
        """Return config with current alerting mode applied."""
        return replace(self._base, alerting_mode=self._current_mode)

    def set_mode(self, mode: AlertingMode) -> PipelineConfig:
        """
        Switch the alerting mode.

        Args:
            mode: "baseline" or "smart"

        Returns:
            Updated PipelineConfig.

        Raises:
            ValueError: If mode is not valid.
        """
        if mode not in VALID_MODES:
            raise ValueError(
                f"Invalid alerting mode: '{mode}'. Must be one of {VALID_MODES}"
            )
        self._current_mode = mode
        logger.info("Alerting mode switched to: %s", mode)
        return self.current_config

    def for_mode(self, mode: AlertingMode) -> PipelineConfig:
        """
        Return a config for the specified mode WITHOUT changing current state.

        Args:
            mode: "baseline" or "smart"

        Returns:
            PipelineConfig with the requested mode set.
        """
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid alerting mode: '{mode}'")
        return replace(self._base, alerting_mode=mode)

    # ── Environment override ─────────────────────────────────────────────

    @classmethod
    def from_environment(cls) -> ConfigurationManager:
        """
        Create a manager with mode set from the ALERTING_MODE env var.
        Falls back to "baseline" if not set.
        """
        config = load_config()
        mgr = cls(base_config=config)
        env_mode = os.getenv("ALERTING_MODE", "baseline").lower()
        if env_mode in VALID_MODES:
            mgr.set_mode(env_mode)  # type: ignore
        return mgr
