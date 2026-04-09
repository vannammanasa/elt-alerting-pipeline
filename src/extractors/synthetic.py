"""
Synthetic Data Generator (AI-1b)
=================================
Generates reproducible tabular datasets with controlled statistical
properties for the ELT pipeline experiment.  Seed-based generation
ensures identical data across experimental runs, strengthening the
reproducibility argument in the dissertation.

The default schema models an **e-commerce transactions** domain —
rich enough to exercise all six failure injection types (null, schema
drift, volume anomaly, freshness delay, duplicates) while remaining
straightforward to reason about in the Chapter 4 analysis.

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from ..core.exceptions import ExtractionError

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Schema definition
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_SCHEMA: dict[str, dict[str, Any]] = {
    "transaction_id": {
        "type": "uuid",
        "nullable": False,
        "primary_key": True,
        "description": "Unique transaction identifier",
    },
    "event_timestamp": {
        "type": "datetime",
        "nullable": False,
        "description": "Transaction creation time (UTC)",
    },
    "customer_id": {
        "type": "integer",
        "nullable": False,
        "min": 1000,
        "max": 99999,
        "description": "Customer account number",
    },
    "product_id": {
        "type": "integer",
        "nullable": False,
        "min": 1,
        "max": 500,
        "description": "Product catalogue identifier",
    },
    "product_category": {
        "type": "category",
        "nullable": False,
        "values": ["electronics", "clothing", "home", "sports", "books"],
        "weights": [0.30, 0.25, 0.20, 0.15, 0.10],
        "description": "Product department",
    },
    "quantity": {
        "type": "integer",
        "nullable": False,
        "min": 1,
        "max": 10,
        "description": "Units purchased",
    },
    "unit_price": {
        "type": "float",
        "nullable": False,
        "min": 1.99,
        "max": 999.99,
        "distribution": "lognormal",
        "description": "Price per unit in GBP",
    },
    "total_amount": {
        "type": "computed",
        "formula": "quantity * unit_price",
        "nullable": False,
        "description": "Line total (quantity × unit_price)",
    },
    "payment_method": {
        "type": "category",
        "nullable": False,
        "values": ["credit_card", "debit_card", "paypal", "bank_transfer"],
        "weights": [0.40, 0.30, 0.20, 0.10],
        "description": "Payment instrument used",
    },
    "order_status": {
        "type": "category",
        "nullable": False,
        "values": ["completed", "pending", "shipped", "cancelled", "refunded"],
        "weights": [0.60, 0.15, 0.12, 0.08, 0.05],
        "description": "Current order status",
    },
    "shipping_country": {
        "type": "category",
        "nullable": True,
        "values": ["GB", "US", "DE", "FR", "IE", "NL", "ES", "IT"],
        "weights": [0.40, 0.15, 0.10, 0.10, 0.08, 0.07, 0.05, 0.05],
        "description": "Shipping destination ISO code",
    },
    "customer_email": {
        "type": "email",
        "nullable": True,
        "description": "Customer contact email",
    },
    "is_returning_customer": {
        "type": "boolean",
        "nullable": False,
        "true_rate": 0.65,
        "description": "Whether customer has prior orders",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SyntheticDataConfig:
    """
    Controls for reproducible synthetic data generation.

    Attributes:
        record_count:  Number of records per batch.
        seed:          Random seed for reproducibility.
        schema:        Column definitions (defaults to e-commerce transactions).
        null_rate:     Base null rate for nullable columns (0.0–1.0).
        date_range_days: How many days of history to simulate.
    """
    record_count: int = 10_000
    seed: int = 42
    schema: dict[str, dict[str, Any]] = field(default_factory=lambda: DEFAULT_SCHEMA.copy())
    null_rate: float = 0.02
    date_range_days: int = 90


# ═══════════════════════════════════════════════════════════════════════════
# Generator
# ═══════════════════════════════════════════════════════════════════════════

class SyntheticDataGenerator:
    """
    Generates a pandas DataFrame of synthetic e-commerce transactions.

    Usage::

        gen = SyntheticDataGenerator(SyntheticDataConfig(record_count=5000, seed=42))
        df  = gen.generate()
        print(df.shape)    # (5000, 13)
        print(df.dtypes)

    Reproducibility guarantee:
        Two ``SyntheticDataGenerator`` instances created with the same
        ``SyntheticDataConfig`` will produce byte-identical DataFrames.
    """

    def __init__(self, config: SyntheticDataConfig | None = None):
        self.config = config or SyntheticDataConfig()
        self._rng = np.random.default_rng(self.config.seed)

    # ── Public API ───────────────────────────────────────────────────────

    def generate(self) -> pd.DataFrame:
        """
        Generate the full synthetic dataset.

        Returns:
            pd.DataFrame with columns matching ``self.config.schema``.

        Raises:
            ExtractionError: If generation fails for any column.
        """
        n = self.config.record_count
        logger.info(
            "Generating synthetic dataset",
            extra={"record_count": n, "seed": self.config.seed},
        )

        data: dict[str, Any] = {}

        try:
            for col_name, col_def in self.config.schema.items():
                col_type = col_def["type"]

                if col_type == "computed":
                    continue  # handled after all base columns

                data[col_name] = self._generate_column(col_name, col_def, n)

            # ── Computed columns ─────────────────────────────────────────
            if "total_amount" in self.config.schema:
                data["total_amount"] = np.round(
                    np.array(data["quantity"], dtype=float)
                    * np.array(data["unit_price"], dtype=float),
                    2,
                )

            # ── Apply nulls to nullable columns ──────────────────────────
            df = pd.DataFrame(data)
            df = self._apply_nulls(df)

            logger.info(
                "Synthetic dataset generated successfully",
                extra={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "null_rate": self.config.null_rate,
                },
            )
            return df

        except Exception as exc:
            raise ExtractionError(
                f"Synthetic data generation failed: {exc}",
                details={"seed": self.config.seed, "record_count": n},
            ) from exc

    def get_schema_metadata(self) -> dict[str, dict[str, Any]]:
        """Return the full schema definition for documentation / validation."""
        return self.config.schema.copy()

    # ── Column generators ────────────────────────────────────────────────

    def _generate_column(
        self, name: str, definition: dict[str, Any], n: int
    ) -> list | np.ndarray:
        """Dispatch to the appropriate type-specific generator."""
        generators = {
            "uuid": self._gen_uuid,
            "datetime": self._gen_datetime,
            "integer": self._gen_integer,
            "float": self._gen_float,
            "category": self._gen_category,
            "boolean": self._gen_boolean,
            "email": self._gen_email,
        }
        gen_fn = generators.get(definition["type"])
        if gen_fn is None:
            raise ExtractionError(
                f"Unknown column type '{definition['type']}' for column '{name}'"
            )
        return gen_fn(definition, n)

    def _gen_uuid(self, _def: dict, n: int) -> list[str]:
        """Generate deterministic UUIDs by combining two 63-bit ints."""
        results = []
        for _ in range(n):
            hi = int(self._rng.integers(0, 2**63))
            lo = int(self._rng.integers(0, 2**63))
            results.append(str(uuid.UUID(int=(hi << 64) | lo)))
        return results

    def _gen_datetime(self, definition: dict, n: int) -> list[datetime]:
        """Generate timestamps uniformly distributed over the date range."""
        # Use a fixed anchor so two generators with the same seed
        # produce identical timestamps regardless of wall-clock time.
        anchor = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = anchor
        start = end - timedelta(days=self.config.date_range_days)
        start_ts = start.timestamp()
        end_ts = end.timestamp()
        timestamps = self._rng.uniform(start_ts, end_ts, size=n)
        return [
            datetime.fromtimestamp(ts, tz=timezone.utc) for ts in sorted(timestamps)
        ]

    def _gen_integer(self, definition: dict, n: int) -> np.ndarray:
        """Generate integers uniformly in [min, max]."""
        lo = definition.get("min", 0)
        hi = definition.get("max", 100)
        return self._rng.integers(lo, hi + 1, size=n)

    def _gen_float(self, definition: dict, n: int) -> np.ndarray:
        """Generate floats — lognormal or uniform depending on config."""
        lo = definition.get("min", 0.0)
        hi = definition.get("max", 100.0)
        dist = definition.get("distribution", "uniform")

        if dist == "lognormal":
            mid = np.log((lo + hi) / 2)
            raw = self._rng.lognormal(mean=mid, sigma=0.8, size=n)
            clipped = np.clip(raw, lo, hi)
            return np.round(clipped, 2)

        return np.round(self._rng.uniform(lo, hi, size=n), 2)

    def _gen_category(self, definition: dict, n: int) -> list[str]:
        """Generate categorical values with optional weights."""
        values = definition["values"]
        weights = definition.get("weights")
        if weights:
            weights = np.array(weights, dtype=float)
            weights /= weights.sum()  # normalise
        return list(self._rng.choice(values, size=n, p=weights))

    def _gen_boolean(self, definition: dict, n: int) -> np.ndarray:
        """Generate booleans with configurable true_rate."""
        rate = definition.get("true_rate", 0.5)
        return self._rng.random(n) < rate

    def _gen_email(self, _def: dict, n: int) -> list[str]:
        """Generate plausible synthetic email addresses."""
        domains = ["example.com", "test.co.uk", "sample.org", "demo.net"]
        emails = []
        for i in range(n):
            user = f"user{self._rng.integers(1000, 99999)}"
            domain = domains[int(self._rng.integers(0, len(domains)))]
            emails.append(f"{user}@{domain}")
        return emails

    # ── Post-processing ──────────────────────────────────────────────────

    def _apply_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Inject nulls into nullable columns at the configured rate."""
        for col_name, col_def in self.config.schema.items():
            if col_def.get("nullable") and col_name in df.columns:
                mask = self._rng.random(len(df)) < self.config.null_rate
                df.loc[mask, col_name] = None
        return df
