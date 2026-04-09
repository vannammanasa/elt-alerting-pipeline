"""
Phase 1 Unit Tests
===================
Verifies the synthetic data generator and raw loader independently
of a running PostgreSQL instance (generator tests) and with a live
database (loader integration tests).

Run with::

    pytest tests/test_phase1.py -v

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

import pytest
import pandas as pd
import numpy as np

from src.extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig


# ═══════════════════════════════════════════════════════════════════════════
# AI-1b: Synthetic Data Generator Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSyntheticDataGenerator:
    """Test suite for the synthetic data generator."""

    def test_default_generation(self):
        """Generator produces a DataFrame with the correct shape."""
        gen = SyntheticDataGenerator(SyntheticDataConfig(record_count=100))
        df = gen.generate()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100
        assert len(df.columns) == 13  # 12 defined + total_amount computed

    def test_reproducibility(self):
        """Two generators with the same seed produce identical data."""
        cfg = SyntheticDataConfig(record_count=500, seed=123)
        df1 = SyntheticDataGenerator(cfg).generate()
        df2 = SyntheticDataGenerator(cfg).generate()
        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seeds_differ(self):
        """Different seeds produce different data."""
        df1 = SyntheticDataGenerator(SyntheticDataConfig(seed=1)).generate()
        df2 = SyntheticDataGenerator(SyntheticDataConfig(seed=2)).generate()
        assert not df1.equals(df2)

    def test_column_names(self):
        """All expected columns are present."""
        df = SyntheticDataGenerator(SyntheticDataConfig(record_count=10)).generate()
        expected = {
            "transaction_id", "event_timestamp", "customer_id", "product_id",
            "product_category", "quantity", "unit_price", "total_amount",
            "payment_method", "order_status", "shipping_country",
            "customer_email", "is_returning_customer",
        }
        assert set(df.columns) == expected

    def test_primary_key_uniqueness(self):
        """transaction_id has no duplicates."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=5000)
        ).generate()
        assert df["transaction_id"].is_unique

    def test_primary_key_no_nulls(self):
        """transaction_id has no null values (non-nullable column)."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=1000)
        ).generate()
        assert df["transaction_id"].notna().all()

    def test_nullable_columns_have_some_nulls(self):
        """Nullable columns contain nulls at approximately the configured rate."""
        cfg = SyntheticDataConfig(record_count=10_000, null_rate=0.05, seed=42)
        df = SyntheticDataGenerator(cfg).generate()
        # shipping_country is nullable
        null_pct = df["shipping_country"].isna().mean()
        assert 0.02 < null_pct < 0.10  # roughly 5% ± tolerance

    def test_non_nullable_columns_no_nulls(self):
        """Non-nullable columns contain zero nulls."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=1000)
        ).generate()
        non_nullable = [
            "transaction_id", "event_timestamp", "customer_id",
            "product_id", "product_category", "quantity", "unit_price",
            "total_amount", "payment_method", "order_status",
            "is_returning_customer",
        ]
        for col in non_nullable:
            assert df[col].notna().all(), f"{col} has unexpected nulls"

    def test_category_values(self):
        """Categorical columns only contain expected values."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=5000)
        ).generate()
        valid_categories = {"electronics", "clothing", "home", "sports", "books"}
        assert set(df["product_category"].unique()).issubset(valid_categories)

    def test_integer_ranges(self):
        """Integer columns fall within specified bounds."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=1000)
        ).generate()
        assert df["customer_id"].between(1000, 99999).all()
        assert df["product_id"].between(1, 500).all()
        assert df["quantity"].between(1, 10).all()

    def test_total_amount_computation(self):
        """total_amount = quantity × unit_price (to 2 decimal places)."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=100)
        ).generate()
        expected = np.round(df["quantity"] * df["unit_price"], 2)
        np.testing.assert_array_almost_equal(
            df["total_amount"].values, expected.values, decimal=2
        )

    def test_timestamps_sorted(self):
        """event_timestamp values are in chronological order."""
        df = SyntheticDataGenerator(
            SyntheticDataConfig(record_count=500)
        ).generate()
        timestamps = pd.to_datetime(df["event_timestamp"])
        assert timestamps.is_monotonic_increasing

    def test_schema_metadata(self):
        """get_schema_metadata returns full column definitions."""
        gen = SyntheticDataGenerator()
        meta = gen.get_schema_metadata()
        assert "transaction_id" in meta
        assert meta["transaction_id"]["primary_key"] is True

    def test_custom_record_count(self):
        """Generator respects custom record counts."""
        for n in [1, 50, 999]:
            df = SyntheticDataGenerator(
                SyntheticDataConfig(record_count=n)
            ).generate()
            assert len(df) == n

    def test_zero_null_rate(self):
        """Setting null_rate=0 produces zero nulls in nullable columns."""
        cfg = SyntheticDataConfig(record_count=1000, null_rate=0.0)
        df = SyntheticDataGenerator(cfg).generate()
        assert df["shipping_country"].notna().all()
        assert df["customer_email"].notna().all()
