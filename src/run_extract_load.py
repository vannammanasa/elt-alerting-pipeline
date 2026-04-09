"""
Phase 1 Runner: Extract & Load
================================
Generates synthetic e-commerce transaction data and loads it into
the PostgreSQL raw schema.  This is the entry point for the E and L
stages of the ELT pipeline.

Usage (inside Docker container)::

    python -m src.run_extract_load

Usage (local, with env vars or .env)::

    export POSTGRES_HOST=localhost
    python -m src.run_extract_load --records 10000 --seed 42

Project : Severity-Based Alerting in ELT Pipelines
Author  : Manasa Vannam
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from src.core.config import load_config
from src.core.database import DatabaseManager
from src.extractors.synthetic import SyntheticDataGenerator, SyntheticDataConfig
from src.loaders.raw_loader import RawLoader


def setup_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure structured logging."""
    if fmt == "json":
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
            )
        )
    logging.basicConfig(level=getattr(logging, level.upper()), handlers=[handler])


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter for structured observability."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            entry.update(record.extra)
        # Merge any extras passed via `extra={}` kwarg
        for key in record.__dict__:
            if key not in logging.LogRecord(
                "", 0, "", 0, "", (), None
            ).__dict__ and not key.startswith("_"):
                try:
                    json.dumps(record.__dict__[key])
                    entry[key] = record.__dict__[key]
                except (TypeError, ValueError):
                    pass
        return json.dumps(entry, default=str)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Extract & Load")
    parser.add_argument(
        "--records", type=int, default=10_000,
        help="Number of synthetic records to generate (default: 10000)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--table", type=str, default="raw_transactions",
        help="Target table name in raw schema (default: raw_transactions)",
    )
    parser.add_argument(
        "--log-format", type=str, choices=["json", "text"], default="text",
        help="Log format (default: text)",
    )
    args = parser.parse_args()

    # ── Setup ────────────────────────────────────────────────────────────
    config = load_config()
    setup_logging(level=config.log_level, fmt=args.log_format)
    logger = logging.getLogger("phase1")

    logger.info("=" * 60)
    logger.info("PHASE 1: Extract & Load — Starting")
    logger.info("=" * 60)

    # ── Connect to database ──────────────────────────────────────────────
    logger.info("Initialising database connections...")
    db = DatabaseManager(config.db)
    conn_status = db.test_connections()
    if not conn_status.get("warehouse"):
        logger.error("Cannot connect to warehouse database — aborting")
        sys.exit(1)
    logger.info("Database connections verified: %s", conn_status)

    # ── AI-1b: Generate synthetic data ───────────────────────────────────
    logger.info("Generating %d synthetic records (seed=%d)...", args.records, args.seed)
    gen_config = SyntheticDataConfig(record_count=args.records, seed=args.seed)
    generator = SyntheticDataGenerator(gen_config)
    df = generator.generate()
    logger.info(
        "Generation complete: %d rows × %d columns",
        len(df), len(df.columns),
    )
    logger.info("Sample data:\n%s", df.head(3).to_string(index=False))
    logger.info("Column dtypes:\n%s", df.dtypes.to_string())

    # ── AI-2: Load into PostgreSQL raw schema ────────────────────────────
    logger.info("Loading data into raw.%s ...", args.table)
    loader = RawLoader(db, source_id="synthetic_v1")
    stats = loader.load(df, table_name=args.table)

    logger.info("=" * 60)
    logger.info("PHASE 1 COMPLETE")
    logger.info("  Rows loaded   : %d", stats["rows_loaded"])
    logger.info("  Batch ID      : %s", stats["batch_id"])
    logger.info("  Table          : %s", stats["table"])
    logger.info("  Duration       : %d ms", stats["duration_ms"])
    logger.info("  Ingestion time : %s", stats["ingestion_timestamp"])
    logger.info("=" * 60)

    # ── Verify ───────────────────────────────────────────────────────────
    row_count = loader.get_row_count(args.table)
    logger.info("Verification: raw.%s now contains %d rows", args.table, row_count)

    db.close()


if __name__ == "__main__":
    main()
