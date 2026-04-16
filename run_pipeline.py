#!/usr/bin/env python3
"""
run_pipeline.py
Single entrypoint for the full medallion pipeline.

Usage:
    python run_pipeline.py                    # run full pipeline
    python run_pipeline.py --stage bronze     # bronze only
    python run_pipeline.py --stage silver     # bronze + silver
    python run_pipeline.py --stage gold       # bronze + silver + gold
    python run_pipeline.py --skip-agents      # skip all AI agents (faster, offline)
    python run_pipeline.py --semantic-limit 100  # classify only first N tickets (cost control)
"""

import argparse
import sys
import time
import os

from dotenv import load_dotenv

load_dotenv()

from pipeline.utils import get_logger, db_cursor
from pipeline import bronze, silver, gold
from pipeline import lineage
from agents import schema_agent, quality_agent, semantic_agent, gold_agent, tagger_agent

logger = get_logger("pipeline")


def wait_for_db(retries: int = 30, delay: float = 2.0) -> None:
    """Block until PostgreSQL is ready (useful when started via docker-compose)."""
    import psycopg2
    logger.info("Waiting for database…")
    for i in range(retries):
        try:
            with db_cursor() as (_, cur):
                cur.execute("SELECT 1")
            logger.info("Database is ready")
            return
        except Exception:
            if i < retries - 1:
                time.sleep(delay)
    raise RuntimeError("Database did not become ready in time")


def run(stage: str, skip_agents: bool, raw_data: str, semantic_limit: int | None) -> None:
    start = time.time()
    logger.info("=" * 60)
    logger.info("MEDALLION PIPELINE — stage=%s, skip_agents=%s", stage, skip_agents)
    logger.info("=" * 60)

    wait_for_db()

    # ── BRONZE ────────────────────────────────────────────────────────────────
    logger.info("\n── BRONZE ──────────────────────────────────────────────────")
    bronze_rows = bronze.ingest(raw_data)
    logger.info("Bronze: %d new rows ingested", bronze_rows)

    # ── BONUS: Metadata Auto-Tagging at landing ────────────────────────────
    if not skip_agents and bronze_rows > 0:
        logger.info("\n── BONUS AGENT: Metadata Tagger ────────────────────────────")
        try:
            tags = tagger_agent.run()
            pii = [t["column_name"] for t in tags if t.get("sensitivity") == "PII"]
            logger.info("Tagger: %d columns catalogued, PII columns: %s", len(tags), pii)
        except Exception as e:
            logger.warning("Tagger agent failed (non-fatal): %s", e)

    if stage == "bronze":
        logger.info("Stage=bronze — stopping here")
        _print_summary(start)
        return

    # ── AGENT 1: Schema Inference ──────────────────────────────────────────
    if not skip_agents:
        logger.info("\n── AGENT 1: Schema Inference & Evolution ───────────────────")
        try:
            sample = bronze.sample_rows(int(os.getenv("AGENT_SAMPLE_SIZE", 50)))
            schema_result = schema_agent.run(sample)
            logger.info(
                "Schema agent: %d columns proposed, drift=%s",
                len(schema_result.get("columns", [])),
                schema_result.get("drift_detected", False),
            )
            schema_agent.apply_drift_migrations(schema_result)
        except Exception as e:
            logger.warning("Schema agent failed (non-fatal): %s", e)

    # ── AGENT 2: Data Quality ──────────────────────────────────────────────
    if not skip_agents:
        logger.info("\n── AGENT 2: Data Quality ───────────────────────────────────")
        try:
            sample = bronze.sample_rows(int(os.getenv("AGENT_SAMPLE_SIZE", 50)))
            quality_result = quality_agent.run(sample)
            logger.info(
                "Quality agent: %d rules proposed",
                len(quality_result.get("rules", [])),
            )
        except Exception as e:
            logger.warning("Quality agent failed (non-fatal): %s", e)

    # ── SILVER ────────────────────────────────────────────────────────────────
    logger.info("\n── SILVER ──────────────────────────────────────────────────")
    silver_result = silver.transform()
    logger.info(
        "Silver: inserted=%d, updated=%d, quarantined=%d",
        silver_result["inserted"],
        silver_result["updated"],
        silver_result["quarantined"],
    )

    if stage == "silver":
        logger.info("Stage=silver — stopping here")
        _print_summary(start)
        return

    # ── AGENT 3: Semantic Classification ──────────────────────────────────
    if not skip_agents:
        logger.info("\n── AGENT 3: Semantic Classification ────────────────────────")
        try:
            sem_result = semantic_agent.run(limit=semantic_limit)
            logger.info(
                "Semantic agent: classified=%d in %d batches (%d errors)",
                sem_result["total_classified"],
                sem_result["batches"],
                sem_result["errors"],
            )
        except Exception as e:
            logger.warning("Semantic agent failed (non-fatal): %s", e)

    # ── AGENT 4: Gold Layer Design ─────────────────────────────────────────
    if not skip_agents:
        logger.info("\n── AGENT 4: Gold Layer Design ──────────────────────────────")
        try:
            gold_result = gold_agent.run()
            executed = sum(
                1 for m in gold_result.get("models", [])
                if m.get("execution_status") == "SUCCESS"
            )
            logger.info(
                "Gold agent: %d/%d models executed",
                executed, len(gold_result.get("models", [])),
            )
        except Exception as e:
            logger.warning("Gold agent failed — falling back to pre-defined SQL: %s", e)
            gold.refresh()
    else:
        logger.info("\n── GOLD (pre-defined SQL) ──────────────────────────────────")
        gold_counts = gold.refresh()
        logger.info("Gold tables refreshed: %s", gold_counts)

    # ── LINEAGE BACKFILL ──────────────────────────────────────────────────
    logger.info("\n── LINEAGE ─────────────────────────────────────────────────")
    try:
        lineage.ensure_lineage_table()
        lineage_counts = lineage.backfill_from_tables()
        lineage.summary_report()
        logger.info("Lineage events recorded: %s", lineage_counts)
    except Exception as e:
        logger.warning("Lineage backfill failed (non-fatal): %s", e)

    _print_summary(start)


def _print_summary(start: float) -> None:
    elapsed = time.time() - start
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline complete in %.1fs", elapsed)
    logger.info("=" * 60)

    try:
        with db_cursor() as (_, cur):
            cur.execute("SELECT COUNT(*) AS n FROM bronze.raw_tickets")
            b = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM silver.tickets")
            s = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM silver.quarantine")
            q = cur.fetchone()["n"]
            cur.execute(
                "SELECT stage, status, rows_processed, rows_rejected, finished_at "
                "FROM meta.pipeline_runs ORDER BY run_id DESC LIMIT 10"
            )
            runs = cur.fetchall()

        logger.info("Bronze rows:     %d", b)
        logger.info("Silver rows:     %d", s)
        logger.info("Quarantined:     %d", q)
        logger.info("\nRecent pipeline runs:")
        for r in runs:
            logger.info(
                "  %-12s %-8s  processed=%-6s  rejected=%-4s",
                r["stage"], r["status"], r["rows_processed"], r["rows_rejected"],
            )
    except Exception as e:
        logger.warning("Could not print summary stats: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Medallion pipeline runner")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold"],
        default="gold",
        help="Stop after this stage (default: gold = full pipeline)",
    )
    parser.add_argument(
        "--skip-agents",
        action="store_true",
        help="Skip all AI agents (runs faster, no API calls)",
    )
    parser.add_argument(
        "--raw-data",
        default=os.getenv("RAW_DATA_PATH", "data/raw_tickets.csv"),
        help="Path to raw CSV file",
    )
    parser.add_argument(
        "--semantic-limit",
        type=int,
        default=None,
        help="Max tickets to classify with semantic agent (cost control; default: all)",
    )
    args = parser.parse_args()

    try:
        run(
            stage=args.stage,
            skip_agents=args.skip_agents,
            raw_data=args.raw_data,
            semantic_limit=args.semantic_limit,
        )
    except Exception as e:
        logger.critical("Pipeline failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()