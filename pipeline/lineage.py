"""
pipeline/lineage.py
End-to-end data lineage tracking.

Records every transformation a row passes through, from the source CSV
all the way to which gold aggregation it contributed to.

Design:
  - meta.lineage_events is an append-only event log (not updated, only inserted)
  - Each event records: what entity, at which stage, what happened, and when
  - The lineage_report() function reconstructs the full journey for any ticket_id

This is intentionally lightweight — a production implementation would use
OpenLineage or Marquez, but for a take-home the principle is more important
than the framework.
"""

from pipeline.utils import db_cursor, get_logger

logger = get_logger("lineage")

# ── DDL (called from init.sql but also safe to call here) ────────────────────

LINEAGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS meta.lineage_events (
    event_id        BIGSERIAL   PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    entity_id       TEXT        NOT NULL,  -- ticket_id (or batch ID for gold)
    entity_type     TEXT        NOT NULL,  -- TICKET / GOLD_TABLE
    stage           TEXT        NOT NULL,  -- BRONZE / SILVER / QUARANTINE / GOLD / AGENT_*
    operation       TEXT        NOT NULL,  -- INSERTED / UPDATED / CLASSIFIED / QUARANTINED / MATERIALISED
    source_ref      TEXT,                  -- e.g. source filename, bronze _row_id
    target_ref      TEXT,                  -- e.g. silver ticket_id, gold table name
    agent_output_id BIGINT,               -- FK to meta.agent_outputs if agent-driven
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS lineage_entity_idx ON meta.lineage_events (entity_id, stage);
"""


def ensure_lineage_table() -> None:
    """Create the lineage table if it doesn't exist (idempotent)."""
    with db_cursor() as (_, cur):
        cur.execute(LINEAGE_TABLE_DDL)


def record(
    entity_id: str,
    entity_type: str,
    stage: str,
    operation: str,
    source_ref: str | None = None,
    target_ref: str | None = None,
    agent_output_id: int | None = None,
    notes: str | None = None,
) -> None:
    """Append one lineage event. Fire-and-forget — errors are logged, not raised."""
    try:
        with db_cursor() as (_, cur):
            cur.execute(
                """
                INSERT INTO meta.lineage_events
                    (entity_id, entity_type, stage, operation,
                     source_ref, target_ref, agent_output_id, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (entity_id, entity_type, stage, operation,
                 source_ref, target_ref, agent_output_id, notes),
            )
    except Exception as e:
        logger.warning("Lineage write failed (non-fatal): %s", e)


def backfill_from_tables() -> dict:
    """
    Populate lineage_events from existing bronze/silver/quarantine data.
    Useful after the first pipeline run to retroactively build the lineage graph.
    Safe to re-run — uses ON CONFLICT DO NOTHING logic via CTE.
    """
    ensure_lineage_table()
    counts = {}

    with db_cursor() as (_, cur):
        # Bronze events
        cur.execute(
            """
            INSERT INTO meta.lineage_events
                (entity_id, entity_type, stage, operation, source_ref, notes)
            SELECT
                COALESCE(ticket_id, _row_hash),
                'TICKET',
                'BRONZE',
                'INSERTED',
                _source_file,
                'Backfilled from bronze.raw_tickets'
            FROM bronze.raw_tickets
            ON CONFLICT DO NOTHING
            """
        )
        counts["bronze"] = cur.rowcount

        # Silver events
        cur.execute(
            """
            INSERT INTO meta.lineage_events
                (entity_id, entity_type, stage, operation, source_ref, target_ref, notes)
            SELECT
                ticket_id,
                'TICKET',
                'SILVER',
                'INSERTED',
                _source_file,
                ticket_id,
                'Backfilled from silver.tickets'
            FROM silver.tickets
            ON CONFLICT DO NOTHING
            """
        )
        counts["silver"] = cur.rowcount

        # Quarantine events
        cur.execute(
            """
            INSERT INTO meta.lineage_events
                (entity_id, entity_type, stage, operation, source_ref, notes)
            SELECT
                COALESCE(ticket_id, _bronze_row_id::TEXT),
                'TICKET',
                'QUARANTINE',
                'QUARANTINED',
                _reason,
                'Backfilled from silver.quarantine'
            FROM silver.quarantine
            ON CONFLICT DO NOTHING
            """
        )
        counts["quarantine"] = cur.rowcount

        # Semantic classification events
        cur.execute(
            """
            INSERT INTO meta.lineage_events
                (entity_id, entity_type, stage, operation, notes)
            SELECT
                ticket_id,
                'TICKET',
                'AGENT_SEMANTIC',
                'CLASSIFIED',
                'classified_category=' || COALESCE(classified_category, 'NULL') ||
                ', urgency=' || COALESCE(urgency_score::TEXT, 'NULL')
            FROM silver.tickets
            WHERE classified_category IS NOT NULL
            ON CONFLICT DO NOTHING
            """
        )
        counts["semantic"] = cur.rowcount

        # Gold materialisation events (one per table)
        for gold_table in ("category_priority_summary", "building_health", "assignee_performance"):
            cur.execute(
                f"SELECT COUNT(*) AS n FROM gold.{gold_table}"
            )
            n = cur.fetchone()["n"]
            if n > 0:
                cur.execute(
                    """
                    INSERT INTO meta.lineage_events
                        (entity_id, entity_type, stage, operation, target_ref, notes)
                    VALUES (%s, 'GOLD_TABLE', 'GOLD', 'MATERIALISED', %s, %s)
                    """,
                    (
                        gold_table,
                        f"gold.{gold_table}",
                        f"Materialised {n} rows",
                    ),
                )
        counts["gold"] = 3

    logger.info("Lineage backfill complete: %s", counts)
    return counts


def lineage_report(ticket_id: str) -> list[dict]:
    """
    Return the full lineage trail for a single ticket_id across all stages.

    Example output:
    [
      {stage: "BRONZE",          operation: "INSERTED",    recorded_at: ..., notes: "raw_tickets.csv"},
      {stage: "SILVER",          operation: "INSERTED",    recorded_at: ..., notes: None},
      {stage: "AGENT_SEMANTIC",  operation: "CLASSIFIED",  recorded_at: ..., notes: "classified_category=Electrical, urgency=5"},
    ]
    """
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT stage, operation, recorded_at, source_ref, target_ref, notes
            FROM meta.lineage_events
            WHERE entity_id = %s
            ORDER BY recorded_at
            """,
            (ticket_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        logger.info("No lineage found for ticket_id=%s", ticket_id)
    else:
        logger.info("Lineage for %s (%d events):", ticket_id, len(rows))
        for r in rows:
            logger.info(
                "  [%s] %-20s %-15s %s",
                r["recorded_at"].strftime("%Y-%m-%d %H:%M:%S") if r["recorded_at"] else "?",
                r["stage"],
                r["operation"],
                r["notes"] or "",
            )
    return rows


def summary_report() -> dict:
    """
    High-level lineage summary: how many entities at each stage.
    """
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT stage, operation, COUNT(DISTINCT entity_id) AS entities
            FROM meta.lineage_events
            GROUP BY stage, operation
            ORDER BY MIN(recorded_at)
            """
        )
        rows = [dict(r) for r in cur.fetchall()]

    logger.info("Lineage summary:")
    for r in rows:
        logger.info("  %-25s %-15s %d entities", r["stage"], r["operation"], r["entities"])

    return {f"{r['stage']}.{r['operation']}": r["entities"] for r in rows}