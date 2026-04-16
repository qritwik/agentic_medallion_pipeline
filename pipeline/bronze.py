"""
pipeline/bronze.py
Bronze layer: raw ingestion with zero transformation.

Responsibilities:
- Read raw CSV as-is (schema-on-read, all columns as text)
- Append lineage metadata: _source_file, _ingested_at, _row_hash
- Upsert into bronze.raw_tickets using row hash for idempotency
- Log how many rows were new vs already seen
"""

import os

import pandas as pd

from pipeline.utils import RunTracker, db_cursor, get_logger, row_hash

logger = get_logger("bronze")


def ingest(csv_path: str) -> int:
    """
    Ingest raw CSV into bronze.raw_tickets.

    Returns:
        Number of new rows inserted (duplicates skipped via ON CONFLICT DO NOTHING).
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Raw data file not found: {csv_path}")

    source_file = os.path.basename(csv_path)
    logger.info("Reading raw CSV: %s", csv_path)

    # Read everything as strings — no type inference, no data loss.
    # keep_default_na=False prevents pandas silently converting empty strings to NaN
    df = pd.read_csv(
        csv_path,
        dtype=str,
        sep="\t",          # TSV as provided; change to "," if source changes
        keep_default_na=False,
        skipinitialspace=True,
    )
    df = df.fillna("")     # normalise any residual NaN → empty string

    # Rename columns to lowercase + underscores for consistency
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    total_rows = len(df)
    logger.info("Loaded %d rows from source", total_rows)

    expected_cols = [
        "ticket_id", "created_at", "resolved_at", "category",
        "priority", "status", "building", "description",
        "submitted_by", "assigned_to", "resolution_notes", "cost", "sla_hours",
    ]
    # Add any missing expected columns as empty strings (handles truncated rows)
    for col in expected_cols:
        if col not in df.columns:
            logger.warning("Column '%s' missing from source — filling with empty string", col)
            df[col] = ""

    with RunTracker("BRONZE") as rt:
        inserted = 0
        skipped = 0

        with db_cursor() as (_, cur):
            for _, row in df.iterrows():
                raw = {col: row.get(col, "") for col in expected_cols}
                h = row_hash(raw)

                cur.execute(
                    """
                    INSERT INTO bronze.raw_tickets
                        (_source_file, _row_hash,
                         ticket_id, created_at, resolved_at, category,
                         priority, status, building, description,
                         submitted_by, assigned_to, resolution_notes,
                         cost, sla_hours)
                    VALUES
                        (%s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s,
                         %s, %s)
                    ON CONFLICT (_row_hash) DO NOTHING
                    """,
                    (
                        source_file, h,
                        raw["ticket_id"], raw["created_at"], raw["resolved_at"],
                        raw["category"], raw["priority"], raw["status"],
                        raw["building"], raw["description"], raw["submitted_by"],
                        raw["assigned_to"], raw["resolution_notes"],
                        raw["cost"], raw["sla_hours"],
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

        rt.rows_processed = inserted
        rt.rows_rejected = skipped
        rt.notes = f"Skipped {skipped} duplicate rows (hash collision)"

    logger.info("Bronze ingest complete — inserted=%d, skipped (duplicates)=%d", inserted, skipped)
    return inserted


def sample_rows(n: int = 50) -> list[dict]:
    """
    Pull a sample of raw rows for agent inspection.
    Returns plain dicts (no psycopg2-specific types).
    """
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT ticket_id, created_at, resolved_at, category,
                   priority, status, building, description,
                   submitted_by, assigned_to, resolution_notes,
                   cost, sla_hours
            FROM bronze.raw_tickets
            ORDER BY random()
            LIMIT %s
            """,
            (n,),
        )
        return [dict(r) for r in cur.fetchall()]


def row_count() -> int:
    with db_cursor() as (_, cur):
        cur.execute("SELECT COUNT(*) AS n FROM bronze.raw_tickets")
        return cur.fetchone()["n"]