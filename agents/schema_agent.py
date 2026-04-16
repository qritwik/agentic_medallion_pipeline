"""
agents/schema_agent.py
Agent 1: Schema Inference & Evolution

What it does:
- Samples raw bronze rows and sends them to Claude
- Claude inspects the data and proposes a fully typed silver schema
- Returns typed column definitions + transformation SQL
- On subsequent runs, detects schema drift and proposes ALTER TABLE migrations

Honest assessment (README):
  Saved significant time on the initial DDL and date-format handling SQL.
  Less useful once the schema is stable — drift detection is the real ongoing value.
"""

import json
import os
import re

import anthropic

from pipeline.utils import db_cursor, get_logger, log_agent_output

logger = get_logger("agent.schema")

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior data engineer specialising in medallion architectures.
You receive sample rows from a raw bronze table (all columns are TEXT) and produce:
1. A typed PostgreSQL silver schema (column name, type, nullable, rationale)
2. A single idempotent INSERT … SELECT transformation SQL that casts bronze → silver

Rules you must follow:
- Output ONLY valid JSON. No prose, no markdown fences, no preamble.
- Every date/time column must be cast with multiple format fallbacks using COALESCE + TO_TIMESTAMP.
- NULL handling: empty string → NULL for numeric/date columns.
- Include a `_confidence` field (0.0–1.0) per column showing how confident you are in the type.
- If a column has mixed data that you cannot cleanly type, fall back to TEXT and explain why.
- The JSON schema must match this structure exactly:

{
  "columns": [
    {
      "name": "ticket_id",
      "pg_type": "TEXT",
      "nullable": false,
      "rationale": "Natural PK, always present",
      "_confidence": 0.99
    }
  ],
  "transformation_sql": "INSERT INTO silver.tickets (...) SELECT ... FROM bronze.raw_tickets WHERE ...",
  "caveats": ["list of any assumptions or edge cases"]
}"""


def _build_user_prompt(sample_rows: list[dict], existing_silver_cols: list[str] | None = None) -> str:
    sample_json = json.dumps(sample_rows[:50], indent=2, default=str)
    drift_context = ""
    if existing_silver_cols:
        drift_context = f"""
The silver table already exists with these columns: {existing_silver_cols}
Focus on detecting NEW columns or TYPE CHANGES vs the existing schema.
Add a top-level "drift_detected" boolean and "migration_sql" array of ALTER TABLE statements.
"""
    return f"""Here are {len(sample_rows)} sample rows from bronze.raw_tickets (all values are TEXT):

{sample_json}

{drift_context}

Analyse the data carefully:
- Look for date formats (ISO, DD-Mon-YYYY, mixed), numeric fields stored as strings, etc.
- Flag columns with severe quality issues and suggest how to handle them.
- The business domain is facility/operations support tickets.

Return the JSON schema as specified."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_existing_silver_columns() -> list[str] | None:
    """Return current silver column names, or None if the table is empty/new."""
    try:
        with db_cursor() as (_, cur):
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'silver' AND table_name = 'tickets'
                ORDER BY ordinal_position
                """
            )
            rows = cur.fetchall()
            return [r["column_name"] for r in rows] if rows else None
    except Exception:
        return None


def _extract_json(text: str) -> dict:
    """
    Robustly extract JSON from LLM output.
    Claude is instructed to return raw JSON, but defensively strip any accidental fences.
    """
    text = text.strip()
    # Strip markdown code fences if present despite instructions
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


# ── Main agent function ───────────────────────────────────────────────────────

def run(sample_rows: list[dict]) -> dict:
    """
    Run the schema inference agent against a sample of bronze rows.

    Args:
        sample_rows: List of raw row dicts from bronze.raw_tickets

    Returns:
        Parsed agent output dict with keys: columns, transformation_sql, caveats
        (and optionally drift_detected, migration_sql)
    """
    logger.info("Schema agent starting — sample size: %d rows", len(sample_rows))
    existing_cols = _get_existing_silver_columns()

    if existing_cols:
        logger.info("Silver table exists (%d cols) — running drift detection", len(existing_cols))
    else:
        logger.info("No existing silver schema — running fresh inference")

    user_prompt = _build_user_prompt(sample_rows, existing_cols)

    response = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw_output = response.content[0].text
    logger.debug("Raw schema agent output:\n%s", raw_output)

    try:
        result = _extract_json(raw_output)
    except json.JSONDecodeError as e:
        logger.error("Schema agent returned invalid JSON: %s", e)
        raise RuntimeError("Schema agent JSON parse failure") from e

    # Audit trail
    log_agent_output(
        agent_name="schema_agent",
        prompt_summary=f"Schema inference on {len(sample_rows)} rows, drift={existing_cols is not None}",
        raw_output=raw_output,
        applied=True,
        notes=f"Confidence scores: {[c.get('_confidence') for c in result.get('columns', [])]}",
    )

    # Log summary
    cols = result.get("columns", [])
    caveats = result.get("caveats", [])
    drift = result.get("drift_detected", False)

    logger.info("Schema agent proposed %d columns", len(cols))
    if caveats:
        for c in caveats:
            logger.warning("Schema caveat: %s", c)
    if drift:
        migrations = result.get("migration_sql", [])
        logger.warning("Schema drift detected — %d migration statement(s) proposed", len(migrations))

    return result


def apply_drift_migrations(agent_result: dict) -> None:
    """
    Execute any ALTER TABLE migration statements proposed by the schema agent.
    Only runs if drift was detected. Idempotent — uses IF NOT EXISTS / IF EXISTS where possible.
    """
    if not agent_result.get("drift_detected"):
        logger.info("No schema drift — skipping migrations")
        return

    migrations: list[str] = agent_result.get("migration_sql", [])
    if not migrations:
        logger.warning("Drift flagged but no migration SQL provided")
        return

    logger.info("Applying %d schema migration(s)", len(migrations))
    with db_cursor() as (_, cur):
        for sql in migrations:
            logger.info("Executing migration: %s", sql[:120])
            cur.execute(sql)

    logger.info("Schema migrations applied successfully")