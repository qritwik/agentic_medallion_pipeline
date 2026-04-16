"""
agents/tagger_agent.py
Bonus: Metadata Auto-Tagging Agent

Runs at bronze landing time. Inspects column names + sample values and
produces per-column metadata tags:
  - sensitivity:  PUBLIC / INTERNAL / CONFIDENTIAL / PII
  - pii_type:     NAME / EMAIL / PHONE / ID / LOCATION / FINANCIAL / NONE
  - domain:       identity of the business domain for this column
  - description:  one-line plain English description of what the column contains

Output is stored in meta.column_catalog — a lightweight data dictionary
that documents itself as data lands, rather than requiring a separate
manual cataloguing step.

Why this matters:
  At scale, knowing which columns contain PII before they hit the data
  warehouse is what separates a compliant pipeline from a GDPR incident.
  Auto-tagging at landing gives the security/compliance team a head-start
  and ensures new columns don't silently slip through uncatalogued.
"""

import json
import os
import re

import anthropic

from pipeline.utils import db_cursor, get_logger, log_agent_output

logger = get_logger("agent.tagger")

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── DDL ───────────────────────────────────────────────────────────────────────

CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS meta.column_catalog (
    catalog_id      BIGSERIAL   PRIMARY KEY,
    catalogued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_name     TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    column_name     TEXT        NOT NULL,
    sensitivity     TEXT,       -- PUBLIC / INTERNAL / CONFIDENTIAL / PII
    pii_type        TEXT,       -- NAME / EMAIL / PHONE / ID / LOCATION / FINANCIAL / NONE
    domain          TEXT,       -- e.g. "identity", "operations", "financial", "temporal"
    description     TEXT,
    sample_values   TEXT,       -- JSON array of sample values (truncated)
    agent_version   TEXT        NOT NULL DEFAULT '1.0',
    UNIQUE (schema_name, table_name, column_name)
);
"""


def ensure_catalog_table() -> None:
    with db_cursor() as (_, cur):
        cur.execute(CATALOG_DDL)


# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a data governance engineer performing automatic metadata classification
for a data catalog. You receive column names and sample values from a database table and must
classify each column for sensitivity, PII risk, business domain, and provide a plain-English
description.

Output ONLY a JSON array with one object per column:
[
  {
    "column_name": "submitted_by",
    "sensitivity": "PII",
    "pii_type": "NAME",
    "domain": "identity",
    "description": "Name or identifier of the person who submitted the support ticket"
  }
]

Sensitivity levels:
- PUBLIC: Can be freely shared externally (ticket_id, building names, categories)
- INTERNAL: Internal business use only (status, priority, sla_hours, costs)
- CONFIDENTIAL: Restricted access, business-sensitive (cost, vendor names)
- PII: Contains or likely contains personally identifiable information

PII types (use NONE if not PII):
- NAME: person's name (first, last, initials, username)
- EMAIL: email address
- PHONE: phone number
- ID: personal identifier (employee ID, SSN, etc.)
- LOCATION: personal address or specific location tied to a person
- FINANCIAL: financial account info, card numbers
- NONE: not PII

Domain categories: identity | operations | financial | temporal | classification | text | system

Be conservative — when in doubt about PII, classify as PII rather than not.
Pipeline metadata columns (starting with _) should be tagged as sensitivity=INTERNAL, domain=system.
"""


def _build_prompt(columns_with_samples: list[dict]) -> str:
    return f"""Table: bronze.raw_tickets (facilities management support tickets)

Columns to classify:
{json.dumps(columns_with_samples, indent=2, default=str)}

Classify every column. Include pipeline metadata columns (_source_file, _row_hash, etc.)."""


# ── Main ──────────────────────────────────────────────────────────────────────

def _collect_column_samples() -> list[dict]:
    """Pull column names + sample values from bronze."""
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT ticket_id, created_at, resolved_at, category, priority, status,
                   building, description, submitted_by, assigned_to, resolution_notes,
                   cost, sla_hours, _source_file, _row_hash, _ingested_at
            FROM bronze.raw_tickets
            ORDER BY random()
            LIMIT 10
            """
        )
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        return []

    columns = list(rows[0].keys())
    result = []
    for col in columns:
        vals = list({str(r.get(col, "")) for r in rows if r.get(col)})[:5]
        result.append({"column_name": col, "sample_values": vals})
    return result


def run() -> list[dict]:
    """
    Run the metadata tagging agent against bronze.raw_tickets.
    Writes results to meta.column_catalog.

    Returns:
        List of tagged column dicts.
    """
    ensure_catalog_table()

    columns_with_samples = _collect_column_samples()
    if not columns_with_samples:
        logger.warning("No bronze data to tag — skipping tagger agent")
        return []

    logger.info("Tagger agent: classifying %d columns…", len(columns_with_samples))

    response = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_prompt(columns_with_samples)}],
    )

    raw_output = response.content[0].text.strip()
    raw_output = re.sub(r"^```(?:json)?\s*", "", raw_output)
    raw_output = re.sub(r"\s*```$", "", raw_output)

    try:
        tags: list[dict] = json.loads(raw_output)
    except json.JSONDecodeError as e:
        logger.error("Tagger agent returned invalid JSON: %s", e)
        raise RuntimeError("Tagger agent JSON parse failure") from e

    log_agent_output(
        agent_name="tagger_agent",
        prompt_summary=f"Column tagging for {len(columns_with_samples)} columns",
        raw_output=raw_output,
        applied=True,
        notes=f"PII columns: {[t['column_name'] for t in tags if t.get('sensitivity') == 'PII']}",
    )

    # Persist to catalog (upsert — re-running is safe)
    with db_cursor() as (_, cur):
        for tag in tags:
            samples = next(
                (c["sample_values"] for c in columns_with_samples
                 if c["column_name"] == tag["column_name"]),
                [],
            )
            cur.execute(
                """
                INSERT INTO meta.column_catalog
                    (schema_name, table_name, column_name, sensitivity, pii_type,
                     domain, description, sample_values)
                VALUES ('bronze', 'raw_tickets', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (schema_name, table_name, column_name) DO UPDATE SET
                    sensitivity   = EXCLUDED.sensitivity,
                    pii_type      = EXCLUDED.pii_type,
                    domain        = EXCLUDED.domain,
                    description   = EXCLUDED.description,
                    sample_values = EXCLUDED.sample_values,
                    catalogued_at = now()
                """,
                (
                    tag["column_name"],
                    tag.get("sensitivity"),
                    tag.get("pii_type"),
                    tag.get("domain"),
                    tag.get("description"),
                    json.dumps(samples),
                ),
            )

    pii_cols = [t["column_name"] for t in tags if t.get("sensitivity") == "PII"]
    logger.info(
        "Tagger agent complete — %d columns catalogued, PII detected in: %s",
        len(tags), pii_cols,
    )

    return tags