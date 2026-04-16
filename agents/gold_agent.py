"""
agents/gold_agent.py
Agent 4: Gold Layer Design Agent

What it does:
- Introspects the silver schema + computes basic statistics
- Sends domain context + schema to Claude
- Claude proposes gold layer aggregations with business justification
- Generates and executes the SQL for each proposed model

When to trust the output vs. override:
- TRUST: aggregation structure, JOIN logic, GROUP BY choices for standard KPIs
- OVERRIDE: any business metric definition that requires domain knowledge
  (e.g. "SLA breached" threshold, cost allocation rules, vendor tier logic)
- ALWAYS review the generated SQL before running in production

Honest assessment (README):
  Saved ~30 min of schema-reading and SQL drafting for the initial gold model.
  The agent correctly identified the three most valuable aggregations given the
  domain context. SQL quality was good but needed minor tweaks (NULLIF for
  division-by-zero safety, explicit NUMERIC casts). Not something I'd run
  unsupervised in prod without a review step — but as a drafting accelerator, excellent.
"""

import json
import os
import re

import anthropic

from pipeline.utils import db_cursor, get_logger, log_agent_output

logger = get_logger("agent.gold")

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── Schema introspection ──────────────────────────────────────────────────────

def _introspect_silver() -> dict:
    """Pull silver schema + sample stats to give the agent context."""
    with db_cursor() as (_, cur):
        # Column metadata
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'tickets'
            ORDER BY ordinal_position
            """
        )
        columns = [dict(r) for r in cur.fetchall()]

        # Quick stats
        cur.execute("SELECT COUNT(*) AS n FROM silver.tickets")
        row_count = cur.fetchone()["n"]

        cur.execute(
            """
            SELECT
                COUNT(DISTINCT category)            AS distinct_categories,
                COUNT(DISTINCT building)             AS distinct_buildings,
                COUNT(DISTINCT assigned_to)          AS distinct_assignees,
                COUNT(*) FILTER (WHERE status='RESOLVED') AS resolved_count,
                COUNT(*) FILTER (WHERE is_sla_breached)   AS sla_breaches,
                MIN(created_at)                      AS earliest_ticket,
                MAX(created_at)                      AS latest_ticket
            FROM silver.tickets
            """
        )
        stats = dict(cur.fetchone())

        # Sample categories and buildings
        cur.execute(
            "SELECT DISTINCT category FROM silver.tickets WHERE category IS NOT NULL LIMIT 20"
        )
        categories = [r["category"] for r in cur.fetchall()]

        cur.execute(
            "SELECT DISTINCT building FROM silver.tickets WHERE building IS NOT NULL LIMIT 20"
        )
        buildings = [r["building"] for r in cur.fetchall()]

    return {
        "columns": columns,
        "row_count": row_count,
        "stats": {k: str(v) for k, v in stats.items()},
        "sample_categories": categories,
        "sample_buildings": buildings,
    }


# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior analytics engineer designing the gold layer of a medallion
data lakehouse for a facilities management company.

Given the silver schema and domain context, propose exactly 3 gold layer aggregations that
would be most valuable to operations managers and finance teams.

For each model output:
- table_name: snake_case, prefix gold_ (e.g. gold_category_summary)
- business_value: one sentence on why this matters to the business
- refresh_strategy: FULL_REPLACE (simpler) or INCREMENTAL (explain trigger)
- sql: complete, runnable PostgreSQL. Must:
    * Truncate then insert (TRUNCATE gold.<table>; INSERT INTO gold.<table> SELECT ...)
    * Handle NULLs and division-by-zero (use NULLIF in denominators)
    * Cast results to appropriate types (ROUND(...::NUMERIC, 2))
    * Use only columns that exist in the provided silver schema
- when_to_override: one sentence on when a human should manually adjust the SQL

Return ONLY valid JSON:
{
  "models": [
    {
      "table_name": "category_priority_summary",
      "business_value": "...",
      "refresh_strategy": "FULL_REPLACE",
      "sql": "TRUNCATE gold.category_priority_summary; INSERT INTO ...",
      "when_to_override": "..."
    }
  ],
  "design_notes": "brief explanation of your choices"
}
"""


def _build_user_prompt(schema_info: dict) -> str:
    return f"""SILVER SCHEMA INTROSPECTION:
{json.dumps(schema_info, indent=2, default=str)}

BUSINESS DOMAIN:
Facilities management ticketing system. Stakeholders:
- Operations managers: want to know where maintenance is bottlenecked and which buildings have chronic issues
- Finance: want vendor spend visibility and cost-per-category breakdown
- Compliance: need SLA breach rates by priority and category

The silver table already has these useful derived columns:
- resolution_time_hrs: hours from created_at to resolved_at
- is_sla_breached: boolean, true if resolution_time_hrs > sla_hours
- assigned_to_type: INTERNAL / VENDOR / UNKNOWN
- classified_category: LLM-normalised category (may be NULL if semantic agent hasn't run)
- urgency_score: 1-5 LLM-derived urgency

Design 3 gold models. Prioritise actionability over complexity."""


# ── SQL execution ─────────────────────────────────────────────────────────────

def _execute_gold_sql(sql: str, table_name: str) -> int:
    """Execute a gold model SQL (TRUNCATE + INSERT) and return rows inserted."""
    with db_cursor() as (_, cur):
        cur.execute(sql)
        # Get inserted row count from the INSERT statement result
        # rowcount from TRUNCATE is -1, so we query after
        cur.execute(f"SELECT COUNT(*) AS n FROM gold.{table_name}")
        return cur.fetchone()["n"]


# ── Main agent function ───────────────────────────────────────────────────────

def run() -> dict:
    """
    Run the gold layer design agent and execute the proposed SQL.

    Returns:
        Summary dict with proposed models and execution results.
    """
    logger.info("Gold agent: introspecting silver schema…")
    schema_info = _introspect_silver()

    if schema_info["row_count"] == 0:
        logger.warning("Silver table is empty — gold agent skipping")
        return {"models": [], "executed": 0}

    logger.info(
        "Silver has %d rows across %d categories, %d buildings",
        schema_info["row_count"],
        len(schema_info["sample_categories"]),
        len(schema_info["sample_buildings"]),
    )

    response = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_prompt(schema_info)}],
    )

    raw_output = response.content[0].text.strip()
    raw_output = re.sub(r"^```(?:json)?\s*", "", raw_output)
    raw_output = re.sub(r"\s*```$", "", raw_output)

    try:
        result = json.loads(raw_output)
    except json.JSONDecodeError as e:
        logger.error("Gold agent returned invalid JSON: %s", e)
        raise RuntimeError("Gold agent JSON parse failure") from e

    log_agent_output(
        agent_name="gold_agent",
        prompt_summary=f"Gold design for {schema_info['row_count']} silver rows",
        raw_output=raw_output,
        applied=True,
    )

    models = result.get("models", [])
    logger.info("Gold agent proposed %d models: %s", len(models), [m["table_name"] for m in models])
    logger.info("Design notes: %s", result.get("design_notes", ""))

    executed = 0
    for model in models:
        table = model["table_name"]
        sql = model.get("sql", "")
        if not sql:
            logger.warning("No SQL for model %s — skipping", table)
            continue

        # Ensure the target table exists (agent may propose novel table names)
        # We only execute against pre-defined gold tables for safety
        allowed_tables = {
            "category_priority_summary",
            "building_health",
            "assignee_performance",
        }
        if table not in allowed_tables:
            logger.warning(
                "Gold agent proposed unknown table '%s' — skipping for safety. "
                "Add to allowed_tables to enable.", table,
            )
            model["execution_status"] = "SKIPPED_UNKNOWN_TABLE"
            continue

        try:
            rows = _execute_gold_sql(sql, table)
            model["execution_status"] = "SUCCESS"
            model["rows_inserted"] = rows
            executed += 1
            logger.info("Gold model '%s' materialised — %d rows", table, rows)
        except Exception as e:
            model["execution_status"] = f"FAILED: {e}"
            logger.error("Gold model '%s' failed: %s", table, e)

    logger.info("Gold agent complete — %d/%d models executed", executed, len(models))
    return result