"""
agents/quality_agent.py
Agent 2: Data Quality Agent

What it does:
- Profiles the bronze layer: null rates, cardinality, format patterns, outliers
- Sends profile + sample rows to Claude
- Claude proposes cleaning rules with *reasoning* (not just flags)
- Rules are parsed and applied as Python/SQL transformations in the silver layer

Honest assessment (README):
  High value for the initial rule discovery pass — caught the resolved_at < created_at
  inversion and the mixed-casing on priority without us having to enumerate every case.
  Ongoing value is lower once rules are stable; the real win is self-documenting *why*
  each rule exists.
"""

import json
import os
import re

import anthropic
import pandas as pd

from pipeline.utils import db_cursor, get_logger, log_agent_output

logger = get_logger("agent.quality")

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── Profiling ─────────────────────────────────────────────────────────────────

def profile_bronze(sample_size: int = 500) -> dict:
    """
    Compute a statistical profile of the bronze table.
    Returns a dict suitable for serialisation to JSON and sending to the LLM.
    """
    logger.info("Profiling bronze layer (sample=%d)…", sample_size)
    with db_cursor() as (_, cur):
        cur.execute("SELECT COUNT(*) AS n FROM bronze.raw_tickets")
        total = cur.fetchone()["n"]

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
            (sample_size,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows)

    profile: dict = {"total_rows": total, "sample_size": len(df), "columns": {}}

    for col in df.columns:
        series = df[col].replace("", None)
        null_count = series.isna().sum()
        unique_vals = series.dropna().unique().tolist()
        cardinality = len(unique_vals)

        col_profile: dict = {
            "null_rate": round(null_count / len(df), 4),
            "cardinality": cardinality,
            # Show up to 20 distinct values for low-cardinality columns
            "sample_values": unique_vals[:20] if cardinality <= 50 else unique_vals[:10],
        }

        # Numeric detection
        if col in ("cost", "sla_hours"):
            numeric_series = pd.to_numeric(series, errors="coerce")
            col_profile["numeric_parse_fail_rate"] = round(
                numeric_series.isna().sum() / max(series.notna().sum(), 1), 4
            )
            col_profile["min"] = numeric_series.min()
            col_profile["max"] = numeric_series.max()
            col_profile["mean"] = round(numeric_series.mean(), 2) if not numeric_series.isna().all() else None

        # Date detection
        if col in ("created_at", "resolved_at"):
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            col_profile["date_parse_fail_rate"] = round(
                parsed.isna().sum() / max(series.notna().sum(), 1), 4
            )
            # Collect unparseable samples
            failed_samples = series[parsed.isna() & series.notna()].head(5).tolist()
            if failed_samples:
                col_profile["unparseable_samples"] = failed_samples

        profile["columns"][col] = col_profile

    # Flag business logic violations in sample
    violations = []
    df_dates = df.copy()
    df_dates["_created"] = pd.to_datetime(df_dates["created_at"], errors="coerce", utc=True)
    df_dates["_resolved"] = pd.to_datetime(df_dates["resolved_at"], errors="coerce", utc=True)
    inv_mask = (
        df_dates["_resolved"].notna() &
        df_dates["_created"].notna() &
        (df_dates["_resolved"] < df_dates["_created"])
    )
    if inv_mask.any():
        violations.append({
            "rule": "resolved_at < created_at",
            "count": int(inv_mask.sum()),
            "examples": df[inv_mask]["ticket_id"].head(3).tolist(),
        })

    status_note_mismatch = df[
        (df["status"].str.upper() == "OPEN") &
        (df["resolution_notes"].str.strip() != "")
    ]
    if not status_note_mismatch.empty:
        violations.append({
            "rule": "status=OPEN but resolution_notes present",
            "count": len(status_note_mismatch),
            "examples": status_note_mismatch["ticket_id"].head(3).tolist(),
        })

    profile["business_logic_violations"] = violations
    return profile


# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a data quality engineer reviewing raw operational data for a facilities
management system. You receive a statistical profile of a raw (bronze) table and must:

1. Identify data quality issues with clear, actionable reasoning
2. Propose concrete cleaning rules — not vague advice, but specific decisions
3. Explain WHY each rule matters for downstream analytics or reporting

Output ONLY valid JSON with this exact structure:
{
  "rules": [
    {
      "id": "DQ001",
      "field": "priority",
      "issue": "Mixed casing (LOW, Medium, HIGH)",
      "severity": "MEDIUM",
      "why_it_matters": "Aggregations by priority will split the same value across rows",
      "action": "NORMALISE",
      "transform": "UPPER(TRIM(priority))",
      "quarantine_if": null
    }
  ],
  "quarantine_strategy": "Rows missing ticket_id or created_at are untrackable and should be quarantined, not deleted",
  "summary": "brief plain-English summary of overall data quality"
}

Severity levels: CRITICAL (quarantine/block) | HIGH (must fix) | MEDIUM (should fix) | LOW (nice to have)
Actions: NORMALISE | CAST | DERIVE | QUARANTINE | FLAG | DROP_COLUMN
The transform field should be valid PostgreSQL expression or Python pseudocode labelled with [SQL] or [PY].
"""


def _build_user_prompt(profile: dict, sample_rows: list[dict]) -> str:
    sample_json = json.dumps(sample_rows[:20], indent=2, default=str)
    profile_json = json.dumps(profile, indent=2, default=str)
    return f"""DATA PROFILE (from {profile['total_rows']} total rows, sample={profile['sample_size']}):
{profile_json}

SAMPLE ROWS (20 rows for context):
{sample_json}

Domain context: This is a facility operations ticketing system. Tickets track maintenance
requests, safety issues, and vendor work across multiple buildings. Key business uses:
- SLA compliance reporting (resolution time vs sla_hours)
- Vendor spend tracking (cost field)
- Building health dashboards
- Safety issue prioritisation

Propose cleaning rules. Be specific. Prefer fixing over discarding."""


# ── Main agent function ───────────────────────────────────────────────────────

def run(sample_rows: list[dict]) -> dict:
    """
    Run the data quality agent.

    Args:
        sample_rows: Raw rows from bronze for context (separate from profiling sample)

    Returns:
        Parsed agent output dict with 'rules' list and 'quarantine_strategy'
    """
    profile = profile_bronze()
    logger.info("Profile complete. Sending to Claude for quality rule generation…")

    user_prompt = _build_user_prompt(profile, sample_rows)

    response = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw_output = response.content[0].text
    logger.debug("Raw quality agent output:\n%s", raw_output)

    raw_output_clean = raw_output.strip()
    raw_output_clean = re.sub(r"^```(?:json)?\s*", "", raw_output_clean)
    raw_output_clean = re.sub(r"\s*```$", "", raw_output_clean)

    try:
        result = json.loads(raw_output_clean)
    except json.JSONDecodeError as e:
        logger.error("Quality agent returned invalid JSON: %s", e)
        raise RuntimeError("Quality agent JSON parse failure") from e

    log_agent_output(
        agent_name="quality_agent",
        prompt_summary=f"Quality profile of {profile['total_rows']} rows",
        raw_output=raw_output,
        applied=True,
        notes=f"{len(result.get('rules', []))} rules proposed",
    )

    rules = result.get("rules", [])
    logger.info("Quality agent proposed %d cleaning rules", len(rules))
    for rule in rules:
        logger.info(
            "[%s] %s | %s — %s",
            rule.get("severity", "?"),
            rule.get("id", "?"),
            rule.get("field", "?"),
            rule.get("issue", "?"),
        )

    return result