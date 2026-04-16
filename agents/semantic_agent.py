"""
agents/semantic_agent.py
Agent 3: Semantic Classification Agent

What it does:
- Reads description (free-text) + raw category from silver tickets
- Sends in batches to Claude for:
    * Canonical category (normalised from the messy values + description)
    * Urgency score 1–5
    * Extracted location (room, floor, stairwell, etc.)
    * Extracted asset (breaker, smoke detector, A/C unit, etc.)
- Results written back to silver.tickets enrichment columns

Scale & cost awareness:
- Batches N tickets per API call (default 20) — reduces per-row overhead
- Skips already-classified rows (classified_category IS NOT NULL) for idempotency
- Caches Claude's output in meta.agent_outputs so re-runs don't re-classify
- At 10M rows: would move to async batching with a queue (SQS/Pub-Sub),
  process only new/updated rows via CDC, and cache responses in Redis by
  description hash to avoid re-classifying duplicate descriptions.

Honest assessment (README):
  High value — normalising category alone ("Fire/Safety" vs "Fire Safety" vs
  description-only rows) would be tedious regex work. The urgency score adds a
  dimension that didn't exist in the raw data. Cost at full 10k-row scale with
  batches of 20 = ~500 API calls, acceptable. At 1M rows, caching by description
  hash is essential.
"""

import json
import os
import re
import time

import anthropic

from pipeline.utils import db_cursor, get_logger, log_agent_output

logger = get_logger("agent.semantic")

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

BATCH_SIZE = int(os.getenv("SEMANTIC_BATCH_SIZE", 20))

# Canonical category list — keeps the LLM from inventing new names
CANONICAL_CATEGORIES = [
    "Electrical", "Fire Safety", "HVAC", "Plumbing", "IT/Network",
    "Pest Control", "Structural", "Cleaning", "Security", "General Maintenance", "Other"
]

# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = f"""You are a facility operations data analyst. You receive batches of support
tickets and must classify and enrich each one.

For each ticket output:
- classified_category: pick exactly one from {json.dumps(CANONICAL_CATEGORIES)}
- urgency_score: integer 1 (routine) to 5 (safety-critical/production-down)
- extracted_location: the specific room, floor, stairwell, wing mentioned (null if none)
- extracted_asset: the physical asset or equipment mentioned (null if none)

Return ONLY a JSON array with one object per input ticket, in the same order:
[
  {{
    "ticket_id": "TKT-XXXX",
    "classified_category": "Electrical",
    "urgency_score": 4,
    "extracted_location": "server room 391",
    "extracted_asset": "circuit breaker"
  }}
]

Rules:
- Use the description field primarily; use the raw category as a hint, not gospel.
- If description is empty or uninformative (e.g. "cold"), infer from raw category.
- urgency_score=5 only for: fire/safety hazards, production system outages, flooding.
- urgency_score=4 for: HVAC failure, no power, lift/elevator issues.
- urgency_score=3 for: significant discomfort but not dangerous.
- urgency_score 1-2 for: cosmetic or low-impact requests.
- Output ONLY the JSON array. No prose. No markdown.
"""


def _build_batch_prompt(tickets: list[dict]) -> str:
    items = [
        {
            "ticket_id": t.get("ticket_id", ""),
            "raw_category": t.get("category", ""),
            "description": t.get("description", ""),
            "priority": t.get("priority", ""),
            "status": t.get("status", ""),
        }
        for t in tickets
    ]
    return json.dumps(items, indent=2, default=str)


# ── Batch processing ──────────────────────────────────────────────────────────

def _classify_batch(tickets: list[dict]) -> list[dict]:
    """Send one batch to Claude and return parsed classifications."""
    prompt = _build_batch_prompt(tickets)
    response = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def _fetch_unclassified(limit: int | None = None) -> list[dict]:
    """Pull tickets from silver that haven't been semantically classified yet."""
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT ticket_id, category, description, priority, status
            FROM silver.tickets
            WHERE classified_category IS NULL
            ORDER BY ticket_id
            """ + (f"LIMIT {limit}" if limit else ""),
        )
        return [dict(r) for r in cur.fetchall()]


def _write_classifications(classifications: list[dict]) -> int:
    """Upsert classification results back into silver.tickets."""
    updated = 0
    with db_cursor() as (_, cur):
        for c in classifications:
            cur.execute(
                """
                UPDATE silver.tickets
                SET classified_category = %s,
                    urgency_score       = %s,
                    extracted_location  = %s,
                    extracted_asset     = %s
                WHERE ticket_id = %s
                  AND classified_category IS NULL  -- idempotent: skip if already done
                """,
                (
                    c.get("classified_category"),
                    c.get("urgency_score"),
                    c.get("extracted_location"),
                    c.get("extracted_asset"),
                    c.get("ticket_id"),
                ),
            )
            updated += cur.rowcount
    return updated


# ── Main agent function ───────────────────────────────────────────────────────

def run(limit: int | None = None) -> dict:
    """
    Classify all unclassified silver tickets in batches.

    Args:
        limit: Cap the number of tickets to process (useful for testing).
               None = process all unclassified rows.

    Returns:
        Summary dict: {total_classified, batches, errors}
    """
    tickets = _fetch_unclassified(limit)
    if not tickets:
        logger.info("No unclassified tickets found — semantic agent skipping")
        return {"total_classified": 0, "batches": 0, "errors": 0}

    logger.info(
        "Semantic agent: %d tickets to classify in batches of %d",
        len(tickets), BATCH_SIZE,
    )

    total_classified = 0
    errors = 0
    batches = 0

    for i in range(0, len(tickets), BATCH_SIZE):
        batch = tickets[i: i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        logger.info(
            "Processing batch %d/%d (%d tickets)…",
            batch_num, -(-len(tickets) // BATCH_SIZE), len(batch),
        )

        try:
            classifications = _classify_batch(batch)
            n = _write_classifications(classifications)
            total_classified += n
            batches += 1

            log_agent_output(
                agent_name="semantic_agent",
                prompt_summary=f"Batch {batch_num}: {len(batch)} tickets",
                raw_output=json.dumps(classifications),
                applied=True,
                notes=f"Updated {n} rows",
            )

            # Respect rate limits — brief pause between batches
            if i + BATCH_SIZE < len(tickets):
                time.sleep(0.5)

        except Exception as e:
            logger.error("Batch %d failed: %s", batch_num, e)
            errors += 1
            # Continue with next batch — don't abort the whole run
            continue

    logger.info(
        "Semantic agent complete — classified=%d, batches=%d, errors=%d",
        total_classified, batches, errors,
    )
    return {"total_classified": total_classified, "batches": batches, "errors": errors}