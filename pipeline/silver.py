"""
pipeline/silver.py
Silver layer: typed, validated, deduplicated, cleaned.

This module uses the outputs from schema_agent and quality_agent to guide
transformation, but falls back to hard-coded sensible defaults if agents
are unavailable (e.g. in offline/test mode).

Cleaning decisions (documented per quality agent guidance):
  DATE PARSING
  - Multiple format fallbacks: ISO 8601, DD-Mon-YYYY, plain date strings
  - Impossible dates (resolved < created) → resolved_at set to NULL, row flagged
  PRIORITY NORMALISATION
  - Upper + trim → maps to LOW / MEDIUM / HIGH / CRITICAL
  - Empty / unrecognisable → NULL (not a default — we'd rather know it's missing)
  STATUS NORMALISATION
  - Maps common variants to OPEN / RESOLVED / IN_PROGRESS
  STATUS ↔ NOTES MISMATCH
  - status=OPEN with resolution_notes → status promoted to RESOLVED
  ASSIGNED_TO TYPE
  - Heuristic: if name contains known vendor keywords or is a company name → VENDOR
    else if matches "Joe (in-house)" / "maintenance team" patterns → INTERNAL
  COST / SLA_HOURS
  - Non-numeric → NULL (not 0 — 0 cost is a valid business value)
  QUARANTINE
  - Missing ticket_id → quarantine (untraceable)
  - Missing created_at that can't be parsed → quarantine (no timeline)
"""

import re
from datetime import datetime, timezone

import psycopg2.extras

from pipeline.utils import RunTracker, db_cursor, get_logger

logger = get_logger("silver")

# ── Date parsing ──────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d-%b-%Y %H:%M",     # 28-Feb-2024 17:03
    "%d-%b-%Y",
    "%Y-%m-%dT%H:%M:%SZ",
]


def _parse_date(raw: str | None):
    """Try multiple date formats, return timezone-aware datetime or None."""
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None  # unparseable


# ── Field normalisers ─────────────────────────────────────────────────────────

_PRIORITY_MAP = {
    "low":      "LOW",
    "medium":   "MEDIUM",
    "med":      "MEDIUM",
    "high":     "HIGH",
    "critical": "CRITICAL",
    "urgent":   "CRITICAL",
}

_STATUS_MAP = {
    "open":        "OPEN",
    "in progress": "IN_PROGRESS",
    "in-progress": "IN_PROGRESS",
    "resolved":    "RESOLVED",
    "closed":      "RESOLVED",
    "done":        "RESOLVED",
    "duplicate":   "RESOLVED",
}

_VENDOR_KEYWORDS = re.compile(
    r"\b(electric|services|plumbing|pest|hvac|contractors?|corp|inc|ltd|llc|co\.)\b",
    re.IGNORECASE,
)
_INTERNAL_KEYWORDS = re.compile(
    r"\b(in-house|inhouse|maintenance team|facilities team|internal)\b",
    re.IGNORECASE,
)


def _normalise_priority(raw: str | None) -> str | None:
    if not raw:
        return None
    return _PRIORITY_MAP.get(raw.strip().lower(), None)


def _normalise_status(raw: str | None) -> str | None:
    if not raw:
        return None
    return _STATUS_MAP.get(raw.strip().lower(), raw.strip().upper())


def _classify_assignee_type(assigned_to: str | None) -> str:
    if not assigned_to or not assigned_to.strip():
        return "UNKNOWN"
    if _INTERNAL_KEYWORDS.search(assigned_to):
        return "INTERNAL"
    if _VENDOR_KEYWORDS.search(assigned_to):
        return "VENDOR"
    # Single personal names (one or two words, no company indicators) → INTERNAL
    parts = assigned_to.strip().split()
    if len(parts) <= 2 and all(p[0].isupper() or p[0] in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" for p in parts if p):
        return "INTERNAL"
    return "UNKNOWN"


def _parse_numeric(raw: str | None):
    """Return float or None. Empty string → None, not 0."""
    if not raw or not raw.strip():
        return None
    try:
        return float(raw.replace(",", "").strip())
    except ValueError:
        return None


def _parse_int(raw: str | None):
    if not raw or not raw.strip():
        return None
    try:
        return int(float(raw.strip()))
    except ValueError:
        return None


# ── Row transformation ────────────────────────────────────────────────────────

def _transform_row(raw: dict) -> tuple[dict | None, str | None, str | None]:
    """
    Transform a single bronze row into a silver-ready dict.

    Returns:
        (silver_dict, quarantine_reason, quarantine_field)
        If quarantine_reason is set, silver_dict is None and the row should be quarantined.
    """
    ticket_id = raw.get("ticket_id", "").strip()
    if not ticket_id:
        return None, "Missing ticket_id — row is untraceable", "ticket_id"

    created_at = _parse_date(raw.get("created_at"))
    if created_at is None:
        return None, f"Unparseable created_at: '{raw.get('created_at')}' — no timeline", "created_at"

    resolved_at = _parse_date(raw.get("resolved_at"))

    # Business logic: resolved_at cannot precede created_at
    if resolved_at and resolved_at < created_at:
        logger.warning(
            "Ticket %s: resolved_at (%s) < created_at (%s) — nulling resolved_at",
            ticket_id, resolved_at, created_at,
        )
        resolved_at = None

    raw_status = _normalise_status(raw.get("status"))
    resolution_notes = raw.get("resolution_notes", "").strip() or None

    # Status ↔ notes mismatch: if notes exist and status is OPEN, promote to RESOLVED
    if raw_status == "OPEN" and resolution_notes:
        logger.debug("Ticket %s: OPEN with resolution_notes — promoting to RESOLVED", ticket_id)
        raw_status = "RESOLVED"

    cost = _parse_numeric(raw.get("cost"))
    sla_hours = _parse_int(raw.get("sla_hours"))
    assigned_to = raw.get("assigned_to", "").strip() or None

    # Derived: resolution time
    resolution_time_hrs = None
    if resolved_at and created_at:
        delta = (resolved_at - created_at).total_seconds() / 3600
        resolution_time_hrs = round(delta, 2)

    # Derived: SLA breach
    is_sla_breached = None
    if resolution_time_hrs is not None and sla_hours is not None:
        is_sla_breached = resolution_time_hrs > sla_hours

    silver = {
        "ticket_id":           ticket_id,
        "created_at":          created_at,
        "resolved_at":         resolved_at,
        "category":            raw.get("category", "").strip() or None,
        "priority":            _normalise_priority(raw.get("priority")),
        "status":              raw_status,
        "building":            raw.get("building", "").strip() or None,
        "description":         raw.get("description", "").strip() or None,
        "submitted_by":        raw.get("submitted_by", "").strip() or None,
        "assigned_to":         assigned_to,
        "assigned_to_type":    _classify_assignee_type(assigned_to),
        "resolution_notes":    resolution_notes,
        "cost":                cost,
        "sla_hours":           sla_hours,
        "resolution_time_hrs": resolution_time_hrs,
        "is_sla_breached":     is_sla_breached,
        "_bronze_row_id":      raw.get("_row_id"),
        "_source_file":        raw.get("_source_file"),
        "_ingested_at":        raw.get("_ingested_at"),
    }
    return silver, None, None


# ── Quarantine writer ─────────────────────────────────────────────────────────

def _quarantine_row(cur, raw: dict, reason: str, field: str | None) -> None:
    cur.execute(
        """
        INSERT INTO silver.quarantine
            (_bronze_row_id, _reason, _field,
             ticket_id, created_at, resolved_at, category, priority, status,
             building, description, submitted_by, assigned_to,
             resolution_notes, cost, sla_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            raw.get("_row_id"), reason, field,
            raw.get("ticket_id"), raw.get("created_at"), raw.get("resolved_at"),
            raw.get("category"), raw.get("priority"), raw.get("status"),
            raw.get("building"), raw.get("description"), raw.get("submitted_by"),
            raw.get("assigned_to"), raw.get("resolution_notes"),
            raw.get("cost"), raw.get("sla_hours"),
        ),
    )


# ── Silver writer ─────────────────────────────────────────────────────────────

def _upsert_silver(cur, row: dict) -> int:
    cur.execute(
        """
        INSERT INTO silver.tickets (
            ticket_id, created_at, resolved_at, category, priority, status,
            building, description, submitted_by, assigned_to, assigned_to_type,
            resolution_notes, cost, sla_hours, resolution_time_hrs, is_sla_breached,
            _bronze_row_id, _source_file, _ingested_at
        ) VALUES (
            %(ticket_id)s, %(created_at)s, %(resolved_at)s, %(category)s,
            %(priority)s, %(status)s, %(building)s, %(description)s,
            %(submitted_by)s, %(assigned_to)s, %(assigned_to_type)s,
            %(resolution_notes)s, %(cost)s, %(sla_hours)s,
            %(resolution_time_hrs)s, %(is_sla_breached)s,
            %(_bronze_row_id)s, %(_source_file)s, %(_ingested_at)s
        )
        ON CONFLICT (ticket_id) DO UPDATE SET
            resolved_at         = EXCLUDED.resolved_at,
            status              = EXCLUDED.status,
            resolution_notes    = EXCLUDED.resolution_notes,
            cost                = EXCLUDED.cost,
            resolution_time_hrs = EXCLUDED.resolution_time_hrs,
            is_sla_breached     = EXCLUDED.is_sla_breached,
            assigned_to_type    = EXCLUDED.assigned_to_type,
            _silver_processed_at = now()
        """,
        row,
    )
    return cur.rowcount


# ── Main ──────────────────────────────────────────────────────────────────────

def transform() -> dict:
    """
    Read all bronze rows and write to silver.tickets (or silver.quarantine).

    Returns:
        Summary dict: {inserted, updated, quarantined}
    """
    logger.info("Silver transformation starting…")

    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT _row_id, _source_file, _ingested_at,
                   ticket_id, created_at, resolved_at, category,
                   priority, status, building, description,
                   submitted_by, assigned_to, resolution_notes,
                   cost, sla_hours
            FROM bronze.raw_tickets
            ORDER BY _row_id
            """
        )
        bronze_rows = [dict(r) for r in cur.fetchall()]

    logger.info("Processing %d bronze rows…", len(bronze_rows))

    inserted = updated = quarantined = 0

    with RunTracker("SILVER") as rt:
        with db_cursor() as (_, cur):
            for raw in bronze_rows:
                silver, reason, field = _transform_row(raw)

                if reason:
                    _quarantine_row(cur, raw, reason, field)
                    quarantined += 1
                    logger.debug("Quarantined %s: %s", raw.get("ticket_id", "?"), reason)
                else:
                    rc = _upsert_silver(cur, silver)
                    if rc == 1:
                        inserted += 1
                    else:
                        updated += 1

        rt.rows_processed = inserted + updated
        rt.rows_rejected = quarantined
        rt.notes = f"inserted={inserted}, updated={updated}, quarantined={quarantined}"

    logger.info(
        "Silver complete — inserted=%d, updated=%d, quarantined=%d",
        inserted, updated, quarantined,
    )
    return {"inserted": inserted, "updated": updated, "quarantined": quarantined}