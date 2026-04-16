-- ============================================================
-- Medallion Pipeline — Database Initialisation
-- Creates three schemas (bronze, silver, gold) and supporting
-- tables. Safe to re-run (idempotent).
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- ── BRONZE ──────────────────────────────────────────────────
-- Schema-on-read: every column is text. No interpretation,
-- no data loss. Pipeline metadata is appended at ingest time.
CREATE TABLE IF NOT EXISTS bronze.raw_tickets (
    _row_id       BIGSERIAL PRIMARY KEY,
    _source_file  TEXT        NOT NULL,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    _row_hash     TEXT        NOT NULL,  -- sha256 of raw row for dedup
    -- Raw columns — all text, exactly as they arrived
    ticket_id        TEXT,
    created_at       TEXT,
    resolved_at      TEXT,
    category         TEXT,
    priority         TEXT,
    status           TEXT,
    building         TEXT,
    description      TEXT,
    submitted_by     TEXT,
    assigned_to      TEXT,
    resolution_notes TEXT,
    cost             TEXT,
    sla_hours        TEXT
);

-- Dedup index: re-running ingestion won't create duplicate rows
CREATE UNIQUE INDEX IF NOT EXISTS bronze_row_hash_idx
    ON bronze.raw_tickets (_row_hash);

-- ── SILVER ──────────────────────────────────────────────────
-- Typed, validated, deduplicated. Bad rows go to quarantine.
CREATE TABLE IF NOT EXISTS silver.tickets (
    ticket_id           TEXT        PRIMARY KEY,
    created_at          TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    category            TEXT,
    priority            TEXT,           -- normalised: LOW / MEDIUM / HIGH / CRITICAL
    status              TEXT,           -- normalised: OPEN / RESOLVED / IN_PROGRESS
    building            TEXT,
    description         TEXT,
    submitted_by        TEXT,
    assigned_to         TEXT,
    assigned_to_type    TEXT,           -- INTERNAL / VENDOR / UNKNOWN
    resolution_notes    TEXT,
    cost                NUMERIC(12,2),
    sla_hours           INTEGER,
    -- Enrichment flags
    resolution_time_hrs NUMERIC(10,2),  -- derived: resolved_at - created_at
    is_sla_breached     BOOLEAN,        -- resolution_time_hrs > sla_hours
    -- Semantic classification columns (populated by Agent 3)
    classified_category TEXT,
    urgency_score       INTEGER,        -- 1-5 LLM-derived urgency
    extracted_location  TEXT,           -- room/floor extracted from description
    extracted_asset     TEXT,           -- asset/equipment mentioned
    -- Lineage
    _bronze_row_id      BIGINT          REFERENCES bronze.raw_tickets(_row_id),
    _source_file        TEXT,
    _ingested_at        TIMESTAMPTZ,
    _silver_processed_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _cleaning_version   TEXT           NOT NULL DEFAULT '1.0'
);

-- Quarantine table: rows that failed validation (not deleted, just isolated)
CREATE TABLE IF NOT EXISTS silver.quarantine (
    _quarantine_id   BIGSERIAL   PRIMARY KEY,
    _bronze_row_id   BIGINT      REFERENCES bronze.raw_tickets(_row_id),
    _quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    _reason          TEXT        NOT NULL,   -- human-readable failure reason
    _field           TEXT,                   -- which field triggered the issue
    -- Mirror of raw columns for inspection
    ticket_id        TEXT,
    created_at       TEXT,
    resolved_at      TEXT,
    category         TEXT,
    priority         TEXT,
    status           TEXT,
    building         TEXT,
    description      TEXT,
    submitted_by     TEXT,
    assigned_to      TEXT,
    resolution_notes TEXT,
    cost             TEXT,
    sla_hours        TEXT
);

-- ── GOLD ────────────────────────────────────────────────────
-- Materialised aggregations. Rebuilt on each pipeline run.

-- g1: Ticket volume + avg resolution time by category and priority
CREATE TABLE IF NOT EXISTS gold.category_priority_summary (
    category              TEXT,
    priority              TEXT,
    total_tickets         INTEGER,
    open_tickets          INTEGER,
    resolved_tickets      INTEGER,
    avg_resolution_hrs    NUMERIC(10,2),
    sla_breach_count      INTEGER,
    sla_breach_rate       NUMERIC(5,4),
    avg_cost              NUMERIC(12,2),
    total_cost            NUMERIC(12,2),
    _refreshed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- g2: Building-level operational health
CREATE TABLE IF NOT EXISTS gold.building_health (
    building              TEXT,
    total_tickets         INTEGER,
    open_tickets          INTEGER,
    critical_open         INTEGER,
    avg_resolution_hrs    NUMERIC(10,2),
    sla_breach_rate       NUMERIC(5,4),
    total_spend           NUMERIC(12,2),
    top_category          TEXT,
    _refreshed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- g3: Vendor / assignee performance
CREATE TABLE IF NOT EXISTS gold.assignee_performance (
    assigned_to           TEXT,
    assigned_to_type      TEXT,
    total_assigned        INTEGER,
    resolved_count        INTEGER,
    resolution_rate       NUMERIC(5,4),
    avg_resolution_hrs    NUMERIC(10,2),
    sla_breach_count      INTEGER,
    total_spend           NUMERIC(12,2),
    _refreshed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── META ────────────────────────────────────────────────────
-- Pipeline run log for observability
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          BIGSERIAL   PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    stage           TEXT        NOT NULL,  -- BRONZE / SILVER / GOLD / AGENT_*
    status          TEXT        NOT NULL,  -- RUNNING / SUCCESS / FAILED
    rows_processed  INTEGER,
    rows_rejected   INTEGER,
    notes           TEXT
);

-- Agent output audit log
CREATE TABLE IF NOT EXISTS meta.agent_outputs (
    output_id       BIGSERIAL   PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    agent_name      TEXT        NOT NULL,
    prompt_summary  TEXT,
    raw_output      TEXT,
    applied         BOOLEAN     NOT NULL DEFAULT FALSE,
    notes           TEXT
);

-- ── LINEAGE ─────────────────────────────────────────────────
-- Append-only event log: tracks every entity through every stage
CREATE TABLE IF NOT EXISTS meta.lineage_events (
    event_id        BIGSERIAL   PRIMARY KEY,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    entity_id       TEXT        NOT NULL,
    entity_type     TEXT        NOT NULL,  -- TICKET / GOLD_TABLE
    stage           TEXT        NOT NULL,  -- BRONZE / SILVER / QUARANTINE / GOLD / AGENT_*
    operation       TEXT        NOT NULL,  -- INSERTED / UPDATED / CLASSIFIED / QUARANTINED / MATERIALISED
    source_ref      TEXT,
    target_ref      TEXT,
    agent_output_id BIGINT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS lineage_entity_idx ON meta.lineage_events (entity_id, stage);

-- ── COLUMN CATALOG ───────────────────────────────────────────
-- Auto-populated by the tagger agent at bronze landing
CREATE TABLE IF NOT EXISTS meta.column_catalog (
    catalog_id      BIGSERIAL   PRIMARY KEY,
    catalogued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_name     TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    column_name     TEXT        NOT NULL,
    sensitivity     TEXT,       -- PUBLIC / INTERNAL / CONFIDENTIAL / PII
    pii_type        TEXT,       -- NAME / EMAIL / PHONE / ID / LOCATION / FINANCIAL / NONE
    domain          TEXT,
    description     TEXT,
    sample_values   TEXT,       -- JSON array
    agent_version   TEXT        NOT NULL DEFAULT '1.0',
    UNIQUE (schema_name, table_name, column_name)
);