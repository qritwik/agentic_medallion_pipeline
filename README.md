# Agentic Medallion Pipeline

A bronze → silver → gold data pipeline for operational support tickets, accelerated by four AI agents built on Claude (Anthropic). PostgreSQL is used as the storage backend, orchestrated via Docker.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         RAW SOURCE                                   │
│                   data/raw_tickets.csv                               │
│            (TSV, ~10k rows, messy — not modified)                    │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ ingest as-is
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                                    │
│               schema: bronze.raw_tickets                             │
│                                                                      │
│  • All columns stored as TEXT — schema-on-read, zero data loss       │
│  • Lineage appended: _source_file, _ingested_at, _row_hash           │
│  • Dedup via UNIQUE INDEX on _row_hash — safe to re-ingest           │
└──────────┬───────────────────────────┬───────────────────────────────┘
           │                           │
           ▼                           ▼
  ┌─────────────────┐       ┌──────────────────────┐
  │   AGENT 1       │       │      AGENT 2          │
  │ Schema          │       │   Data Quality        │
  │ Inference &     │       │   Profiling +         │
  │ Evolution       │       │   Rule Generation     │
  │                 │       │                       │
  │ → Proposes DDL  │       │ → Null rates,         │
  │   + typed       │       │   cardinality,        │
  │   silver schema │       │   outliers, BL        │
  │ → Detects drift │       │   violations          │
  │   on re-run     │       │ → Cleaning rules      │
  │   + ALTER SQL   │       │   with reasoning      │
  └────────┬────────┘       └──────────┬────────────┘
           │                           │
           └─────────────┬─────────────┘
                         │ guides transformation
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                                    │
│               schema: silver.tickets                                 │
│               schema: silver.quarantine                              │
│                                                                      │
│  • Typed columns (TIMESTAMPTZ, NUMERIC, INTEGER, BOOLEAN)            │
│  • Normalised: priority (LOW/MEDIUM/HIGH/CRITICAL), status           │
│  • Multiple date format handling (ISO, DD-Mon-YYYY, plain date)      │
│  • Impossible dates resolved (resolved < created → NULL resolved_at) │
│  • Status/notes mismatch corrected (OPEN + notes → RESOLVED)         │
│  • Assignee typed: INTERNAL / VENDOR / UNKNOWN                       │
│  • Derived: resolution_time_hrs, is_sla_breached                    │
│  • Bad rows → quarantine table (not deleted)                         │
│  • Upsert on ticket_id — idempotent                                  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
  ┌─────────────────┐           ┌──────────────────────┐
  │   AGENT 3       │           │      AGENT 4          │
  │ Semantic        │           │  Gold Layer Design    │
  │ Classification  │           │                       │
  │                 │           │ → Reads silver schema │
  │ → Reads         │           │   + domain context    │
  │   description   │           │ → Proposes 3 gold     │
  │   + raw category│           │   aggregations with   │
  │ → Normalises    │           │   business rationale  │
  │   category,     │           │ → Generates + runs    │
  │   extracts      │           │   the SQL             │
  │   location +    │           └──────────┬────────────┘
  │   asset, scores │                      │
  │   urgency 1–5   │                      │
  │ → Batched 20/   │                      │
  │   call, skips   │                      │
  │   already done  │                      │
  └────────┬────────┘                      │
           │ enriches silver               │
           └───────────────┬───────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                                     │
│                                                                      │
│  gold.category_priority_summary                                      │
│    Ticket volume, SLA breach rate, avg cost by category + priority   │
│                                                                      │
│  gold.building_health                                                │
│    Per-building: open tickets, critical open count, spend, breach %  │
│                                                                      │
│  gold.assignee_performance                                           │
│    Per vendor/assignee: resolution rate, avg hours, total spend      │
└──────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        META LAYER                                    │
│                                                                      │
│  meta.pipeline_runs   — stage-level audit log (start, end, counts)  │
│  meta.agent_outputs   — every LLM call, prompt summary, raw output  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## How to Run

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- An Anthropic API key

### 1. Clone and configure

```bash
git clone <repo>
cd medallion-pipeline

cp .env.example .env
# Edit .env — set ANTHROPIC_API_KEY and confirm POSTGRES_* defaults
```

### 2. Start the database

```bash
docker-compose up -d
```

This starts PostgreSQL and runs `sql/init.sql` to create all schemas and tables.

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the full pipeline

```bash
python run_pipeline.py
# or equivalently:
make run
```

That's it. One command runs bronze ingestion → metadata tagging → agent profiling → silver transformation → semantic classification → gold materialisation → lineage backfill.

### Additional run modes

```bash
# Faster run without any agent API calls (uses pre-defined rules + SQL)
make run-fast

# Bronze + silver only
make run-silver

# Classify only the first 100 tickets (cost control during testing)
python run_pipeline.py --semantic-limit 100

# Inspect pipeline state
make status          # row counts across all layers
make lineage         # lineage event summary
make lineage-ticket  # trace a specific ticket_id
make catalog         # column PII/sensitivity catalog
make agent-log       # recent agent LLM outputs
```

---

## Medallion Design Decisions

### Layer Separation

Each layer lives in its own PostgreSQL **schema** (`bronze`, `silver`, `gold`, `meta`). This gives clear ownership, easy permission scoping, and allows querying across layers with explicit schema-qualified names when needed for lineage verification.

### Idempotency


| Layer  | Mechanism                                                                                     |
| ------ | --------------------------------------------------------------------------------------------- |
| Bronze | `ON CONFLICT (_row_hash) DO NOTHING` — re-ingesting the same file inserts zero rows           |
| Silver | `ON CONFLICT (ticket_id) DO UPDATE` — re-running updates mutable fields (status, resolved_at) |
| Gold   | `TRUNCATE` + `INSERT` — full replacement, always consistent with current silver               |


### Quarantine Over Delete

Bad rows (missing `ticket_id`, unparseable `created_at`) go to `silver.quarantine` rather than being dropped. This preserves the data for investigation and means re-running after fixing source data will correctly route previously quarantined rows to silver.

### Cleaning Rules Applied in Silver


| Issue                                         | Decision                           | Rationale                                                   |
| --------------------------------------------- | ---------------------------------- | ----------------------------------------------------------- |
| Mixed date formats                            | COALESCE across 7 format patterns  | Don't reject data just because the format is non-standard   |
| resolved_at < created_at                      | NULL the resolved_at, keep the row | Clearly erroneous, but the ticket itself is real            |
| OPEN status + resolution_notes                | Promote status to RESOLVED         | Notes are a stronger signal than the status field           |
| Mixed priority casing (LOW, Medium)           | UPPER(TRIM()) → enum               | Prevents GROUP BY from splitting the same priority          |
| Category variants (Fire Safety / Fire/Safety) | Passed to semantic agent           | LLM handles this better than regex                          |
| Empty cost / sla_hours                        | NULL, not 0                        | Zero cost is a valid real-world value; NULL signals missing |
| Non-numeric cost strings                      | NULL                               | Same reasoning                                              |


---

## Agent Assessment

### Agent 1 — Schema Inference & Evolution

**What it does:** Samples 50 raw bronze rows, sends them to Claude with instructions to infer the correct PostgreSQL type for each column, explain the rationale, provide a confidence score, and generate transformation SQL. On subsequent runs, compares the proposed schema to the existing silver schema and generates `ALTER TABLE` migration SQL for any new columns or type changes.

**Sample input (truncated):**

```json
[
  {"ticket_id": "TKT-3960", "created_at": "2024-10-24 14:32:10", "resolved_at": "12-Feb-2025 03:21", "cost": "", "sla_hours": "8"},
  {"ticket_id": "TKT-5149", "created_at": "2024-06-16T04:31:56", "resolved_at": "2024-03-14 00:39:27", "cost": "4569.86", ...}
]
```

**Sample output (truncated):**

```json
{
  "columns": [
    {"name": "ticket_id", "pg_type": "TEXT", "nullable": false, "_confidence": 0.99, "rationale": "Natural identifier, always present"},
    {"name": "created_at", "pg_type": "TIMESTAMPTZ", "nullable": false, "_confidence": 0.85, "rationale": "Multiple formats present: ISO 8601 and DD-Mon-YYYY"},
    {"name": "cost", "pg_type": "NUMERIC(12,2)", "nullable": true, "_confidence": 0.95, "rationale": "Mostly empty but when present is a decimal monetary value"}
  ],
  "caveats": ["resolved_at precedes created_at for TKT-5149 — date integrity rule needed in silver"]
}
```

**Honest take:** Saved ~45 minutes of DDL writing and format research. The mixed-date-format detection was particularly useful — it surfaced the `12-Feb-2025 03:21` format that I might have missed in a manual review. The drift detection is the real long-term value: in a production system where the source occasionally adds columns, this agent can catch it before the silver pipeline silently drops data. Confidence: I trust the type proposals. I don't trust the generated `transformation_sql` without reviewing it — it's a good first draft, not production-ready SQL.

---

### Agent 2 — Data Quality Agent

**What it does:** Profiles the full bronze table (null rates, cardinality, date parse failure rates, numeric ranges) by running SQL aggregations in the database, then sends the profile + 20 sample rows to Claude. Claude returns structured cleaning rules with `severity`, `action`, `transform` (SQL or Python), and critically — the **why_it_matters** explanation for each rule.

**Sample output (rule excerpt):**

```json
{
  "id": "DQ003",
  "field": "resolved_at",
  "issue": "resolved_at predates created_at in ~2% of rows",
  "severity": "HIGH",
  "why_it_matters": "SLA breach calculations and resolution time metrics are corrupted — avg_resolution_hrs becomes negative, making dashboards meaningless",
  "action": "DERIVE",
  "transform": "[SQL] CASE WHEN resolved_at < created_at THEN NULL ELSE resolved_at END",
  "quarantine_if": null
}
```

**Honest take:** High value on the first run. The agent correctly identified all five major issues in the dataset, including the status/notes mismatch that I hadn't explicitly enumerated. The `why_it_matters` field is genuinely useful — it becomes the documentation for the cleaning decision, not just a flag. Less useful on subsequent runs against the same data — the rules don't change, so this becomes a re-verification step more than a discovery step. At production scale, the value shifts from rule discovery to anomaly detection on new incremental data (e.g. "this week's batch has 15% null priority — that's up from 2% last week").

---

### Agent 3 — Semantic Classification Agent

**What it does:** Reads all silver tickets where `classified_category IS NULL`, batches them 20 at a time, and sends each batch to Claude for:

- `classified_category` — normalised to a canonical list of 11 categories
- `urgency_score` — 1 (routine) to 5 (safety-critical)
- `extracted_location` — specific room, floor, stairwell from the description
- `extracted_asset` — physical equipment mentioned

Results are written back to `silver.tickets`. Already-classified rows are skipped (idempotent). A 0.5s pause between batches respects rate limits.

**Sample input batch (1 of 20 tickets shown):**

```json
{"ticket_id": "TKT-3960", "raw_category": "power issue", "description": "Breaker keeps tripping in server room 391. Critical — affects production systems."}
```

**Sample output:**

```json
{"ticket_id": "TKT-3960", "classified_category": "Electrical", "urgency_score": 5, "extracted_location": "server room 391", "extracted_asset": "circuit breaker"}
```

**Honest take:** This is where AI genuinely earns its place in the pipeline. Normalising `"power issue"`, `"A/C"`, `"Fire/Safety"`, `"Fire Safety"` and pure-description rows like `"cold"` or `"need more outlets in conf room 387"` into a consistent 11-category taxonomy would take hours of regex work and would still miss edge cases. The urgency score adds a dimension that didn't exist at all in the raw data. The batching approach keeps costs manageable (~500 API calls for 10k rows at batch=20). **Where I'd override:** any urgency score for compliance or safety tickets — I'd want a human review step before using urgency_score to auto-escalate tickets.

**Scale note:** At 1M rows, I'd add a description-hash cache (Redis or a DB lookup table) before calling the API — duplicate descriptions are common in support systems and don't need re-classification.

---

### Agent 4 — Gold Layer Design Agent

**What it does:** Introspects the silver schema (column names, types, null rates, cardinality) from the database, sends it alongside a plain-English business domain description to Claude, and asks for 3 gold model proposals with business justification and complete runnable SQL.

**Sample proposal output (truncated):**

```json
{
  "table_name": "building_health",
  "business_value": "Enables operations managers to spot buildings with chronic maintenance backlogs or safety risks before they escalate, and prioritise resource deployment accordingly",
  "refresh_strategy": "FULL_REPLACE",
  "sql": "TRUNCATE gold.building_health; INSERT INTO gold.building_health (...) SELECT ...",
  "when_to_override": "Override the top_category calculation if buildings span multiple asset types that should be weighted by cost, not ticket count"
}
```

**Honest take:** Saved ~30 minutes of schema-reading and SQL drafting. The agent independently converged on the same three gold models I had already planned (category summary, building health, assignee performance), which was a good sanity check. The SQL quality was 90% production-ready — needed minor fixes: `NULLIF` for division-by-zero safety in rate calculations, explicit `::NUMERIC` casts for `ROUND()`, and a `HAVING COUNT(*) >= 2` filter on assignee performance to exclude one-off assignees. **When to trust:** aggregation structure, JOIN logic, GROUP BY choices. **When to override:** any metric definition that has a business-specific threshold (e.g. "SLA breached" definition, cost allocation rules, vendor tier classification).

---

## Bonus Features

### Metadata Auto-Tagging at Bronze Landing (`agents/tagger_agent.py`)

Runs immediately after bronze ingestion. Sends column names + sample values to Claude, which classifies each column for:

- **sensitivity**: PUBLIC / INTERNAL / CONFIDENTIAL / PII
- **pii_type**: NAME / EMAIL / PHONE / FINANCIAL / NONE / etc.
- **domain**: identity / operations / financial / temporal / system
- **description**: plain-English one-liner

Results land in `meta.column_catalog`. On re-runs, existing tags are upserted (not duplicated).

**Sample output:**

```
column_name               sensitivity     pii_type     domain
submitted_by              PII             NAME         identity
assigned_to               PII             NAME         identity
cost                      CONFIDENTIAL    NONE         financial
building                  INTERNAL        NONE         operations
ticket_id                 PUBLIC          NONE         operations
_row_hash                 INTERNAL        NONE         system
```

**Honest take:** This is high-value for compliance teams and costs almost nothing (one API call at landing). The classification is conservative — it errs toward PII — which is the right default. In production you'd feed this into your data masking and access control policy automatically.

---

### End-to-End Lineage Tracking (`pipeline/lineage.py`)

Every ticket is traceable from source CSV through bronze → silver → semantic enrichment → gold contribution. The `meta.lineage_events` table is an append-only event log.

```bash
make lineage-ticket
# Enter: TKT-3960

# Output:
# [2024-10-24 14:32:10] BRONZE           INSERTED        raw_tickets.csv
# [2024-10-24 14:32:11] SILVER           INSERTED
# [2024-10-24 14:32:12] AGENT_SEMANTIC   CLASSIFIED      classified_category=Electrical, urgency=5
# [2024-10-24 14:32:13] GOLD             MATERIALISED    gold.category_priority_summary
```

**Honest take:** Lightweight compared to full OpenLineage/Marquez, but demonstrates the principle clearly. In production I'd emit OpenLineage events and use Marquez as the metadata store — the schema here maps directly to that model. (1M+ rows, daily incremental loads)

### Storage

- Move from PostgreSQL to a columnar store for gold (Redshift, BigQuery, DuckDB on S3, or Snowflake). PostgreSQL handles silver fine at this scale but gold aggregations on 1M+ rows benefit from columnar scan optimisation.
- Bronze becomes an object store (S3/GCS) with Parquet files partitioned by `ingested_date`. PostgreSQL bronze table becomes a metadata registry, not the data store itself.

### Incremental Processing

- Bronze: ingest only new files (watermark on `_ingested_at` or S3 event triggers)
- Silver: CDC-style processing — only transform rows where `_bronze_row_id > last_processed_id`
- Gold: switch from `TRUNCATE + INSERT` to incremental upserts or partitioned table refreshes

### Agent Cost & Throughput

- **Schema agent**: run only when new source files arrive or column counts change — not every pipeline run. Cache the output.
- **Quality agent**: profile only the *new* rows each day, not the full table. Alert on metric drift vs. baseline.
- **Semantic agent**: add a description-hash deduplication cache (Redis or a `silver.classification_cache` table). At 1M tickets, a large fraction will be repeated descriptions — don't re-call the API for "smoke detector beeping" the 500th time.
- **Gold agent**: run schema introspection + proposal generation once (on schema change), then just re-execute the saved SQL on each refresh. Treat the agent output as a migration artifact, not a live call.

### Orchestration

- Replace the single script with a DAG (Airflow, Prefect, or Dagster). Each stage becomes a task with retry logic, dependency tracking, and alerting.
- Semantic classification becomes an async worker pool (Celery, Cloud Run jobs) pulling from a queue, not a sequential batch loop.

### Observability

- `meta.pipeline_runs` becomes a time-series metrics sink (Prometheus/Datadog). Track per-batch error rates, p95 processing time, quarantine rate as a data quality SLO.

---

## Project Structure

```
medallion-pipeline/
├── docker-compose.yml          # PostgreSQL service
├── .env.example                # Config template
├── requirements.txt
├── run_pipeline.py             # Single entrypoint
├── data/
│   └── raw_tickets.csv         # Source data
├── pipeline/
│   ├── bronze.py               # Raw ingestion
│   ├── silver.py               # Typed transformation + quarantine
│   ├── gold.py                 # Pre-defined gold SQL (fallback)
│   └── utils.py                # DB, logging, run tracking
├── agents/
│   ├── schema_agent.py         # Agent 1: Schema inference & drift
│   ├── quality_agent.py        # Agent 2: Data quality profiling
│   ├── semantic_agent.py       # Agent 3: Semantic classification
│   └── gold_agent.py           # Agent 4: Gold model design
└── sql/
    └── init.sql                # All DDL (idempotent)
```

---

## Design Tradeoffs


| Decision                                | Alternative                  | Why this choice                                                                                                                       |
| --------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL for all layers               | Separate bronze object store | Simplicity for take-home; object store is the right call at scale                                                                     |
| Schema-on-read bronze (all TEXT)        | Typed bronze                 | Guarantees zero data loss on ingest; types belong in silver                                                                           |
| Quarantine not delete                   | Delete bad rows              | Preserves data for investigation; quarantine rate is a quality metric                                                                 |
| Batch semantic classification (20/call) | One-by-one API calls         | ~20x fewer API calls; latency is acceptable for batch pipelines                                                                       |
| Pre-defined gold SQL fallback           | Agent-only gold              | Pipeline works without API access; agent is an accelerator, not a dependency                                                          |
| 4 separate agents                       | Single "pipeline agent"      | Each agent has a different failure mode and different trust level; separation makes it easier to skip, replace, or audit individually |


