# Makefile — convenience shortcuts for the medallion pipeline
.PHONY: up down run run-fast run-bronze run-silver reset logs lineage status help

# ── Infrastructure ────────────────────────────────────────────
up:
	docker-compose up -d
	@echo "Waiting for Postgres to be ready..."
	@sleep 3
	@echo "Database is up. Run 'make run' to start the pipeline."

down:
	docker-compose down

reset:
	docker-compose down -v
	docker-compose up -d
	@sleep 3
	@echo "Database reset. All data cleared."

logs:
	docker-compose logs -f postgres

# ── Pipeline runs ─────────────────────────────────────────────
run:
	@echo "Running full pipeline (bronze → silver → gold + all agents)..."
	python run_pipeline.py

run-fast:
	@echo "Running pipeline WITHOUT agents (faster, no API calls)..."
	python run_pipeline.py --skip-agents

run-bronze:
	python run_pipeline.py --stage bronze --skip-agents

run-silver:
	python run_pipeline.py --stage silver --skip-agents

run-gold-only:
	python run_pipeline.py --skip-agents

run-sample:
	@echo "Running with semantic classification capped at 50 tickets (cost control)..."
	python run_pipeline.py --semantic-limit 50

# ── Inspection ────────────────────────────────────────────────
lineage:
	@echo "Printing lineage summary..."
	python -c "from pipeline.lineage import summary_report; summary_report()"

lineage-ticket:
	@read -p "Ticket ID (e.g. TKT-3960): " tid; \
	python -c "from pipeline.lineage import lineage_report; lineage_report('$$tid')"

catalog:
	@echo "Column sensitivity catalog:"
	python -c "
from pipeline.utils import db_cursor
with db_cursor() as (_, cur):
    cur.execute('SELECT column_name, sensitivity, pii_type, domain, description FROM meta.column_catalog ORDER BY sensitivity, column_name')
    rows = cur.fetchall()
    for r in rows:
        print(f'  {r[\"column_name\"]:<25} {r[\"sensitivity\"]:<15} {r[\"pii_type\"]:<12} {r[\"domain\"] or \"\"}')
"

status:
	@python -c "
from pipeline.utils import db_cursor
with db_cursor() as (_, cur):
    cur.execute('SELECT COUNT(*) AS n FROM bronze.raw_tickets')
    b = cur.fetchone()['n']
    cur.execute('SELECT COUNT(*) AS n FROM silver.tickets')
    s = cur.fetchone()['n']
    cur.execute('SELECT COUNT(*) AS n FROM silver.quarantine')
    q = cur.fetchone()['n']
    cur.execute('SELECT COUNT(*) AS n FROM silver.tickets WHERE classified_category IS NOT NULL')
    classified = cur.fetchone()['n']
    cur.execute('SELECT table_name, COUNT(*) AS n FROM gold.category_priority_summary GROUP BY table_name UNION ALL SELECT '\''building_health'\'', COUNT(*) FROM gold.building_health UNION ALL SELECT '\''assignee_perf'\'', COUNT(*) FROM gold.assignee_performance')
    gold_rows = {r['table_name']: r['n'] for r in cur.fetchall()}
print(f'Bronze rows:          {b}')
print(f'Silver rows:          {s}')
print(f'Quarantined:          {q}')
print(f'Classified (semantic):{classified}')
print(f'Gold — category:      {gold_rows.get(\"category_priority_summary\", 0)}')
print(f'Gold — buildings:     {gold_rows.get(\"building_health\", 0)}')
print(f'Gold — assignees:     {gold_rows.get(\"assignee_perf\", 0)}')
"

agent-log:
	@python -c "
from pipeline.utils import db_cursor
with db_cursor() as (_, cur):
    cur.execute('SELECT agent_name, prompt_summary, applied, notes, recorded_at FROM meta.agent_outputs ORDER BY output_id DESC LIMIT 20')
    for r in cur.fetchall():
        print(f'[{r[\"recorded_at\"].strftime(\"%H:%M:%S\")}] {r[\"agent_name\"]:<20} applied={r[\"applied\"]}  {r[\"notes\"] or \"\"}')
"

# ── Help ──────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Medallion Pipeline — available commands:"
	@echo ""
	@echo "  make up            Start PostgreSQL via docker-compose"
	@echo "  make down          Stop PostgreSQL"
	@echo "  make reset         Wipe and restart the database"
	@echo ""
	@echo "  make run           Full pipeline + all AI agents"
	@echo "  make run-fast      Full pipeline, no AI agents (offline/test)"
	@echo "  make run-sample    Full pipeline, semantic limit=50 (cost control)"
	@echo "  make run-silver    Bronze + silver only"
	@echo ""
	@echo "  make status        Row counts across all layers"
	@echo "  make lineage       Print lineage event summary"
	@echo "  make lineage-ticket  Trace a specific ticket_id"
	@echo "  make catalog       Print column sensitivity catalog"
	@echo "  make agent-log     Show recent agent outputs"
	@echo ""