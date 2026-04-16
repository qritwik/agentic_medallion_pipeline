"""
pipeline/gold.py
Gold layer: materialise business-ready aggregations.

The gold_agent proposes and executes SQL dynamically. This module provides:
  1. Pre-defined fallback SQL for the three standard gold tables (in case the
     agent is skipped or fails)
  2. A refresh() function that re-materialises all gold tables idempotently

The three gold models and why we chose them:
  g1. category_priority_summary
      → Operations managers' primary view: where are tickets clustered, what's
        the resolution rate, and are we breaching SLAs? Combines category +
        priority for max drill-down flexibility.

  g2. building_health
      → Enables building-by-building comparison. Facilities directors use this
        to decide where to deploy maintenance resources. The "critical_open"
        column surfaces safety risks quickly.

  g3. assignee_performance
      → Finance and vendor management: which vendors are resolving tickets and
        at what cost? Internal vs external split helps benchmark in-house team
        vs contracted vendors.
"""

from pipeline.utils import RunTracker, db_cursor, get_logger

logger = get_logger("gold")


# ── SQL definitions ───────────────────────────────────────────────────────────

GOLD_SQL: dict[str, str] = {
    "category_priority_summary": """
        TRUNCATE gold.category_priority_summary;
        INSERT INTO gold.category_priority_summary (
            category, priority, total_tickets, open_tickets, resolved_tickets,
            avg_resolution_hrs, sla_breach_count, sla_breach_rate,
            avg_cost, total_cost
        )
        SELECT
            COALESCE(classified_category, category, 'Uncategorised') AS category,
            COALESCE(priority, 'UNKNOWN')                             AS priority,
            COUNT(*)                                                  AS total_tickets,
            COUNT(*) FILTER (WHERE status = 'OPEN')                  AS open_tickets,
            COUNT(*) FILTER (WHERE status = 'RESOLVED')              AS resolved_tickets,
            ROUND(AVG(resolution_time_hrs)::NUMERIC, 2)              AS avg_resolution_hrs,
            COUNT(*) FILTER (WHERE is_sla_breached)                  AS sla_breach_count,
            ROUND(
                COUNT(*) FILTER (WHERE is_sla_breached)::NUMERIC
                / NULLIF(COUNT(*) FILTER (WHERE is_sla_breached IS NOT NULL), 0),
                4
            )                                                         AS sla_breach_rate,
            ROUND(AVG(cost)::NUMERIC, 2)                             AS avg_cost,
            ROUND(SUM(cost)::NUMERIC, 2)                             AS total_cost
        FROM silver.tickets
        GROUP BY 1, 2
        ORDER BY total_tickets DESC;
    """,

    "building_health": """
        TRUNCATE gold.building_health;
        INSERT INTO gold.building_health (
            building, total_tickets, open_tickets, critical_open,
            avg_resolution_hrs, sla_breach_rate, total_spend, top_category
        )
        WITH base AS (
            SELECT
                COALESCE(building, 'Unknown')                          AS building,
                COUNT(*)                                               AS total_tickets,
                COUNT(*) FILTER (WHERE status = 'OPEN')               AS open_tickets,
                COUNT(*) FILTER (
                    WHERE status = 'OPEN' AND priority = 'CRITICAL'
                )                                                      AS critical_open,
                ROUND(AVG(resolution_time_hrs)::NUMERIC, 2)           AS avg_resolution_hrs,
                ROUND(
                    COUNT(*) FILTER (WHERE is_sla_breached)::NUMERIC
                    / NULLIF(COUNT(*) FILTER (WHERE is_sla_breached IS NOT NULL), 0),
                    4
                )                                                      AS sla_breach_rate,
                ROUND(SUM(cost)::NUMERIC, 2)                          AS total_spend
            FROM silver.tickets
            GROUP BY 1
        ),
        top_cats AS (
            SELECT
                COALESCE(building, 'Unknown')                          AS building,
                COALESCE(classified_category, category, 'Uncategorised') AS category,
                COUNT(*)                                               AS n,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(building, 'Unknown')
                    ORDER BY COUNT(*) DESC
                )                                                      AS rn
            FROM silver.tickets
            GROUP BY 1, 2
        )
        SELECT
            b.building, b.total_tickets, b.open_tickets, b.critical_open,
            b.avg_resolution_hrs, b.sla_breach_rate, b.total_spend,
            tc.category AS top_category
        FROM base b
        LEFT JOIN top_cats tc ON tc.building = b.building AND tc.rn = 1
        ORDER BY b.total_tickets DESC;
    """,

    "assignee_performance": """
        TRUNCATE gold.assignee_performance;
        INSERT INTO gold.assignee_performance (
            assigned_to, assigned_to_type, total_assigned, resolved_count,
            resolution_rate, avg_resolution_hrs, sla_breach_count, total_spend
        )
        SELECT
            COALESCE(assigned_to, 'Unassigned')                       AS assigned_to,
            COALESCE(assigned_to_type, 'UNKNOWN')                     AS assigned_to_type,
            COUNT(*)                                                   AS total_assigned,
            COUNT(*) FILTER (WHERE status = 'RESOLVED')               AS resolved_count,
            ROUND(
                COUNT(*) FILTER (WHERE status = 'RESOLVED')::NUMERIC
                / NULLIF(COUNT(*), 0),
                4
            )                                                          AS resolution_rate,
            ROUND(AVG(resolution_time_hrs)::NUMERIC, 2)               AS avg_resolution_hrs,
            COUNT(*) FILTER (WHERE is_sla_breached)                   AS sla_breach_count,
            ROUND(SUM(cost)::NUMERIC, 2)                              AS total_spend
        FROM silver.tickets
        GROUP BY 1, 2
        HAVING COUNT(*) >= 2   -- exclude one-off assignees from performance view
        ORDER BY total_assigned DESC;
    """,
}


# ── Materialisation ───────────────────────────────────────────────────────────

def refresh() -> dict:
    """
    Materialise all three gold tables using pre-defined SQL.
    Safe to re-run (TRUNCATE + INSERT pattern).

    Returns:
        {table_name: row_count} for each gold table.
    """
    logger.info("Gold refresh starting — %d tables", len(GOLD_SQL))
    results = {}

    with RunTracker("GOLD") as rt:
        total_rows = 0
        with db_cursor() as (_, cur):
            for table, sql in GOLD_SQL.items():
                logger.info("Materialising gold.%s…", table)
                # Execute the multi-statement SQL (TRUNCATE + INSERT)
                for statement in sql.strip().split(";"):
                    stmt = statement.strip()
                    if stmt:
                        cur.execute(stmt)

                cur.execute(f"SELECT COUNT(*) AS n FROM gold.{table}")
                n = cur.fetchone()["n"]
                results[table] = n
                total_rows += n
                logger.info("gold.%s — %d rows materialised", table, n)

        rt.rows_processed = total_rows
        rt.notes = str(results)

    logger.info("Gold refresh complete: %s", results)
    return results