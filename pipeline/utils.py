"""
pipeline/utils.py
Shared utilities: DB connection, structured logging, pipeline run tracking.
"""

import hashlib
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import colorlog
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


# ── Logging ──────────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a colour-coded logger scoped to the given name."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)-8s] %(name)s%(reset)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        },
    ))
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ── Database ──────────────────────────────────────────────────────────────────

def get_connection() -> psycopg2.extensions.connection:
    """Open a new PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "medallion"),
        user=os.getenv("POSTGRES_USER", "medallion"),
        password=os.getenv("POSTGRES_PASSWORD", "medallion"),
    )


@contextmanager
def db_cursor(autocommit: bool = False):
    """
    Context manager yielding (conn, cursor).
    Commits on clean exit, rolls back on exception.
    """
    conn = get_connection()
    conn.autocommit = autocommit
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield conn, cur
        if not autocommit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Hashing ───────────────────────────────────────────────────────────────────

def row_hash(row: dict) -> str:
    """SHA-256 hash of the raw row values (order-stable, null-safe)."""
    normalised = "|".join(str(row.get(k, "")) for k in sorted(row.keys()))
    return hashlib.sha256(normalised.encode()).hexdigest()


# ── Pipeline run tracking ─────────────────────────────────────────────────────

class RunTracker:
    """
    Records pipeline stage start/finish in meta.pipeline_runs.
    Use as a context manager:

        with RunTracker("BRONZE") as rt:
            rt.rows_processed = 9999
    """

    def __init__(self, stage: str):
        self.stage = stage
        self.run_id: int | None = None
        self.rows_processed: int = 0
        self.rows_rejected: int = 0
        self.notes: str = ""
        self._logger = get_logger("RunTracker")

    def __enter__(self):
        with db_cursor() as (_, cur):
            cur.execute(
                """
                INSERT INTO meta.pipeline_runs (stage, status)
                VALUES (%s, 'RUNNING')
                RETURNING run_id
                """,
                (self.stage,),
            )
            self.run_id = cur.fetchone()["run_id"]
        self._logger.info("Stage %s started (run_id=%s)", self.stage, self.run_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "FAILED" if exc_type else "SUCCESS"
        with db_cursor() as (_, cur):
            cur.execute(
                """
                UPDATE meta.pipeline_runs
                SET finished_at    = now(),
                    status         = %s,
                    rows_processed = %s,
                    rows_rejected  = %s,
                    notes          = %s
                WHERE run_id = %s
                """,
                (status, self.rows_processed, self.rows_rejected,
                 self.notes or exc_val and str(exc_val), self.run_id),
            )
        self._logger.info(
            "Stage %s finished — status=%s, processed=%d, rejected=%d",
            self.stage, status, self.rows_processed, self.rows_rejected,
        )
        return False  # don't suppress exceptions


def log_agent_output(agent_name: str, prompt_summary: str,
                     raw_output: str, applied: bool = False, notes: str = "") -> int:
    """Persist an agent's raw LLM output to meta.agent_outputs for auditability."""
    with db_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO meta.agent_outputs
                (agent_name, prompt_summary, raw_output, applied, notes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING output_id
            """,
            (agent_name, prompt_summary, raw_output, applied, notes),
        )
        return cur.fetchone()["output_id"]