"""SQLite metadata store: one row per job and one row per tile."""
import contextlib
import sqlite3
import threading
import time

from common import config

_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    filename    TEXT,
    operation   TEXT,
    width       INTEGER,
    height      INTEGER,
    tile_count  INTEGER,
    tiles_done  INTEGER DEFAULT 0,
    status      TEXT,          -- processing | stitching | done | failed
    error       TEXT,
    created_at  REAL,
    finished_at REAL,
    split_ms    REAL,
    stitch_ms   REAL
);
CREATE TABLE IF NOT EXISTS tiles (
    job_id   TEXT,
    idx      INTEGER,
    x INTEGER, y INTEGER, w INTEGER, h INTEGER,
    partition_no INTEGER,
    status   TEXT,             -- queued | done
    worker_id TEXT,
    proc_ms  REAL,
    done_at  REAL,
    PRIMARY KEY (job_id, idx)
);
"""


@contextlib.contextmanager
def conn():
    with _lock:
        c = sqlite3.connect(config.DB_PATH, timeout=30)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()


def init():
    config.STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    with conn() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript(SCHEMA)


def create_job(job_id, filename, op, width, height, grid, split_ms, partitions):
    with conn() as c:
        c.execute(
            "INSERT INTO jobs (id, filename, operation, width, height, tile_count, status, created_at, split_ms)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (job_id, filename, op, width, height, len(grid), "processing", time.time(), split_ms))
        c.executemany(
            "INSERT INTO tiles (job_id, idx, x, y, w, h, partition_no, status) VALUES (?,?,?,?,?,?,?,'queued')",
            [(job_id, i, x, y, w, h, i % partitions) for i, (x, y, w, h) in enumerate(grid)])


def get_job(job_id):
    with conn() as c:
        r = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(r) if r else None


def list_jobs(limit=30):
    with conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))]


def get_tiles(job_id):
    with conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM tiles WHERE job_id=? ORDER BY idx", (job_id,))]


def mark_tile_done(job_id, idx, worker_id, proc_ms):
    """Idempotent. Returns (is_new, should_stitch)."""
    with conn() as c:
        cur = c.execute(
            "UPDATE tiles SET status='done', worker_id=?, proc_ms=?, done_at=?"
            " WHERE job_id=? AND idx=? AND status!='done'",
            (worker_id, proc_ms, time.time(), job_id, idx))
        if cur.rowcount == 0:
            return False, False          # duplicate delivery (at-least-once) - ignore
        done = c.execute("SELECT COUNT(*) FROM tiles WHERE job_id=? AND status='done'",
                         (job_id,)).fetchone()[0]
        c.execute("UPDATE jobs SET tiles_done=? WHERE id=?", (done, job_id))
        total = c.execute("SELECT tile_count FROM jobs WHERE id=?", (job_id,)).fetchone()[0]
        should = False
        if done >= total:
            cur = c.execute("UPDATE jobs SET status='stitching' WHERE id=? AND status='processing'",
                            (job_id,))
            should = cur.rowcount == 1
        return True, should


def finish_job(job_id, stitch_ms):
    with conn() as c:
        c.execute("UPDATE jobs SET status='done', finished_at=?, stitch_ms=? WHERE id=?",
                  (time.time(), stitch_ms, job_id))


def fail_job(job_id, error):
    with conn() as c:
        c.execute("UPDATE jobs SET status='failed', error=?, finished_at=? WHERE id=?",
                  (str(error)[:500], time.time(), job_id))


def worker_stats():
    with conn() as c:
        rows = c.execute(
            "SELECT worker_id, COUNT(*) AS tiles, AVG(proc_ms) AS avg_ms FROM tiles"
            " WHERE status='done' GROUP BY worker_id")
        return {r["worker_id"]: {"tiles": r["tiles"], "avg_ms": round(r["avg_ms"] or 0, 1)}
                for r in rows}


def summary():
    with conn() as c:
        rows = c.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
        by = {r["status"]: r["n"] for r in rows}
        tiles = c.execute("SELECT COUNT(*) FROM tiles WHERE status='done'").fetchone()[0]
    return {"jobs_total": sum(by.values()),
            "jobs_processing": by.get("processing", 0) + by.get("stitching", 0),
            "jobs_done": by.get("done", 0),
            "jobs_failed": by.get("failed", 0),
            "tiles_processed_total": tiles}
