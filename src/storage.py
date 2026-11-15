"""
Persistent Write-Ahead Storage using SQLite (WAL mode).
Job states: PENDING, RUNNING, COMPLETED, FAILED, DEAD_LETTER.
Automatic crash recovery of orphaned RUNNING jobs.
"""

import sqlite3
import json
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
import uuid


class JobState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


@dataclass
class Job:
    id: str
    queue: str
    payload: Dict[str, Any]
    priority: int  # 0=HIGH, 1=NORMAL, 2=LOW
    state: str
    max_retries: int
    attempt: int
    created_at: float
    updated_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    visibility_timeout: int = 30
    lease_expires_at: Optional[float] = None


@dataclass
class QueueConfig:
    name: str
    rate_limit: float  # jobs per second
    created_at: float


class Storage:
    """Thread-safe SQLite storage with WAL mode and crash recovery."""

    SCHEMA = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA busy_timeout=5000;

    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        queue TEXT NOT NULL,
        payload TEXT NOT NULL,
        priority INTEGER NOT NULL,
        state TEXT NOT NULL,
        max_retries INTEGER NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        started_at REAL,
        completed_at REAL,
        worker_id TEXT,
        result TEXT,
        error TEXT,
        visibility_timeout INTEGER NOT NULL DEFAULT 30,
        lease_expires_at REAL
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_priority
        ON jobs(queue, state, priority, created_at);
    CREATE INDEX IF NOT EXISTS idx_jobs_lease_expires
        ON jobs(lease_expires_at) WHERE lease_expires_at IS NOT NULL;

    CREATE TABLE IF NOT EXISTS queues (
        name TEXT PRIMARY KEY,
        rate_limit REAL NOT NULL DEFAULT 10.0,
        created_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY,
        queues TEXT NOT NULL,
        last_heartbeat REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'active'
    );

    CREATE TABLE IF NOT EXISTS server_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """

    def __init__(self, db_path: str = "queue.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None  # autocommit mode
            )
            self._local.conn.row_factory = sqlite3.Row
            # Enable WAL mode
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
        return self._local.conn

    def _init_db(self) -> None:
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.executescript(self.SCHEMA)
        conn.close()

    @contextmanager
    def transaction(self):
        """Context manager for transactions."""
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            id=row['id'],
            queue=row['queue'],
            payload=json.loads(row['payload']),
            priority=row['priority'],
            state=row['state'],
            max_retries=row['max_retries'],
            attempt=row['attempt'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            started_at=row['started_at'],
            completed_at=row['completed_at'],
            worker_id=row['worker_id'],
            result=json.loads(row['result']) if row['result'] else None,
            error=row['error'],
            visibility_timeout=row['visibility_timeout'],
            lease_expires_at=row['lease_expires_at']
        )

    # Job operations
    def create_job(self, queue: str, payload: Dict, priority: str, max_retries: int, visibility_timeout: int = 30) -> Job:
        """Create a new job."""
        now = time.time()
        priority_map = {'HIGH': 0, 'NORMAL': 1, 'LOW': 2}
        job = Job(
            id=str(uuid.uuid4()),
            queue=queue,
            payload=payload,
            priority=priority_map.get(priority.upper(), 1),
            state=JobState.PENDING.value,
            max_retries=max_retries,
            attempt=0,
            created_at=now,
            updated_at=now,
            visibility_timeout=visibility_timeout
        )

        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO jobs (id, queue, payload, priority, state, max_retries, attempt,
                    created_at, updated_at, visibility_timeout)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (job.id, job.queue, json.dumps(job.payload), job.priority, job.state,
                 job.max_retries, job.attempt, job.created_at, job.updated_at, job.visibility_timeout)
            )
            # Ensure queue exists
            conn.execute(
                "INSERT OR IGNORE INTO queues (name, rate_limit, created_at) VALUES (?, ?, ?)",
                (queue, 10.0, now)
            )
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def get_pending_jobs(self, queues: List[str], limit: int = 100) -> List[Job]:
        """Get pending jobs for given queues, ordered by priority and creation time."""
        if not queues:
            return []

        placeholders = ','.join('?' * len(queues))
        conn = self._get_conn()
        rows = conn.execute(
            f"""SELECT * FROM jobs
                WHERE queue IN ({placeholders}) AND state = ?
                ORDER BY priority, created_at
                LIMIT ?""",
            (*queues, JobState.PENDING.value, limit)
        ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def lease_job(self, job_id: str, worker_id: str, visibility_timeout: int) -> Optional[Job]:
        """Atomically lease a job to a worker."""
        now = time.time()
        lease_expires = now + visibility_timeout

        with self.transaction() as conn:
            # Check job exists and is in PENDING state
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ? AND state = ?",
                (job_id, JobState.PENDING.value)
            ).fetchone()

            if not row:
                return None

            # Update job to RUNNING
            conn.execute(
                """UPDATE jobs SET state = ?, worker_id = ?, started_at = ?,
                    updated_at = ?, lease_expires_at = ?, attempt = attempt + 1
                   WHERE id = ?""",
                (JobState.RUNNING.value, worker_id, now, now, lease_expires, job_id)
            )

            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row)

    def complete_job(self, job_id: str, worker_id: str, result: Any) -> bool:
        """Mark job as completed."""
        now = time.time()
        with self.transaction() as conn:
            cursor = conn.execute(
                """UPDATE jobs SET state = ?, result = ?, completed_at = ?,
                    updated_at = ?, lease_expires_at = NULL
                   WHERE id = ? AND worker_id = ? AND state = ?""",
                (JobState.COMPLETED.value, json.dumps(result), now, now, job_id, worker_id, JobState.RUNNING.value)
            )
            return cursor.rowcount > 0

    def fail_job(self, job_id: str, worker_id: str, error: str, retry: bool) -> Optional[Job]:
        """Mark job as failed, potentially requeue for retry."""
        now = time.time()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ? AND worker_id = ? AND state = ?",
                (job_id, worker_id, JobState.RUNNING.value)
            ).fetchone()

            if not row:
                return None

            job = self._row_to_job(row)

            if retry and job.attempt < job.max_retries:
                # Requeue with exponential backoff
                delay = min(2 ** job.attempt, 60)  # Cap at 60 seconds
                # Add jitter: ±25%
                import random
                delay = delay * (0.75 + random.random() * 0.5)
                lease_expires = now + delay

                conn.execute(
                    """UPDATE jobs SET state = ?, error = ?, updated_at = ?,
                        lease_expires_at = ?, worker_id = NULL
                       WHERE id = ?""",
                    (JobState.PENDING.value, error, now, lease_expires, job_id)
                )
            else:
                # Dead letter
                conn.execute(
                    """UPDATE jobs SET state = ?, error = ?, completed_at = ?,
                        updated_at = ?, lease_expires_at = NULL
                       WHERE id = ?""",
                    (JobState.DEAD_LETTER.value, error, now, now, job_id)
                )

            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row)

    def requeue_orphaned_jobs(self) -> int:
        """Requeue jobs that were RUNNING but lease expired (crash recovery)."""
        now = time.time()
        with self.transaction() as conn:
            # Find jobs where lease expired
            cursor = conn.execute(
                """UPDATE jobs SET state = ?, worker_id = NULL, updated_at = ?,
                    lease_expires_at = NULL
                   WHERE state = ? AND lease_expires_at IS NOT NULL AND lease_expires_at < ?""",
                (JobState.PENDING.value, now, JobState.RUNNING.value, now)
            )
            return cursor.rowcount

    def get_jobs_by_state(self, state: str, limit: int = 100) -> List[Job]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT ?",
            (state, limit)
        ).fetchall()
        return [self._row_to_job(r) for r in rows]

    # Queue operations
    def get_queue_config(self, queue: str) -> Optional[QueueConfig]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
        if row:
            return QueueConfig(name=row['name'], rate_limit=row['rate_limit'], created_at=row['created_at'])
        return None

    def set_queue_rate_limit(self, queue: str, rate_limit: float) -> None:
        now = time.time()
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO queues (name, rate_limit, created_at) VALUES (?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET rate_limit = ?""",
                (queue, rate_limit, now, rate_limit)
            )

    def list_queues(self) -> List[str]:
        conn = self._get_conn()
        rows = conn.execute("SELECT name FROM queues ORDER BY name").fetchall()
        return [r['name'] for r in rows]

    def get_queue_stats(self, queue: str) -> Dict[str, int]:
        conn = self._get_conn()
        row = conn.execute(
            """SELECT
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as running,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as dead_letter
               FROM jobs WHERE queue = ?""",
            (JobState.PENDING.value, JobState.RUNNING.value, JobState.COMPLETED.value,
             JobState.FAILED.value, JobState.DEAD_LETTER.value, queue)
        ).fetchone()
        return {
            'pending': row['pending'] or 0,
            'running': row['running'] or 0,
            'completed': row['completed'] or 0,
            'failed': row['failed'] or 0,
            'dead_letter': row['dead_letter'] or 0
        }

    # Worker operations
    def register_worker(self, worker_id: str, queues: List[str]) -> None:
        now = time.time()
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO workers (id, queues, last_heartbeat, status) VALUES (?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET queues = ?, last_heartbeat = ?, status = ?""",
                (worker_id, json.dumps(queues), now, 'active', json.dumps(queues), now, 'active')
            )

    def worker_heartbeat(self, worker_id: str) -> bool:
        now = time.time()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE workers SET last_heartbeat = ? WHERE id = ? AND status = ?",
            (now, worker_id, 'active')
        )
        return cursor.rowcount > 0

    def get_active_workers(self, timeout: float = 60) -> List[Dict]:
        now = time.time()
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM workers WHERE status = ? AND last_heartbeat > ?",
            ('active', now - timeout)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None