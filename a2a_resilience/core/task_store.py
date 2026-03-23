"""
Task Store — Persistent storage for resilient tasks.

SQLite-backed storage with idempotency support.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from .task import Task, TaskState


class TaskStore:
    """
    Persistent task storage with idempotency.
    
    Usage:
        store = TaskStore("~/.a2a-resilience/tasks.db")
        
        # Store a task
        store.save(task)
        
        # Get by ID
        task = store.get("task-001")
        
        # Get by idempotency key (dedup)
        existing = store.get_by_idempotency_key("unique-request-id")
        
        # Get tasks by state
        pending = store.get_by_state(TaskState.PENDING)
        retry = store.get_by_state(TaskState.RETRY_QUEUED)
    """
    
    def __init__(self, db_path: str = "~/.a2a-resilience/tasks.db"):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()
    
    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            self._local.conn = sqlite3.connect(str(self.db_path))
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                agent_url TEXT NOT NULL,
                payload TEXT NOT NULL,
                idempotency_key TEXT,
                state TEXT NOT NULL,
                result TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                assigned_agent TEXT,
                transition_history TEXT DEFAULT '[]'
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_state ON tasks(state)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_idempotency ON tasks(idempotency_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent ON tasks(assigned_agent)")
        conn.commit()
    
    def save(self, task: Task):
        """Save or update a task."""
        conn = self._get_conn()
        data = task.to_dict()
        
        conn.execute("""
            INSERT OR REPLACE INTO tasks
            (task_id, agent_url, payload, idempotency_key, state, result, error,
             retry_count, max_retries, created_at, updated_at, assigned_agent, transition_history)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["task_id"],
            data["agent_url"],
            json.dumps(data["payload"]),
            data.get("idempotency_key"),
            data["state"],
            json.dumps(data["result"]) if data.get("result") else None,
            data.get("error"),
            data["retry_count"],
            data["max_retries"],
            data["created_at"],
            data["updated_at"],
            data.get("assigned_agent"),
            json.dumps(data["transition_history"]),
        ))
        conn.commit()
    
    def get(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)
    
    def get_by_idempotency_key(self, key: str) -> Optional[Task]:
        """Get a task by idempotency key (for deduplication)."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)
    
    def get_by_state(self, state: TaskState, limit: int = 100) -> List[Task]:
        """Get all tasks in a given state."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM tasks WHERE state = ? ORDER BY created_at LIMIT ?",
            (state.value, limit),
        ).fetchall()
        return [self._row_to_task(r) for r in rows]
    
    def get_by_agent(self, agent_id: str) -> List[Task]:
        """Get all tasks assigned to a specific agent."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM tasks WHERE assigned_agent = ?",
            (agent_id,),
        ).fetchall()
        return [self._row_to_task(r) for r in rows]
    
    def get_retry_queue(self) -> List[Task]:
        """Get all tasks queued for retry."""
        return self.get_by_state(TaskState.RETRY_QUEUED)
    
    def get_stats(self) -> Dict[str, int]:
        """Get task statistics by state."""
        conn = self._get_conn()
        rows = conn.execute("SELECT state, COUNT(*) as count FROM tasks GROUP BY state").fetchall()
        return {row["state"]: row["count"] for row in rows}
    
    def cleanup_old(self, older_than_days: int = 30):
        """Remove completed/dead tasks older than specified days."""
        conn = self._get_conn()
        cutoff = time.time() - (older_than_days * 86400)
        conn.execute(
            "DELETE FROM tasks WHERE state IN ('completed', 'dead') AND updated_at < ?",
            (cutoff,),
        )
        conn.commit()
    
    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task.from_dict({
            "task_id": row["task_id"],
            "agent_url": row["agent_url"],
            "payload": json.loads(row["payload"]),
            "idempotency_key": row["idempotency_key"],
            "state": row["state"],
            "result": json.loads(row["result"]) if row["result"] else None,
            "error": row["error"],
            "retry_count": row["retry_count"],
            "max_retries": row["max_retries"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "assigned_agent": row["assigned_agent"],
            "transition_history": json.loads(row["transition_history"]),
        })
