"""
a2a-resilience — Idempotent Task Execution with Agent Failover for A2A.

Provides resilient task management for the Agent-to-Agent (A2A) protocol:

- Idempotent task submission (deduplication via idempotency_key)
- State machine with well-defined transitions
- Agent heartbeat monitoring
- Automatic failover when agents die
- Persistent task storage (SQLite)

Basic Usage:
    from a2a_resilience import TaskScheduler, TaskStore, HealthChecker
    
    scheduler = TaskScheduler()
    
    # Submit task (idempotent)
    task = scheduler.submit(
        agent_url="https://agent.example.com",
        payload={"query": "research A2A"},
        idempotency_key="unique-123",
    )
    
    # Assign to agent
    scheduler.assign(task.task_id, "agent-001")
    
    # Complete
    scheduler.complete(task.task_id, {"result": "..."})
"""

__version__ = "0.1.0"

from .core.task import Task, TaskState, InvalidStateTransition
from .core.task_store import TaskStore
from .core.health import HealthChecker, FailoverManager
from .core.scheduler import TaskScheduler

__all__ = [
    "Task",
    "TaskState",
    "InvalidStateTransition",
    "TaskStore",
    "HealthChecker",
    "FailoverManager",
    "TaskScheduler",
]
