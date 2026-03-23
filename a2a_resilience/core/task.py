"""
Task Model — Idempotent task with state machine for A2A resilience.

Defines the Task data model and state transitions that ensure:
1. Tasks are uniquely identified (task_id)
2. State transitions are well-defined and auditable
3. Idempotency is enforced at the task level
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class TaskState(str, Enum):
    """Task lifecycle states."""
    PENDING = "pending"           # Task created, not yet assigned
    RUNNING = "running"           # Task being processed by an agent
    PAUSED = "paused"             # Task paused (agent slow, manually paused)
    COMPLETED = "completed"       # Task completed successfully
    FAILED = "failed"             # Task failed (non-retryable)
    RETRY_QUEUED = "retry_queued" # Task queued for retry after failure
    DEAD = "dead"                 # Task exhausted retries, permanent failure


# Valid state transitions
VALID_TRANSITIONS: Dict[TaskState, List[TaskState]] = {
    TaskState.PENDING: [TaskState.RUNNING, TaskState.FAILED],
    TaskState.RUNNING: [TaskState.COMPLETED, TaskState.FAILED, TaskState.PAUSED, TaskState.RETRY_QUEUED, TaskState.DEAD],
    TaskState.PAUSED: [TaskState.RUNNING, TaskState.FAILED],
    TaskState.FAILED: [TaskState.RETRY_QUEUED, TaskState.DEAD],
    TaskState.RETRY_QUEUED: [TaskState.RUNNING, TaskState.DEAD],
    TaskState.COMPLETED: [],  # Terminal state
    TaskState.DEAD: [],       # Terminal state
}


class InvalidStateTransition(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


@dataclass
class StateTransition:
    """Records a single state transition."""
    from_state: TaskState
    to_state: TaskState
    timestamp: float
    reason: Optional[str] = None
    agent_id: Optional[str] = None


@dataclass
class Task:
    """
    A resilient task with state machine and idempotency.
    
    Attributes:
        task_id: Unique task identifier
        agent_url: Target agent endpoint
        payload: Task payload
        idempotency_key: Key for deduplication
        state: Current task state
        result: Task result (set on completion)
        error: Error message (set on failure)
        retry_count: Number of retries so far
        max_retries: Maximum allowed retries
        created_at: Creation timestamp
        updated_at: Last update timestamp
        assigned_agent: Currently assigned agent ID
        transition_history: Full history of state transitions
    """
    task_id: str
    agent_url: str
    payload: Dict[str, Any]
    idempotency_key: Optional[str] = None
    state: TaskState = TaskState.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    assigned_agent: Optional[str] = None
    transition_history: List[StateTransition] = field(default_factory=list)
    
    @classmethod
    def create(
        cls,
        agent_url: str,
        payload: Dict[str, Any],
        task_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        max_retries: int = 3,
    ) -> "Task":
        """Create a new task."""
        task = cls(
            task_id=task_id or f"task-{uuid.uuid4().hex[:12]}",
            agent_url=agent_url,
            payload=payload,
            idempotency_key=idempotency_key,
            max_retries=max_retries,
        )
        return task
    
    def transition(
        self,
        new_state: TaskState,
        reason: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> "Task":
        """
        Transition to a new state.
        
        Raises InvalidStateTransition if the transition is not allowed.
        """
        valid = VALID_TRANSITIONS.get(self.state, [])
        if new_state not in valid:
            raise InvalidStateTransition(
                f"Cannot transition from {self.state} to {new_state}. "
                f"Valid transitions: {[s.value for s in valid]}"
            )
        
        transition = StateTransition(
            from_state=self.state,
            to_state=new_state,
            timestamp=time.time(),
            reason=reason,
            agent_id=agent_id or self.assigned_agent,
        )
        
        self.state = new_state
        self.updated_at = time.time()
        self.transition_history.append(transition)
        
        # Track retry count
        if new_state == TaskState.RETRY_QUEUED:
            self.retry_count += 1
        
        return self
    
    def mark_running(self, agent_id: str) -> "Task":
        """Mark task as running on a specific agent."""
        self.assigned_agent = agent_id
        return self.transition(TaskState.RUNNING, reason="Assigned to agent", agent_id=agent_id)
    
    def mark_completed(self, result: Dict[str, Any]) -> "Task":
        """Mark task as completed with result."""
        self.result = result
        return self.transition(TaskState.COMPLETED, reason="Task completed")
    
    def mark_failed(self, error: str, retry: bool = True) -> "Task":
        """Mark task as failed, optionally queuing for retry.
        
        max_retries defines how many failures are tolerated before the task becomes DEAD.
        With max_retries=2: after 2 failures, the task becomes DEAD.
        
        Args:
            error: Error message
            retry: If True, queue for retry (up to max_retries times)
        """
        self.error = error
        
        if retry:
            # If retry_count already equals max_retries, we've exhausted retries
            if self.retry_count >= self.max_retries:
                return self.transition(TaskState.DEAD, reason="Max retries exceeded")
            return self.transition(TaskState.RETRY_QUEUED, reason=f"Failed: {error}")
        else:
            return self.transition(TaskState.FAILED, reason=f"Failed: {error}")
    
    def mark_agent_dead(self) -> "Task":
        """Mark task as needing reassignment due to agent failure."""
        if self.state == TaskState.RUNNING:
            self.assigned_agent = None
            if self.retry_count < self.max_retries:
                return self.transition(TaskState.RETRY_QUEUED, reason="Agent became unresponsive")
            else:
                self.transition(TaskState.RETRY_QUEUED, reason="Agent became unresponsive")
                return self.transition(TaskState.DEAD, reason="Max retries exceeded")
        return self
    
    @property
    def is_terminal(self) -> bool:
        """Check if task is in a terminal state."""
        return self.state in (TaskState.COMPLETED, TaskState.DEAD)
    
    @property
    def can_retry(self) -> bool:
        """Check if task can be retried (including RUNNING tasks with dead agents)."""
        if self.state == TaskState.RUNNING:
            return self.retry_count < self.max_retries  # Agent died, can reassign
        return self.state in (TaskState.FAILED, TaskState.RETRY_QUEUED) and self.retry_count < self.max_retries
    
    @property
    def age_seconds(self) -> float:
        """Time since task creation."""
        return time.time() - self.created_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for storage."""
        return {
            "task_id": self.task_id,
            "agent_url": self.agent_url,
            "payload": self.payload,
            "idempotency_key": self.idempotency_key,
            "state": self.state.value,
            "result": self.result,
            "error": self.error,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "assigned_agent": self.assigned_agent,
            "transition_history": [
                {
                    "from": t.from_state.value,
                    "to": t.to_state.value,
                    "timestamp": t.timestamp,
                    "reason": t.reason,
                    "agent_id": t.agent_id,
                }
                for t in self.transition_history
            ],
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Deserialize from dict."""
        transitions = []
        for t in data.pop("transition_history", []):
            transitions.append(StateTransition(
                from_state=TaskState(t["from"]),
                to_state=TaskState(t["to"]),
                timestamp=t["timestamp"],
                reason=t.get("reason"),
                agent_id=t.get("agent_id"),
            ))
        
        task = cls(
            task_id=data["task_id"],
            agent_url=data["agent_url"],
            payload=data["payload"],
            idempotency_key=data.get("idempotency_key"),
            state=TaskState(data["state"]),
            result=data.get("result"),
            error=data.get("error"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            assigned_agent=data.get("assigned_agent"),
            transition_history=transitions,
        )
        return task
