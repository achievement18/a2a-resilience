"""
Task Scheduler — Idempotent task submission with failover support.

The scheduler is the main entry point for task management.
It handles:
1. Idempotent task submission
2. Task assignment to agents
3. Retry queue processing
4. Failover coordination
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from .task import Task, TaskState, InvalidStateTransition
from .task_store import TaskStore
from .health import HealthChecker, FailoverManager


class TaskScheduler:
    """
    Schedules and manages resilient A2A tasks.
    
    Usage:
        store = TaskStore()
        health = HealthChecker()
        scheduler = TaskScheduler(store, health)
        
        # Submit task (idempotent)
        task = scheduler.submit(
            agent_url="https://agent.example.com",
            payload={"query": "research A2A"},
            idempotency_key="unique-123",
        )
        
        # Process retry queue
        retried = scheduler.process_retry_queue()
    """
    
    def __init__(
        self,
        task_store: Optional[TaskStore] = None,
        health_checker: Optional[HealthChecker] = None,
    ):
        self.store = task_store or TaskStore()
        self.health = health_checker or HealthChecker()
        self.failover = FailoverManager(self.store, self.health)
    
    def submit(
        self,
        agent_url: str,
        payload: Dict[str, Any],
        task_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        max_retries: int = 3,
    ) -> Task:
        """
        Submit a task (idempotent).
        
        If a task with the same idempotency_key already exists,
        returns the existing task without creating a duplicate.
        
        Args:
            agent_url: Target agent endpoint
            payload: Task payload
            task_id: Optional task ID
            idempotency_key: Key for deduplication
            max_retries: Maximum retry attempts
        
        Returns:
            Task (new or existing)
        """
        # Check idempotency
        if idempotency_key:
            existing = self.store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing
        
        # Create new task
        task = Task.create(
            agent_url=agent_url,
            payload=payload,
            task_id=task_id,
            idempotency_key=idempotency_key,
            max_retries=max_retries,
        )
        
        self.store.save(task)
        return task
    
    def assign(self, task_id: str, agent_id: str) -> Task:
        """
        Assign a task to an agent.
        
        Args:
            task_id: Task ID
            agent_id: Agent to assign to
        
        Returns:
            Updated task
        
        Raises:
            ValueError: If task not found
            InvalidStateTransition: If task can't be assigned
        """
        task = self.store.get(task_id)
        if task is None:
            raise ValueError(f"Task not found: {task_id}")
        
        if not self.health.is_alive(agent_id):
            raise ValueError(f"Agent not alive: {agent_id}")
        
        task.mark_running(agent_id)
        self.store.save(task)
        return task
    
    def complete(self, task_id: str, result: Dict[str, Any]) -> Task:
        """Mark a task as completed."""
        task = self.store.get(task_id)
        if task is None:
            raise ValueError(f"Task not found: {task_id}")
        
        task.mark_completed(result)
        self.store.save(task)
        return task
    
    def fail(self, task_id: str, error: str, retry: bool = True) -> Task:
        """Mark a task as failed, optionally queuing for retry."""
        task = self.store.get(task_id)
        if task is None:
            raise ValueError(f"Task not found: {task_id}")
        
        task.mark_failed(error, retry=retry)
        self.store.save(task)
        return task
    
    def retry(self, task_id: str) -> Task:
        """Manually retry a failed task."""
        task = self.store.get(task_id)
        if task is None:
            raise ValueError(f"Task not found: {task_id}")
        
        if not task.can_retry:
            raise InvalidStateTransition(f"Task {task_id} cannot be retried (state: {task.state})")
        
        task.transition(TaskState.RETRY_QUEUED, reason="Manual retry requested")
        self.store.save(task)
        return task
    
    def process_retry_queue(self, agent_picker: Optional[callable] = None) -> List[Task]:
        """
        Process tasks in retry queue, assigning them to available agents.
        
        Args:
            agent_picker: Optional function to pick an agent (default: round-robin)
        
        Returns:
            List of tasks that were successfully reassigned
        """
        retry_tasks = self.store.get_retry_queue()
        reassigned = []
        
        for task in retry_tasks:
            # Pick an agent (exclude previously failed agent)
            exclude = {task.assigned_agent} if task.assigned_agent else None
            agent_id = agent_picker() if agent_picker else self.failover.pick_agent(exclude=exclude)
            
            if agent_id is None:
                continue  # No available agent
            
            try:
                self.assign(task.task_id, agent_id)
                reassigned.append(task)
            except (ValueError, InvalidStateTransition):
                continue
        
        return reassigned
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        return self.store.get(task_id)
    
    def get_tasks_by_state(self, state: TaskState) -> List[Task]:
        """Get all tasks in a given state."""
        return self.store.get_by_state(state)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        stats = self.store.get_stats()
        stats["alive_agents"] = len(self.health.get_alive_agents())
        stats["dead_agents"] = len(self.health.get_dead_agents())
        return stats
    
    def register_agent(self, agent_id: str, agent_url: str):
        """Register an agent for failover monitoring."""
        self.failover.register_agent(agent_id, agent_url)
    
    def heartbeat(self, agent_id: str, status: str = "alive"):
        """Record agent heartbeat."""
        self.health.heartbeat(agent_id, status)
