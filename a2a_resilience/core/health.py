"""
Health Checker — Agent heartbeat monitoring for failover.

Detects unresponsive agents and marks their tasks for reassignment.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

from .task import Task, TaskState
from .task_store import TaskStore


@dataclass
class AgentStatus:
    """Status of a monitored agent."""
    agent_id: str
    last_heartbeat: float
    state: str = "alive"  # alive / stale / dead
    missed_beats: int = 0
    metadata: Dict[str, str] = field(default_factory=dict)


class HealthChecker:
    """
    Monitors agent health via heartbeats.
    
    Usage:
        checker = HealthChecker(timeout_seconds=30)
        
        # Agent sends heartbeat
        checker.heartbeat("agent-001", status="alive")
        
        # Check if agent is alive
        checker.is_alive("agent-001")  # True/False
        
        # Get dead agents
        dead = checker.get_dead_agents()
    """
    
    def __init__(
        self,
        timeout_seconds: float = 30,
        check_interval: float = 5.0,
    ):
        self.timeout_seconds = timeout_seconds
        self.check_interval = check_interval
        self.agents: Dict[str, AgentStatus] = {}
        self._lock = threading.Lock()
        self._callbacks: List[Callable[[str], None]] = []
    
    def heartbeat(
        self,
        agent_id: str,
        status: str = "alive",
        metadata: Optional[Dict[str, str]] = None,
    ):
        """
        Record a heartbeat from an agent.
        
        Args:
            agent_id: Agent identifier
            status: Agent status ("alive", "busy", etc.)
            metadata: Optional agent metadata
        """
        with self._lock:
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                agent.last_heartbeat = time.time()
                agent.state = "alive"
                agent.missed_beats = 0
                if metadata:
                    agent.metadata = metadata
            else:
                self.agents[agent_id] = AgentStatus(
                    agent_id=agent_id,
                    last_heartbeat=time.time(),
                    state="alive",
                    metadata=metadata or {},
                )
    
    def is_alive(self, agent_id: str) -> bool:
        """Check if an agent is alive (heartbeat received recently)."""
        with self._lock:
            agent = self.agents.get(agent_id)
            if agent is None:
                return False
            return agent.state == "alive"
    
    def get_dead_agents(self) -> List[str]:
        """Get list of agent IDs that have timed out."""
        self._check_timeouts()
        with self._lock:
            return [
                agent_id
                for agent_id, agent in self.agents.items()
                if agent.state == "dead"
            ]
    
    def get_stale_agents(self) -> List[str]:
        """Get list of agent IDs that are stale (approaching timeout)."""
        self._check_timeouts()
        with self._lock:
            return [
                agent_id
                for agent_id, agent in self.agents.items()
                if agent.state == "stale"
            ]
    
    def get_alive_agents(self) -> List[str]:
        """Get list of alive agent IDs."""
        with self._lock:
            return [
                agent_id
                for agent_id, agent in self.agents.items()
                if agent.state == "alive"
            ]
    
    def get_agent_status(self, agent_id: str) -> Optional[AgentStatus]:
        """Get status of a specific agent."""
        with self._lock:
            return self.agents.get(agent_id)
    
    def on_agent_dead(self, callback: Callable[[str], None]):
        """Register callback for when an agent dies."""
        self._callbacks.append(callback)
    
    def remove_agent(self, agent_id: str):
        """Remove an agent from monitoring."""
        with self._lock:
            self.agents.pop(agent_id, None)
    
    def get_all_status(self) -> Dict[str, AgentStatus]:
        """Get status of all monitored agents."""
        self._check_timeouts()
        with self._lock:
            return dict(self.agents)
    
    def _check_timeouts(self):
        """Check for timed-out agents and update their state."""
        now = time.time()
        stale_threshold = self.timeout_seconds * 0.7  # 70% of timeout
        dead_threshold = self.timeout_seconds
        
        with self._lock:
            for agent_id, agent in self.agents.items():
                elapsed = now - agent.last_heartbeat
                
                if elapsed > dead_threshold and agent.state != "dead":
                    agent.state = "dead"
                    agent.missed_beats += 1
                    # Notify callbacks
                    for cb in self._callbacks:
                        try:
                            cb(agent_id)
                        except Exception:
                            pass
                elif elapsed > stale_threshold and agent.state == "alive":
                    agent.state = "stale"
                    agent.missed_beats += 1


class FailoverManager:
    """
    Manages task failover when agents die.
    
    Integrates HealthChecker with TaskStore to automatically
    reassign tasks from dead agents to healthy ones.
    
    Usage:
        failover = FailoverManager(task_store, health_checker)
        
        # Register agents
        failover.register_agent("agent-001", "https://agent-001.com")
        failover.register_agent("agent-002", "https://agent-002.com")
        
        # Run failover check (typically called periodically)
        reassigned = failover.check_and_reassign()
        print(f"Reassigned {reassigned} tasks")
    """
    
    def __init__(
        self,
        task_store: TaskStore,
        health_checker: HealthChecker,
    ):
        self.store = task_store
        self.health = health_checker
        self.agent_urls: Dict[str, str] = {}  # agent_id -> url
        
        # Register death callback
        self.health.on_agent_dead(self._on_agent_dead)
    
    def register_agent(self, agent_id: str, agent_url: str):
        """Register an agent for failover."""
        self.agent_urls[agent_id] = agent_url
        self.health.heartbeat(agent_id)
    
    def _on_agent_dead(self, agent_id: str):
        """Handle agent death - mark tasks for reassignment."""
        tasks = self.store.get_by_agent(agent_id)
        for task in tasks:
            if task.state == TaskState.RUNNING:
                task.mark_agent_dead()
                self.store.save(task)
    
    def check_and_reassign(self) -> int:
        """
        Check for dead agents and reassign their tasks.
        
        Returns:
            Number of tasks reassigned
        """
        dead_agents = self.health.get_dead_agents()
        reassigned = 0
        
        for agent_id in dead_agents:
            tasks = self.store.get_by_agent(agent_id)
            for task in tasks:
                if task.can_retry:
                    task.mark_agent_dead()
                    self.store.save(task)
                    reassigned += 1
        
        return reassigned
    
    def get_available_agents(self) -> List[str]:
        """Get list of alive agent IDs."""
        return self.health.get_alive_agents()
    
    def pick_agent(self, exclude: Optional[Set[str]] = None) -> Optional[str]:
        """
        Pick an available agent for task assignment.
        
        Uses round-robin among alive agents.
        """
        available = [
            aid for aid in self.health.get_alive_agents()
            if not exclude or aid not in exclude
        ]
        
        if not available:
            return None
        
        # Simple round-robin (first available)
        return available[0]
