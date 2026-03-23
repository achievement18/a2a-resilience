"""Tests for a2a-resilience core modules."""

import os
import tempfile
import time
import pytest
from a2a_resilience import (
    Task, TaskState, InvalidStateTransition,
    TaskStore, HealthChecker, TaskScheduler,
)


# ─── Task Tests ───

class TestTask:
    def test_create_task(self):
        task = Task.create(
            agent_url="https://agent.example.com",
            payload={"query": "test"},
        )
        assert task.state == TaskState.PENDING
        assert task.task_id.startswith("task-")
    
    def test_create_with_id(self):
        task = Task.create(
            agent_url="https://agent.example.com",
            payload={},
            task_id="my-task-001",
        )
        assert task.task_id == "my-task-001"
    
    def test_valid_transitions(self):
        task = Task.create(agent_url="https://x.com", payload={})
        task.mark_running("agent-001")
        assert task.state == TaskState.RUNNING
        assert task.assigned_agent == "agent-001"
        
        task.mark_completed({"done": True})
        assert task.state == TaskState.COMPLETED
        assert task.result == {"done": True}
    
    def test_invalid_transition(self):
        task = Task.create(agent_url="https://x.com", payload={})
        with pytest.raises(InvalidStateTransition):
            task.mark_completed({"done": True})  # Can't complete from PENDING
    
    def test_retry_on_failure(self):
        task = Task.create(agent_url="https://x.com", payload={}, max_retries=3)
        task.mark_running("agent-001")
        task.mark_failed("timeout", retry=True)
        
        assert task.state == TaskState.RETRY_QUEUED
        assert task.retry_count == 1
    
    def test_max_retries(self):
        task = Task.create(agent_url="https://x.com", payload={}, max_retries=2)
        task.mark_running("agent-001")
        task.mark_failed("err1", retry=True)
        assert task.state == TaskState.RETRY_QUEUED  # 1st failure, queued for retry
        assert task.retry_count == 1
        
        task.mark_running("agent-002")
        task.mark_failed("err2", retry=True)
        assert task.state == TaskState.RETRY_QUEUED  # 2nd failure, queued for retry (2nd)
        assert task.retry_count == 2
        
        task.mark_running("agent-003")
        task.mark_failed("err3", retry=True)
        # 3rd failure: max_retries=2 exhausted, should be DEAD
        assert task.state == TaskState.DEAD
    
    def test_agent_dead_handling(self):
        task = Task.create(agent_url="https://x.com", payload={})
        task.mark_running("agent-001")
        
        task.mark_agent_dead()
        assert task.state == TaskState.RETRY_QUEUED
        assert task.assigned_agent is None
    
    def test_is_terminal(self):
        task = Task.create(agent_url="https://x.com", payload={})
        assert not task.is_terminal
        
        task.mark_running("a1")
        task.mark_completed({})
        assert task.is_terminal
    
    def test_serialization(self):
        task = Task.create(agent_url="https://x.com", payload={"q": 1}, task_id="t1")
        task.mark_running("a1")
        
        data = task.to_dict()
        restored = Task.from_dict(data)
        
        assert restored.task_id == task.task_id
        assert restored.state == task.state
        assert len(restored.transition_history) == len(task.transition_history)


# ─── TaskStore Tests ───

class TestTaskStore:
    @pytest.fixture
    def store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield TaskStore(os.path.join(tmpdir, "test.db"))
    
    def test_save_and_get(self, store):
        task = Task.create(agent_url="https://x.com", payload={}, task_id="t1")
        store.save(task)
        
        loaded = store.get("t1")
        assert loaded is not None
        assert loaded.task_id == "t1"
    
    def test_idempotency_key(self, store):
        task = Task.create(
            agent_url="https://x.com",
            payload={},
            idempotency_key="unique-123",
        )
        store.save(task)
        
        found = store.get_by_idempotency_key("unique-123")
        assert found is not None
        assert found.task_id == task.task_id
    
    def test_get_by_state(self, store):
        for i in range(3):
            task = Task.create(agent_url="https://x.com", payload={}, task_id=f"t{i}")
            if i < 2:
                task.mark_running("a1")
                task.mark_completed({})
            store.save(task)
        
        completed = store.get_by_state(TaskState.COMPLETED)
        assert len(completed) == 2
    
    def test_get_stats(self, store):
        for i in range(2):
            task = Task.create(agent_url="https://x.com", payload={}, task_id=f"t{i}")
            store.save(task)
        
        stats = store.get_stats()
        assert stats.get("pending", 0) == 2


# ─── HealthChecker Tests ───

class TestHealthChecker:
    def test_heartbeat(self):
        hc = HealthChecker(timeout_seconds=10)
        hc.heartbeat("agent-001")
        
        assert hc.is_alive("agent-001")
        assert "agent-001" in hc.get_alive_agents()
    
    def test_timeout_detection(self):
        hc = HealthChecker(timeout_seconds=0.1)
        hc.heartbeat("agent-001")
        
        time.sleep(0.2)
        hc._check_timeouts()
        
        assert not hc.is_alive("agent-001")
        assert "agent-001" in hc.get_dead_agents()
    
    def test_dead_callback(self):
        hc = HealthChecker(timeout_seconds=0.1)
        dead = []
        hc.on_agent_dead(lambda agent_id: dead.append(agent_id))
        
        hc.heartbeat("agent-001")
        time.sleep(0.2)
        hc._check_timeouts()
        
        assert "agent-001" in dead


# ─── TaskScheduler Tests ───

class TestTaskScheduler:
    @pytest.fixture
    def scheduler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(os.path.join(tmpdir, "tasks.db"))
            yield TaskScheduler(task_store=store)
    
    def test_submit_task(self, scheduler):
        task = scheduler.submit(
            agent_url="https://agent.example.com",
            payload={"query": "test"},
        )
        assert task.state == TaskState.PENDING
    
    def test_idempotent_submit(self, scheduler):
        task1 = scheduler.submit(
            agent_url="https://x.com",
            payload={"q": 1},
            idempotency_key="dup-key",
        )
        task2 = scheduler.submit(
            agent_url="https://x.com",
            payload={"q": 999},  # Different payload
            idempotency_key="dup-key",  # Same key
        )
        
        assert task1.task_id == task2.task_id
    
    def test_full_lifecycle(self, scheduler):
        scheduler.register_agent("agent-001", "https://agent-001.com")
        
        task = scheduler.submit(
            agent_url="https://agent.example.com",
            payload={"q": "test"},
            task_id="lifecycle-001",
        )
        
        scheduler.assign(task.task_id, "agent-001")
        updated = scheduler.get_task("lifecycle-001")
        assert updated.state == TaskState.RUNNING
        
        scheduler.complete(task.task_id, {"result": "done"})
        updated = scheduler.get_task("lifecycle-001")
        assert updated.state == TaskState.COMPLETED
    
    def test_failover(self, scheduler):
        scheduler.register_agent("agent-001", "https://a1.com")
        scheduler.register_agent("agent-002", "https://a2.com")
        scheduler.heartbeat("agent-001")
        scheduler.heartbeat("agent-002")
        
        task = scheduler.submit(agent_url="https://x.com", payload={}, task_id="fo-001")
        scheduler.assign("fo-001", "agent-001")
        
        # Simulate agent-001 dying
        scheduler.health.agents["agent-001"].state = "dead"
        
        # Check and reassign
        reassigned = scheduler.failover.check_and_reassign()
        assert reassigned > 0
        
        updated = scheduler.get_task("fo-001")
        assert updated.state in (TaskState.RETRY_QUEUED, TaskState.DEAD)
