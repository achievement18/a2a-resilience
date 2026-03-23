# 🛡️ a2a-resilience

**Idempotent Task Execution with Agent Failover for A2A**

A resilience layer for the Agent-to-Agent (A2A) protocol. Handles task failures, agent crashes, and automatic retry with guaranteed idempotency.

## ✨ Features

- **Idempotent Submission** — Same `idempotency_key` returns cached task, never duplicates
- **State Machine** — Well-defined transitions: `PENDING → RUNNING → COMPLETED/FAILED/RETRY_QUEUED → DEAD`
- **Agent Heartbeat** — Detect unresponsive agents automatically
- **Automatic Failover** — Tasks from dead agents are reassigned to healthy ones
- **Persistent Storage** — SQLite-backed task history, survives restarts

## 🚀 Quick Start

```bash
pip install a2a-resilience
```

### Submit a Task

```python
from a2a_resilience import TaskScheduler

scheduler = TaskScheduler()

# Idempotent submission
task = scheduler.submit(
    agent_url="https://agent.example.com",
    payload={"query": "research A2A protocol"},
    idempotency_key="unique-request-123",  # Prevents duplicates
    max_retries=3,
)
```

### Assign & Complete

```python
# Register agents
scheduler.register_agent("agent-001", "https://agent-001.com")
scheduler.heartbeat("agent-001")

# Assign task to agent
scheduler.assign(task.task_id, "agent-001")

# On completion
scheduler.complete(task.task_id, {"result": "A2A is a protocol..."})

# On failure (auto-retry)
scheduler.fail(task.task_id, error="timeout", retry=True)
```

### Agent Heartbeat

```python
# Agent sends heartbeat periodically
scheduler.heartbeat("agent-001", status="alive")

# If heartbeat stops, tasks are automatically reassigned
```

## 🔄 State Machine

```
┌─────────┐    ┌─────────┐    ┌───────────┐
│ PENDING │───▶│ RUNNING │───▶│ COMPLETED │
└─────────┘    └─────────┘    └───────────┘
                   │ │              ▲
        ┌──────────┘ └──────────┐  │
        ▼                       ▼  │
┌───────────────┐    ┌───────────┐ │
│ RETRY_QUEUED  │───▶│  RUNNING  │─┘
└───────────────┘    └───────────┘
        │ (max retries exceeded)
        ▼
┌─────────┐
│  DEAD   │
└─────────┘
```

## 🏗️ Architecture

```
┌──────────────────────────────────────────────┐
│              TaskScheduler                    │
│  submit() → assign() → complete()/fail()     │
└──────────────────┬───────────────────────────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
┌──────────┐ ┌──────────┐ ┌────────────┐
│ TaskStore│ │HealthChk │ │ FailoverMgr│
│ (SQLite) │ │heartbeat │ │ auto-reassign│
└──────────┘ └──────────┘ └────────────┘
```

## 📊 Task Stats

```python
stats = scheduler.get_stats()
# {'pending': 5, 'running': 2, 'completed': 150, 'dead': 3, 'alive_agents': 4}
```

## 📄 License

MIT
