# A2A Task Resilience Layer — 技术方案

## 项目名称
**a2a-resilience** — Idempotent Task Execution with Agent Failover

## 核心问题

A2A 协议中 Task 状态（`submitted/working/completed/failed`）存在，但没有定义：
1. **幂等语义** — Task 重新派发会不会执行两次？
2. **恢复语义** — Agent 挂掉后，新 Agent 怎么接手半完成的任务？
3. **状态持久化** — Task 状态存在哪里？谁维护 source of truth？

## 设计：Task 幂等模型

### 幂等保证

```
task_id + idempotency_key = 幂等键

同一个 idempotency_key 的重复请求 → 返回缓存结果，不重新执行
```

### Task 状态机

```
           ┌─────────────────────────────────────┐
           │                                     │
           ▼                                     │
     ┌──────────┐    ┌──────────┐    ┌──────────┐│   ┌──────────┐
     │ PENDING  │───▶│ RUNNING  │───▶│ COMPLETED││   │ FAILED   │
     └──────────┘    └──────────┘    └──────────┘│   └──────────┘
                          │                      │        │
                          │         ┌────────────┘        │
                          ▼         ▼                     ▼
                    ┌──────────┐         ┌──────────┐
                    │PAUSED    │         │RETRY_QUEUED│──┐
                    └──────────┘         └──────────┘  │
                                                        │
           ┌───────────────────────────────────────────┘
           │
           ▼ (max retries exceeded)
     ┌──────────┐
     │ DEAD     │
     └──────────┘
```

### Agent 心跳检测

每个 Agent 定期发送心跳，Resilience Layer 检测离线 Agent：
- 心跳超时 → Agent 标记为 `STALE`
- 超过阈值 → Agent 上的 `RUNNING` 任务标记为 `RETRY_QUEUED`
- 调度器重新分配任务

## 目录结构

```
a2a-resilience/
├── README.md
├── pyproject.toml
├── a2a_resilience/
│   ├── __init__.py
│   ├── core/
│   │   ├── task.py           # Task 数据模型 + 状态机
│   │   ├── task_store.py     # 持久化存储
│   │   ├── idempotency.py    # 幂等键管理
│   │   ├── scheduler.py      # 任务调度 + 失败重分配
│   │   └── health.py         # Agent 心跳检测
│   └── server/
│       ├── middleware.py      # 任务持久化中间件
│       └── web.py            # 任务状态仪表板
├── examples/
│   └── resilient_workflow.py
└── tests/
```

## 核心 API

```python
from a2a_resilience import TaskScheduler, TaskStore, HealthChecker

# 创建调度器
store = TaskStore("~/.a2a-resilience/tasks")
health = HealthChecker(timeout_seconds=30)
scheduler = TaskScheduler(store, health)

# 提发任务（幂等）
task = scheduler.submit(
    task_id="task-001",  # 可选，不传自动生成
    agent_url="https://agent.example.com",
    payload={"query": "..."},
    idempotency_key="unique-request-id",
)

# Agent 心跳
health.heartbeat(agent_id="agent-001", status="alive")

# 查询任务状态
task = store.get("task-001")
print(task.state, task.result)

# 失败重试
scheduler.retry("task-001")
```
