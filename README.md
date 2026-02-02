# Distributed Task Scheduler

Production-ready distributed task scheduler with DAG execution, retry logic, and dead letter queue.

## Features

- **DAG-Based Execution**
  - Define tasks with dependencies
  - Automatic dependency resolution
  - Cycle detection
  
- **Scheduling**
  - Cron-based scheduling
  - Manual/API triggers
  - Priority queues (critical, high, normal, low)
  
- **Retry Logic**
  - Exponential backoff with jitter
  - Configurable max retries
  - Per-task retry settings
  
- **Dead Letter Queue**
  - Automatic DLQ for failed jobs
  - Replay capability
  - Manual resolution
  
- **Worker Pool**
  - Distributed workers
  - Heartbeat monitoring
  - Concurrent task execution
  
- **Real-Time Updates**
  - WebSocket for live status
  - Event-driven architecture

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   REST API  │────▶│  Scheduler  │────▶│   Workers   │
│   (FastAPI) │     │  (Background)│     │   (Pool)    │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                    │
       │            ┌──────┴──────┐      ┌──────┴──────┐
       │            │    Redis    │      │   Execute   │
       │            │   (Queue)   │      │    Tasks    │
       │            └─────────────┘      └──────┬──────┘
       │                                        │
       └────────────────┬───────────────────────┘
                        │
                 ┌──────┴──────┐
                 │  PostgreSQL │
                 │   (State)   │
                 └─────────────┘
```

## Quick Start

```bash
# Start all services
docker-compose up -d

# Check health
curl http://localhost:8000/health

# Create a DAG
curl -X POST http://localhost:8000/dags \
  -H "Content-Type: application/json" \
  -d '{
    "name": "etl-pipeline",
    "cron_expression": "0 * * * *",
    "tasks": [
      {"name": "extract", "task_type": "http", "config": {"url": "https://api.example.com/data"}},
      {"name": "transform", "task_type": "python", "config": {"code": "process()"}, "dependencies": ["extract"]},
      {"name": "load", "task_type": "http", "config": {"url": "https://db.example.com"}, "dependencies": ["transform"]}
    ]
  }'

# Trigger a job
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"dag_id": 1, "priority": "high"}'

# Check job status
curl http://localhost:8000/jobs/{run_id}
```

## API Endpoints

### DAGs
- `POST /dags` - Create DAG
- `GET /dags` - List DAGs
- `GET /dags/{id}` - Get DAG details
- `PATCH /dags/{id}` - Update DAG
- `DELETE /dags/{id}` - Delete DAG
- `POST /dags/{id}/pause` - Pause DAG
- `POST /dags/{id}/resume` - Resume DAG

### Jobs
- `POST /jobs` - Trigger job
- `GET /jobs` - List jobs
- `GET /jobs/{run_id}` - Get job details
- `POST /jobs/{run_id}/cancel` - Cancel job
- `GET /jobs/queue/stats` - Queue statistics

### Workers
- `GET /workers` - List workers
- `GET /workers/stats` - Worker statistics
- `GET /workers/dlq/entries` - List DLQ entries
- `POST /workers/dlq/{id}/replay` - Replay DLQ entry
- `POST /workers/dlq/{id}/resolve` - Resolve DLQ entry

### WebSocket
- `WS /ws/jobs` - All job events
- `WS /ws/job/{run_id}` - Single job events

## Task Types

### HTTP Task
```json
{
  "task_type": "http",
  "config": {
    "url": "https://api.example.com",
    "method": "POST",
    "headers": {"Authorization": "Bearer token"},
    "body": {"key": "value"}
  }
}
```

### Python Task
```json
{
  "task_type": "python",
  "config": {
    "code": "def run(): return process_data()"
  }
}
```

### Shell Task
```json
{
  "task_type": "shell",
  "config": {
    "command": "echo 'Hello World'"
  }
}
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection URL |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `MAX_CONCURRENT_JOBS` | `10` | Max parallel jobs per worker |
| `MAX_RETRIES` | `3` | Default max retry attempts |
| `RETRY_BASE_DELAY` | `60` | Base retry delay (seconds) |
| `RETRY_MAX_DELAY` | `3600` | Max retry delay (seconds) |
| `DLQ_ENABLED` | `true` | Enable dead letter queue |

## Retry Strategy

Exponential backoff with jitter:

```
delay = min(base_delay * (2 ^ attempt), max_delay) ± 10% jitter
```

Example:
- Attempt 1: ~60 seconds
- Attempt 2: ~120 seconds
- Attempt 3: ~240 seconds

## Testing

```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=app --cov-report=term-missing
```

## License

MIT
