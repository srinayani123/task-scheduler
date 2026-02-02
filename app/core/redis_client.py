"""
Redis client for job queue management.
"""

import json
from datetime import datetime
from typing import Optional
import redis.asyncio as redis
from app.config import settings


class RedisClient:
    """Async Redis client for job queuing."""
    
    # Queue names
    QUEUE_HIGH = "jobs:queue:high"
    QUEUE_NORMAL = "jobs:queue:normal"
    QUEUE_LOW = "jobs:queue:low"
    QUEUE_CRITICAL = "jobs:queue:critical"
    
    PROCESSING_SET = "jobs:processing"
    DELAYED_ZSET = "jobs:delayed"
    
    def __init__(self):
        self._client: Optional[redis.Redis] = None
    
    async def initialize(self) -> None:
        """Initialize Redis connection."""
        self._client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client."""
        if not self._client:
            raise RuntimeError("Redis client not initialized")
        return self._client
    
    def _get_queue_for_priority(self, priority: str) -> str:
        """Get queue name for priority level."""
        queues = {
            "critical": self.QUEUE_CRITICAL,
            "high": self.QUEUE_HIGH,
            "normal": self.QUEUE_NORMAL,
            "low": self.QUEUE_LOW
        }
        return queues.get(priority, self.QUEUE_NORMAL)
    
    # Job Queue Operations
    async def enqueue_job(self, job_data: dict, priority: str = "normal") -> None:
        """Add job to queue based on priority."""
        queue = self._get_queue_for_priority(priority)
        await self._client.lpush(queue, json.dumps(job_data))
        
        # Publish event for real-time updates
        await self._client.publish("jobs:events", json.dumps({
            "event": "job_queued",
            "job_id": job_data.get("job_run_id"),
            "priority": priority
        }))
    
    async def dequeue_job(self) -> Optional[dict]:
        """
        Get next job from queue (priority order).
        Uses BRPOP with timeout for blocking.
        """
        # Check queues in priority order
        queues = [
            self.QUEUE_CRITICAL,
            self.QUEUE_HIGH,
            self.QUEUE_NORMAL,
            self.QUEUE_LOW
        ]
        
        result = await self._client.brpop(queues, timeout=1)
        if result:
            _, job_json = result
            job_data = json.loads(job_json)
            
            # Add to processing set
            await self._client.sadd(
                self.PROCESSING_SET,
                json.dumps(job_data)
            )
            
            return job_data
        
        return None
    
    async def complete_job(self, job_data: dict, success: bool = True) -> None:
        """Mark job as completed and remove from processing."""
        await self._client.srem(self.PROCESSING_SET, json.dumps(job_data))
        
        # Publish completion event
        await self._client.publish("jobs:events", json.dumps({
            "event": "job_completed" if success else "job_failed",
            "job_id": job_data.get("job_run_id"),
            "success": success
        }))
    
    async def schedule_delayed_job(
        self,
        job_data: dict,
        execute_at: datetime
    ) -> None:
        """Schedule job for future execution."""
        score = execute_at.timestamp()
        await self._client.zadd(
            self.DELAYED_ZSET,
            {json.dumps(job_data): score}
        )
    
    async def get_due_delayed_jobs(self) -> list[dict]:
        """Get all delayed jobs that are due for execution."""
        now = datetime.utcnow().timestamp()
        
        # Get jobs with score <= now
        jobs = await self._client.zrangebyscore(
            self.DELAYED_ZSET,
            0,
            now
        )
        
        due_jobs = []
        for job_json in jobs:
            # Remove from delayed set
            await self._client.zrem(self.DELAYED_ZSET, job_json)
            due_jobs.append(json.loads(job_json))
        
        return due_jobs
    
    # Queue Stats
    async def get_queue_stats(self) -> dict:
        """Get statistics for all queues."""
        stats = {
            "critical": await self._client.llen(self.QUEUE_CRITICAL),
            "high": await self._client.llen(self.QUEUE_HIGH),
            "normal": await self._client.llen(self.QUEUE_NORMAL),
            "low": await self._client.llen(self.QUEUE_LOW),
            "processing": await self._client.scard(self.PROCESSING_SET),
            "delayed": await self._client.zcard(self.DELAYED_ZSET)
        }
        stats["total"] = sum([
            stats["critical"],
            stats["high"],
            stats["normal"],
            stats["low"]
        ])
        return stats
    
    # Worker Management
    async def register_worker(self, worker_id: str, hostname: str) -> None:
        """Register a worker."""
        await self._client.hset(
            f"workers:{worker_id}",
            mapping={
                "hostname": hostname,
                "status": "idle",
                "last_heartbeat": datetime.utcnow().isoformat()
            }
        )
        await self._client.sadd("workers:active", worker_id)
    
    async def worker_heartbeat(self, worker_id: str, status: str = "idle") -> None:
        """Update worker heartbeat."""
        await self._client.hset(
            f"workers:{worker_id}",
            mapping={
                "status": status,
                "last_heartbeat": datetime.utcnow().isoformat()
            }
        )
    
    async def get_active_workers(self) -> list[str]:
        """Get list of active worker IDs."""
        return list(await self._client.smembers("workers:active"))
    
    # Pub/Sub for real-time updates
    async def publish_event(self, event_type: str, data: dict) -> None:
        """Publish event to jobs channel."""
        await self._client.publish("jobs:events", json.dumps({
            "event": event_type,
            **data
        }))
    
    async def subscribe_events(self):
        """Subscribe to job events."""
        pubsub = self._client.pubsub()
        await pubsub.subscribe("jobs:events")
        return pubsub


# Global Redis client instance
redis_client = RedisClient()
