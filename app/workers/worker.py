"""
Worker Service - Processes jobs from the queue.

Workers:
- Pull jobs from Redis queue
- Execute tasks
- Handle retries and failures
- Report status via heartbeat
"""

import asyncio
import socket
from datetime import datetime
from uuid import uuid4
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis_client import redis_client
from app.models.models import (
    TaskExecution, JobRun, Worker,
    JobStatus
)
from app.services.executor import (
    task_executor,
    retry_handler,
    dead_letter_handler,
    job_completion_handler
)


class WorkerService:
    """
    Worker process that executes tasks from the queue.
    """
    
    def __init__(self, worker_id: str = None, max_concurrent: int = None):
        self.worker_id = worker_id or str(uuid4())
        self.hostname = socket.gethostname()
        self.max_concurrent = max_concurrent or settings.max_concurrent_jobs
        
        self._running = False
        self._current_tasks = 0
        self._tasks_completed = 0
        self._tasks_failed = 0
        
        self._worker_task: asyncio.Task = None
        self._heartbeat_task: asyncio.Task = None
    
    async def start(self) -> None:
        """Start the worker."""
        self._running = True
        
        # Register worker
        await self._register()
        
        # Start worker loop
        self._worker_task = asyncio.create_task(self._worker_loop())
        
        # Start heartbeat
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        print(f"Worker {self.worker_id} started on {self.hostname}")
    
    async def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        # Deregister worker
        await self._deregister()
        
        print(f"Worker {self.worker_id} stopped")
    
    async def _register(self) -> None:
        """Register worker in database and Redis."""
        async with AsyncSessionLocal() as db:
            worker = Worker(
                worker_id=self.worker_id,
                hostname=self.hostname,
                status="idle",
                max_concurrent_tasks=self.max_concurrent
            )
            db.add(worker)
            await db.commit()
        
        await redis_client.register_worker(self.worker_id, self.hostname)
    
    async def _deregister(self) -> None:
        """Remove worker from active set."""
        await redis_client.client.srem("workers:active", self.worker_id)
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running:
            try:
                status = "busy" if self._current_tasks > 0 else "idle"
                await redis_client.worker_heartbeat(self.worker_id, status)
                
                # Update database
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(Worker).where(
                            Worker.worker_id == self.worker_id
                        )
                    )
                    worker = result.scalar_one_or_none()
                    if worker:
                        worker.status = status
                        worker.last_heartbeat = datetime.utcnow()
                        worker.current_tasks = self._current_tasks
                        worker.tasks_completed = self._tasks_completed
                        worker.tasks_failed = self._tasks_failed
                        await db.commit()
                
                await asyncio.sleep(settings.worker_heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Heartbeat error: {e}")
                await asyncio.sleep(5)
    
    async def _worker_loop(self) -> None:
        """Main worker loop - pull and execute jobs."""
        while self._running:
            try:
                # Check if we can take more tasks
                if self._current_tasks >= self.max_concurrent:
                    await asyncio.sleep(0.5)
                    continue
                
                # Try to get a job
                job_data = await redis_client.dequeue_job()
                
                if job_data:
                    # Execute in background
                    asyncio.create_task(self._process_job(job_data))
                else:
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Worker loop error: {e}")
                await asyncio.sleep(1)
    
    async def _process_job(self, job_data: dict) -> None:
        """Process a single job."""
        self._current_tasks += 1
        
        task_execution_id = job_data.get("task_execution_id")
        
        try:
            async with AsyncSessionLocal() as db:
                # Get task execution
                execution = await db.get(TaskExecution, task_execution_id)
                if not execution:
                    print(f"Task execution {task_execution_id} not found")
                    return
                
                # Update status to running
                execution.status = JobStatus.RUNNING
                execution.started_at = datetime.utcnow()
                execution.worker_id = self.worker_id
                await db.commit()
                
                await redis_client.publish_event("task_started", {
                    "job_run_id": job_data.get("job_run_id"),
                    "task_name": job_data.get("task_name"),
                    "worker_id": self.worker_id
                })
                
                # Execute the task
                result = await task_executor.execute_task(job_data)
                
                # Handle result
                if result["success"]:
                    await self._handle_success(db, execution, job_data, result)
                else:
                    await self._handle_failure(db, execution, job_data, result)
                
                # Check if job run is complete
                await job_completion_handler.check_job_completion(
                    db, execution.job_run_id
                )
            
            # Mark job complete in Redis
            await redis_client.complete_job(job_data, result["success"])
            
        except Exception as e:
            print(f"Error processing job: {e}")
            self._tasks_failed += 1
            
            async with AsyncSessionLocal() as db:
                execution = await db.get(TaskExecution, task_execution_id)
                if execution:
                    await self._handle_failure(
                        db, execution, job_data,
                        {"success": False, "error": str(e), "result": None}
                    )
        
        finally:
            self._current_tasks -= 1
    
    async def _handle_success(
        self,
        db: AsyncSession,
        execution: TaskExecution,
        job_data: dict,
        result: dict
    ) -> None:
        """Handle successful task execution."""
        execution.status = JobStatus.SUCCESS
        execution.result = result["result"]
        execution.completed_at = datetime.utcnow()
        
        await db.commit()
        
        self._tasks_completed += 1
        
        await redis_client.publish_event("task_completed", {
            "job_run_id": job_data.get("job_run_id"),
            "task_name": job_data.get("task_name"),
            "status": "success"
        })
    
    async def _handle_failure(
        self,
        db: AsyncSession,
        execution: TaskExecution,
        job_data: dict,
        result: dict
    ) -> None:
        """Handle failed task execution."""
        error = result.get("error", "Unknown error")
        
        # Try to schedule retry
        retry_scheduled = await retry_handler.schedule_retry(
            db, execution, job_data, error
        )
        
        if not retry_scheduled:
            # Max retries exceeded - send to DLQ
            if settings.dlq_enabled:
                await dead_letter_handler.send_to_dlq(
                    db, execution, job_data, error
                )
            else:
                execution.status = JobStatus.FAILED
                execution.error = error
                execution.completed_at = datetime.utcnow()
                await db.commit()
            
            self._tasks_failed += 1
            
            await redis_client.publish_event("task_failed", {
                "job_run_id": job_data.get("job_run_id"),
                "task_name": job_data.get("task_name"),
                "error": error
            })


def create_worker(
    worker_id: str = None,
    max_concurrent: int = None
) -> WorkerService:
    """Factory function to create a worker."""
    return WorkerService(worker_id, max_concurrent)
