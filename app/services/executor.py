"""
Task Executor Service.

Handles:
- Task execution (HTTP, Python, Shell)
- Retry with exponential backoff
- Dead letter queue
- Result handling
"""

import asyncio
import httpx
import subprocess
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import select, and_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis_client import redis_client
from app.models.models import (
    TaskExecution, JobRun, Task, DAG, DeadLetterJob,
    JobStatus
)


class TaskExecutor:
    """
    Executes tasks with retry logic and error handling.
    """
    
    def __init__(self):
        self._http_client = httpx.AsyncClient(timeout=settings.worker_timeout)
    
    async def close(self):
        """Close HTTP client."""
        await self._http_client.aclose()
    
    async def execute_task(self, job_data: dict) -> dict:
        """
        Execute a task based on its type.
        
        Returns:
            dict with 'success', 'result', and 'error' keys
        """
        task_type = job_data.get("task_type")
        config = job_data.get("config", {})
        
        try:
            if task_type == "http":
                return await self._execute_http_task(config)
            elif task_type == "python":
                return await self._execute_python_task(config)
            elif task_type == "shell":
                return await self._execute_shell_task(config)
            else:
                return {
                    "success": False,
                    "result": None,
                    "error": f"Unknown task type: {task_type}"
                }
        except Exception as e:
            return {
                "success": False,
                "result": None,
                "error": str(e)
            }
    
    async def _execute_http_task(self, config: dict) -> dict:
        """Execute HTTP request task."""
        url = config.get("url")
        method = config.get("method", "GET").upper()
        headers = config.get("headers", {})
        body = config.get("body")
        
        if not url:
            return {
                "success": False,
                "result": None,
                "error": "URL is required for HTTP task"
            }
        
        response = await self._http_client.request(
            method=method,
            url=url,
            headers=headers,
            json=body if body else None
        )
        
        success = 200 <= response.status_code < 300
        
        return {
            "success": success,
            "result": {
                "status_code": response.status_code,
                "body": response.text[:1000]  # Limit result size
            },
            "error": None if success else f"HTTP {response.status_code}"
        }
    
    async def _execute_python_task(self, config: dict) -> dict:
        """Execute Python function task."""
        # In production, this would use a sandboxed executor
        # For demo, just simulate execution
        code = config.get("code", "")
        
        # Simulated execution
        await asyncio.sleep(0.5)  # Simulate work
        
        return {
            "success": True,
            "result": {"executed": True, "code_length": len(code)},
            "error": None
        }
    
    async def _execute_shell_task(self, config: dict) -> dict:
        """Execute shell command task."""
        command = config.get("command")
        
        if not command:
            return {
                "success": False,
                "result": None,
                "error": "Command is required for shell task"
            }
        
        # Execute with timeout
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=settings.worker_timeout
            )
            
            success = process.returncode == 0
            
            return {
                "success": success,
                "result": {
                    "return_code": process.returncode,
                    "stdout": stdout.decode()[:1000],
                    "stderr": stderr.decode()[:500]
                },
                "error": None if success else stderr.decode()[:500]
            }
            
        except asyncio.TimeoutError:
            return {
                "success": False,
                "result": None,
                "error": f"Command timed out after {settings.worker_timeout}s"
            }


class RetryHandler:
    """
    Handles retry logic with exponential backoff.
    """
    
    @staticmethod
    def calculate_next_retry(attempt: int) -> datetime:
        """
        Calculate next retry time using exponential backoff.
        
        Formula: base_delay * (exponential_base ^ attempt)
        With jitter and max delay cap.
        """
        import random
        
        delay = settings.retry_base_delay * (
            settings.retry_exponential_base ** (attempt - 1)
        )
        
        # Cap at max delay
        delay = min(delay, settings.retry_max_delay)
        
        # Add jitter (±10%)
        jitter = delay * 0.1 * (2 * random.random() - 1)
        delay = delay + jitter
        
        return datetime.utcnow() + timedelta(seconds=delay)
    
    @staticmethod
    async def schedule_retry(
        db: AsyncSession,
        execution: TaskExecution,
        job_data: dict,
        error: str
    ) -> bool:
        """
        Schedule a retry for failed task.
        
        Returns True if retry scheduled, False if max retries exceeded.
        """
        max_retries = job_data.get("max_retries", settings.max_retries)
        
        if execution.attempt >= max_retries:
            return False
        
        # Update execution for retry
        execution.status = JobStatus.RETRYING
        execution.attempt += 1
        execution.error = error
        execution.next_retry_at = RetryHandler.calculate_next_retry(
            execution.attempt
        )
        
        await db.commit()
        
        # Schedule delayed job
        job_data["attempt"] = execution.attempt
        await redis_client.schedule_delayed_job(
            job_data,
            execution.next_retry_at
        )
        
        await redis_client.publish_event("task_retry_scheduled", {
            "job_run_id": job_data.get("job_run_id"),
            "task_name": job_data.get("task_name"),
            "attempt": execution.attempt,
            "next_retry_at": execution.next_retry_at.isoformat()
        })
        
        return True


class DeadLetterHandler:
    """
    Handles dead letter queue operations.
    """
    
    @staticmethod
    async def send_to_dlq(
        db: AsyncSession,
        execution: TaskExecution,
        job_data: dict,
        error: str
    ) -> DeadLetterJob:
        """Send failed task to dead letter queue."""
        dlq_entry = DeadLetterJob(
            original_job_run_id=execution.job_run_id,
            original_task_execution_id=execution.id,
            dag_name=job_data.get("dag_name", "unknown"),
            task_name=job_data.get("task_name", "unknown"),
            error=error,
            attempts=execution.attempt,
            last_attempt_at=datetime.utcnow(),
            task_config=job_data.get("config", {})
        )
        
        db.add(dlq_entry)
        
        # Update execution status
        execution.status = JobStatus.DEAD_LETTER
        execution.error = error
        execution.completed_at = datetime.utcnow()
        
        await db.commit()
        
        await redis_client.publish_event("task_dead_lettered", {
            "job_run_id": job_data.get("job_run_id"),
            "task_name": job_data.get("task_name"),
            "error": error,
            "dlq_id": dlq_entry.id
        })
        
        return dlq_entry
    
    @staticmethod
    async def replay_dlq_entry(
        db: AsyncSession,
        dlq_id: int
    ) -> Optional[dict]:
        """Replay a dead letter queue entry."""
        dlq_entry = await db.get(DeadLetterJob, dlq_id)
        if not dlq_entry or dlq_entry.is_resolved:
            return None
        
        # Create new job data for replay
        job_data = {
            "task_name": dlq_entry.task_name,
            "dag_name": dlq_entry.dag_name,
            "config": dlq_entry.task_config,
            "is_replay": True,
            "original_dlq_id": dlq_id
        }
        
        # Mark as resolved
        dlq_entry.is_resolved = True
        dlq_entry.resolved_at = datetime.utcnow()
        dlq_entry.resolution_notes = "Replayed"
        
        await db.commit()
        
        return job_data


class JobCompletionHandler:
    """
    Handles job run completion logic.
    """
    
    @staticmethod
    async def check_job_completion(
        db: AsyncSession,
        job_run_id: int
    ) -> None:
        """Check if all tasks in job run are complete."""
        # Get job run
        job_run = await db.get(JobRun, job_run_id)
        if not job_run or job_run.status != JobStatus.RUNNING:
            return
        
        # Get all task executions
        result = await db.execute(
            select(TaskExecution).where(
                TaskExecution.job_run_id == job_run_id
            )
        )
        executions = result.scalars().all()
        
        # Check statuses
        all_complete = True
        any_failed = False
        
        for execution in executions:
            if execution.status in [
                JobStatus.PENDING,
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                JobStatus.RETRYING
            ]:
                all_complete = False
                break
            
            if execution.status in [JobStatus.FAILED, JobStatus.DEAD_LETTER]:
                any_failed = True
        
        if all_complete:
            job_run.status = JobStatus.FAILED if any_failed else JobStatus.SUCCESS
            job_run.completed_at = datetime.utcnow()
            
            await db.commit()
            
            await redis_client.publish_event("job_completed", {
                "job_run_id": job_run.run_id,
                "status": job_run.status.value,
                "completed_at": job_run.completed_at.isoformat()
            })


# Global instances
task_executor = TaskExecutor()
retry_handler = RetryHandler()
dead_letter_handler = DeadLetterHandler()
job_completion_handler = JobCompletionHandler()
