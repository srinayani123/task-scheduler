"""
Scheduler Service - Core scheduling logic.

Handles:
- Cron-based scheduling
- DAG dependency resolution
- Job dispatching to workers
- Delayed job processing
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from uuid import uuid4
from croniter import croniter
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis_client import redis_client
from app.models.models import (
    DAG, Task, JobRun, TaskExecution,
    JobStatus, JobPriority
)


class SchedulerService:
    """
    Main scheduler service.
    
    Responsibilities:
    - Check for scheduled DAGs (cron)
    - Move delayed jobs to queue when due
    - Dispatch ready tasks to workers
    """
    
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the scheduler background task."""
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
    
    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                async with AsyncSessionLocal() as db:
                    # Check scheduled DAGs
                    await self._check_scheduled_dags(db)
                    
                    # Process delayed jobs
                    await self._process_delayed_jobs()
                    
                    # Check for ready tasks in running jobs
                    await self._dispatch_ready_tasks(db)
                    
                await asyncio.sleep(settings.scheduler_interval_seconds)
                
            except Exception as e:
                print(f"Scheduler error: {e}")
                await asyncio.sleep(5)  # Back off on error
    
    async def _check_scheduled_dags(self, db: AsyncSession) -> None:
        """Check for DAGs due for scheduled execution."""
        now = datetime.utcnow()
        
        # Find active DAGs with cron that are due
        result = await db.execute(
            select(DAG).where(
                and_(
                    DAG.is_active == True,
                    DAG.is_paused == False,
                    DAG.cron_expression.isnot(None),
                    DAG.next_run_at <= now
                )
            )
        )
        
        dags = result.scalars().all()
        
        for dag in dags:
            # Create new job run
            await self.trigger_dag(db, dag.id, triggered_by="schedule")
            
            # Calculate next run time
            cron = croniter(dag.cron_expression, now)
            dag.next_run_at = cron.get_next(datetime)
            dag.last_run_at = now
        
        await db.commit()
    
    async def _process_delayed_jobs(self) -> None:
        """Move due delayed jobs to the execution queue."""
        due_jobs = await redis_client.get_due_delayed_jobs()
        
        for job_data in due_jobs:
            priority = job_data.get("priority", "normal")
            await redis_client.enqueue_job(job_data, priority)
    
    async def _dispatch_ready_tasks(self, db: AsyncSession) -> None:
        """Find and dispatch tasks that are ready to run."""
        # Find running job runs
        result = await db.execute(
            select(JobRun).where(JobRun.status == JobStatus.RUNNING)
        )
        job_runs = result.scalars().all()
        
        for job_run in job_runs:
            await self._dispatch_job_tasks(db, job_run)
    
    async def _dispatch_job_tasks(
        self,
        db: AsyncSession,
        job_run: JobRun
    ) -> None:
        """Dispatch ready tasks for a job run."""
        # Get all task executions for this run
        result = await db.execute(
            select(TaskExecution).where(TaskExecution.job_run_id == job_run.id)
        )
        executions = {e.task_id: e for e in result.scalars().all()}
        
        # Get DAG tasks
        result = await db.execute(
            select(Task).where(Task.dag_id == job_run.dag_id)
        )
        tasks = result.scalars().all()
        task_map = {t.name: t for t in tasks}
        
        # Find tasks ready to run
        for task in tasks:
            execution = executions.get(task.id)
            
            if not execution or execution.status != JobStatus.PENDING:
                continue
            
            # Check dependencies
            deps_satisfied = True
            for dep_name in task.dependencies:
                dep_task = task_map.get(dep_name)
                if dep_task:
                    dep_exec = executions.get(dep_task.id)
                    if not dep_exec or dep_exec.status != JobStatus.SUCCESS:
                        deps_satisfied = False
                        break
            
            if deps_satisfied:
                # Queue the task
                await self._queue_task_execution(db, job_run, task, execution)
    
    async def _queue_task_execution(
        self,
        db: AsyncSession,
        job_run: JobRun,
        task: Task,
        execution: TaskExecution
    ) -> None:
        """Queue a task for execution."""
        execution.status = JobStatus.QUEUED
        await db.commit()
        
        # Get DAG for context
        dag = await db.get(DAG, job_run.dag_id)
        
        job_data = {
            "job_run_id": job_run.run_id,
            "task_execution_id": execution.id,
            "task_id": task.id,
            "task_name": task.name,
            "task_type": task.task_type,
            "config": task.config,
            "dag_name": dag.name,
            "attempt": execution.attempt,
            "max_retries": task.max_retries or settings.max_retries,
            "timeout": task.timeout or settings.worker_timeout,
            "priority": job_run.priority.value
        }
        
        await redis_client.enqueue_job(job_data, job_run.priority.value)
        
        await redis_client.publish_event("task_queued", {
            "job_run_id": job_run.run_id,
            "task_name": task.name
        })
    
    async def trigger_dag(
        self,
        db: AsyncSession,
        dag_id: int,
        triggered_by: str = "manual",
        priority: JobPriority = JobPriority.NORMAL
    ) -> JobRun:
        """Trigger a new DAG run."""
        # Get DAG and tasks
        dag = await db.get(DAG, dag_id)
        if not dag:
            raise ValueError(f"DAG {dag_id} not found")
        
        result = await db.execute(
            select(Task).where(Task.dag_id == dag_id)
        )
        tasks = result.scalars().all()
        
        if not tasks:
            raise ValueError(f"DAG {dag_id} has no tasks")
        
        # Create job run
        job_run = JobRun(
            dag_id=dag_id,
            run_id=str(uuid4()),
            status=JobStatus.RUNNING,
            priority=priority,
            triggered_by=triggered_by,
            started_at=datetime.utcnow()
        )
        db.add(job_run)
        await db.flush()
        
        # Create task executions
        for task in tasks:
            execution = TaskExecution(
                job_run_id=job_run.id,
                task_id=task.id,
                status=JobStatus.PENDING,
                attempt=1
            )
            db.add(execution)
        
        await db.commit()
        
        await redis_client.publish_event("dag_triggered", {
            "dag_name": dag.name,
            "job_run_id": job_run.run_id,
            "triggered_by": triggered_by
        })
        
        return job_run


# Global scheduler instance
scheduler_service = SchedulerService()
