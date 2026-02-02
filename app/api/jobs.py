"""
Jobs API endpoints.
"""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy import select, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.core.redis_client import redis_client
from app.models.models import (
    JobRun, TaskExecution, DAG, Task,
    JobStatus, JobPriority
)
from app.services.scheduler import scheduler_service


router = APIRouter()


# Request/Response Models
class TriggerJobRequest(BaseModel):
    dag_id: int
    priority: Optional[str] = "normal"


class JobResponse(BaseModel):
    id: int
    run_id: str
    dag_id: int
    dag_name: Optional[str] = None
    status: str
    priority: str
    triggered_by: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class TaskExecutionResponse(BaseModel):
    id: int
    task_name: str
    status: str
    attempt: int
    error: Optional[str] = None
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class JobDetailResponse(BaseModel):
    job: JobResponse
    tasks: list[TaskExecutionResponse]


class QueueStatsResponse(BaseModel):
    critical: int
    high: int
    normal: int
    low: int
    processing: int
    delayed: int
    total: int


# Endpoints
@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def trigger_job(
    request: TriggerJobRequest,
    db: AsyncSession = Depends(get_db)
):
    """Trigger a new job run for a DAG."""
    # Validate priority
    try:
        priority = JobPriority(request.priority)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid priority: {request.priority}"
        )
    
    try:
        job_run = await scheduler_service.trigger_dag(
            db,
            request.dag_id,
            triggered_by="api",
            priority=priority
        )
        
        # Get DAG name
        dag = await db.get(DAG, request.dag_id)
        
        return JobResponse(
            id=job_run.id,
            run_id=job_run.run_id,
            dag_id=job_run.dag_id,
            dag_name=dag.name if dag else None,
            status=job_run.status.value,
            priority=job_run.priority.value,
            triggered_by=job_run.triggered_by,
            created_at=job_run.created_at,
            started_at=job_run.started_at
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    status_filter: Optional[str] = Query(None, alias="status"),
    dag_id: Optional[int] = None,
    limit: int = Query(20, le=100),
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """List job runs with optional filters."""
    query = select(JobRun).options(
        selectinload(JobRun.dag)
    ).order_by(desc(JobRun.created_at))
    
    # Apply filters
    if status_filter:
        try:
            job_status = JobStatus(status_filter)
            query = query.where(JobRun.status == job_status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status_filter}"
            )
    
    if dag_id:
        query = query.where(JobRun.dag_id == dag_id)
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    jobs = result.scalars().all()
    
    return [
        JobResponse(
            id=job.id,
            run_id=job.run_id,
            dag_id=job.dag_id,
            dag_name=job.dag.name if job.dag else None,
            status=job.status.value,
            priority=job.priority.value,
            triggered_by=job.triggered_by,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at
        )
        for job in jobs
    ]


@router.get("/queue/stats", response_model=QueueStatsResponse)
async def get_queue_stats():
    """Get queue statistics."""
    stats = await redis_client.get_queue_stats()
    return QueueStatsResponse(**stats)


@router.get("/{run_id}", response_model=JobDetailResponse)
async def get_job(
    run_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get job details including task executions."""
    result = await db.execute(
        select(JobRun)
        .options(selectinload(JobRun.dag))
        .where(JobRun.run_id == run_id)
    )
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {run_id} not found"
        )
    
    # Get task executions
    result = await db.execute(
        select(TaskExecution, Task)
        .join(Task)
        .where(TaskExecution.job_run_id == job.id)
        .order_by(TaskExecution.id)
    )
    task_executions = result.all()
    
    return JobDetailResponse(
        job=JobResponse(
            id=job.id,
            run_id=job.run_id,
            dag_id=job.dag_id,
            dag_name=job.dag.name if job.dag else None,
            status=job.status.value,
            priority=job.priority.value,
            triggered_by=job.triggered_by,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at
        ),
        tasks=[
            TaskExecutionResponse(
                id=exec.id,
                task_name=task.name,
                status=exec.status.value,
                attempt=exec.attempt,
                error=exec.error,
                worker_id=exec.worker_id,
                started_at=exec.started_at,
                completed_at=exec.completed_at
            )
            for exec, task in task_executions
        ]
    )


@router.post("/{run_id}/cancel")
async def cancel_job(
    run_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Cancel a running job."""
    result = await db.execute(
        select(JobRun).where(JobRun.run_id == run_id)
    )
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {run_id} not found"
        )
    
    if job.status not in [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.QUEUED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status {job.status.value}"
        )
    
    job.status = JobStatus.CANCELLED
    job.completed_at = datetime.utcnow()
    
    # Cancel pending task executions
    result = await db.execute(
        select(TaskExecution).where(
            and_(
                TaskExecution.job_run_id == job.id,
                TaskExecution.status.in_([
                    JobStatus.PENDING,
                    JobStatus.QUEUED,
                    JobStatus.RETRYING
                ])
            )
        )
    )
    executions = result.scalars().all()
    
    for exec in executions:
        exec.status = JobStatus.CANCELLED
        exec.completed_at = datetime.utcnow()
    
    await db.commit()
    
    await redis_client.publish_event("job_cancelled", {
        "job_run_id": run_id
    })
    
    return {"message": f"Job {run_id} cancelled"}
