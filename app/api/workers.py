"""
Workers API endpoints.
"""

from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis_client import redis_client
from app.models.models import Worker, DeadLetterJob
from app.services.executor import dead_letter_handler


router = APIRouter()


# Response Models
class WorkerResponse(BaseModel):
    id: int
    worker_id: str
    hostname: str
    status: str
    current_tasks: int
    max_concurrent_tasks: int
    tasks_completed: int
    tasks_failed: int
    last_heartbeat: datetime
    is_healthy: bool
    
    class Config:
        from_attributes = True


class DeadLetterResponse(BaseModel):
    id: int
    dag_name: str
    task_name: str
    error: str
    attempts: int
    last_attempt_at: datetime
    is_resolved: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


class WorkerStatsResponse(BaseModel):
    total_workers: int
    active_workers: int
    idle_workers: int
    busy_workers: int
    total_tasks_completed: int
    total_tasks_failed: int


# Endpoints
@router.get("", response_model=list[WorkerResponse])
async def list_workers(
    db: AsyncSession = Depends(get_db)
):
    """List all registered workers."""
    result = await db.execute(select(Worker))
    workers = result.scalars().all()
    
    now = datetime.utcnow()
    timeout = timedelta(seconds=60)  # Consider offline after 60s without heartbeat
    
    return [
        WorkerResponse(
            id=w.id,
            worker_id=w.worker_id,
            hostname=w.hostname,
            status=w.status,
            current_tasks=w.current_tasks,
            max_concurrent_tasks=w.max_concurrent_tasks,
            tasks_completed=w.tasks_completed,
            tasks_failed=w.tasks_failed,
            last_heartbeat=w.last_heartbeat,
            is_healthy=(now - w.last_heartbeat) < timeout
        )
        for w in workers
    ]


@router.get("/stats", response_model=WorkerStatsResponse)
async def get_worker_stats(
    db: AsyncSession = Depends(get_db)
):
    """Get aggregated worker statistics."""
    result = await db.execute(select(Worker))
    workers = result.scalars().all()
    
    now = datetime.utcnow()
    timeout = timedelta(seconds=60)
    
    active_workers = [w for w in workers if (now - w.last_heartbeat) < timeout]
    
    return WorkerStatsResponse(
        total_workers=len(workers),
        active_workers=len(active_workers),
        idle_workers=len([w for w in active_workers if w.status == "idle"]),
        busy_workers=len([w for w in active_workers if w.status == "busy"]),
        total_tasks_completed=sum(w.tasks_completed for w in workers),
        total_tasks_failed=sum(w.tasks_failed for w in workers)
    )


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get worker details."""
    result = await db.execute(
        select(Worker).where(Worker.worker_id == worker_id)
    )
    worker = result.scalar_one_or_none()
    
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found"
        )
    
    now = datetime.utcnow()
    timeout = timedelta(seconds=60)
    
    return WorkerResponse(
        id=worker.id,
        worker_id=worker.worker_id,
        hostname=worker.hostname,
        status=worker.status,
        current_tasks=worker.current_tasks,
        max_concurrent_tasks=worker.max_concurrent_tasks,
        tasks_completed=worker.tasks_completed,
        tasks_failed=worker.tasks_failed,
        last_heartbeat=worker.last_heartbeat,
        is_healthy=(now - worker.last_heartbeat) < timeout
    )


# Dead Letter Queue Endpoints
@router.get("/dlq/entries", response_model=list[DeadLetterResponse])
async def list_dlq_entries(
    include_resolved: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """List dead letter queue entries."""
    query = select(DeadLetterJob)
    
    if not include_resolved:
        query = query.where(DeadLetterJob.is_resolved == False)
    
    query = query.order_by(DeadLetterJob.created_at.desc())
    
    result = await db.execute(query)
    entries = result.scalars().all()
    
    return [
        DeadLetterResponse(
            id=e.id,
            dag_name=e.dag_name,
            task_name=e.task_name,
            error=e.error,
            attempts=e.attempts,
            last_attempt_at=e.last_attempt_at,
            is_resolved=e.is_resolved,
            created_at=e.created_at
        )
        for e in entries
    ]


@router.post("/dlq/{dlq_id}/replay")
async def replay_dlq_entry(
    dlq_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Replay a dead letter queue entry."""
    job_data = await dead_letter_handler.replay_dlq_entry(db, dlq_id)
    
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DLQ entry {dlq_id} not found or already resolved"
        )
    
    # Queue the job
    await redis_client.enqueue_job(job_data, "normal")
    
    return {"message": f"DLQ entry {dlq_id} replayed"}


@router.post("/dlq/{dlq_id}/resolve")
async def resolve_dlq_entry(
    dlq_id: int,
    notes: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Mark a dead letter queue entry as resolved."""
    entry = await db.get(DeadLetterJob, dlq_id)
    
    if not entry:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DLQ entry {dlq_id} not found"
        )
    
    if entry.is_resolved:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"DLQ entry {dlq_id} is already resolved"
        )
    
    entry.is_resolved = True
    entry.resolved_at = datetime.utcnow()
    entry.resolution_notes = notes or "Manually resolved"
    
    await db.commit()
    
    return {"message": f"DLQ entry {dlq_id} resolved"}
