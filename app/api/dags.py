"""
DAGs API endpoints.
"""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from croniter import croniter

from app.core.database import get_db
from app.models.models import DAG, Task


router = APIRouter()


# Request/Response Models
class TaskConfig(BaseModel):
    name: str
    task_type: str  # http, python, shell
    config: dict = {}
    dependencies: list[str] = []
    max_retries: Optional[int] = None
    retry_delay: Optional[int] = None
    timeout: Optional[int] = None


class CreateDAGRequest(BaseModel):
    name: str
    description: Optional[str] = None
    cron_expression: Optional[str] = None
    tasks: list[TaskConfig]


class UpdateDAGRequest(BaseModel):
    description: Optional[str] = None
    cron_expression: Optional[str] = None
    is_active: Optional[bool] = None
    is_paused: Optional[bool] = None


class TaskResponse(BaseModel):
    id: int
    name: str
    task_type: str
    config: dict
    dependencies: list[str]
    
    class Config:
        from_attributes = True


class DAGResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    cron_expression: Optional[str]
    is_active: bool
    is_paused: bool
    created_at: datetime
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    tasks: list[TaskResponse] = []
    
    class Config:
        from_attributes = True


class DAGListResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    cron_expression: Optional[str]
    is_active: bool
    is_paused: bool
    task_count: int
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    
    class Config:
        from_attributes = True


# Endpoints
@router.post("", response_model=DAGResponse, status_code=status.HTTP_201_CREATED)
async def create_dag(
    request: CreateDAGRequest,
    db: AsyncSession = Depends(get_db)
):
    """Create a new DAG with tasks."""
    # Validate cron expression
    if request.cron_expression:
        try:
            croniter(request.cron_expression)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid cron expression: {request.cron_expression}"
            )
    
    # Check for duplicate name
    result = await db.execute(
        select(DAG).where(DAG.name == request.name)
    )
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"DAG with name '{request.name}' already exists"
        )
    
    # Validate task dependencies
    task_names = {t.name for t in request.tasks}
    for task in request.tasks:
        for dep in task.dependencies:
            if dep not in task_names:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task '{task.name}' depends on unknown task '{dep}'"
                )
    
    # Detect cycles in dependencies
    if _has_cycle(request.tasks):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="DAG contains circular dependencies"
        )
    
    # Create DAG
    dag = DAG(
        name=request.name,
        description=request.description,
        cron_expression=request.cron_expression
    )
    
    # Calculate next run time if cron is set
    if request.cron_expression:
        cron = croniter(request.cron_expression, datetime.utcnow())
        dag.next_run_at = cron.get_next(datetime)
    
    db.add(dag)
    await db.flush()
    
    # Create tasks
    for task_config in request.tasks:
        task = Task(
            dag_id=dag.id,
            name=task_config.name,
            task_type=task_config.task_type,
            config=task_config.config,
            dependencies=task_config.dependencies,
            max_retries=task_config.max_retries,
            retry_delay=task_config.retry_delay,
            timeout=task_config.timeout
        )
        db.add(task)
    
    await db.commit()
    await db.refresh(dag)
    
    # Load tasks for response
    result = await db.execute(
        select(Task).where(Task.dag_id == dag.id)
    )
    tasks = result.scalars().all()
    
    return DAGResponse(
        id=dag.id,
        name=dag.name,
        description=dag.description,
        cron_expression=dag.cron_expression,
        is_active=dag.is_active,
        is_paused=dag.is_paused,
        created_at=dag.created_at,
        last_run_at=dag.last_run_at,
        next_run_at=dag.next_run_at,
        tasks=[
            TaskResponse(
                id=t.id,
                name=t.name,
                task_type=t.task_type,
                config=t.config,
                dependencies=t.dependencies
            )
            for t in tasks
        ]
    )


@router.get("", response_model=list[DAGListResponse])
async def list_dags(
    db: AsyncSession = Depends(get_db)
):
    """List all DAGs."""
    result = await db.execute(
        select(DAG).options(selectinload(DAG.tasks))
    )
    dags = result.scalars().all()
    
    return [
        DAGListResponse(
            id=dag.id,
            name=dag.name,
            description=dag.description,
            cron_expression=dag.cron_expression,
            is_active=dag.is_active,
            is_paused=dag.is_paused,
            task_count=len(dag.tasks),
            last_run_at=dag.last_run_at,
            next_run_at=dag.next_run_at
        )
        for dag in dags
    ]


@router.get("/{dag_id}", response_model=DAGResponse)
async def get_dag(
    dag_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get DAG details."""
    result = await db.execute(
        select(DAG).options(selectinload(DAG.tasks)).where(DAG.id == dag_id)
    )
    dag = result.scalar_one_or_none()
    
    if not dag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG {dag_id} not found"
        )
    
    return DAGResponse(
        id=dag.id,
        name=dag.name,
        description=dag.description,
        cron_expression=dag.cron_expression,
        is_active=dag.is_active,
        is_paused=dag.is_paused,
        created_at=dag.created_at,
        last_run_at=dag.last_run_at,
        next_run_at=dag.next_run_at,
        tasks=[
            TaskResponse(
                id=t.id,
                name=t.name,
                task_type=t.task_type,
                config=t.config,
                dependencies=t.dependencies
            )
            for t in dag.tasks
        ]
    )


@router.patch("/{dag_id}", response_model=DAGResponse)
async def update_dag(
    dag_id: int,
    request: UpdateDAGRequest,
    db: AsyncSession = Depends(get_db)
):
    """Update DAG settings."""
    dag = await db.get(DAG, dag_id)
    
    if not dag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG {dag_id} not found"
        )
    
    # Update fields
    if request.description is not None:
        dag.description = request.description
    
    if request.cron_expression is not None:
        try:
            croniter(request.cron_expression)
            dag.cron_expression = request.cron_expression
            cron = croniter(request.cron_expression, datetime.utcnow())
            dag.next_run_at = cron.get_next(datetime)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid cron expression: {request.cron_expression}"
            )
    
    if request.is_active is not None:
        dag.is_active = request.is_active
    
    if request.is_paused is not None:
        dag.is_paused = request.is_paused
    
    await db.commit()
    await db.refresh(dag)
    
    # Load tasks
    result = await db.execute(
        select(Task).where(Task.dag_id == dag.id)
    )
    tasks = result.scalars().all()
    
    return DAGResponse(
        id=dag.id,
        name=dag.name,
        description=dag.description,
        cron_expression=dag.cron_expression,
        is_active=dag.is_active,
        is_paused=dag.is_paused,
        created_at=dag.created_at,
        last_run_at=dag.last_run_at,
        next_run_at=dag.next_run_at,
        tasks=[
            TaskResponse(
                id=t.id,
                name=t.name,
                task_type=t.task_type,
                config=t.config,
                dependencies=t.dependencies
            )
            for t in tasks
        ]
    )


@router.delete("/{dag_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dag(
    dag_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a DAG and all its tasks."""
    dag = await db.get(DAG, dag_id)
    
    if not dag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG {dag_id} not found"
        )
    
    await db.delete(dag)
    await db.commit()


@router.post("/{dag_id}/pause")
async def pause_dag(
    dag_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Pause a DAG."""
    dag = await db.get(DAG, dag_id)
    
    if not dag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG {dag_id} not found"
        )
    
    dag.is_paused = True
    await db.commit()
    
    return {"message": f"DAG '{dag.name}' paused"}


@router.post("/{dag_id}/resume")
async def resume_dag(
    dag_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Resume a paused DAG."""
    dag = await db.get(DAG, dag_id)
    
    if not dag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG {dag_id} not found"
        )
    
    dag.is_paused = False
    await db.commit()
    
    return {"message": f"DAG '{dag.name}' resumed"}


def _has_cycle(tasks: list[TaskConfig]) -> bool:
    """Detect cycles in task dependencies using DFS."""
    graph = {t.name: set(t.dependencies) for t in tasks}
    
    visited = set()
    rec_stack = set()
    
    def dfs(node):
        visited.add(node)
        rec_stack.add(node)
        
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                if dfs(neighbor):
                    return True
            elif neighbor in rec_stack:
                return True
        
        rec_stack.remove(node)
        return False
    
    for node in graph:
        if node not in visited:
            if dfs(node):
                return True
    
    return False
