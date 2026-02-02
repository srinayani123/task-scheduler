"""
Database models for Task Scheduler.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class JobStatus(str, Enum):
    """Job execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


class JobPriority(str, Enum):
    """Job priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class DAG(Base):
    """Directed Acyclic Graph for task dependencies."""
    __tablename__ = "dags"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Schedule (cron expression or null for manual)
    cron_expression: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # State
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_paused: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    next_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    tasks: Mapped[list["Task"]] = relationship("Task", back_populates="dag", cascade="all, delete-orphan")
    job_runs: Mapped[list["JobRun"]] = relationship("JobRun", back_populates="dag", cascade="all, delete-orphan")


class Task(Base):
    """Individual task within a DAG."""
    __tablename__ = "tasks"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    dag_id: Mapped[int] = mapped_column(Integer, ForeignKey("dags.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    
    # Task type and configuration
    task_type: Mapped[str] = mapped_column(String(50))  # http, python, shell
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    
    # Dependencies (list of task names)
    dependencies: Mapped[list] = mapped_column(JSON, default=list)
    
    # Retry configuration (overrides DAG defaults)
    max_retries: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    retry_delay: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    timeout: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    dag: Mapped["DAG"] = relationship("DAG", back_populates="tasks")


class JobRun(Base):
    """Single execution run of a DAG."""
    __tablename__ = "job_runs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    dag_id: Mapped[int] = mapped_column(Integer, ForeignKey("dags.id"), index=True)
    
    # Run info
    run_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)  # UUID
    status: Mapped[JobStatus] = mapped_column(SQLEnum(JobStatus), default=JobStatus.PENDING, index=True)
    priority: Mapped[JobPriority] = mapped_column(SQLEnum(JobPriority), default=JobPriority.NORMAL)
    
    # Trigger info
    triggered_by: Mapped[str] = mapped_column(String(50), default="manual")  # manual, schedule, api
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    dag: Mapped["DAG"] = relationship("DAG", back_populates="job_runs")
    task_executions: Mapped[list["TaskExecution"]] = relationship(
        "TaskExecution", back_populates="job_run", cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("ix_job_runs_status_created", "status", "created_at"),
    )


class TaskExecution(Base):
    """Execution record for a single task in a job run."""
    __tablename__ = "task_executions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_run_id: Mapped[int] = mapped_column(Integer, ForeignKey("job_runs.id"), index=True)
    task_id: Mapped[int] = mapped_column(Integer, ForeignKey("tasks.id"), index=True)
    
    # Execution state
    status: Mapped[JobStatus] = mapped_column(SQLEnum(JobStatus), default=JobStatus.PENDING, index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    
    # Results
    result: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Worker info
    worker_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="task_executions")
    task: Mapped["Task"] = relationship("Task")


class DeadLetterJob(Base):
    """Dead letter queue for failed jobs."""
    __tablename__ = "dead_letter_jobs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # Original job info
    original_job_run_id: Mapped[int] = mapped_column(Integer, index=True)
    original_task_execution_id: Mapped[int] = mapped_column(Integer, index=True)
    dag_name: Mapped[str] = mapped_column(String(255))
    task_name: Mapped[str] = mapped_column(String(255))
    
    # Failure info
    error: Mapped[str] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer)
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime)
    
    # Full context for replay
    task_config: Mapped[dict] = mapped_column(JSON)
    
    # State
    is_resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class Worker(Base):
    """Registered worker nodes."""
    __tablename__ = "workers"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    worker_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    hostname: Mapped[str] = mapped_column(String(255))
    
    # State
    status: Mapped[str] = mapped_column(String(20), default="idle")  # idle, busy, offline
    current_job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Capacity
    max_concurrent_tasks: Mapped[int] = mapped_column(Integer, default=4)
    current_tasks: Mapped[int] = mapped_column(Integer, default=0)
    
    # Heartbeat
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Stats
    tasks_completed: Mapped[int] = mapped_column(Integer, default=0)
    tasks_failed: Mapped[int] = mapped_column(Integer, default=0)
    
    # Timestamps
    registered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
