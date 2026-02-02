"""
Distributed Task Scheduler - Main Application

Production-ready task scheduler with:
- DAG-based task execution
- Retry with exponential backoff
- Dead letter queue
- Cron scheduling
- Real-time status via WebSocket
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.api import jobs, dags, workers, health, websocket
from app.core.database import init_db
from app.core.redis_client import redis_client
from app.services.scheduler import scheduler_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    print("Starting Task Scheduler...")
    
    # Initialize database
    await init_db()
    print("Database initialized")
    
    # Initialize Redis
    await redis_client.initialize()
    print("Redis connected")
    
    # Start scheduler background task
    await scheduler_service.start()
    print("Scheduler started")
    
    yield
    
    # Cleanup
    await scheduler_service.stop()
    await redis_client.close()
    print("Shutdown complete")


app = FastAPI(
    title=settings.app_name,
    description="Distributed Task Scheduler with DAG execution, retries, and dead letter queue",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
app.include_router(dags.router, prefix="/dags", tags=["DAGs"])
app.include_router(workers.router, prefix="/workers", tags=["Workers"])
app.include_router(websocket.router, tags=["WebSocket"])
