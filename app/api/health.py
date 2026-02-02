"""
Health check endpoints.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text
from app.core.redis_client import redis_client
from app.core.database import engine


router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
    scheduler: str
    version: str = "1.0.0"


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Basic health check."""
    db_status = "healthy"
    redis_status = "healthy"
    scheduler_status = "healthy"
    
    # Check database
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        db_status = "unhealthy"
    
    # Check Redis
    try:
        await redis_client.client.ping()
    except Exception:
        redis_status = "unhealthy"
    
    # Overall status
    if db_status == "unhealthy" or redis_status == "unhealthy":
        overall = "degraded"
    else:
        overall = "healthy"
    
    return HealthResponse(
        status=overall,
        database=db_status,
        redis=redis_status,
        scheduler=scheduler_status
    )


@router.get("/ready")
async def readiness_check():
    """Kubernetes readiness probe."""
    try:
        await redis_client.client.ping()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"ready": True}
    except Exception:
        return {"ready": False}


@router.get("/live")
async def liveness_check():
    """Kubernetes liveness probe."""
    return {"alive": True}
