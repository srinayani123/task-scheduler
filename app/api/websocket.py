"""
WebSocket endpoint for real-time job updates.
"""

import asyncio
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.core.redis_client import redis_client


router = APIRouter()


class ConnectionManager:
    """Manages WebSocket connections."""
    
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        """Send message to all connected clients."""
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        # Remove disconnected clients
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


@router.websocket("/ws/jobs")
async def websocket_jobs(websocket: WebSocket):
    """
    WebSocket endpoint for real-time job updates.
    
    Clients receive events like:
    - job_queued
    - job_started
    - job_completed
    - job_failed
    - task_started
    - task_completed
    - task_failed
    - task_retry_scheduled
    """
    await manager.connect(websocket)
    
    # Create Redis subscription task
    async def redis_listener():
        try:
            pubsub = await redis_client.subscribe_events()
            
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    await manager.broadcast(data)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Redis listener error: {e}")
    
    listener_task = asyncio.create_task(redis_listener())
    
    try:
        while True:
            # Keep connection alive, handle client messages
            data = await websocket.receive_text()
            
            # Handle ping/pong
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        listener_task.cancel()
        
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)
        listener_task.cancel()


@router.websocket("/ws/job/{run_id}")
async def websocket_job_detail(websocket: WebSocket, run_id: str):
    """
    WebSocket endpoint for single job updates.
    Only receives events for the specified job run.
    """
    await manager.connect(websocket)
    
    async def redis_listener():
        try:
            pubsub = await redis_client.subscribe_events()
            
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    
                    # Filter for this job only
                    if data.get("job_run_id") == run_id:
                        await websocket.send_json(data)
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Redis listener error: {e}")
    
    listener_task = asyncio.create_task(redis_listener())
    
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        listener_task.cancel()
        
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)
        listener_task.cancel()
