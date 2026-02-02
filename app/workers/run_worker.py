"""
Worker runner script.

Run with: python -m app.workers.run_worker
"""

import asyncio
import os
import signal

from app.core.database import init_db
from app.core.redis_client import redis_client
from app.workers.worker import create_worker


async def main():
    """Main worker entry point."""
    # Get worker config from environment
    worker_id = os.getenv("WORKER_ID")
    max_concurrent = int(os.getenv("MAX_CONCURRENT_JOBS", "4"))
    
    # Initialize connections
    await init_db()
    await redis_client.initialize()
    
    # Create and start worker
    worker = create_worker(worker_id, max_concurrent)
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    
    def shutdown():
        print("Shutdown signal received...")
        asyncio.create_task(worker.stop())
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)
    
    try:
        await worker.start()
        
        # Keep running until stopped
        while worker._running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        pass
    finally:
        await worker.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
