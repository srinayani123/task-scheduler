"""
Test configuration and fixtures.
"""

import os
# Set test database BEFORE importing app
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

import pytest
import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Now import app modules
from app.main import app
from app.core.database import get_db, Base
from app.core.redis_client import redis_client


# Test database
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL, 
    echo=False,
    connect_args={"check_same_thread": False}
)
TestSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False
)


class MockRedis:
    """Mock Redis client for testing."""
    
    def __init__(self):
        self._data = {}
        self._lists = {}
        self._sets = {}
        self._sorted_sets = {}
        self._hashes = {}
    
    async def ping(self):
        return True
    
    async def get(self, key):
        return self._data.get(key)
    
    async def set(self, key, value, ex=None):
        self._data[key] = str(value)
        return True
    
    async def lpush(self, key, *values):
        if key not in self._lists:
            self._lists[key] = []
        for v in values:
            self._lists[key].insert(0, v)
        return len(self._lists[key])
    
    async def brpop(self, keys, timeout=0):
        for key in keys:
            if key in self._lists and self._lists[key]:
                value = self._lists[key].pop()
                return (key, value)
        return None
    
    async def llen(self, key):
        return len(self._lists.get(key, []))
    
    async def sadd(self, key, *values):
        if key not in self._sets:
            self._sets[key] = set()
        self._sets[key].update(values)
        return len(values)
    
    async def srem(self, key, *values):
        if key in self._sets:
            for v in values:
                self._sets[key].discard(v)
        return len(values)
    
    async def scard(self, key):
        return len(self._sets.get(key, set()))
    
    async def smembers(self, key):
        return self._sets.get(key, set())
    
    async def zadd(self, key, mapping):
        if key not in self._sorted_sets:
            self._sorted_sets[key] = {}
        self._sorted_sets[key].update(mapping)
        return len(mapping)
    
    async def zrangebyscore(self, key, min_score, max_score):
        if key not in self._sorted_sets:
            return []
        return [
            k for k, v in self._sorted_sets[key].items()
            if v >= min_score and v <= max_score
        ]
    
    async def zrem(self, key, *values):
        if key in self._sorted_sets:
            for v in values:
                self._sorted_sets[key].pop(v, None)
        return len(values)
    
    async def zcard(self, key):
        return len(self._sorted_sets.get(key, {}))
    
    async def hset(self, key, mapping):
        if key not in self._hashes:
            self._hashes[key] = {}
        self._hashes[key].update(mapping)
        return len(mapping)
    
    async def publish(self, channel, message):
        return 1
    
    def pubsub(self):
        return MockPubSub()


class MockPubSub:
    """Mock Redis PubSub."""
    
    async def subscribe(self, *channels):
        pass
    
    async def listen(self):
        while False:
            yield {}


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    return MockRedis()


@pytest.fixture
def client(mock_redis):
    """Create test client with fresh database for each test."""
    import asyncio
    from app.core import database
    from app.models.models import Base
    
    # Override the engine and sessionmaker in the database module
    original_engine = database.engine
    original_session = database.AsyncSessionLocal
    
    database.engine = test_engine
    database.AsyncSessionLocal = TestSessionLocal
    
    # Clean database before each test
    async def reset_db():
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    
    asyncio.get_event_loop().run_until_complete(reset_db())
    
    # Override dependencies
    async def override_get_db():
        async with TestSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    redis_client._client = mock_redis
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client
    
    # Cleanup
    app.dependency_overrides.clear()
    database.engine = original_engine
    database.AsyncSessionLocal = original_session
    
@pytest.fixture
async def async_client(mock_redis):
    """Create async test client."""
    from app.core import database
    
    original_engine = database.engine
    original_session = database.AsyncSessionLocal
    
    database.engine = test_engine
    database.AsyncSessionLocal = TestSessionLocal
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async def override_get_db():
        async with TestSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    redis_client._client = mock_redis
    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    
    app.dependency_overrides.clear()
    database.engine = original_engine
    database.AsyncSessionLocal = original_session
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)