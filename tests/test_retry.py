"""
Tests for retry logic and dead letter queue.
"""

import pytest
from datetime import datetime, timedelta
from app.services.executor import RetryHandler


class TestRetryHandler:
    """Tests for retry logic."""
    
    def test_calculate_first_retry(self):
        """First retry should use base delay."""
        next_retry = RetryHandler.calculate_next_retry(1)
        
        now = datetime.utcnow()
        expected_min = now + timedelta(seconds=50)  # Base delay - jitter
        expected_max = now + timedelta(seconds=70)  # Base delay + jitter
        
        assert expected_min <= next_retry <= expected_max
    
    def test_calculate_exponential_backoff(self):
        """Subsequent retries should increase exponentially."""
        retry_1 = RetryHandler.calculate_next_retry(1)
        retry_2 = RetryHandler.calculate_next_retry(2)
        retry_3 = RetryHandler.calculate_next_retry(3)
        
        now = datetime.utcnow()
        
        # Each retry should be further in the future
        assert retry_1 < retry_2
        assert retry_2 < retry_3
    
    def test_max_delay_cap(self):
        """Retry delay should be capped at max_delay."""
        # With high attempt number, delay should still be capped
        next_retry = RetryHandler.calculate_next_retry(100)
        
        now = datetime.utcnow()
        max_expected = now + timedelta(seconds=3700)  # Max delay + jitter
        
        assert next_retry <= max_expected


class TestDeadLetterQueue:
    """Tests for dead letter queue API."""
    
    def test_list_dlq_entries(self, client):
        """List DLQ entries."""
        response = client.get("/workers/dlq/entries")
        
        assert response.status_code == 200
        assert isinstance(response.json(), list)
    
    def test_list_dlq_with_resolved(self, client):
        """List DLQ entries including resolved."""
        response = client.get("/workers/dlq/entries?include_resolved=true")
        
        assert response.status_code == 200
    
    def test_replay_dlq_not_found(self, client):
        """Replay non-existent DLQ entry."""
        response = client.post("/workers/dlq/9999/replay")
        
        assert response.status_code == 404
    
    def test_resolve_dlq_not_found(self, client):
        """Resolve non-existent DLQ entry."""
        response = client.post("/workers/dlq/9999/resolve")
        
        assert response.status_code == 404
