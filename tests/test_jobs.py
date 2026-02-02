"""
Tests for Jobs API.
"""

import pytest


class TestTriggerJob:
    """Tests for job triggering."""
    
    def test_trigger_job(self, client):
        """Trigger a job run."""
        # Create DAG first
        dag_resp = client.post("/dags", json={
            "name": "trigger-test",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = dag_resp.json()["id"]
        
        # Trigger job
        response = client.post("/jobs", json={
            "dag_id": dag_id,
            "priority": "normal"
        })
        
        assert response.status_code == 201
        data = response.json()
        assert data["dag_id"] == dag_id
        assert data["status"] == "running"
        assert data["triggered_by"] == "api"
    
    def test_trigger_job_with_priority(self, client):
        """Trigger job with high priority."""
        # Create DAG
        dag_resp = client.post("/dags", json={
            "name": "priority-test",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = dag_resp.json()["id"]
        
        # Trigger with high priority
        response = client.post("/jobs", json={
            "dag_id": dag_id,
            "priority": "high"
        })
        
        assert response.status_code == 201
        assert response.json()["priority"] == "high"
    
    def test_trigger_job_invalid_dag(self, client):
        """Trigger job for non-existent DAG."""
        response = client.post("/jobs", json={
            "dag_id": 9999,
            "priority": "normal"
        })
        
        assert response.status_code == 404
    
    def test_trigger_job_invalid_priority(self, client):
        """Trigger job with invalid priority."""
        # Create DAG
        dag_resp = client.post("/dags", json={
            "name": "invalid-priority-test",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = dag_resp.json()["id"]
        
        response = client.post("/jobs", json={
            "dag_id": dag_id,
            "priority": "invalid"
        })
        
        assert response.status_code == 400


class TestListJobs:
    """Tests for listing jobs."""
    
    def test_list_jobs(self, client):
        """List all jobs."""
        response = client.get("/jobs")
        
        assert response.status_code == 200
        assert isinstance(response.json(), list)
    
    def test_list_jobs_filter_status(self, client):
        """List jobs filtered by status."""
        response = client.get("/jobs?status=running")
        
        assert response.status_code == 200
    
    def test_list_jobs_invalid_status(self, client):
        """List jobs with invalid status filter."""
        response = client.get("/jobs?status=invalid")
        
        assert response.status_code == 400


class TestGetJob:
    """Tests for getting job details."""
    
    def test_get_job(self, client):
        """Get job details."""
        # Create and trigger
        dag_resp = client.post("/dags", json={
            "name": "get-test",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = dag_resp.json()["id"]
        
        job_resp = client.post("/jobs", json={"dag_id": dag_id})
        run_id = job_resp.json()["run_id"]
        
        # Get details
        response = client.get(f"/jobs/{run_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["job"]["run_id"] == run_id
        assert len(data["tasks"]) == 1
    
    def test_get_job_not_found(self, client):
        """Get non-existent job."""
        response = client.get("/jobs/nonexistent-id")
        
        assert response.status_code == 404


class TestCancelJob:
    """Tests for job cancellation."""
    
    def test_cancel_job(self, client):
        """Cancel a running job."""
        # Create and trigger
        dag_resp = client.post("/dags", json={
            "name": "cancel-test",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = dag_resp.json()["id"]
        
        job_resp = client.post("/jobs", json={"dag_id": dag_id})
        run_id = job_resp.json()["run_id"]
        
        # Cancel
        response = client.post(f"/jobs/{run_id}/cancel")
        
        assert response.status_code == 200
        
        # Verify
        get_resp = client.get(f"/jobs/{run_id}")
        assert get_resp.json()["job"]["status"] == "cancelled"


class TestQueueStats:
    """Tests for queue statistics."""
    
    def test_get_queue_stats(self, client):
        """Get queue statistics."""
        response = client.get("/jobs/queue/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "critical" in data
        assert "processing" in data
