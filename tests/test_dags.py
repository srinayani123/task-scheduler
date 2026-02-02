"""
Tests for DAG API.
"""

import pytest


class TestCreateDAG:
    """Tests for DAG creation."""
    
    def test_create_simple_dag(self, client):
        """Create a simple DAG with one task."""
        response = client.post(
            "/dags",
            json={
                "name": "test-dag",
                "description": "Test DAG",
                "tasks": [
                    {
                        "name": "task1",
                        "task_type": "http",
                        "config": {"url": "https://example.com"}
                    }
                ]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "test-dag"
        assert len(data["tasks"]) == 1
    
    def test_create_dag_with_dependencies(self, client):
        """Create DAG with task dependencies."""
        response = client.post(
            "/dags",
            json={
                "name": "dag-with-deps",
                "tasks": [
                    {"name": "extract", "task_type": "http", "config": {}},
                    {"name": "transform", "task_type": "python", "config": {}, "dependencies": ["extract"]},
                    {"name": "load", "task_type": "http", "config": {}, "dependencies": ["transform"]}
                ]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert len(data["tasks"]) == 3
    
    def test_create_dag_with_invalid_dependency(self, client):
        """Reject DAG with invalid dependency."""
        response = client.post(
            "/dags",
            json={
                "name": "invalid-dag",
                "tasks": [
                    {"name": "task1", "task_type": "http", "config": {}, "dependencies": ["nonexistent"]}
                ]
            }
        )
        
        assert response.status_code == 400
        assert "unknown task" in response.json()["detail"]
    
    def test_create_dag_with_cycle(self, client):
        """Reject DAG with circular dependencies."""
        response = client.post(
            "/dags",
            json={
                "name": "cyclic-dag",
                "tasks": [
                    {"name": "a", "task_type": "http", "config": {}, "dependencies": ["c"]},
                    {"name": "b", "task_type": "http", "config": {}, "dependencies": ["a"]},
                    {"name": "c", "task_type": "http", "config": {}, "dependencies": ["b"]}
                ]
            }
        )
        
        assert response.status_code == 400
        assert "circular" in response.json()["detail"].lower()
    
    def test_create_dag_with_cron(self, client):
        """Create DAG with cron schedule."""
        response = client.post(
            "/dags",
            json={
                "name": "scheduled-dag",
                "cron_expression": "0 * * * *",  # Every hour
                "tasks": [
                    {"name": "task1", "task_type": "http", "config": {}}
                ]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["cron_expression"] == "0 * * * *"
        assert data["next_run_at"] is not None
    
    def test_create_dag_with_invalid_cron(self, client):
        """Reject DAG with invalid cron expression."""
        response = client.post(
            "/dags",
            json={
                "name": "invalid-cron-dag",
                "cron_expression": "invalid",
                "tasks": [
                    {"name": "task1", "task_type": "http", "config": {}}
                ]
            }
        )
        
        assert response.status_code == 400
        assert "cron" in response.json()["detail"].lower()


class TestListDAGs:
    """Tests for listing DAGs."""
    
    def test_list_empty(self, client):
        """List DAGs when none exist."""
        response = client.get("/dags")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_list_dags(self, client):
        """List DAGs after creating some."""
        # Create DAGs
        client.post("/dags", json={
            "name": "dag1",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        client.post("/dags", json={
            "name": "dag2",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        
        response = client.get("/dags")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2


class TestUpdateDAG:
    """Tests for updating DAGs."""
    
    def test_pause_dag(self, client):
        """Pause a DAG."""
        # Create DAG
        create_resp = client.post("/dags", json={
            "name": "pausable-dag",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = create_resp.json()["id"]
        
        # Pause
        response = client.post(f"/dags/{dag_id}/pause")
        
        assert response.status_code == 200
        
        # Verify
        get_resp = client.get(f"/dags/{dag_id}")
        assert get_resp.json()["is_paused"] == True
    
    def test_resume_dag(self, client):
        """Resume a paused DAG."""
        # Create and pause
        create_resp = client.post("/dags", json={
            "name": "resumable-dag",
            "tasks": [{"name": "t1", "task_type": "http", "config": {}}]
        })
        dag_id = create_resp.json()["id"]
        client.post(f"/dags/{dag_id}/pause")
        
        # Resume
        response = client.post(f"/dags/{dag_id}/resume")
        
        assert response.status_code == 200
        
        # Verify
        get_resp = client.get(f"/dags/{dag_id}")
        assert get_resp.json()["is_paused"] == False
