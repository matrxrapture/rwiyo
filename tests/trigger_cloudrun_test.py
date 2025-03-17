import json
import pytest
import responses
from datetime import datetime
from unittest import mock

# Import the functions from your DAG file
# Assuming the DAG file is named cloud_run_pipeline_dag.py
from cloud_run_pipeline_dag import (
    trigger_cloud_run,
    handle_failure,
    create_cloud_run_dag
)

# Mock data
TEST_CLOUD_RUN_URL = "https://example-service.run.app"
TEST_FAILURE_URL = "https://failure-handler.run.app"
TEST_PIPELINE_ID = "test-pipeline-123"
TEST_JSON_PAYLOAD = {"key": "value"}


class TestCloudRunPipelineDag:
    
    @responses.activate
    def test_trigger_cloud_run_success(self):
        # Mock the successful response from Cloud Run
        responses.add(
            responses.POST,
            TEST_CLOUD_RUN_URL,
            json={"batch_id": "batch-456", "status": "success"},
            status=200
        )
        
        # Mock the context with a task instance
        mock_ti = mock.MagicMock()
        mock_context = {"ti": mock_ti}
        
        # Call the function
        result = trigger_cloud_run(
            TEST_CLOUD_RUN_URL,
            TEST_JSON_PAYLOAD,
            TEST_PIPELINE_ID,
            **mock_context
        )
        
        # Assertions
        assert result["batch_id"] == "batch-456"
        assert result["status"] == "success"
        
        # Verify XCom push was called
        mock_ti.xcom_push.assert_called_once_with(
            key='cloud_run_result',
            value={"batch_id": "batch-456", "status": "success"}
        )
        
        # Verify request payload included pipeline_id
        request = responses.calls[0].request
        sent_payload = json.loads(request.body)
        assert sent_payload["pipeline_id"] == TEST_PIPELINE_ID
        assert sent_payload["key"] == "value"
    
    @responses.activate
    def test_trigger_cloud_run_no_batch_id(self):
        # Mock response without batch_id
        responses.add(
            responses.POST,
            TEST_CLOUD_RUN_URL,
            json={"status": "success"},  # No batch_id
            status=200
        )
        
        # Mock context
        mock_context = {"ti": mock.MagicMock()}
        
        # Test that it raises ValueError
        with pytest.raises(ValueError, match="Response does not contain 'batch_id'"):
            trigger_cloud_run(
                TEST_CLOUD_RUN_URL,
                TEST_JSON_PAYLOAD,
                TEST_PIPELINE_ID,
                **mock_context
            )
    
    @responses.activate
    def test_trigger_cloud_run_http_error(self):
        # Mock a failed HTTP response
        responses.add(
            responses.POST,
            TEST_CLOUD_RUN_URL,
            json={"error": "Internal server error"},
            status=500
        )
        
        # Mock context
        mock_context = {"ti": mock.MagicMock()}
        
        # Test that it raises an exception
        with pytest.raises(requests.exceptions.HTTPError):
            trigger_cloud_run(
                TEST_CLOUD_RUN_URL,
                TEST_JSON_PAYLOAD,
                TEST_PIPELINE_ID,
                **mock_context
            )
    
    @responses.activate
    def test_handle_failure(self):
        # Mock the response from failure handler
        responses.add(
            responses.POST,
            TEST_FAILURE_URL,
            json={"status": "failure_recorded"},
            status=200
        )
        
        # Mock context with exception and task instance
        mock_task_instance = mock.MagicMock()
        mock_task_instance.task_id = "trigger_cloud_run"
        
        mock_context = {
            "task_instance": mock_task_instance,
            "exception": "Test exception",
            "ti": mock.MagicMock()
        }
        
        # Call the function
        result = handle_failure(
            TEST_FAILURE_URL,
            TEST_PIPELINE_ID,
            **mock_context
        )
        
        # Assertions
        assert result["status"] == "failure_recorded"
        
        # Verify the payload sent to the failure handler
        request = responses.calls[0].request
        sent_payload = json.loads(request.body)
        assert sent_payload["pipeline_id"] == TEST_PIPELINE_ID
        assert sent_payload["status"] == "failed"
        assert sent_payload["task_id"] == "trigger_cloud_run"
        assert sent_payload["failure_details"] == "Test exception"
        assert "failure_time" in sent_payload
    
    @responses.activate
    def test_handle_failure_error(self):
        # Mock a failed response from failure handler
        responses.add(
            responses.POST,
            TEST_FAILURE_URL,
            json={"error": "Failed to record"},
            status=500
        )
        
        # Mock context
        mock_context = {
            "task_instance": mock.MagicMock(),
            "exception": "Test exception",
            "ti": mock.MagicMock()
        }
        
        # Call the function - should not raise an exception
        result = handle_failure(
            TEST_FAILURE_URL,
            TEST_PIPELINE_ID,
            **mock_context
        )
        
        # Verify it returns a status indicating failure reporting failed
        assert result["status"] == "failure_reporting_failed"
        assert "error" in result
    
    def test_create_cloud_run_dag(self):
        # Test DAG creation
        dag = create_cloud_run_dag(
            dag_id="test_dag",
            cloud_run_url=TEST_CLOUD_RUN_URL,
            failure_cloud_run_url=TEST_FAILURE_URL,
            json_payload=TEST_JSON_PAYLOAD,
            pipeline_id=TEST_PIPELINE_ID
        )
        
        # Verify DAG properties
        assert dag.dag_id == "test_dag"
        assert dag.description == f"Trigger Cloud Run service and handle failures for pipeline {TEST_PIPELINE_ID}"
        assert dag.schedule_interval == "@once"
        
        # Verify tasks
        tasks = dag.tasks
        assert len(tasks) == 1
        assert tasks[0].task_id == "trigger_cloud_run"
        assert tasks[0].on_failure_callback is not None

if __name__ == "__main__":
    pytest.main(["-v", "test_cloud_run_pipeline_dag.py"])