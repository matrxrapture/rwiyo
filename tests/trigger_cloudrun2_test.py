# tests/test_cloud_run_dag.py
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
import requests
from airflow.exceptions import AirflowFailException
from airflow.models.dagbag import DagBag

from trigger_cloud_run_dag import trigger_cloud_run_with_assertion


def test_dag_loaded():
    dagbag = DagBag(include_examples=False)
    dag = dagbag.get_dag(dag_id="trigger_cloud_run_with_assertion")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4


def test_call_cloud_run_success(mocker):
    dag = trigger_cloud_run_with_assertion()
    task = dag.get_task("call_cloud_run")
    mock_requests = mocker.patch("requests.post")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"batch_id": "123", "other": "data"}
    mock_requests.return_value = mock_response

    result = task.function(
        cloud_run_url="http://test.cloudrun",
        json_payload='{"key": "value"}',
        pipeline_id="test_pipeline",
    )

    assert result == {"batch_id": "123", "other": "data"}
    mock_requests.assert_called_once_with(
        "http://test.cloudrun",
        headers={"Content-Type": "application/json"},
        data='{"key": "value"}',
        timeout=60,
    )


def test_call_cloud_run_no_batch_id(mocker):
    dag = trigger_cloud_run_with_assertion()
    task = dag.get_task("call_cloud_run")
    mock_requests = mocker.patch("requests.post")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"no_batch_id": "data"}
    mock_requests.return_value = mock_response

    with pytest.raises(AirflowFailException) as excinfo:
        task.function(
            cloud_run_url="http://test.cloudrun",
            json_payload='{"key": "value"}',
            pipeline_id="test_pipeline",
        )

    assert "Cloud Run response does not contain 'batch_id'" in str(excinfo.value)


def test_call_cloud_run_request_error(mocker):
    dag = trigger_cloud_run_with_assertion()
    task = dag.get_task("call_cloud_run")
    mock_requests = mocker.patch("requests.post")
    mock_requests.side_effect = requests.exceptions.RequestException("Test error")

    with pytest.raises(AirflowFailException) as excinfo:
        task.function(
            cloud_run_url="http://test.cloudrun",
            json_payload='{"key": "value"}',
            pipeline_id="test_pipeline",
        )

    assert "Error calling Cloud Run: Test error" in str(excinfo.value)


def test_notify_failure_success(mocker):
    dag = trigger_cloud_run_with_assertion()
    task = dag.get_task("notify_failure")
    mock_requests = mocker.patch("requests.post")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = "Notification sent"
    mock_requests.return_value = mock_response

    task.function(
        pipeline_id="test_pipeline",
        failure_details="test_failure",
        failure_notification_url="http://test.failure",
    )

    mock_requests.assert_called_once_with(
        "http://test.failure",
        headers={"Content-Type": "application/json"},
        data='{"pipeline_id": "test_pipeline", "failure_details": "test_failure", "status": "failed"}',
        timeout=30,
    )

def test_notify_failure_request_error(mocker):
    dag = trigger_cloud_run_with_assertion()
    task = dag.get_task("notify_failure")
    mock_requests = mocker.patch("requests.post")
    mock_requests.side_effect = requests.exceptions.RequestException("Test error")

    task.function(
        pipeline_id="test_pipeline",
        failure_details="test_failure",
        failure_notification_url="http://test.failure",
    )
    # the function should not raise an exception, but log the error.
    # We can check that the log was called by using caplog, but that is outside of the scope of this test.