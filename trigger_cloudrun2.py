from __future__ import annotations

import json
import logging

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.empty import Empty
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["cloudrun"],
    params={
        "cloud_run_url": "YOUR_CLOUD_RUN_URL_HERE",  # Replace with your Cloud Run URL
        "json_payload": '{"key": "value"}',  # Replace with your default JSON payload
        "pipeline_id": "YOUR_PIPELINE_ID_HERE",  # Replace with your default pipeline ID
        "failure_notification_url": "YOUR_FAILURE_NOTIFICATION_URL_HERE",  # Replace with your failure notification Cloud Run URL
    },
)
def trigger_cloud_run_with_assertion():
    @task(retries=3)
    def call_cloud_run(cloud_run_url: str, json_payload: str, pipeline_id: str):
        """
        Triggers the Cloud Run URL with the provided payload and checks for batch_id.
        """
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(cloud_run_url, headers=headers, data=json_payload, timeout=60)
            response.raise_for_status()  # Raise an exception for bad status codes
            response_json = response.json()
            if "batch_id" not in response_json:
                raise AirflowFailException(
                    f"Cloud Run response does not contain 'batch_id': {response_json}"
                )
            log.info(f"Cloud Run triggered successfully. Response: {response_json}")
            return response_json
        except requests.exceptions.RequestException as e:
            raise AirflowFailException(f"Error calling Cloud Run: {e}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_failure(pipeline_id: str, failure_details: str, failure_notification_url: str):
        """
        Triggers a Cloud Run URL to notify about the failure.
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "pipeline_id": pipeline_id,
            "failure_details": failure_details,
            "status": "failed",
        }
        try:
            response = requests.post(
                failure_notification_url, headers=headers, data=json.dumps(payload), timeout=30
            )
            response.raise_for_status()
            log.info(f"Failure notification sent successfully. Response: {response.text}")
        except requests.exceptions.RequestException as e:
            log.error(f"Error sending failure notification: {e}")

    start = Empty(task_id="start")
    end = Empty(task_id="end")

    trigger_task = call_cloud_run(
        cloud_run_url="{{ params.cloud_run_url }}",
        json_payload="{{ params.json_payload }}",
        pipeline_id="{{ params.pipeline_id }}",
    )

    failure_notification_task = notify_failure.override(trigger_rule=TriggerRule.ONE_FAILED)(
        pipeline_id="{{ params.pipeline_id }}",
        failure_details="{{ ti.error }}",
        failure_notification_url="{{ params.failure_notification_url }}",
    )

    chain(start, trigger_task, end)
    chain(trigger_task, failure_notification_task)


trigger_cloud_run_dag = trigger_cloud_run_with_assertion()