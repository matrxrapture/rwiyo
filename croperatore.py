# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Operator to run a Google Cloud Run Job."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud.run_v2 import JobsClient
from google.cloud.run_v2.types import RunJobRequest

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SimpleCloudRunRunJobOperator(BaseOperator):
    """
    Executes a Google Cloud Run Job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudRunRunJobOperator`

    :param job_name: Required. The name of the Cloud Run job to run.
        Format: projects/{project}/locations/{location}/jobs/{job_name}
        It can also be provided without the full path, just the job name id.
        If the job name is provided without the full path, the project_id and
        region must be provided.
    :param region: Required. The region where the job is located.
    :param project_id: Required. The Google Cloud project ID where the job is located.
    :param overrides: Optional. Overrides for the job execution.
        See https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.RunJobRequest#google_cloud_run_v2_types_RunJobRequest_overrides
        Example:
        overrides = {
            "container_overrides": [
                {
                    "name": "my-container", # Must match the container name in the job template
                    "args": ["--new-arg", "new-value"],
                    "env": [{"name": "NEW_ENV_VAR", "value": "value"}],
                    "clear_args": False, # Set to True to clear original args
                }
            ],
            "task_count": 2, # Optional: Number of tasks to run
            "timeout": "600s", # Optional: Task timeout
        }
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, a string format is also accepted.
    :param wait_for_completion: (Optional) If True, the operator will wait for the job execution
        to complete before marking the task as success. Defaults to True.
    :param polling_interval_seconds: (Optional) The interval in seconds to poll for the job
        execution status. Used only if wait_for_completion is True. Defaults to 30 seconds.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "job_name",
        "overrides",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        job_name: str,
        region: str | None = None,
        project_id: str | None = None,
        overrides: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        wait_for_completion: bool = True,
        polling_interval_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.region = region
        self.project_id = project_id
        self.overrides = overrides or {}
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.wait_for_completion = wait_for_completion
        self.polling_interval_seconds = polling_interval_seconds
        self._job_execution_name: str | None = None # To store the execution name

    def _get_hook(self) -> GoogleBaseHook:
        """Helper function to get the GoogleBaseHook."""
        return GoogleBaseHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def _get_jobs_client(self) -> JobsClient:
        """Helper function to get the JobsClient."""
        credentials = self._get_hook().get_credentials()
        return JobsClient(credentials=credentials)

    def execute(self, context: Context) -> str | None:
        """Executes the operator."""
        hook = self._get_hook()
        self.project_id = self.project_id or hook.project_id
        if not self.project_id:
            raise AirflowException(
                "The project ID must be set, either as an operator parameter "
                "or in the Google Cloud connection."
            )
        if not self.region:
             raise AirflowException("The region must be set.")

        # If job_name doesn't contain projects/, construct the full path
        if "projects/" not in self.job_name:
            self.job_name = f"projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}"
        elif not self.region or not self.project_id:
             # Extract project_id and region if full path is given and they weren't provided explicitly
             parts = self.job_name.split('/')
             if len(parts) == 6 and parts[0] == 'projects' and parts[2] == 'locations' and parts[4] == 'jobs':
                 self.project_id = self.project_id or parts[1]
                 self.region = self.region or parts[3]
             else:
                 raise AirflowException(f"Invalid job_name format: {self.job_name}. Expected format: projects/{{project}}/locations/{{location}}/jobs/{{job_name}}")


        client = self._get_jobs_client()
        request = RunJobRequest(
            name=self.job_name,
            overrides=self.overrides,
            # Setting validate_only=False is the default, but explicit for clarity
            # validate_only=False,
        )

        self.log.info("Submitting Cloud Run Job: %s", self.job_name)
        self.log.info("Overrides: %s", self.overrides)

        try:
            operation = client.run_job(request=request)
            self.log.info("Cloud Run Job submitted. Operation name: %s", operation.operation.name)

            if self.wait_for_completion:
                self.log.info("Waiting for job execution to complete...")
                # The result() method on the LRO polls until completion
                # It returns the final state of the Execution resource
                # Default timeout is based on client configuration, can be overridden
                # result_timeout = 3600 # Example: 1 hour timeout for result()
                # execution = operation.result(timeout=result_timeout)
                execution = operation.result() # Use default polling timeout

                self._job_execution_name = execution.name
                self.log.info("Job Execution finished with name: %s", execution.name)
                self.log.debug("Full Execution details: %s", execution) # Log full details at debug level

                # Check execution status (simplified check)
                # See Execution resource for detailed conditions:
                # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.Execution
                # Common terminal conditions: JobSucceeded, JobFailed
                succeeded_condition = next((c for c in execution.conditions if c.type_ == "JobSucceeded"), None)

                if succeeded_condition and succeeded_condition.state == "CONDITION_SUCCEEDED":
                     self.log.info("Cloud Run Job execution %s succeeded.", execution.name)
                     # Push the execution name to XComs
                     context["ti"].xcom_push(key="job_execution_name", value=execution.name)
                     return execution.name # Return execution name on success
                else:
                    # Find the failure reason if available
                    failed_condition = next((c for c in execution.conditions if c.type_ == "JobFailed"), None)
                    failure_message = failed_condition.message if failed_condition else "Unknown failure reason."
                    self.log.error("Cloud Run Job execution %s failed. Reason: %s", execution.name, failure_message)
                    raise AirflowException(f"Cloud Run Job execution {execution.name} failed. Reason: {failure_message}")

            else:
                self.log.info("Job submitted, not waiting for completion.")
                # Push the LRO name to XComs if not waiting
                context["ti"].xcom_push(key="job_operation_name", value=operation.operation.name)
                return operation.operation.name # Return LRO name if not waiting

        except FailedPrecondition as e:
            self.log.error("FailedPrecondition error submitting job: %s", e)
            raise AirflowException(f"Failed to submit Cloud Run job {self.job_name}: {e}")
        except NotFound as e:
            self.log.error("Cloud Run Job not found: %s", self.job_name)
            raise AirflowException(f"Cloud Run Job {self.job_name} not found: {e}")
        except Exception as e:
            self.log.error("An unexpected error occurred: %s", e)
            raise AirflowException(f"An error occurred while running Cloud Run Job {self.job_name}: {e}")

# Example DAG usage:
"""
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
# from airflow.providers.google.cloud.operators.cloud_run import CloudRunRunJobOperator # Use your custom operator path

# Assuming the operator file is saved as 'custom_cloud_run_operator.py' in the 'plugins' folder
# or made available through other means in your Airflow environment.
# from plugins.custom_cloud_run_operator import SimpleCloudRunRunJobOperator

# Define project_id, region, and job_name
PROJECT_ID = "your-gcp-project-id"
REGION = "your-cloud-run-region" # e.g., us-central1
JOB_NAME = "your-cloud-run-job-name" # Just the name, not the full path

with DAG(
    dag_id="cloud_run_job_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["cloud-run", "example"],
) as dag:
    run_cloud_run_job = SimpleCloudRunRunJobOperator(
        task_id="run_my_cloud_run_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "container_overrides": [
                {
                    "name": "my-container", # Replace with your job's container name
                    "args": ["--input-date", "{{ ds }}"], # Example override using Jinja templating
                }
            ],
            "task_count": 1, # Optional: Run a single task instance
        },
        gcp_conn_id="google_cloud_default", # Ensure this connection exists in Airflow
        wait_for_completion=True, # Wait for the job to finish
    )
"""
