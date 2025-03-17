from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

def trigger_cloud_run(cloud_run_url, json_payload, pipeline_id, **context):
    """
    Triggers the Cloud Run service with the provided JSON payload.
    
    Args:
        cloud_run_url: URL of the Cloud Run service
        json_payload: JSON payload to send to the service
        pipeline_id: ID of the pipeline
    
    Returns:
        dict: Response from the Cloud Run service
    """
    # Add pipeline_id to the payload
    if isinstance(json_payload, str):
        payload = json.loads(json_payload)
    else:
        payload = json_payload.copy()
    
    payload['pipeline_id'] = pipeline_id
    
    # Make request to Cloud Run service
    try:
        response = requests.post(
            cloud_run_url,
            json=payload,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()  # Raise exception for non-2xx status codes
        
        # Parse response
        result = response.json()
        
        # Assert that batch_id exists in the response
        if 'batch_id' not in result:
            raise ValueError("Response does not contain 'batch_id'")
        
        # Push result to XCom for other tasks to use
        context['ti'].xcom_push(key='cloud_run_result', value=result)
        return result
    
    except Exception as e:
        # Log the error
        print(f"Error triggering Cloud Run: {str(e)}")
        # Re-raise to trigger retry or failure handling
        raise

def handle_failure(failure_cloud_run_url, pipeline_id, **context):
    """
    Handles failure by sending details to a second Cloud Run URL.
    
    Args:
        failure_cloud_run_url: URL of the failure handler Cloud Run service
        pipeline_id: ID of the pipeline
    """
    # Get task instance
    task_instance = context['ti']
    
    # Get information about the failed task
    failed_task_id = context.get('task_instance').task_id
    
    # Construct failure details
    failure_payload = {
        'pipeline_id': pipeline_id,
        'status': 'failed',
        'task_id': failed_task_id,
        'failure_details': str(context.get('exception', 'Unknown error')),
        'failure_time': datetime.now().isoformat(),
    }
    
    # Send failure details to the second Cloud Run URL
    try:
        response = requests.post(
            failure_cloud_run_url,
            json=failure_payload,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error sending failure details: {str(e)}")
        # We don't want to raise an exception here as this is the failure handler
        return {'status': 'failure_reporting_failed', 'error': str(e)}

def create_cloud_run_dag(dag_id, cloud_run_url, failure_cloud_run_url, json_payload, pipeline_id, schedule=None):
    """
    Creates a DAG that triggers a Cloud Run service and handles failures.
    
    Args:
        dag_id: ID for the DAG
        cloud_run_url: URL of the primary Cloud Run service
        failure_cloud_run_url: URL of the failure handler Cloud Run service
        json_payload: JSON payload to send to the primary service
        pipeline_id: ID of the pipeline
        schedule: Schedule interval for the DAG
    
    Returns:
        DAG: Configured Airflow DAG
    """
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Trigger Cloud Run service and handle failures for pipeline {pipeline_id}',
        schedule_interval=schedule or '@once',
        catchup=False,
    )
    
    with dag:
        # Task to trigger Cloud Run service
        trigger_task = PythonOperator(
            task_id='trigger_cloud_run',
            python_callable=trigger_cloud_run,
            op_kwargs={
                'cloud_run_url': cloud_run_url,
                'json_payload': json_payload,
                'pipeline_id': pipeline_id
            },
            provide_context=True,
        )
        
        # Configure on_failure_callback for the task
        trigger_task.on_failure_callback = lambda context: handle_failure(
            failure_cloud_run_url=failure_cloud_run_url,
            pipeline_id=pipeline_id,
            **context
        )
    
    return dag

# Example usage - this can be customized with your actual parameters
# This section is executed when the file is loaded by Airflow
cloud_run_url = "{{ var.value.cloud_run_url }}"
failure_cloud_run_url = "{{ var.value.failure_cloud_run_url }}"
json_payload = "{{ var.value.json_payload }}"
pipeline_id = "{{ var.value.pipeline_id }}"

# Create the DAG instance that will be picked up by Airflow
globals()['cloud_run_pipeline'] = create_cloud_run_dag(
    dag_id='cloud_run_pipeline',
    cloud_run_url=cloud_run_url,
    failure_cloud_run_url=failure_cloud_run_url,
    json_payload=json_payload,
    pipeline_id=pipeline_id
)