from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import docker_compose_up


def wait_for_celery_result(result, timeout=60, poll_interval=2):
    """Wait for Celery task to complete"""
    elapsed = 0
    while elapsed < timeout:
        if result.ready():
            if result.successful():
                return result.result
            else:
                raise Exception(f"Celery task failed: {result.result}")
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Celery task {result.id} timed out after {timeout} seconds")


def start_spark_master(**context):
    """Start Spark Master service"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')

    print(f"[DEBUG] Starting Spark Master")
    print(f"[DEBUG] Compose path: {compose_path}")
    print(f"[DEBUG] Sending task to queue: spark_master")

    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-master',
            'detach': True,
            'build': False,
            'force_recreate': False,
        },
        queue='spark_master'  # Route to spark_master capability
    )

    print(f"[DEBUG] Task sent! Task ID: {result.id}")
    print(f"[DEBUG] Waiting for result...")

    try:
        output = wait_for_celery_result(result, timeout=300)
        print(f"[DEBUG] Task completed successfully!")
        print(f"[DEBUG] Output: {output}")
        return {
            'task_id': result.id,
            'service': 'spark-master',
            'capability': 'spark_master',
            'output': output,
            'status': 'success'
        }
    except Exception as e:
        print(f"[ERROR] Task failed: {str(e)}")
        raise Exception(f"Failed to start Spark Master: {str(e)}")


def start_spark_worker(**context):
    """Start Spark Worker service"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')

    print(f"[DEBUG] Starting Spark Worker")
    print(f"[DEBUG] Compose path: {compose_path}")
    print(f"[DEBUG] Sending task to queue: spark_worker")

    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-worker',
            'detach': True,
            'build': False,
            'force_recreate': False,
        },
        queue='spark_worker'  # Route to spark_worker capability
    )

    print(f"[DEBUG] Task sent! Task ID: {result.id}")
    print(f"[DEBUG] Waiting for result...")

    try:
        output = wait_for_celery_result(result, timeout=300)
        print(f"[DEBUG] Task completed successfully!")
        print(f"[DEBUG] Output: {output}")
        return {
            'task_id': result.id,
            'service': 'spark-worker',
            'capability': 'spark_worker',
            'output': output,
            'status': 'success'
        }
    except Exception as e:
        print(f"[ERROR] Task failed: {str(e)}")
        raise Exception(f"Failed to start Spark Worker: {str(e)}")


# DAG Definition
with DAG(
    dag_id='spark_cluster_start',
    description='Start Spark Master and Worker services',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['spark', 'cluster', 'start'],
    params={
        'spark_compose_path': Param(
            '~/bd/spark/docker-compose.yml',
            type='string',
            description='Path to Spark docker-compose.yml file'
        ),
    }
) as dag:

    # Task 1: Start Spark Master
    task_start_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
        doc_md="""
        ## Start Spark Master

        This task starts the Spark Master service on the machine with `spark_master` capability.

        - **Queue**: spark_master
        - **Service**: spark-master
        - **Timeout**: 300 seconds
        """
    )

    # Task 2: Start Spark Worker
    task_start_worker = PythonOperator(
        task_id='start_spark_worker',
        python_callable=start_spark_worker,
        doc_md="""
        ## Start Spark Worker

        This task starts the Spark Worker service on the machine with `spark_worker` capability.

        - **Queue**: spark_worker
        - **Service**: spark-worker
        - **Timeout**: 300 seconds
        """
    )

    # Task dependencies: Master must start before Worker
    task_start_master >> task_start_worker
