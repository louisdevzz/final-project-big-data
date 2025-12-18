from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime

# Import Celery tasks từ mycelery.system_worker
from mycelery.system_worker import (
    docker_compose_up,
)


def start_spark_master(**context):
    """Start Spark Master service using docker-compose"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')

    print(f"[INFO] Starting Spark Master")
    print(f"[INFO] Compose path: {compose_path}")

    # Call Celery task to run docker-compose up on remote worker
    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-master',  # Service name in docker-compose.yml
            'detach': True,
            'build': False,
            'force_recreate': False,
        },
        queue='spark_master'  # Route to worker with spark_master capability
    )

    print(f"[INFO] Task sent to queue: spark_master")
    print(f"[INFO] Celery task ID: {result.id}")

    return {
        'task_id': result.id,
        'service': 'spark-master',
        'queue': 'spark_master',
        'compose_path': compose_path
    }


def start_spark_worker(**context):
    """Start Spark Worker service using docker-compose"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')

    print(f"[INFO] Starting Spark Worker")
    print(f"[INFO] Compose path: {compose_path}")

    # Call Celery task to run docker-compose up on remote worker
    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-worker',  # Service name in docker-compose.yml
            'detach': True,
            'build': False,
            'force_recreate': False,
        },
        queue='spark_worker'  # Route to worker with spark_worker capability
    )

    print(f"[INFO] Task sent to queue: spark_worker")
    print(f"[INFO] Celery task ID: {result.id}")

    return {
        'task_id': result.id,
        'service': 'spark-worker',
        'queue': 'spark_worker',
        'compose_path': compose_path
    }


# DAG Definition: Start Spark Cluster
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
) as dag_start:

    # Task 1: Start Spark Master
    task_start_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
        doc_md="""
        ## Start Spark Master

        This task starts the Spark Master service on the remote machine with `spark_master` capability.

        - **Queue**: spark_master
        - **Service**: spark-master
        - **Method**: docker-compose up
        """
    )

    # Task 2: Start Spark Worker
    task_start_worker = PythonOperator(
        task_id='start_spark_worker',
        python_callable=start_spark_worker,
        doc_md="""
        ## Start Spark Worker

        This task starts the Spark Worker service on the remote machine with `spark_worker` capability.

        - **Queue**: spark_worker
        - **Service**: spark-worker
        - **Method**: docker-compose up
        """
    )

    # Task dependencies: Master must start before Worker
    task_start_master >> task_start_worker
