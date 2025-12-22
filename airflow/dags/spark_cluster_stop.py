from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime

# Import Celery tasks từ mycelery.system_worker
from mycelery.system_worker import (
    docker_compose_down,
)


def stop_spark_worker(**context):
    """Stop Spark Worker service using docker-compose"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')
    remove_volumes = params.get('remove_volumes', False)

    print(f"[INFO] Stopping Spark Worker")
    print(f"[INFO] Compose path: {compose_path}")
    print(f"[INFO] Remove volumes: {remove_volumes}")

    # Call Celery task to run docker-compose down on remote worker
    result = docker_compose_down.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-worker',  # Service name in docker-compose.yml
            'volumes': remove_volumes,
            'remove_orphans': False,
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


def stop_spark_master(**context):
    """Stop Spark Master service using docker-compose"""
    params = context['params']
    compose_path = params.get('spark_compose_path', '~/bd/spark/docker-compose.yml')
    remove_volumes = params.get('remove_volumes', False)

    print(f"[INFO] Stopping Spark Master")
    print(f"[INFO] Compose path: {compose_path}")
    print(f"[INFO] Remove volumes: {remove_volumes}")

    # Call Celery task to run docker-compose down on remote worker
    result = docker_compose_down.apply_async(
        args=[compose_path],
        kwargs={
            'services': 'spark-master',  # Service name in docker-compose.yml
            'volumes': remove_volumes,
            'remove_orphans': False,
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


# DAG Definition: Stop Spark Cluster
with DAG(
    dag_id='spark_cluster_stop',
    description='Stop Spark Master and Worker services',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['spark', 'cluster', 'stop'],
    params={
        'spark_compose_path': Param(
            '~/bd/spark/docker-compose.yml',
            type='string',
            description='Path to Spark docker-compose.yml file'
        ),
        'remove_volumes': Param(
            False,
            type='boolean',
            description='Remove named volumes declared in the volumes section'
        ),
    }
) as dag_stop:

    # Task 1: Stop Spark Worker (stop worker first)
    task_stop_worker = PythonOperator(
        task_id='stop_spark_worker',
        python_callable=stop_spark_worker,
        doc_md="""
        ## Stop Spark Worker

        This task stops the Spark Worker service on the remote machine with `spark_worker` capability.

        - **Queue**: spark_worker
        - **Service**: spark-worker
        - **Method**: docker-compose down

        **Note**: Worker is stopped before Master to ensure clean shutdown.
        """
    )

    # Task 2: Stop Spark Master (stop master after worker)
    task_stop_master = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_spark_master,
        doc_md="""
        ## Stop Spark Master

        This task stops the Spark Master service on the remote machine with `spark_master` capability.

        - **Queue**: spark_master
        - **Service**: spark-master
        - **Method**: docker-compose down

        **Note**: Master is stopped after Worker to ensure clean shutdown.
        """
    )

    # Task dependencies: Worker must stop before Master
    task_stop_worker >> task_stop_master
