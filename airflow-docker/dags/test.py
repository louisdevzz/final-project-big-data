# celery_trigger_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import task từ Celery worker
from mycelery.worker import heavy_job


def trigger_celery_task():
    result = heavy_job.delay(10, 20)   # gửi job sang worker
    return result.id                   # lưu ID lại để tracking


with DAG(
    dag_id='trigger_celery_demo',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):
    task_trigger = PythonOperator(
        task_id='call_celery_heavy_job',
        python_callable=trigger_celery_task
    )
