from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks directly from mycelery.system_worker
from mycelery.system_worker import (
    docker_compose_up,
    docker_compose_down,
    run_command
)

# ============== CONFIGURATION ==============

BIGDATA_SERVICES = {
    'spark-master': {
        'capability': 'spark_master',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-master',
        'description': 'Spark Master service',
    },
    'spark-worker': {
        'capability': 'spark_worker',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-worker',
        'description': 'Spark Worker service',
    },
    'hadoop-namenode': {
        'capability': 'hadoop_namenode',
        'path': '~/bd/hadoop/docker-compose.namenode.yml',
        'service': None,
        'description': 'Hadoop Namenode',
    },
    'hadoop-datanode': {
        'capability': 'hadoop_datanode',
        'path': '~/bd/hadoop/docker-compose.datanode.yml',
        'service': None,
        'description': 'Hadoop Datanode',
    },
    'kafka': {
        'capability': 'kafka',
        'path': '~/bd/kafka/docker-compose.yml',
        'service': None,
        'description': 'Kafka broker',
    },
}

# ============== HELPER FUNCTIONS ==============

def wait_for_celery_result(result, timeout=600, poll_interval=5):
    """Blocking wait for Celery result. Used for data jobs that must complete."""
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

# ============== DAG 4: Docker Compose Stop ==============

with DAG(
    dag_id='docker_compose_stop',
    description='DAG to stop arbitrary Docker Compose services',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'compose', 'stop'],
    params={
        'compose_path': Param('~/bd/spark/docker-compose.yml', type='string', description='Path to docker-compose.yml'),
        'services': Param('', type='string', description='Services to stop (comma separated). Empty for all.'),
        'remove_volumes': Param(False, type='boolean', description='Remove volumes'),
        'remove_orphans': Param(False, type='boolean', description='Remove orphans'),
    }
) as dag_compose_stop:

    def task_compose_stop(**context):
        """Stop docker-compose services (Non-blocking submission)"""
        params = context['params']
        path = params.get('compose_path')
        services_str = params.get('services', '')
        volumes = params.get('remove_volumes', False)
        remove_orphans = params.get('remove_orphans', False)

        services = [s.strip() for s in services_str.split(',')] if services_str.strip() else None

        # Fire and forget / Async submission
        result = docker_compose_down.apply_async(
            args=[path],
            kwargs={
                'services': services,
                'volumes': volumes,
                'remove_orphans': remove_orphans
            }
            # No specific queue enforced here, relies on default or routing
        )
        
        return {'task_id': result.id, 'compose_path': path, 'status': 'submitted'}

    stop_compose_task = PythonOperator(
        task_id='stop_docker_compose',
        python_callable=task_compose_stop,
    )

# ============== DAG 5: Big Data Pipeline Start ==============

with DAG(
    dag_id='bigdata_pipeline_start',
    description='Start Big Data Cluster and Run Pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'start'],
    params={
        'start_hadoop': Param(True, type='boolean'),
        'start_spark': Param(True, type='boolean'),
        'start_kafka': Param(True, type='boolean'),
    }
) as dag_bigdata_start:

    def start_service(service_key, **context):
        """Start a service using non-blocking Celery task"""
        # check skip
        if 'spark' in service_key and not context['params'].get('start_spark', True):
            return {'skipped': True}
        if 'hadoop' in service_key and not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        if 'kafka' in service_key and not context['params'].get('start_kafka', True):
            return {'skipped': True}

        config = BIGDATA_SERVICES[service_key]
        
        # Non-blocking submission
        result = docker_compose_up.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=config['capability']
        )
        
        print(f"Started {service_key} on {config['capability']}: {result.id}")
        return {'task_id': result.id, 'service': service_key}

    def run_spark_job(job_name, **context):
        """Run a Spark job (Blocking wait for completion)"""
        if not context['params'].get('start_spark', True):
            return {'skipped': True}

        scripts = {
            'prepare_data': 'sh ~/bd/fp_pr_tasks/credit_card/exes/prepare.sh',
            'train_model': 'sh ~/bd/fp_pr_tasks/credit_card/exes/train.sh',
            'streaming_data': 'sh ~/bd/fp_pr_tasks/credit_card/exes/producer.sh',
            'predict': 'sh ~/bd/fp_pr_tasks/credit_card/exes/predict.sh'
        }
        
        command = scripts.get(job_name)
        if not command:
             raise ValueError(f"Unknown job: {job_name}")

        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }

        # Submit to master
        result = run_command.apply_async(
            args=[command],
            kwargs={'env_vars': env_vars},
            queue='spark_master'
        )

        # BLOCKING WAIT
        output = wait_for_celery_result(result, timeout=900)
        return {'task_id': result.id, 'job': job_name, 'output': output}

    # --- Tasks Definition ---

    # 1. Services (Non-blocking)
    t_namenode = PythonOperator(
        task_id='start_hadoop_namenode',
        python_callable=start_service,
        op_kwargs={'service_key': 'hadoop-namenode'},
    )
    
    t_datanode = PythonOperator(
        task_id='start_hadoop_datanode',
        python_callable=start_service,
        op_kwargs={'service_key': 'hadoop-datanode'},
    )

    t_spark_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_service,
        op_kwargs={'service_key': 'spark-master'},
    )

    t_spark_worker = PythonOperator(
        task_id='start_spark_worker',
        python_callable=start_service,
        op_kwargs={'service_key': 'spark-worker'},
    )

    t_kafka = PythonOperator(
        task_id='start_kafka',
        python_callable=start_service,
        op_kwargs={'service_key': 'kafka'},
    )

    # 2. Jobs (Blocking)
    t_prepare = PythonOperator(
        task_id='prepare_data',
        python_callable=run_spark_job,
        op_kwargs={'job_name': 'prepare_data'},
    )

    t_train = PythonOperator(
        task_id='train_model',
        python_callable=run_spark_job,
        op_kwargs={'job_name': 'train_model'},
    )

    t_streaming = PythonOperator(
        task_id='streaming_data',
        python_callable=run_spark_job,
        op_kwargs={'job_name': 'streaming_data'},
    )

    t_predict = PythonOperator(
        task_id='predict',
        python_callable=run_spark_job,
        op_kwargs={'job_name': 'predict'},
    )

    # --- Dependencies ---
    
    t_namenode >> t_datanode
    t_spark_master >> t_spark_worker
    
    # Wait for infrastructure before running jobs
    cluster_ready = [t_datanode, t_spark_worker, t_kafka]
    
    cluster_ready >> t_prepare >> t_train
    
    # Parallel paths after training
    t_train >> t_predict
    t_train >> t_streaming


# ============== DAG 6: Big Data Pipeline Stop ==============

with DAG(
    dag_id='bigdata_pipeline_stop',
    description='Stop Big Data Cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'stop'],
    params={
        'stop_hadoop': Param(True, type='boolean'),
        'stop_spark': Param(True, type='boolean'),
        'stop_kafka': Param(True, type='boolean'),
        'remove_volumes': Param(False, type='boolean'),
    }
) as dag_bigdata_stop:

    def stop_service(service_key, **context):
        """Stop service (Non-blocking)"""
        # check skip
        if 'spark' in service_key and not context['params'].get('stop_spark', True):
            return {'skipped': True}
        if 'hadoop' in service_key and not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        if 'kafka' in service_key and not context['params'].get('stop_kafka', True):
            return {'skipped': True}

        config = BIGDATA_SERVICES[service_key]
        remove_volumes = context['params'].get('remove_volumes', False)

        result = docker_compose_down.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'volumes': remove_volumes,
                'remove_orphans': False,
            },
            queue=config['capability']
        )
        return {'task_id': result.id, 'service': service_key}

    # --- Stop Tasks ---

    t_stop_kafka = PythonOperator(
        task_id='stop_kafka',
        python_callable=stop_service,
        op_kwargs={'service_key': 'kafka'},
    )

    t_stop_spark_w = PythonOperator(
        task_id='stop_spark_worker',
        python_callable=stop_service,
        op_kwargs={'service_key': 'spark-worker'},
    )

    t_stop_spark_m = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_service,
        op_kwargs={'service_key': 'spark-master'},
    )

    t_stop_hadoop_d = PythonOperator(
        task_id='stop_hadoop_datanode',
        python_callable=stop_service,
        op_kwargs={'service_key': 'hadoop-datanode'},
    )

    t_stop_hadoop_n = PythonOperator(
        task_id='stop_hadoop_namenode',
        python_callable=stop_service,
        op_kwargs={'service_key': 'hadoop-namenode'},
    )

    # Order: Worker -> Master (or Dataplane -> Controlplane)
    t_stop_spark_w >> t_stop_spark_m
    t_stop_hadoop_d >> t_stop_hadoop_n
    t_stop_kafka
