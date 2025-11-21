# system_control_dag.py
# DAG điều khiển Docker và Docker Compose

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

# Import tasks từ system_worker
from mycelery.system_worker import (
    docker_run,
    docker_stop,
    docker_remove,
    docker_ps,
    docker_compose_up,
    docker_compose_down,
    docker_compose_ps,
    docker_compose_logs
)


# ============== TASK FUNCTIONS ==============

def task_docker_run(**context):
    """Task chạy Docker container"""
    params = context['params']
    image = params.get('image', 'hello-world')
    container_name = params.get('container_name', None)
    ports = params.get('ports', None)

    # Parse ports nếu là string
    if ports and isinstance(ports, str) and ports.strip():
        ports = [p.strip() for p in ports.split(',')]
    else:
        ports = None

    result = docker_run.delay(image, container_name, ports)
    return {'task_id': result.id, 'image': image}


def task_docker_stop(**context):
    """Task dừng Docker container"""
    params = context['params']
    container_name = params.get('container_name', '')

    result = docker_stop.delay(container_name)
    return {'task_id': result.id, 'container_name': container_name}


def task_docker_remove(**context):
    """Task xóa Docker container"""
    params = context['params']
    container_name = params.get('container_name', '')

    result = docker_remove.delay(container_name)
    return {'task_id': result.id, 'container_name': container_name}


def task_docker_ps(**context):
    """Task liệt kê Docker containers"""
    params = context['params']
    all_containers = params.get('all_containers', False)

    result = docker_ps.delay(all_containers)
    return {'task_id': result.id}


def task_docker_compose_up(**context):
    """Task chạy docker-compose up"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    services = params.get('services', None)
    detach = params.get('detach', True)
    build = params.get('build', False)
    force_recreate = params.get('force_recreate', False)

    # Parse services nếu là string
    if services and isinstance(services, str) and services.strip():
        services = [s.strip() for s in services.split(',')]
    else:
        services = None

    result = docker_compose_up.delay(path, services, detach, build, force_recreate)
    return {'task_id': result.id, 'compose_path': path, 'services': services}


def task_docker_compose_down(**context):
    """Task chạy docker-compose down"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    services = params.get('services', None)
    volumes = params.get('remove_volumes', False)
    remove_orphans = params.get('remove_orphans', False)

    # Parse services nếu là string
    if services and isinstance(services, str) and services.strip():
        services = [s.strip() for s in services.split(',')]
    else:
        services = None

    result = docker_compose_down.delay(path, services, volumes, remove_orphans)
    return {'task_id': result.id, 'compose_path': path, 'services': services}


def task_docker_compose_ps(**context):
    """Task liệt kê containers của docker-compose"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')

    result = docker_compose_ps.delay(path)
    return {'task_id': result.id, 'compose_path': path}


def task_docker_compose_logs(**context):
    """Task lấy logs từ docker-compose"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    service = params.get('service', None)
    tail = params.get('tail', 100)

    if service and isinstance(service, str) and not service.strip():
        service = None

    result = docker_compose_logs.delay(path, service, tail)
    return {'task_id': result.id, 'compose_path': path}


# ============== DAG 1: Docker Container Management ==============

with DAG(
    dag_id='docker_container_management',
    description='DAG quản lý Docker containers: run, stop, remove, list',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'container'],
    params={
        'image': Param('hello-world', type='string', description='Docker image'),
        'container_name': Param('', type='string', description='Tên container'),
        'ports': Param('', type='string', description='Port mapping (vd: 8080:80,3000:3000)'),
        'all_containers': Param(False, type='boolean', description='Hiển thị tất cả containers'),
    }
) as dag_docker:

    docker_ps_task = PythonOperator(
        task_id='docker_list_containers',
        python_callable=task_docker_ps,
    )

    docker_run_task = PythonOperator(
        task_id='docker_run_container',
        python_callable=task_docker_run,
    )

    docker_stop_task = PythonOperator(
        task_id='docker_stop_container',
        python_callable=task_docker_stop,
    )

    docker_remove_task = PythonOperator(
        task_id='docker_remove_container',
        python_callable=task_docker_remove,
    )


# ============== DAG 2: Docker Compose Management ==============

with DAG(
    dag_id='docker_compose_management',
    description='DAG quản lý Docker Compose: up, down, ps, logs',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'compose'],
    params={
        'compose_path': Param('~/bd/spark/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
        'services': Param('spark-master', type='string', description='Services cụ thể (vd: spark-master,spark-worker). Để trống = tất cả'),
        'detach': Param(True, type='boolean', description='Chạy ở chế độ detached'),
        'build': Param(False, type='boolean', description='Build images trước khi start'),
        'force_recreate': Param(False, type='boolean', description='Force recreate containers'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes khi down'),
        'remove_orphans': Param(False, type='boolean', description='Xóa orphan containers'),
        'service': Param('spark-master', type='string', description='Service cho logs (để trống = tất cả)'),
        'tail': Param(100, type='integer', description='Số dòng logs'),
    }
) as dag_compose:

    compose_up_task = PythonOperator(
        task_id='docker_compose_up',
        python_callable=task_docker_compose_up,
    )

    compose_down_task = PythonOperator(
        task_id='docker_compose_down',
        python_callable=task_docker_compose_down,
    )

    compose_ps_task = PythonOperator(
        task_id='docker_compose_ps',
        python_callable=task_docker_compose_ps,
    )

    compose_logs_task = PythonOperator(
        task_id='docker_compose_logs',
        python_callable=task_docker_compose_logs,
    )


# ============== DAG 3: Docker Compose Pipeline ==============

with DAG(
    dag_id='docker_compose_pipeline',
    description='Pipeline: kiểm tra status -> up -> kiểm tra lại',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'compose', 'pipeline'],
    params={
        'compose_path': Param('~/bd/spark/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
        'build': Param(False, type='boolean', description='Build images trước khi start'),
    }
) as dag_compose_pipeline:

    def task_compose_check_before(**context):
        """Kiểm tra status trước khi up"""
        params = context['params']
        path = params.get('compose_path')
        result = docker_compose_ps.delay(path)
        return {'task_id': result.id, 'step': 'check_before'}

    def task_compose_start(**context):
        """Start docker-compose"""
        params = context['params']
        path = params.get('compose_path')
        build = params.get('build', False)
        result = docker_compose_up.delay(path, detach=True, build=build)
        return {'task_id': result.id, 'step': 'start'}

    def task_compose_check_after(**context):
        """Kiểm tra status sau khi up"""
        params = context['params']
        path = params.get('compose_path')
        result = docker_compose_ps.delay(path)
        return {'task_id': result.id, 'step': 'check_after'}

    step1_check = PythonOperator(
        task_id='step1_check_status',
        python_callable=task_compose_check_before,
    )

    step2_start = PythonOperator(
        task_id='step2_compose_up',
        python_callable=task_compose_start,
    )

    step3_verify = PythonOperator(
        task_id='step3_verify_status',
        python_callable=task_compose_check_after,
    )

    step1_check >> step2_start >> step3_verify


# ============== DAG 4: Docker Compose Stop ==============

with DAG(
    dag_id='docker_compose_stop',
    description='DAG dừng Docker Compose services',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'compose', 'stop'],
    params={
        'compose_path': Param('~/bd/spark/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
        'services': Param('', type='string', description='Services cần dừng (vd: spark-master,spark-worker). Để trống = tất cả'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes'),
        'remove_orphans': Param(False, type='boolean', description='Xóa orphan containers'),
    }
) as dag_compose_stop:

    def task_compose_stop(**context):
        """Dừng docker-compose services"""
        params = context['params']
        path = params.get('compose_path')
        services = params.get('services', None)
        volumes = params.get('remove_volumes', False)
        remove_orphans = params.get('remove_orphans', False)

        # Parse services nếu là string
        if services and isinstance(services, str) and services.strip():
            services = [s.strip() for s in services.split(',')]
        else:
            services = None

        result = docker_compose_down.delay(path, services, volumes, remove_orphans)
        return {'task_id': result.id, 'compose_path': path, 'services': services}

    stop_compose_task = PythonOperator(
        task_id='stop_docker_compose',
        python_callable=task_compose_stop,
    )


# ============== DAG 5: Big Data Pipeline ==============
# Pipeline chạy trên nhiều node khác nhau

# Định nghĩa cấu hình các services
BIGDATA_SERVICES = {
    'spark-master': {
        'host': '192.168.80.55',
        'queue': 'node_55',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-master',
    },
    'spark-worker': {
        'host': '192.168.80.53',
        'queue': 'node_53',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-worker',
    },
    'hadoop-namenode': {
        'host': '192.168.80.57',
        'queue': 'node_57',
        'path': '~/bd/hadoop/docker-compose.namenode.yml',
        'service': None,  # Chạy tất cả services trong file
    },
    'hadoop-datanode': {
        'host': '192.168.80.87',
        'queue': 'node_87',
        'path': '~/bd/hadoop/docker-compose.datanode.yml',
        'service': None,
    },
    'kafka': {
        'host': '192.168.80.57',
        'queue': 'node_57',
        'path': '~/bd/kafka/docker-compose.yml',
        'service': None,
    },
}


with DAG(
    dag_id='bigdata_pipeline_start',
    description='Pipeline khởi động Big Data cluster: Hadoop -> Spark -> Kafka',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'start'],
    params={
        'start_hadoop': Param(True, type='boolean', description='Khởi động Hadoop (Namenode + Datanode)'),
        'start_spark': Param(True, type='boolean', description='Khởi động Spark (Master + Worker)'),
        'start_kafka': Param(True, type='boolean', description='Khởi động Kafka'),
    }
) as dag_bigdata_start:

    def start_service(service_name, **context):
        """Khởi động một service trên node tương ứng"""
        config = BIGDATA_SERVICES[service_name]

        result = docker_compose_up.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=config['queue']
        )
        return {
            'task_id': result.id,
            'service': service_name,
            'host': config['host'],
            'queue': config['queue']
        }

    # Hadoop tasks
    def start_hadoop_namenode(**context):
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        return start_service('hadoop-namenode', **context)

    def start_hadoop_datanode(**context):
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        return start_service('hadoop-datanode', **context)

    # Spark tasks
    def start_spark_master(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        return start_service('spark-master', **context)

    def start_spark_worker(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        return start_service('spark-worker', **context)

    # Kafka task
    def start_kafka(**context):
        if not context['params'].get('start_kafka', True):
            return {'skipped': True}
        return start_service('kafka', **context)

    # Tạo tasks
    task_hadoop_namenode = PythonOperator(
        task_id='start_hadoop_namenode',
        python_callable=start_hadoop_namenode,
    )

    task_hadoop_datanode = PythonOperator(
        task_id='start_hadoop_datanode',
        python_callable=start_hadoop_datanode,
    )

    task_spark_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
    )

    task_spark_worker = PythonOperator(
        task_id='start_spark_worker',
        python_callable=start_spark_worker,
    )

    task_kafka = PythonOperator(
        task_id='start_kafka',
        python_callable=start_kafka,
    )

    # Định nghĩa thứ tự chạy:
    # 1. Hadoop Namenode trước
    # 2. Hadoop Datanode sau Namenode
    # 3. Spark Master (có thể song song với Hadoop Datanode)
    # 4. Spark Worker sau Spark Master
    # 5. Kafka sau khi Hadoop đã sẵn sàng

    task_hadoop_namenode >> task_hadoop_datanode
    task_hadoop_namenode >> task_spark_master >> task_spark_worker
    task_hadoop_datanode >> task_kafka


# ============== DAG 6: Big Data Pipeline Stop ==============

with DAG(
    dag_id='bigdata_pipeline_stop',
    description='Pipeline dừng Big Data cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'stop'],
    params={
        'stop_hadoop': Param(True, type='boolean', description='Dừng Hadoop'),
        'stop_spark': Param(True, type='boolean', description='Dừng Spark'),
        'stop_kafka': Param(True, type='boolean', description='Dừng Kafka'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes'),
    }
) as dag_bigdata_stop:

    def stop_service(service_name, remove_volumes=False, **context):
        """Dừng một service trên node tương ứng"""
        config = BIGDATA_SERVICES[service_name]

        result = docker_compose_down.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'volumes': remove_volumes,
                'remove_orphans': False,
            },
            queue=config['queue']
        )
        return {
            'task_id': result.id,
            'service': service_name,
            'host': config['host'],
        }

    # Stop tasks
    def stop_kafka(**context):
        if not context['params'].get('stop_kafka', True):
            return {'skipped': True}
        return stop_service('kafka', context['params'].get('remove_volumes', False), **context)

    def stop_spark_worker(**context):
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        return stop_service('spark-worker', context['params'].get('remove_volumes', False), **context)

    def stop_spark_master(**context):
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        return stop_service('spark-master', context['params'].get('remove_volumes', False), **context)

    def stop_hadoop_datanode(**context):
        if not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        return stop_service('hadoop-datanode', context['params'].get('remove_volumes', False), **context)

    def stop_hadoop_namenode(**context):
        if not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        return stop_service('hadoop-namenode', context['params'].get('remove_volumes', False), **context)

    # Tạo tasks
    task_stop_kafka = PythonOperator(
        task_id='stop_kafka',
        python_callable=stop_kafka,
    )

    task_stop_spark_worker = PythonOperator(
        task_id='stop_spark_worker',
        python_callable=stop_spark_worker,
    )

    task_stop_spark_master = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_spark_master,
    )

    task_stop_hadoop_datanode = PythonOperator(
        task_id='stop_hadoop_datanode',
        python_callable=stop_hadoop_datanode,
    )

    task_stop_hadoop_namenode = PythonOperator(
        task_id='stop_hadoop_namenode',
        python_callable=stop_hadoop_namenode,
    )

    # Thứ tự dừng ngược lại với khởi động:
    # 1. Dừng Kafka trước
    # 2. Dừng Spark Worker
    # 3. Dừng Spark Master
    # 4. Dừng Hadoop Datanode
    # 5. Dừng Hadoop Namenode cuối cùng

    task_stop_kafka >> task_stop_spark_worker >> task_stop_spark_master
    task_stop_spark_master >> task_stop_hadoop_datanode >> task_stop_hadoop_namenode
