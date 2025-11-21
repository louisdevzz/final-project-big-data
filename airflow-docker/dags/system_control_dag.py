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
    detach = params.get('detach', True)
    build = params.get('build', False)
    force_recreate = params.get('force_recreate', False)

    result = docker_compose_up.delay(path, detach, build, force_recreate)
    return {'task_id': result.id, 'compose_path': path}


def task_docker_compose_down(**context):
    """Task chạy docker-compose down"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    volumes = params.get('remove_volumes', False)
    remove_orphans = params.get('remove_orphans', False)

    result = docker_compose_down.delay(path, volumes, remove_orphans)
    return {'task_id': result.id, 'compose_path': path}


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
        'compose_path': Param('/path/to/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
        'detach': Param(True, type='boolean', description='Chạy ở chế độ detached'),
        'build': Param(False, type='boolean', description='Build images trước khi start'),
        'force_recreate': Param(False, type='boolean', description='Force recreate containers'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes khi down'),
        'remove_orphans': Param(False, type='boolean', description='Xóa orphan containers'),
        'service': Param('', type='string', description='Tên service (để trống = tất cả)'),
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
        'compose_path': Param('/path/to/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
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
