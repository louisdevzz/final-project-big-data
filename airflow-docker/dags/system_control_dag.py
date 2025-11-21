# system_control_dag.py
# DAG điều khiển hệ thống: tạo file, chạy script, quản lý Docker

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

# Import tasks từ system_worker
from mycelery.system_worker import (
    create_file,
    run_script,
    run_command,
    docker_run,
    docker_stop,
    docker_remove,
    docker_ps,
    list_files,
    delete_file
)


# ============== TASK FUNCTIONS ==============

def task_create_file(**context):
    """Task tạo file"""
    params = context['params']
    file_path = params.get('file_path', '/tmp/test_file.txt')
    content = params.get('content', 'Hello from Airflow!')

    result = create_file.delay(file_path, content)
    return {'task_id': result.id, 'file_path': file_path}


def task_run_script(**context):
    """Task chạy script"""
    params = context['params']
    script_path = params.get('script_path', '/tmp/test_script.py')

    result = run_script.delay(script_path)
    return {'task_id': result.id, 'script_path': script_path}


def task_run_command(**context):
    """Task chạy lệnh shell"""
    params = context['params']
    command = params.get('command', 'echo "Hello from Celery!"')

    result = run_command.delay(command)
    return {'task_id': result.id, 'command': command}


def task_docker_run(**context):
    """Task chạy Docker container"""
    params = context['params']
    image = params.get('image', 'hello-world')
    container_name = params.get('container_name', None)
    ports = params.get('ports', None)

    result = docker_run.delay(image, container_name, ports)
    return {'task_id': result.id, 'image': image}


def task_docker_stop(**context):
    """Task dừng Docker container"""
    params = context['params']
    container_name = params.get('container_name', '')

    result = docker_stop.delay(container_name)
    return {'task_id': result.id, 'container_name': container_name}


def task_docker_ps(**context):
    """Task liệt kê Docker containers"""
    params = context['params']
    all_containers = params.get('all_containers', False)

    result = docker_ps.delay(all_containers)
    return {'task_id': result.id}


def task_list_files(**context):
    """Task liệt kê files"""
    params = context['params']
    directory = params.get('directory', '/tmp')

    result = list_files.delay(directory)
    return {'task_id': result.id, 'directory': directory}


# ============== DAG 1: File Operations ==============

with DAG(
    dag_id='system_file_operations',
    description='DAG quản lý file: tạo, xóa, liệt kê files',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['system', 'file'],
    params={
        'file_path': Param('/tmp/airflow_test.txt', type='string', description='Đường dẫn file'),
        'content': Param('Hello from Airflow DAG!', type='string', description='Nội dung file'),
        'directory': Param('/tmp', type='string', description='Thư mục cần liệt kê'),
    }
) as dag_file:

    create_file_task = PythonOperator(
        task_id='create_file',
        python_callable=task_create_file,
    )

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=task_list_files,
    )

    create_file_task >> list_files_task


# ============== DAG 2: Script Execution ==============

with DAG(
    dag_id='system_script_execution',
    description='DAG chạy script và lệnh shell',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['system', 'script'],
    params={
        'script_path': Param('/tmp/test_script.py', type='string', description='Đường dẫn script'),
        'command': Param('echo "Hello World"', type='string', description='Lệnh shell'),
    }
) as dag_script:

    run_command_task = PythonOperator(
        task_id='run_command',
        python_callable=task_run_command,
    )

    run_script_task = PythonOperator(
        task_id='run_script',
        python_callable=task_run_script,
    )


# ============== DAG 3: Docker Management ==============

with DAG(
    dag_id='system_docker_management',
    description='DAG quản lý Docker containers',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['system', 'docker'],
    params={
        'image': Param('hello-world', type='string', description='Docker image'),
        'container_name': Param('', type='string', description='Tên container'),
        'ports': Param('', type='string', description='Port mapping (vd: 8080:80)'),
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


# ============== DAG 4: Demo Pipeline ==============

def task_create_demo_script(**context):
    """Tạo script demo"""
    script_content = '''
import datetime
print(f"Script executed at: {datetime.datetime.now()}")
print("Hello from demo script!")
for i in range(5):
    print(f"Processing step {i+1}...")
print("Done!")
'''
    result = create_file.delay('/tmp/demo_script.py', script_content)
    return {'task_id': result.id}


def task_run_demo_script(**context):
    """Chạy script demo"""
    result = run_script.delay('/tmp/demo_script.py')
    return {'task_id': result.id}


def task_cleanup_demo(**context):
    """Cleanup sau khi chạy xong"""
    result = delete_file.delay('/tmp/demo_script.py')
    return {'task_id': result.id}


with DAG(
    dag_id='system_demo_pipeline',
    description='DAG demo: tạo script -> chạy script -> cleanup',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['system', 'demo'],
) as dag_demo:

    step1_create = PythonOperator(
        task_id='step1_create_script',
        python_callable=task_create_demo_script,
    )

    step2_run = PythonOperator(
        task_id='step2_run_script',
        python_callable=task_run_demo_script,
    )

    step3_cleanup = PythonOperator(
        task_id='step3_cleanup',
        python_callable=task_cleanup_demo,
    )

    step1_create >> step2_run >> step3_cleanup
