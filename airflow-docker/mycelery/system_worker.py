from celery import Celery
import subprocess
import os

app = Celery(
    'system_worker',
    broker='redis://192.168.80.49:6379/0',
    backend='db+postgresql://airflow:airflow@192.168.80.49/airflow'
)

# Cấu hình Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh',
    enable_utc=True,
)

@app.task(bind=True)
def run_command(self, command):
    """Chạy một lệnh shell"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'message': 'Command execution timed out (300s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_run(self, image, container_name=None, ports=None, volumes=None, env_vars=None, detach=True):
    """Chạy Docker container"""
    try:
        cmd = ['docker', 'run']

        if detach:
            cmd.append('-d')

        if container_name:
            cmd.extend(['--name', container_name])

        if ports:
            for port in ports:
                cmd.extend(['-p', port])

        if volumes:
            for volume in volumes:
                cmd.extend(['-v', volume])

        if env_vars:
            for key, value in env_vars.items():
                cmd.extend(['-e', f'{key}={value}'])

        cmd.append(image)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )
        return {
            'status': 'success',
            'container_id': result.stdout.strip(),
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_stop(self, container_name):
    """Dừng Docker container"""
    try:
        result = subprocess.run(
            ['docker', 'stop', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'message': f'Container {container_name} stopped',
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_remove(self, container_name):
    """Xóa Docker container"""
    try:
        result = subprocess.run(
            ['docker', 'rm', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'message': f'Container {container_name} removed',
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_ps(self, all_containers=False):
    """Liệt kê Docker containers"""
    try:
        cmd = ['docker', 'ps']
        if all_containers:
            cmd.append('-a')

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        return {
            'status': 'success',
            'output': result.stdout,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_compose_up(self, path, detach=True, build=False, force_recreate=False):
    """Chạy docker-compose up với path được chỉ định"""
    try:
        # Expand ~ thành home directory
        path = os.path.expanduser(path)
        cmd = ['docker', 'compose', '-f', path, 'up']

        if detach:
            cmd.append('-d')

        if build:
            cmd.append('--build')

        if force_recreate:
            cmd.append('--force-recreate')

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600
        )
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'message': 'Docker compose up timed out (600s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_compose_down(self, path, volumes=False, remove_orphans=False):
    """Dừng và xóa containers với docker-compose down"""
    try:
        # Expand ~ thành home directory
        path = os.path.expanduser(path)
        cmd = ['docker', 'compose', '-f', path, 'down']

        if volumes:
            cmd.append('-v')

        if remove_orphans:
            cmd.append('--remove-orphans')

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'message': 'Docker compose down timed out (300s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_compose_ps(self, path):
    """Liệt kê containers của docker-compose"""
    try:
        # Expand ~ thành home directory
        path = os.path.expanduser(path)
        cmd = ['docker', 'compose', '-f', path, 'ps']

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'output': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_compose_logs(self, path, service=None, tail=100):
    """Lấy logs từ docker-compose"""
    try:
        # Expand ~ thành home directory
        path = os.path.expanduser(path)
        cmd = ['docker', 'compose', '-f', path, 'logs', '--tail', str(tail)]

        if service:
            cmd.append(service)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'output': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }
