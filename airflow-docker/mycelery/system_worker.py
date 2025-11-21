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
def create_file(self, file_path, content):
    """Tạo file với nội dung được chỉ định"""
    try:
        with open(file_path, 'w') as f:
            f.write(content)
        return {
            'status': 'success',
            'message': f'File created: {file_path}',
            'file_path': file_path
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def run_script(self, script_path):
    """Chạy một script file"""
    try:
        result = subprocess.run(
            ['python', script_path],
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
            'message': 'Script execution timed out (300s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


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
def list_files(self, directory):
    """Liệt kê files trong thư mục"""
    try:
        files = os.listdir(directory)
        return {
            'status': 'success',
            'files': files,
            'count': len(files)
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def delete_file(self, file_path):
    """Xóa file"""
    try:
        os.remove(file_path)
        return {
            'status': 'success',
            'message': f'File deleted: {file_path}'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }
