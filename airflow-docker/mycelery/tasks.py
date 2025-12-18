"""
Custom Celery tasks registered with Airflow's CeleryExecutor
These tasks will be available to all Airflow workers
"""
import subprocess
import os
import logging

logger = logging.getLogger(__name__)

# Import Airflow's Celery app (NOT creating a new one)
from airflow.providers.celery.executors.celery_executor import app

logger.info("[mycelery.tasks] Registering custom tasks with Airflow's Celery executor")


@app.task(name='mycelery.system_worker.run_command', bind=True)
def run_command(self, command, env_vars=None):
    """
    Run a shell command

    Args:
        command: Shell command to execute
        env_vars: Optional dict of environment variables

    Returns:
        dict with status, stdout, stderr, return_code
    """
    logger.info(f"[run_command] Executing: {command}")
    try:
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300,
            env=env
        )

        logger.info(f"[run_command] Completed with return code: {result.returncode}")
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        logger.error("[run_command] Command timed out after 300s")
        return {
            'status': 'error',
            'message': 'Command execution timed out (300s)'
        }
    except Exception as e:
        logger.error(f"[run_command] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(name='mycelery.system_worker.docker_compose_up', bind=True)
def docker_compose_up(self, path, services=None, detach=True, build=False, force_recreate=False):
    """
    Run docker-compose up

    Args:
        path: Path to docker-compose.yml file
        services: Optional list or string of service names
        detach: Run in background
        build: Build images before starting
        force_recreate: Force recreate containers

    Returns:
        dict with status, stdout, stderr, return_code
    """
    logger.info(f"[docker_compose_up] Starting services from: {path}")
    try:
        # Expand ~ to home directory
        path = os.path.expanduser(path)

        if not os.path.exists(path):
            logger.error(f"[docker_compose_up] File not found: {path}")
            return {
                'status': 'error',
                'message': f'docker-compose file not found: {path}'
            }

        cmd = ['docker-compose', '-f', path, 'up']

        if detach:
            cmd.append('-d')
        if build:
            cmd.append('--build')
        if force_recreate:
            cmd.append('--force-recreate')

        if services:
            if isinstance(services, str):
                cmd.append(services)
            else:
                cmd.extend(services)

        logger.info(f"[docker_compose_up] Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )

        logger.info(f"[docker_compose_up] Completed with return code: {result.returncode}")
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        logger.error("[docker_compose_up] Command timed out after 300s")
        return {
            'status': 'error',
            'message': 'docker-compose up timed out (300s)'
        }
    except Exception as e:
        logger.error(f"[docker_compose_up] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(name='mycelery.system_worker.docker_compose_down', bind=True)
def docker_compose_down(self, path, services=None, volumes=False, remove_orphans=False):
    """
    Run docker-compose down

    Args:
        path: Path to docker-compose.yml file
        services: Optional list or string of service names
        volumes: Remove named volumes
        remove_orphans: Remove containers for services not in compose file

    Returns:
        dict with status, stdout, stderr, return_code
    """
    logger.info(f"[docker_compose_down] Stopping services from: {path}")
    try:
        # Expand ~ to home directory
        path = os.path.expanduser(path)

        if not os.path.exists(path):
            logger.error(f"[docker_compose_down] File not found: {path}")
            return {
                'status': 'error',
                'message': f'docker-compose file not found: {path}'
            }

        cmd = ['docker-compose', '-f', path, 'down']

        if volumes:
            cmd.append('--volumes')
        if remove_orphans:
            cmd.append('--remove-orphans')

        if services:
            if isinstance(services, str):
                cmd.append(services)
            else:
                cmd.extend(services)

        logger.info(f"[docker_compose_down] Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )

        logger.info(f"[docker_compose_down] Completed with return code: {result.returncode}")
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        logger.error("[docker_compose_down] Command timed out after 120s")
        return {
            'status': 'error',
            'message': 'docker-compose down timed out (120s)'
        }
    except Exception as e:
        logger.error(f"[docker_compose_down] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


# Additional helper tasks for docker operations
@app.task(name='mycelery.system_worker.docker_ps', bind=True)
def docker_ps(self, all_containers=False):
    """List Docker containers"""
    logger.info("[docker_ps] Listing containers")
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
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        logger.error(f"[docker_ps] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(name='mycelery.system_worker.docker_stop', bind=True)
def docker_stop(self, container_name):
    """Stop a Docker container"""
    logger.info(f"[docker_stop] Stopping container: {container_name}")
    try:
        result = subprocess.run(
            ['docker', 'stop', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )

        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        logger.error(f"[docker_stop] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(name='mycelery.system_worker.docker_remove', bind=True)
def docker_remove(self, container_name):
    """Remove a Docker container"""
    logger.info(f"[docker_remove] Removing container: {container_name}")
    try:
        result = subprocess.run(
            ['docker', 'rm', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )

        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        logger.error(f"[docker_remove] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


logger.info("[mycelery.tasks] Successfully registered all custom tasks")
logger.info("[mycelery.tasks] Available tasks: run_command, docker_compose_up, docker_compose_down, docker_ps, docker_stop, docker_remove")
