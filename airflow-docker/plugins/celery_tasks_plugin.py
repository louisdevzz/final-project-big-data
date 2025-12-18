"""
Airflow plugin to register custom Celery tasks from mycelery.system_worker
This re-registers the tasks with Airflow's CeleryExecutor
"""
from airflow.plugins_manager import AirflowPlugin
import subprocess
import os
import logging

logger = logging.getLogger(__name__)

# Get Airflow's Celery executor app
try:
    from airflow.providers.celery.executors.celery_executor import app as celery_app
    logger.info("[CeleryTasksPlugin] Loaded Celery app from providers")
except ImportError:
    try:
        from airflow.executors.celery_executor import app as celery_app
        logger.info("[CeleryTasksPlugin] Loaded Celery app from executors")
    except Exception as e:
        logger.error(f"[CeleryTasksPlugin] Failed to import Celery app: {e}")
        celery_app = None


# Only register tasks if celery_app is available
if celery_app:
    @celery_app.task(name='mycelery.system_worker.run_command', bind=True)
    def run_command(self, command, env_vars=None):
        """Run a shell command"""
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


    @celery_app.task(name='mycelery.system_worker.docker_compose_up', bind=True)
    def docker_compose_up(self, path, services=None, detach=True, build=False, force_recreate=False):
        """Run docker-compose up"""
        try:
            # Expand ~ to home directory
            path = os.path.expanduser(path)

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
                'message': 'docker-compose up timed out (300s)'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }


    @celery_app.task(name='mycelery.system_worker.docker_compose_down', bind=True)
    def docker_compose_down(self, path, services=None, volumes=False, remove_orphans=False):
        """Run docker-compose down"""
        try:
            # Expand ~ to home directory
            path = os.path.expanduser(path)

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

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
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
                'message': 'docker-compose down timed out (120s)'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }

    logger.info("[CeleryTasksPlugin] Successfully registered 3 custom Celery tasks")
    logger.info("[CeleryTasksPlugin] Tasks: run_command, docker_compose_up, docker_compose_down")
else:
    logger.error("[CeleryTasksPlugin] Celery app not available - tasks not registered")


class CeleryTasksPlugin(AirflowPlugin):
    name = "celery_tasks_plugin"
