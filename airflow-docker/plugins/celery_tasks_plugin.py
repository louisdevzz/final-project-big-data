"""
Airflow plugin to ensure custom Celery tasks are loaded
This imports mycelery.tasks which registers tasks with Airflow's CeleryExecutor
"""
from airflow.plugins_manager import AirflowPlugin
import logging

logger = logging.getLogger(__name__)

# Import the tasks module to register all tasks with Airflow's Celery app
try:
    import mycelery.tasks
    logger.info("[CeleryTasksPlugin] Successfully imported mycelery.tasks - all custom tasks registered")
except ImportError as e:
    logger.error(f"[CeleryTasksPlugin] Failed to import mycelery.tasks: {e}")
except Exception as e:
    logger.error(f"[CeleryTasksPlugin] Unexpected error loading tasks: {e}")


class CeleryTasksPlugin(AirflowPlugin):
    name = "celery_tasks_plugin"
