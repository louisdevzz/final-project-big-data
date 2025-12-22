"""
Celery Configuration cho Capability-based Workers
Auto-detect queues từ worker capabilities
"""
from mycelery.worker_config import get_worker_queues

# Celery configuration
# Broker: Redis for message queue
broker_url = 'redis://192.168.80.229:6379/0'
# Result Backend: PostgreSQL for task results (shared with Airflow)
result_backend = 'db+postgresql://airflow:airflow@192.168.80.229/airflow'

# Serialization
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'

# Timezone
timezone = 'Asia/Ho_Chi_Minh'
enable_utc = True

# Task routing - không hardcode nữa
# Chỉ định nghĩa pattern routing
# Note: Explicit queue parameter in send_task() will override these routes
task_routes = {
    # Docker operations default to docker_host, but can be overridden
    # 'mycelery.system_worker.docker_*': {'queue': 'docker_host'},  # Commented out to allow explicit queue override
    'mycelery.system_worker.run_command': {'queue': 'default'},  # Will be overridden by explicit queue param
}

# Worker sẽ tự động subscribe vào queues dựa trên capabilities
# Được define trong worker_config.py

# Task execution settings
task_time_limit = 3600  # 1 hour hard limit
task_soft_time_limit = 3300  # 55 minutes soft limit
task_acks_late = True
worker_prefetch_multiplier = 1  # Fetch 1 task at a time (fair distribution)

# Result backend settings
result_expires = 3600  # Results expire after 1 hour
result_persistent = True

# Worker settings
worker_max_tasks_per_child = 100  # Restart worker after 100 tasks (prevent memory leaks)
worker_disable_rate_limits = True

# Concurrency
worker_concurrency = 4  # 4 concurrent tasks per worker

# Enable events for monitoring
worker_send_task_events = True
task_send_sent_event = True

# Logging
worker_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] [%(task_name)s(%(task_id)s)] %(message)s'
worker_task_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] [%(task_name)s(%(task_id)s)] %(message)s'

print("=" * 60)
print("Celery Configuration Loaded")
print("=" * 60)
print(f"Broker: {broker_url}")
print(f"Backend: {result_backend}")
print(f"Auto-detected queues: {get_worker_queues()}")
print("=" * 60)
