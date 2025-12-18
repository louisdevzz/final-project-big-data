"""
Worker Configuration Module
Tự động phát hiện capabilities của worker dựa trên file config local
"""
import os
import json
import socket

# Đường dẫn file config capabilities
CAPABILITIES_FILE = os.getenv('WORKER_CAPABILITIES_FILE', '/etc/celery/worker_capabilities.json')

def get_worker_capabilities():
    """
    Đọc capabilities từ file config local
    Fallback về auto-detect nếu không có file
    """
    # Thử đọc từ file
    if os.path.exists(CAPABILITIES_FILE):
        try:
            with open(CAPABILITIES_FILE, 'r') as f:
                config = json.load(f)
                return config.get('capabilities', [])
        except Exception as e:
            print(f"Warning: Could not read capabilities file: {e}")

    # Fallback: auto-detect dựa trên hostname (tạm thời)
    hostname = socket.gethostname()
    print(f"Auto-detecting capabilities for hostname: {hostname}")

    # Mặc định mọi worker đều có thể chạy Docker commands
    capabilities = ['docker_host']

    return capabilities

def get_worker_queues():
    """
    Lấy danh sách queues mà worker này sẽ subscribe
    Dựa trên capabilities
    """
    capabilities = get_worker_capabilities()

    # Map capabilities -> queues
    # Một capability có thể map tới nhiều queues
    queue_mapping = {
        'spark_master': ['spark_master', 'spark_common'],
        'spark_worker': ['spark_worker', 'spark_common'],
        'hadoop_namenode': ['hadoop_namenode', 'hadoop_common'],
        'hadoop_datanode': ['hadoop_datanode', 'hadoop_common'],
        'kafka': ['kafka'],
        'docker_host': ['docker_host'],
        'gpu': ['gpu'],
    }

    queues = set()
    for cap in capabilities:
        if cap in queue_mapping:
            queues.update(queue_mapping[cap])

    # Always add default queue
    queues.add('celery')

    return list(queues)

def get_worker_name():
    """
    Tạo tên worker dựa trên hostname và capabilities
    """
    hostname = socket.gethostname()
    capabilities = get_worker_capabilities()
    cap_str = '-'.join(sorted(capabilities))
    return f"worker@{hostname}[{cap_str}]"

# Print config khi import
if __name__ != '__main__':
    print("=" * 60)
    print("Worker Configuration Loaded")
    print("=" * 60)
    print(f"Capabilities: {get_worker_capabilities()}")
    print(f"Queues: {get_worker_queues()}")
    print(f"Worker name: {get_worker_name()}")
    print("=" * 60)
