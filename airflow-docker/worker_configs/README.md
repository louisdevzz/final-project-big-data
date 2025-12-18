# Worker Capabilities Configuration

## Tổng quan

Hệ thống đã được chuyển sang mô hình **capability-based routing**:

- Workers không còn được hardcode theo IP/host
- Mỗi worker tự khai báo capabilities của mình
- Tasks được route theo capabilities, không phải theo node cụ thể
- IP thay đổi không ảnh hưởng hệ thống

## Cấu trúc

### Queues theo Capabilities

| Capability | Queue Name | Mô tả |
|-----------|-----------|-------|
| `spark_master` | `spark_master` | Spark Master node - chạy spark-submit commands |
| `spark_worker` | `spark_worker` | Spark Worker nodes - distributed processing |
| `hadoop_namenode` | `hadoop_namenode` | Hadoop Namenode - HDFS metadata |
| `hadoop_datanode` | `hadoop_datanode` | Hadoop Datanode - HDFS storage |
| `kafka` | `kafka` | Kafka broker services |
| `docker_host` | `docker_host` | Bất kỳ máy nào có Docker |

## Cách triển khai

### Bước 1: Copy config file tới từng máy

```bash
# Máy Spark Master (192.168.80.192)
sudo mkdir -p /etc/celery
sudo cp worker_capabilities.spark_master.json /etc/celery/worker_capabilities.json

# Máy Spark Worker (192.168.80.53)
sudo mkdir -p /etc/celery
sudo cp worker_capabilities.spark_worker.json /etc/celery/worker_capabilities.json

# Máy Hadoop Namenode + Kafka (192.168.80.57)
sudo mkdir -p /etc/celery
sudo cp worker_capabilities.hadoop_namenode.json /etc/celery/worker_capabilities.json

# Máy Hadoop Datanode (192.168.80.87)
sudo mkdir -p /etc/celery
sudo cp worker_capabilities.hadoop_datanode.json /etc/celery/worker_capabilities.json
```

### Bước 2: Start worker

**Tất cả các máy đều chạy lệnh GIỐNG NHAU:**

```bash
# Cách 1: Tự động detect queues từ config file
celery -A mycelery.system_worker worker --loglevel=info

# Cách 2: Manual specify queues (nếu muốn override)
celery -A mycelery.system_worker worker \
  --queues=spark_master,docker_host \
  --loglevel=info \
  --hostname=worker-spark-master@%h
```

### Bước 3: Verify

Kiểm tra worker đã subscribe đúng queues:

```bash
# Trên máy có Redis
redis-cli

# List active workers
SMEMBERS celery.workers

# Check queues
KEYS *queue*
```

## Lợi ích

### ✅ So với mô hình cũ (node-based)

| Trước | Sau |
|-------|-----|
| `queue=node_55` (hardcode IP) | `queue=spark_master` (capability) |
| Phải biết máy nào là node_55 | Không cần biết IP |
| IP đổi → code phải sửa | IP đổi → không ảnh hưởng |
| Thêm node mới → sửa code | Thêm node mới → copy config |
| Airflow biết worker nào chạy | Airflow chỉ biết task cần gì |

### ✅ Flexibility

```python
# Task chỉ cần khai báo:
result = docker_compose_up.apply_async(
    args=[path],
    queue='spark_master'  # Máy nào có capability này sẽ nhận
)

# Không cần biết:
# - IP của máy
# - Hostname
# - Node ID
```

### ✅ High Availability

```bash
# Có thể chạy nhiều workers cùng capability:
# Máy A: spark_master
# Máy B: spark_master (backup)

# → Tự động load balancing
# → Một máy chết → máy khác nhận tiếp
```

## Testing

### Test 1: Verify worker registration

```bash
# Trên máy worker
celery -A mycelery.system_worker inspect active_queues
```

Expected output:
```json
{
  "worker-spark-master@hostname": [
    {"name": "spark_master"},
    {"name": "docker_host"},
    {"name": "celery"}
  ]
}
```

### Test 2: Send test task

```python
from mycelery.system_worker import docker_ps

# Send task tới spark_master capability
result = docker_ps.apply_async(
    args=[False],
    queue='spark_master'
)

print(result.get(timeout=30))
```

### Test 3: Verify IP không ảnh hưởng

```bash
# Đổi IP của máy Spark Master
sudo ip addr add 192.168.80.200/24 dev eth0

# Worker tự động kết nối lại Redis
# → Không cần restart
# → Tasks vẫn route đúng
```

## Troubleshooting

### Worker không nhận tasks

```bash
# Check 1: Worker có đăng ký queues chưa?
celery -A mycelery.system_worker inspect active_queues

# Check 2: Config file có đúng không?
cat /etc/celery/worker_capabilities.json

# Check 3: Redis connection
redis-cli ping
```

### Task stuck in queue

```bash
# Check có worker nào subscribe queue này không
celery -A mycelery.system_worker inspect active_queues | grep spark_master

# Nếu không có → cần start worker với capability đó
```

## Advanced: Dynamic Capabilities

Nếu muốn worker tự động detect capabilities:

```python
# mycelery/worker_config.py đã hỗ trợ auto-detect
# Có thể extend thêm logic:

def get_worker_capabilities():
    caps = []

    # Check if Spark is installed
    if os.path.exists('/opt/spark'):
        caps.append('spark_master')

    # Check if Hadoop is installed
    if os.path.exists('/opt/hadoop'):
        caps.append('hadoop_namenode')

    # Always has Docker
    if shutil.which('docker'):
        caps.append('docker_host')

    return caps
```
