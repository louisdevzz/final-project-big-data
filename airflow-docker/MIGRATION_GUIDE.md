# Migration Guide: Node-based → Capability-based Routing

## Tổng quan

Hệ thống đã được chuyển từ **node-based routing** sang **capability-based routing**.

### Sau (Capability-based)

```python
# ✅ Chỉ khai báo capability cần thiết
BIGDATA_SERVICES = {
    'spark-master': {'capability': 'spark_master'},
}

result = task.apply_async(queue='spark_master')  # Không cần biết IP
```

## Các thay đổi chính

### 1. Queue Naming

| Trước | Sau | Ý nghĩa |
|-------|-----|---------|
| `node_55` | `spark_master` | Máy nào có Spark Master capability |
| `node_53` | `spark_worker` | Máy nào có Spark Worker capability |
| `node_57` | `hadoop_namenode`, `kafka` | Máy nào có Hadoop/Kafka capabilities |
| `node_87` | `hadoop_datanode` | Máy nào có Hadoop Datanode capability |

### 2. Task Routing

**Sau:**
```python
# Route theo capability
result = run_command.apply_async(
    args=[command],
    queue='spark_master'  # Máy nào có spark_master sẽ nhận
)
```

### 3. Worker Configuration

**Sau:**
```bash
# Tất cả máy chạy lệnh giống nhau
# Config file quyết định capabilities
celery -A mycelery.system_worker worker --loglevel=info
```

## Hướng dẫn Migration

### Bước 1: Backup hệ thống hiện tại

```bash
# Backup code
cp -r airflow-docker airflow-docker.backup

# Stop tất cả workers hiện tại
# (trên mỗi node)
pkill -f "celery.*worker"
```

### Bước 2: Pull code mới

```bash
cd airflow-docker
git pull origin main  # hoặc fetch changes
```

### Bước 3: Deploy worker configs

```bash
# Option A: Tự động deploy tới tất cả nodes
cd airflow-docker
./scripts/deploy_worker_config.sh

# Option B: Manual deploy từng node
# Trên máy Spark Master (192.168.80.192):
sudo mkdir -p /etc/celery
sudo cp worker_configs/worker_capabilities.spark_master.json /etc/celery/worker_capabilities.json

# Trên máy Spark Worker (192.168.80.53):
sudo mkdir -p /etc/celery
sudo cp worker_configs/worker_capabilities.spark_worker.json /etc/celery/worker_capabilities.json

# Trên máy Hadoop Namenode + Kafka (192.168.80.57):
sudo mkdir -p /etc/celery
sudo cp worker_configs/worker_capabilities.hadoop_namenode.json /etc/celery/worker_capabilities.json

# Trên máy Hadoop Datanode (192.168.80.87):
sudo mkdir -p /etc/celery
sudo cp worker_configs/worker_capabilities.hadoop_datanode.json /etc/celery/worker_capabilities.json
```

### Bước 4: Restart Airflow scheduler

```bash
# Trên máy Airflow
airflow scheduler restart

# Hoặc nếu dùng systemd:
sudo systemctl restart airflow-scheduler
```

### Bước 5: Start workers với config mới

**Trên MỖI node:**

```bash
cd /path/to/airflow-docker

# Cách 1: Dùng script có sẵn
./scripts/start_worker.sh

# Cách 2: Manual start
celery -A mycelery.system_worker worker \
  --loglevel=info \
  --concurrency=4 \
  --max-tasks-per-child=100
```

### Bước 6: Verify

```bash
# 1. Check workers đã đăng ký chưa
celery -A mycelery.system_worker inspect active_queues

# Expected output:
# {
#   "worker@spark-master": [
#     {"name": "spark_master"},
#     {"name": "docker_host"},
#     {"name": "celery"}
#   ],
#   "worker@spark-worker": [
#     {"name": "spark_worker"},
#     {"name": "docker_host"},
#     {"name": "celery"}
#   ],
#   ...
# }

# 2. Test chạy DAG
# Vào Airflow UI -> DAGs -> bigdata_pipeline_start
# Trigger DAG và xem logs

# 3. Monitor tasks
celery -A mycelery.system_worker events
```

## Rollback Plan

Nếu có vấn đề, rollback như sau:

### 1. Stop workers mới

```bash
# Trên mỗi node
pkill -f "celery.*worker"
```

### 2. Restore code cũ

```bash
cd /path/to
mv airflow-docker airflow-docker.new
mv airflow-docker.backup airflow-docker
cd airflow-docker
```

### 3. Start workers với config cũ

```bash
# Trên node_55 (192.168.80.192)
celery -A mycelery.system_worker worker --queues=node_55 --hostname=worker-55@%h

# Trên node_53 (192.168.80.53)
celery -A mycelery.system_worker worker --queues=node_53 --hostname=worker-53@%h

# Etc...
```

## Lợi ích sau Migration

### 1. IP Independence

```bash
# Thay đổi IP không ảnh hưởng hệ thống
# Chỉ cần worker kết nối được Redis là được

# Trước: Đổi IP → phải sửa code, restart Airflow
# Sau: Đổi IP → chỉ cần restart worker trên máy đó
```

### 2. Easier Scaling

```bash
# Thêm Spark Worker mới:
# 1. Copy config file
sudo cp worker_capabilities.spark_worker.json /etc/celery/worker_capabilities.json

# 2. Start worker (lệnh giống hệt máy khác)
./scripts/start_worker.sh

# 3. Done! Tự động nhận tasks từ queue spark_worker
```

### 3. Flexibility

```python
# Có thể chạy nhiều workers cùng capability
# → Load balancing tự động
# → High availability

# Ví dụ:
# - Máy A: spark_master (primary)
# - Máy B: spark_master (backup)
# → Máy A chết → Máy B tự động nhận tasks
```

### 4. Better Monitoring

```bash
# Xem workers theo capability
celery -A mycelery.system_worker inspect active_queues | grep spark_master

# Xem task distribution
celery -A mycelery.system_worker inspect stats
```

## Troubleshooting

### Issue 1: Worker không nhận tasks

**Triệu chứng:**
- DAG chạy nhưng tasks stuck ở queued state
- Không có error logs

**Giải pháp:**
```bash
# 1. Check worker có đăng ký queue chưa
celery -A mycelery.system_worker inspect active_queues

# 2. Check config file
cat /etc/celery/worker_capabilities.json

# 3. Check Redis connection
redis-cli -h 192.168.80.192 ping

# 4. Restart worker
pkill -f "celery.*worker"
./scripts/start_worker.sh
```

### Issue 2: Tasks route sai worker

**Triệu chứng:**
- Task chạy nhưng fail với "command not found" hoặc "path not found"

**Nguyên nhân:**
- Task được route tới worker không có capability phù hợp

**Giải pháp:**
```bash
# 1. Check task được send tới queue nào
# Trong DAG logs, tìm dòng:
# "Sent task ... to queue: <queue_name>"

# 2. Check workers nào subscribe queue đó
celery -A mycelery.system_worker inspect active_queues | grep <queue_name>

# 3. Verify config file trên worker đó
ssh user@host cat /etc/celery/worker_capabilities.json
```

### Issue 3: ImportError sau migration

**Triệu chứng:**
```
ImportError: cannot import name 'get_worker_queues' from 'mycelery.worker_config'
```

**Giải pháp:**
```bash
# 1. Verify file structure
ls -la airflow-docker/mycelery/
# Should have: system_worker.py, worker_config.py, celeryconfig.py

# 2. Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# 3. Reinstall if needed
pip install -e .
```

## Testing Checklist

- [ ] Tất cả workers start thành công
- [ ] Workers đăng ký đúng queues (check bằng `inspect active_queues`)
- [ ] Redis connection OK trên tất cả workers
- [ ] Test DAG `bigdata_pipeline_start` chạy thành công
- [ ] Test DAG `bigdata_pipeline_stop` chạy thành công
- [ ] Tasks được phân phối đúng workers (check logs)
- [ ] Không có tasks stuck trong queues
- [ ] Airflow UI hiển thị tasks success

## Support

Nếu gặp vấn đề:

1. Check logs:
   - Worker logs: `tail -f /var/log/celery/worker.log`
   - Airflow scheduler logs: `tail -f /var/log/airflow/scheduler.log`
   - Redis logs: `tail -f /var/log/redis/redis.log`

2. Check system:
   ```bash
   # Workers status
   celery -A mycelery.system_worker inspect stats

   # Active tasks
   celery -A mycelery.system_worker inspect active

   # Registered tasks
   celery -A mycelery.system_worker inspect registered
   ```

3. Tạo issue với thông tin:
   - Error logs
   - Worker configuration (`cat /etc/celery/worker_capabilities.json`)
   - Output của `celery inspect active_queues`
   - Airflow DAG run logs
