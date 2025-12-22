# Airflow Big Data Pipeline System

## 🏗️ Kiến trúc hệ thống

### Sơ đồ tổng quan

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Scheduler                         │
│              (Quản lý DAGs và lập lịch tasks)               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Redis Broker                              │
│              (Message queue cho Celery tasks)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┬──────────────┐
        │              │               │              │
        ▼              ▼               ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  Worker  │   │  Worker  │   │  Worker  │   │  Worker  │
│  Spark   │   │  Spark   │   │  Hadoop  │   │  Hadoop  │
│  Master  │   │  Worker  │   │Namenode  │   │Datanode  │
└──────────┘   └──────────┘   └──────────┘   └──────────┘
     │              │               │              │
     └──────────────┴───────────────┴──────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                       │
│          (Lưu metadata Airflow & kết quả Celery)            │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Cài đặt

### Bước 1: Clone và chuẩn bị môi trường

```bash
cd ~/Documents/airflow/airflow
```

### Bước 2: Cài đặt dependencies

```bash
pip install -r requirements.txt
```

Hoặc sử dụng `uv` (khuyến nghị):

```bash
uv sync
source .venv/bin/activate
```

### Bước 3: Khởi động Airflow cluster

```bash
# Tạo file .env nếu chưa có
echo "AIRFLOW_UID=$(id -u)" > .env

# Khởi động tất cả services
docker compose up -d

# Kiểm tra trạng thái
docker compose ps
```

Airflow Web UI sẽ có sẵn tại: `http://localhost:9090`

- Username: `airflow`
- Password: `airflow`

### Bước 4: Cấu hình môi trường

Cập nhật các biến môi trường trong `docker-compose.yaml` hoặc `.env`:

```bash
# Redis broker (điều chỉnh IP theo môi trường của bạn)
AIRFLOW__CELERY__BROKER_URL=redis://:@192.168.80.229:6379/0

# PostgreSQL database
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

---

## 🔧 Cấu hình Worker

### Triển khai Workers

#### Bước 1: Deploy config files tới workers

**Cách 1: Automated Deployment (Khuyến nghị - Deploy tất cả cùng lúc)**

Sử dụng script `deploy_worker_config.sh` để deploy config tự động từ máy trung tâm đến tất cả workers:

```bash
cd ~/Documents/airflow/airflow

# Cấu hình mapping trong scripts/deploy_worker_config.sh
# HOSTS: Danh sách IP của workers
# CONFIGS: Config file tương ứng với mỗi worker
# SSH_USER: Username để SSH (mặc định: donghuynh0)

# Deploy tới tất cả workers
./scripts/deploy_worker_config.sh
```

Script sẽ:

- ✅ Hiển thị deployment plan (host → config mapping)
- ✅ Copy config files qua SSH tới từng máy
- ✅ Tạo thư mục `/etc/celery/` với sudo
- ✅ Verify deployment thành công
- ✅ Hướng dẫn next steps

**Cách 2: Manual Deployment (Từng máy một)**

Nếu muốn deploy thủ công trên từng máy:

```bash
# Trên máy worker
sudo mkdir -p /etc/celery

# Copy config tương ứng với vai trò của máy
# Ví dụ: Máy Spark Master
sudo cp worker_configs/worker_capabilities.spark_master.json /etc/celery/worker_capabilities.json
```

**Các file config mẫu:**

- `worker_capabilities.spark_master.json` - Cho Spark Master
- `worker_capabilities.spark_worker.json` - Cho Spark Worker
- `worker_capabilities.hadoop_namenode.json` - Cho Hadoop Namenode
- `worker_capabilities.hadoop_datanode.json` - Cho Hadoop Datanode

**Ví dụ nội dung config:**

```json
{
  "worker_name": "spark-master-worker",
  "capabilities": [
    "spark_master",
    "docker_host",
    "prepare_data",
    "train_model",
    "streaming_data",
    "predict"
  ],
  "description": "Spark Master node - handles master services and data pipeline tasks"
}
```

> 💡 **Tip**: Sử dụng automated deployment khi có nhiều workers để tiết kiệm thời gian và tránh sai sót. Chỉ cần cấu hình mapping trong `deploy_worker_config.sh` một lần, sau đó có thể re-deploy dễ dàng khi cần update config.

#### Bước 2: Start worker

**Cách 1: Sử dụng script tự động (Khuyến nghị)**

```bash
cd ~/Documents/airflow/airflow
./scripts/start_worker.sh
```

Script sẽ:

- ✅ Tự động detect capabilities từ config file
- ✅ Kiểm tra kết nối Redis
- ✅ Subscribe vào các queues phù hợp
- ✅ Start worker với cấu hình tối ưu

**Cách 2: Manual start**

```bash
# Worker tự động load config từ /etc/celery/worker_capabilities.json
celery -A mycelery.system_worker worker --loglevel=info

# Hoặc chỉ định queues thủ công
celery -A mycelery.system_worker worker \
  --queues=spark_master,docker_host,prepare_data,train_model \
  --loglevel=info \
  --concurrency=4 \
  --hostname=worker-spark-master@%h
```

#### Bước 3: Verify workers

```bash
# Kiểm tra workers đang hoạt động
celery -A mycelery.system_worker inspect active

# Kiểm tra queues mà worker subscribe
celery -A mycelery.system_worker inspect active_queues

# Kiểm tra stats
celery -A mycelery.system_worker inspect stats
```

#### Cấu hình Deployment Script

Để sử dụng automated deployment, cần cấu hình mapping trong `scripts/deploy_worker_config.sh`:

```bash
# Chỉnh sửa file scripts/deploy_worker_config.sh

# 1. Cập nhật danh sách hosts (IP của workers)
HOSTS=(
    "192.168.80.55"   # Spark Master
    "192.168.80.53"   # Spark Worker
    "192.168.80.57"   # Hadoop Namenode + Kafka
    "192.168.80.87"   # Hadoop Datanode
)

# 2. Config files tương ứng
CONFIGS=(
    "worker_capabilities.spark_master.json"
    "worker_capabilities.spark_worker.json"
    "worker_capabilities.hadoop_namenode.json"
    "worker_capabilities.hadoop_datanode.json"
)

# 3. SSH username (hoặc set env var)
SSH_USER="${SSH_USER:-donghuynh0}"  # Thay bằng username của bạn
```

**Lưu ý:**

- ✅ Thứ tự trong `HOSTS` phải khớp với thứ tự trong `CONFIGS`
- ✅ User cần có quyền sudo trên các máy workers
- ✅ Setup SSH key-based authentication để tránh nhập password nhiều lần:
  ```bash
  # Trên máy trung tâm
  ssh-copy-id user@worker-host
  ```

### Cấu hình nâng cao

Worker settings có thể điều chỉnh qua environment variables:

```bash
export CELERY_WORKER_CONCURRENCY=4          # Số tasks chạy đồng thời
export CELERY_WORKER_MAX_TASKS_PER_CHILD=100  # Restart sau N tasks
export CELERY_WORKER_TIME_LIMIT=3600        # Timeout (seconds)
export CELERY_WORKER_LOG_LEVEL=info         # Log level
```

---

## 🔄 Luồng hoạt động

### 1. Kiến trúc thực thi

```
Airflow Scheduler
    │
    ├─► DAG: bigdata_pipeline_start
    │      │
    │      ├─► [PythonOperator] start_hadoop_namenode
    │      │        └─► Celery Task → Queue: hadoop_namenode
    │      │                 └─► Worker (capability: hadoop_namenode)
    │      │                      └─► docker_compose_up(~/bd/hadoop/...)
    │      │
    │      ├─► [PythonOperator] start_spark_master
    │      │        └─► Celery Task → Queue: spark_master
    │      │                 └─► Worker (capability: spark_master)
    │      │
    │      └─► [PythonOperator] prepare_data
    │               └─► Celery Task → Queue: prepare_data
    │                        └─► Worker (capability: prepare_data)
    │                             └─► run_command(sh prepare.sh)
    │
    └─► DAG: bigdata_pipeline_stop
           └─► [PythonOperator] stop services...
```

### 2. Task Routing Process

```
┌──────────────────────────┐
│  Airflow Task Submit     │
│  op='start_spark_master' │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Celery Task Created     │
│  queue='spark_master'    │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Redis Broker            │
│  Add to queue list       │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Worker Selection        │
│  Find workers with       │
│  'spark_master' cap      │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Task Execution          │
│  docker_compose_up(...)  │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Result to PostgreSQL    │
│  Airflow tracks status   │
└──────────────────────────┘
```

### 3. Configuration Files

#### **DAG Configuration** (`dags/system_control_dag.py`)

Định nghĩa services và mapping capabilities:

```python
BIGDATA_SERVICES = {
    'spark-master': {
        'capability': 'spark_master',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-master',
    },
    'hadoop-namenode': {
        'capability': 'hadoop_namenode',
        'path': '~/bd/hadoop/docker-compose.namenode.yml',
    },
    # ...
}
```

#### **Worker Configuration** (`mycelery/worker_config.py`)

Map capabilities sang queues:

```python
queue_mapping = {
    'spark_master': ['spark_master', 'spark_common'],
    'prepare_data': ['prepare_data'],
    'train_model': ['train_model'],
    # ...
}
```

#### **Celery Configuration** (`mycelery/celeryconfig.py`)

```python
broker_url = 'redis://192.168.80.229:6379/0'
result_backend = 'db+postgresql://airflow:airflow@192.168.80.229/airflow'
task_time_limit = 3600
worker_concurrency = 4
```

---

## 🔍 Troubleshooting

### 1. Worker không nhận tasks

**Triệu chứng**: Tasks stuck ở trạng thái "queued"

**Kiểm tra:**

```bash
# 1. Worker có đang chạy không?
celery -A mycelery.system_worker inspect active_queues

# 2. Config file có đúng không?
cat /etc/celery/worker_capabilities.json

# 3. Redis connection
redis-cli -h 192.168.80.229 ping
```

**Giải pháp:**

```bash
# Restart worker với verbose logging
celery -A mycelery.system_worker worker \
  --loglevel=debug \
  --queues=spark_master
```

### 2. Task timeout

**Triệu chứng**: Task báo lỗi "TimeLimitExceeded"

**Tăng timeout:**

```python
# Trong DAG
result = run_command.apply_async(
    args=[command],
    kwargs={'env_vars': env_vars},
    queue='train_model',
    time_limit=7200  # 2 hours
)
```

Hoặc cấu hình global trong `celeryconfig.py`:

```python
task_time_limit = 7200
task_soft_time_limit = 6600
```

### 3. Airflow không thấy DAGs

**Kiểm tra:**

```bash
# 1. Xem logs của dag-processor
docker compose logs airflow-dag-processor

# 2. Verify PYTHONPATH
docker compose exec airflow-scheduler printenv PYTHONPATH
# Should contain: /opt/airflow:/opt/airflow/dags:/opt/airflow/mycelery

# 3. Check DAG syntax
docker compose exec airflow-scheduler \
  python /opt/airflow/dags/system_control_dag.py
```

### 4. Redis connection error

**Lỗi**: "Error connecting to Redis"

**Kiểm tra:**

```bash
# 1. Redis có chạy không?
docker compose ps redis

# 2. Test connection
redis-cli -h 192.168.80.229 -p 6379 ping

# 3. Check firewall
telnet 192.168.80.229 6379
```

**Cập nhật broker URL:**

```bash
# Trong docker-compose.yaml
AIRFLOW__CELERY__BROKER_URL: redis://:@<IP_MỚI>:6379/0
```

### 5. PostgreSQL connection error

**Kiểm tra:**

```bash
# Test connection
docker compose exec postgres psql -U airflow -d airflow -c "SELECT 1;"

# Xem logs
docker compose logs postgres
```

### 6. Worker không load capabilities

**Triệu chứng**: Worker chỉ subscribe vào queue "celery"

**Debug:**

```bash
# Test worker config
python3 -c "
from mycelery.worker_config import get_worker_capabilities, get_worker_queues
print('Capabilities:', get_worker_capabilities())
print('Queues:', get_worker_queues())
"

# Verify file exists
ls -la /etc/celery/worker_capabilities.json

# Check permissions
sudo chmod 644 /etc/celery/worker_capabilities.json
```

### 7. Docker Compose command fails

**Lỗi trong logs**: "docker compose up failed"

**Kiểm tra:**

```bash
# Test command manually trên worker
docker compose -f ~/bd/spark/docker-compose.yml ps

# Verify path exists
ls -la ~/bd/spark/docker-compose.yml

# Check Docker installation
docker --version
docker compose version
```
