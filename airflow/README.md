# Airflow Big Data Pipeline System

## 📋 Mục lục

- [Giới thiệu](#giới-thiệu)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cấu hình Worker](#cấu-hình-worker)
- [Luồng hoạt động](#luồng-hoạt-động)
- [Sử dụng](#sử-dụng)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Giới thiệu

Hệ thống quản lý Big Data Pipeline sử dụng **Apache Airflow** và **Celery** để điều phối các tác vụ phân tán trên nhiều máy chủ. Hệ thống được thiết kế theo mô hình **capability-based routing**, cho phép linh hoạt trong việc phân bổ tác vụ mà không cần hardcode IP hoặc hostname của từng máy.

### ✨ Tính năng chính

- ✅ **Capability-based routing**: Tasks được định tuyến dựa trên khả năng của worker
- ✅ **Distributed execution**: Chạy tasks song song trên nhiều máy
- ✅ **Docker orchestration**: Quản lý các dịch vụ Big Data qua Docker Compose
- ✅ **Data pipeline automation**: Tự động hóa quy trình xử lý dữ liệu với Spark, Hadoop, Kafka
- ✅ **High availability**: Hỗ trợ nhiều workers cùng capability cho load balancing và failover

---

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

### Các thành phần

#### 1. **Airflow Components** (Docker containers)

| Service                 | Port | Mô tả                                                 |
| ----------------------- | ---- | ----------------------------------------------------- |
| `postgres`              | 5432 | Cơ sở dữ liệu PostgreSQL lưu metadata và task results |
| `redis`                 | 6379 | Message broker cho Celery                             |
| `airflow-apiserver`     | 9090 | API server của Airflow 3.0+                           |
| `airflow-scheduler`     | -    | Lập lịch và trigger các DAGs                          |
| `airflow-dag-processor` | -    | Xử lý DAG files                                       |
| `airflow-worker`        | -    | Celery worker mặc định (default queue)                |
| `airflow-triggerer`     | -    | Xử lý deferred tasks                                  |

#### 2. **Celery Workers** (Chạy trên các máy riêng biệt)

Workers được phân loại theo **capabilities** thay vì hardcode IP:

| Capability        | Queue             | Chức năng                    |
| ----------------- | ----------------- | ---------------------------- |
| `spark_master`    | `spark_master`    | Chạy spark-submit commands   |
| `spark_worker`    | `spark_worker`    | Distributed Spark processing |
| `hadoop_namenode` | `hadoop_namenode` | Quản lý HDFS metadata        |
| `hadoop_datanode` | `hadoop_datanode` | Lưu trữ HDFS data            |
| `kafka`           | `kafka`           | Kafka broker services        |
| `docker_host`     | `docker_host`     | Bất kỳ máy có Docker         |
| `prepare_data`    | `prepare_data`    | Data preparation task        |
| `train_model`     | `train_model`     | Model training task          |
| `streaming_data`  | `streaming_data`  | Real-time streaming task     |
| `predict`         | `predict`         | Prediction/inference task    |

#### 3. **Big Data Services**

Các services được quản lý qua Docker Compose:

- **Spark**: Master + Workers
- **Hadoop**: Namenode + Datanodes
- **Kafka**: Broker + Zookeeper

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

### Capability-based Routing

Hệ thống sử dụng mô hình **capability-based routing** để phân phối tasks linh hoạt:

**Lợi ích:**

- ✅ IP thay đổi không ảnh hưởng hệ thống
- ✅ Dễ dàng scale workers (thêm/bớt máy)
- ✅ Tự động load balancing
- ✅ High availability (nhiều workers cùng capability)

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

### 2. Flow chi tiết của một DAG

#### **DAG: bigdata_pipeline_start**

Pipeline này khởi động cluster Big Data và chạy các tasks xử lý dữ liệu:

**Phase 1: Infrastructure Setup (Non-blocking)**

```
start_hadoop_namenode  ──► start_hadoop_datanode
                                    │
start_spark_master     ──► start_spark_worker
                                    │
start_kafka ─────────────────────────┘
                                    │
                          [Cluster Ready]
```

**Phase 2: Data Processing (Blocking - chờ hoàn thành)**

```
[Cluster Ready]
      │
      ├─► prepare_data (blocking wait)
      │        │
      │        └─► Chạy script chuẩn bị dữ liệu
      │
      ├─► train_model (blocking wait)
      │        │
      │        └─► Train model ML với Spark
      │
      ├─► predict (parallel với streaming)
      │        │
      │        └─► Chạy dự đoán
      │
      └─► streaming_data (parallel với predict)
               │
               └─► Stream dữ liệu thời gian thực qua Kafka
```

#### **DAG: bigdata_pipeline_stop**

Dừng cluster theo thứ tự an toàn:

```
stop_kafka

stop_spark_worker  ──► stop_spark_master

stop_hadoop_datanode  ──► stop_hadoop_namenode
```

### 3. Task Routing Process

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

### 4. Task Types

#### **Non-blocking Tasks** (Infrastructure)

- **Mục đích**: Khởi động services nhanh, không chờ hoàn thành
- **Cơ chế**: `apply_async()` trả về ngay task ID
- **Ví dụ**: Start Docker services

```python
result = docker_compose_up.apply_async(
    args=[config['path']],
    kwargs={'detach': True},
    queue='spark_master'  # Route to worker with spark_master capability
)
print(f"Task submitted: {result.id}")
# Không chờ, tiếp tục task tiếp theo
```

#### **Blocking Tasks** (Data Processing)

- **Mục đích**: Phải đợi task hoàn thành mới tiếp tục
- **Cơ chế**: `wait_for_celery_result()` poll cho đến khi done
- **Ví dụ**: Spark jobs xử lý dữ liệu

```python
result = run_command.apply_async(
    args=['sh ~/bd/fp_pr_tasks/credit_card/exes/train.sh'],
    queue='train_model'
)
# BLOCKING - đợi đến khi task hoàn thành
output = wait_for_celery_result(result, timeout=900)
```

### 5. Configuration Files

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

## 🚀 Sử dụng

### 1. Truy cập Airflow Web UI

```
http://localhost:9090
```

Login với credentials mặc định:

- **Username**: `airflow`
- **Password**: `airflow`

### 2. Chạy Big Data Pipeline

#### **Start Pipeline**

1. Vào DAGs tab → Tìm `bigdata_pipeline_start`
2. Click nút ▶️ "Trigger DAG"
3. Cấu hình tùy chọn (nếu cần):
   ```json
   {
     "start_hadoop": true,
     "start_spark": true,
     "start_kafka": true
   }
   ```
4. Click "Trigger"

Pipeline sẽ:

- ✅ Khởi động Hadoop cluster (Namenode → Datanode)
- ✅ Khởi động Spark cluster (Master → Worker)
- ✅ Khởi động Kafka broker
- ✅ Chạy data preparation
- ✅ Train machine learning model
- ✅ Chạy streaming và prediction song song

#### **Stop Pipeline**

1. Vào DAGs tab → Tìm `bigdata_pipeline_stop`
2. Trigger với options:
   ```json
   {
     "stop_hadoop": true,
     "stop_spark": true,
     "stop_kafka": true,
     "remove_volumes": false
   }
   ```

### 3. Monitoring

#### **Xem logs trong Airflow UI**

1. Click vào DAG run
2. Click vào task cụ thể
3. Tab "Logs" hiển thị output chi tiết

#### **Monitor Celery workers**

```bash
# Xem active tasks
celery -A mycelery.system_worker inspect active

# Xem registered tasks
celery -A mycelery.system_worker inspect registered

# Xem worker stats
celery -A mycelery.system_worker inspect stats
```

#### **Flower UI (Optional)**

Start Flower để xem Celery dashboard:

```bash
docker compose --profile flower up -d

# Truy cập: http://localhost:5555
```

### 4. Manual Task Execution

Có thể chạy Celery tasks trực tiếp từ Python:

```python
from mycelery.system_worker import docker_compose_up, run_command

# Start Spark Master
result = docker_compose_up.apply_async(
    args=['~/bd/spark/docker-compose.yml'],
    kwargs={'services': 'spark-master', 'detach': True},
    queue='spark_master'
)

# Lấy kết quả
print(result.get(timeout=60))

# Chạy command
result = run_command.apply_async(
    args=['echo "Hello from worker"'],
    queue='spark_master'
)
print(result.get())
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

---

## 📚 Tài liệu tham khảo

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Celery Documentation](https://docs.celeryq.dev/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

---

## 📝 Lưu ý quan trọng

1. **IP Addresses**: Cập nhật địa chỉ IP trong các file sau cho phù hợp với môi trường của bạn:

   - `docker-compose.yaml`: `AIRFLOW__CELERY__BROKER_URL`
   - `mycelery/system_worker.py`: `REDIS_BROKER`, `CELERY_BACKEND`
   - `mycelery/celeryconfig.py`: `broker_url`, `result_backend`

2. **Security**: Configuration hiện tại chỉ phù hợp cho môi trường development. Với production:

   - Sử dụng password cho Redis
   - Mã hóa kết nối database
   - Cấu hình authentication cho Airflow
   - Sử dụng secrets management

3. **Resource Requirements**:

   - RAM: Tối thiểu 4GB cho Airflow cluster
   - CPU: Khuyến nghị 2+ cores
   - Disk: Tối thiểu 10GB

4. **Backup**: Thường xuyên backup:
   - PostgreSQL database: `docker-compose exec postgres pg_dump airflow > backup.sql`
   - DAG files: Git repository
   - Worker configs: `/etc/celery/`
