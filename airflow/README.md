# Airflow Data Pipeline Platform

A production-ready Apache Airflow setup with Celery executor, Redis, PostgreSQL, Spark, and Hadoop integration for building scalable data pipelines.

## Architecture

```
                    +------------------+
                    |   Airflow UI     |
                    |   (Port 9090)    |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v----+  +------v------+  +----v--------+
     |  Scheduler  |  | DAG Processor|  |  Triggerer  |
     +-------------+  +-------------+  +-------------+
              |              |              |
              +--------------+--------------+
                             |
                    +--------v---------+
                    |  Celery Workers  |
                    +--------+---------+
                             |
         +-------------------+-------------------+
         |                   |                   |
+--------v-------+  +--------v-------+  +--------v-------+
|     Redis      |  |   PostgreSQL   |  |  Spark/Hadoop  |
|  (Message Queue)|  |   (Metadata)   |  | (Processing)   |
|   Port 6379    |  |   Port 5432    |  |                |
+----------------+  +----------------+  +----------------+
```

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 3.1.1 | Workflow orchestration |
| Celery | - | Distributed task execution |
| Redis | 7.2 | Message broker for Celery |
| PostgreSQL | 16 | Metadata database |
| Apache Spark | - | Distributed data processing |
| Hadoop | - | Distributed storage (HDFS) |
| Flower | - | Celery monitoring (optional) |

## Prerequisites

- Docker & Docker Compose
- Minimum 4GB RAM
- Minimum 2 CPUs
- 10GB+ disk space

## Quick Start

### 1. Clone and Setup

```bash
cd airflow-docker

# Set Airflow UID (Linux users)
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Initialize Airflow

```bash
docker-compose up airflow-init
```

### 3. Start Services

```bash
# Start all services
docker-compose up -d

# Start with Flower monitoring
docker-compose --profile flower up -d
```

### 4. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:9090 | airflow / airflow |
| Flower | http://localhost:5555 | - |

## Project Structure

```
airflow-docker/
├── dags/                  # DAG definitions
├── logs/                  # Airflow logs
├── plugins/               # Custom plugins
├── config/                # Airflow configuration
│   └── airflow.cfg
├── celery/                # Celery configuration
├── docker-compose.yaml    # Docker services
├── .env                   # Environment variables
└── README.md
```

## Services

| Service | Description |
|---------|-------------|
| `postgres` | PostgreSQL database for Airflow metadata |
| `redis` | Redis message broker for Celery |
| `airflow-apiserver` | Airflow REST API and Web UI |
| `airflow-scheduler` | Schedules and triggers DAG runs |
| `airflow-dag-processor` | Processes DAG files |
| `airflow-worker` | Celery worker for task execution |
| `airflow-triggerer` | Handles deferred tasks |
| `flower` | Celery monitoring dashboard (optional) |

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.1
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Additional pip packages (for quick testing only)
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-apache-spark apache-airflow-providers-apache-hdfs

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# Hadoop
HADOOP_CONF_DIR=/etc/hadoop/conf
```

### Spark Integration

Add Spark provider to your DAGs:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_task = SparkSubmitOperator(
    task_id='spark_job',
    application='/opt/airflow/dags/spark_jobs/my_job.py',
    conn_id='spark_default',
    verbose=True,
)
```

### Hadoop/HDFS Integration

```python
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

hdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
```

## Common Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Scale workers
docker-compose up -d --scale airflow-worker=3

# Access Airflow CLI
docker-compose run airflow-cli airflow dags list

# Restart a service
docker-compose restart airflow-worker

docker compose restart airflow-dag-processor

# Clean up (remove volumes)
docker-compose down -v
```

## Celery Worker Management Scripts

Hệ thống sử dụng kiến trúc **capability-based routing** cho Celery workers. Mỗi worker được cấu hình với các capabilities cụ thể (spark_master, hadoop_namenode, docker_host, v.v.) và tự động subscribe vào các queues tương ứng.

### 1. `scripts/start_worker.sh`

**Mục đích**: Khởi động Celery worker với cấu hình capability-based routing

**Chức năng**:
- Tự động đọc file cấu hình capabilities từ `/etc/celery/worker_capabilities.json`
- Kiểm tra kết nối Redis broker trước khi start
- Khởi động worker với auto queue detection (worker tự động subscribe queues dựa trên capabilities)
- Không cần chỉ định queues thủ công - worker tự động xác định dựa trên config

**Cách sử dụng**:
```bash
# Source environment variables
source .env

# Khởi động worker
./scripts/start_worker.sh
```

**Worker sẽ tự động**:
- Đọc capabilities từ config file
- Subscribe vào các queues phù hợp (vd: spark_master, hadoop_namenode)
- Xử lý tasks với concurrency=4, timeout=3600s

**Environment variables** (trong `scripts/.env`):
```bash
CELERY_BROKER_URL=redis://192.168.80.192:6379/0
CELERY_RESULT_BACKEND=redis://192.168.80.192:6379/0
WORKER_CAPABILITIES_FILE=/etc/celery/worker_capabilities.json
CELERY_WORKER_CONCURRENCY=4
CELERY_WORKER_MAX_TASKS_PER_CHILD=100
CELERY_WORKER_TIME_LIMIT=3600
CELERY_WORKER_SOFT_TIME_LIMIT=3300
CELERY_WORKER_LOG_LEVEL=info
```

### 2. `scripts/deploy_worker_config.sh`

**Mục đích**: Deploy file cấu hình capabilities lên các remote workers

**Chức năng**:
- Deploy worker capabilities config từ `worker_configs/` lên nhiều remote hosts
- Mapping tự động: mỗi host → config file tương ứng
  - `192.168.80.192` → `worker_capabilities.spark_master.json`
  - `192.168.80.53` → `worker_capabilities.spark_worker.json`
  - `192.168.80.57` → `worker_capabilities.hadoop_namenode.json`
  - `192.168.80.87` → `worker_capabilities.hadoop_datanode.json`
- Tự động tạo thư mục `/etc/celery/` trên remote hosts
- Copy và set permissions cho config files
- Verify deployment thành công

**Cách sử dụng**:
```bash
# Source environment variables
source scripts/.env

# Deploy configs to all remote workers
./scripts/deploy_worker_config.sh

# Script sẽ hiển thị deployment plan và yêu cầu xác nhận
# Sau khi deploy, restart workers trên các remote hosts:
ssh user@192.168.80.192 'cd /path/to/airflow-docker && ./scripts/start_worker.sh'
```

**Workflow deploy workers**:
1. Tạo/chỉnh sửa config files trong `worker_configs/`
2. Chạy `deploy_worker_config.sh` để deploy lên remote hosts
3. Start/restart workers trên từng host bằng `start_worker.sh`
4. Verify bằng: `celery -A mycelery.system_worker inspect active_queues`

### Run Celery Worker (Manual - Legacy Method)

Nếu muốn chạy Celery worker thủ công (ngoài Docker) - phương pháp cũ:

```bash
# Từ thư mục airflow-docker/
celery -A mycelery.worker.app worker --loglevel=INFO

# Hoặc từ thư mục mycelery/
cd mycelery
celery -A worker.app worker --loglevel=INFO
```

## Creating DAGs

Place your DAG files in the `dags/` directory:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def my_task():
    print("Hello from Airflow!")

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task,
    )
```

## Monitoring

### Flower Dashboard

Enable Flower for Celery monitoring:

```bash
docker-compose --profile flower up -d
```

Access at http://localhost:5555

### Health Checks

```bash
# Check scheduler health
curl http://localhost:8974/health

# Check API server
curl http://localhost:9090/api/v2/version
```

## Troubleshooting

### Common Issues

**Memory issues:**
```bash
# Increase Docker memory allocation to at least 4GB
```

**Permission issues (Linux):**
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down && docker-compose up -d
```

**DAGs not appearing:**
```bash
# Check DAG processor logs
docker-compose logs airflow-dag-processor

# Verify DAG syntax
docker-compose run airflow-cli airflow dags list
```

**Worker not picking up tasks:**
```bash
# Check worker logs
docker-compose logs airflow-worker

# Verify Redis connection
docker-compose exec redis redis-cli ping
```

## Production Considerations

- Use secrets management for credentials
- Configure external PostgreSQL and Redis
- Set up proper logging and monitoring
- Use custom Docker image with pre-installed dependencies
- Configure resource limits for containers
- Enable SSL/TLS for all connections
- Set up proper backup strategies

## License

Apache License 2.0

## Worker Setup - UV Run Commands

### Automatic Queue Detection (Recommended)

Deploy capability config và start worker tự động:

```bash
# 1. Deploy worker capabilities config
bash scripts/deploy_worker_config.sh

# 2. Start worker with auto queue detection
bash scripts/start_worker.sh
```

### Manual Setup with UV Run

Nếu không dùng script, có thể start worker thủ công với `uv run`:

**1. Trên máy 192.168.80.55 (Spark Master)**
```bash
# Foreground (để debug)
uv run celery -A mycelery.system_worker worker \
    --queues=spark_master,spark_common,docker_host,celery \
    --loglevel=INFO \
    --concurrency=4 \
    --max-tasks-per-child=100 \
    --time-limit=3600 \
    --soft-time-limit=3300

# Background (production)
nohup uv run celery -A mycelery.system_worker worker \
    --queues=spark_master,spark_common,docker_host,celery \
    --loglevel=INFO \
    --concurrency=4 \
    --max-tasks-per-child=100 \
    --time-limit=3600 \
    --soft-time-limit=3300 > worker.log 2>&1 &
```

**2. Trên máy 192.168.80.53 (Spark Worker)**
```bash
nohup uv run celery -A mycelery.system_worker worker \
    --queues=spark_worker,spark_common,docker_host,celery \
    --loglevel=INFO \
    --concurrency=4 \
    --max-tasks-per-child=100 \
    --time-limit=3600 \
    --soft-time-limit=3300 > worker.log 2>&1 &
```

**3. Trên máy 192.168.80.57 (Hadoop Namenode + Kafka)**
```bash
nohup uv run celery -A mycelery.system_worker worker \
    --queues=hadoop_namenode,hadoop_common,kafka,docker_host,celery \
    --loglevel=INFO \
    --concurrency=4 \
    --max-tasks-per-child=100 \
    --time-limit=3600 \
    --soft-time-limit=3300 > worker.log 2>&1 &
```

**4. Trên máy 192.168.80.87 (Hadoop Datanode)**
```bash
nohup uv run celery -A mycelery.system_worker worker \
    --queues=hadoop_datanode,hadoop_common,docker_host,celery \
    --loglevel=INFO \
    --concurrency=4 \
    --max-tasks-per-child=100 \
    --time-limit=3600 \
    --soft-time-limit=3300 > worker.log 2>&1 &
```

### Verify Workers

```bash
# Check active workers
uv run celery -A mycelery.system_worker inspect ping

# Check queues each worker subscribes to
uv run celery -A mycelery.system_worker inspect active_queues

# Check worker stats
uv run celery -A mycelery.system_worker inspect stats

# Monitor tasks
uv run celery -A mycelery.system_worker inspect active
```

### Stop Workers

```bash
# Graceful shutdown
pkill -TERM -f "celery.*mycelery.system_worker"

# Force kill (không khuyến khích)
pkill -9 -f "celery.*mycelery.system_worker"
```

DAGs đã tạo:

| DAG                    | Mô tả                                                                                    |
|------------------------|------------------------------------------------------------------------------------------|
| bigdata_pipeline_start | Khởi động cluster theo thứ tự: Namenode → Datanode + Spark Master → Spark Worker + Kafka |
| bigdata_pipeline_stop  | Dừng cluster theo thứ tự ngược: Kafka → Spark → Hadoop                                   |

Pipeline Flow:

Start:
Hadoop Namenode (55)
    ├── Hadoop Datanode (87) → Kafka (57)
    └── Spark Master (55) → Spark Worker (53)

Stop:
Kafka (57) → Spark Worker (53) → Spark Master (55) → Hadoop Datanode (87) → Hadoop Namenode (57)