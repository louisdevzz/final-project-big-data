#!/usr/bin/env bash

# ===============================================
# Celery Worker Start Script
# Capability-based routing - tự động detect queues
# ===============================================

set -e

# Activate uv environment if uv is available
if command -v uv &> /dev/null; then
    echo -e "${GREEN}Activating uv environment...${NC}"
    # Check if .venv exists
    if [ -d ".venv" ]; then
        source .venv/bin/activate
    else
        echo -e "${YELLOW}Warning: .venv not found, trying to sync with uv...${NC}"
        uv sync
        source .venv/bin/activate
    fi
fi

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Starting Celery Worker (Capability-based)${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Get worker directory first
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORKER_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if exists
if [ -f "$SCRIPT_DIR/.env" ]; then
    echo -e "${GREEN}Loading environment from .env${NC}"
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
    echo ""
fi

# Config file location
CAPABILITIES_FILE="${WORKER_CAPABILITIES_FILE:-/etc/celery/worker_capabilities.json}"

# Check if config file exists
if [ ! -f "$CAPABILITIES_FILE" ]; then
    echo -e "${YELLOW}Warning: Capabilities config not found at $CAPABILITIES_FILE${NC}"
    echo -e "${YELLOW}Worker will start with default capabilities (docker_host only)${NC}"
    echo ""
    echo -e "${YELLOW}To configure capabilities, create:${NC}"
    echo -e "${YELLOW}  sudo cp worker_configs/worker_capabilities.*.json /etc/celery/worker_capabilities.json${NC}"
    echo ""
else
    echo -e "${GREEN}✓ Found capabilities config: $CAPABILITIES_FILE${NC}"
    echo ""
    echo "Capabilities:"
    grep -A 10 "capabilities" "$CAPABILITIES_FILE" || echo "  (Config file exists)"
    echo ""
fi

cd "$WORKER_DIR"

# Check Redis connection
REDIS_HOST="${CELERY_BROKER_URL:-redis://192.168.80.192:6379/0}"
echo -e "${GREEN}Checking Redis connection...${NC}"
echo "Redis: $REDIS_HOST"
echo ""

# Extract host and port from REDIS_HOST
REDIS_ADDR=$(echo "$REDIS_HOST" | sed -E 's|redis://([^:]+):([0-9]+)/.*|\1 -p \2|')

if command -v redis-cli &> /dev/null; then
    if eval "redis-cli -h $REDIS_ADDR ping" &> /dev/null; then
        echo -e "${GREEN}✓ Redis connection OK${NC}"
    else
        echo -e "${RED}✗ Cannot connect to Redis${NC}"
        echo -e "${RED}Please check CELERY_BROKER_URL: $REDIS_HOST${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}Warning: redis-cli not installed, skipping connection check${NC}"
fi

echo ""
echo -e "${GREEN}Starting worker...${NC}"
echo "Working directory: $WORKER_DIR"
echo ""

# Worker configuration from environment variables
WORKER_CONCURRENCY="${CELERY_WORKER_CONCURRENCY:-4}"
WORKER_MAX_TASKS="${CELERY_WORKER_MAX_TASKS_PER_CHILD:-100}"
WORKER_TIME_LIMIT="${CELERY_WORKER_TIME_LIMIT:-3600}"
WORKER_SOFT_TIME_LIMIT="${CELERY_WORKER_SOFT_TIME_LIMIT:-3300}"
WORKER_LOG_LEVEL="${CELERY_WORKER_LOG_LEVEL:-info}"

echo "Worker Settings:"
echo "  Concurrency: $WORKER_CONCURRENCY"
echo "  Max tasks per child: $WORKER_MAX_TASKS"
echo "  Time limit: ${WORKER_TIME_LIMIT}s"
echo "  Soft time limit: ${WORKER_SOFT_TIME_LIMIT}s"
echo "  Log level: $WORKER_LOG_LEVEL"
echo ""

# Get queues from worker config
WORKER_QUEUES=$(python3 -c "from mycelery.worker_config import get_worker_queues; print(','.join(get_worker_queues()))")

echo "Subscribing to queues: $WORKER_QUEUES"
echo ""

# Start worker with auto-detected queues
exec celery -A mycelery.system_worker worker \
    --queues="$WORKER_QUEUES" \
    --loglevel="$WORKER_LOG_LEVEL" \
    --concurrency="$WORKER_CONCURRENCY" \
    --max-tasks-per-child="$WORKER_MAX_TASKS" \
    --time-limit="$WORKER_TIME_LIMIT" \
    --soft-time-limit="$WORKER_SOFT_TIME_LIMIT"
