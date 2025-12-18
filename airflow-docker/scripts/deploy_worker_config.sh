#!/usr/bin/env bash

# ===============================================
# Deploy Worker Capabilities Config to Remote Hosts
# ===============================================

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIGS_DIR="$PROJECT_DIR/worker_configs"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deploy Worker Capabilities Configuration${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Define mapping: host -> config file
# Using parallel arrays for bash 3.2 compatibility
HOSTS=(
    "192.168.80.55"
    "192.168.80.53"
    "192.168.80.57"
    "192.168.80.87"
)

CONFIGS=(
    "worker_capabilities.spark_master.json"
    "worker_capabilities.spark_worker.json"
    "worker_capabilities.hadoop_namenode.json"
    "worker_capabilities.hadoop_datanode.json"
)

# SSH user (có thể override bằng env var)
SSH_USER="${SSH_USER:-donghuynh0}"

echo "Deployment Plan:"
echo "----------------"

# Descriptions for each config
DESCRIPTIONS=(
    "Spark Master"
    "Spark Worker"
    "Hadoop Namenode + Kafka"
    "Hadoop Datanode"
)

for i in "${!HOSTS[@]}"; do
    echo "  ${HOSTS[$i]} -> ${CONFIGS[$i]}"
    echo "                      (${DESCRIPTIONS[$i]})"
done
echo ""

read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo ""

# Deploy to each host
for i in "${!HOSTS[@]}"; do
    host="${HOSTS[$i]}"
    config="${CONFIGS[$i]}"
    config_path="$CONFIGS_DIR/$config"

    echo -e "${YELLOW}Deploying to $host...${NC}"

    # Check if config file exists
    if [ ! -f "$config_path" ]; then
        echo -e "${RED}✗ Config file not found: $config_path${NC}"
        continue
    fi

    # Copy config file to remote host home directory first
    scp "$config_path" "${SSH_USER}@${host}:~/worker_capabilities.json" || {
        echo -e "${RED}✗ Failed to copy config to $host${NC}"
        continue
    }

    # Create directory and move file with sudo (using -S to read password from stdin)
    # Note: Yêu cầu user phải có quyền sudo
    ssh -t "${SSH_USER}@${host}" "sudo mkdir -p /etc/celery && \
        sudo mv ~/worker_capabilities.json /etc/celery/worker_capabilities.json && \
        sudo chmod 644 /etc/celery/worker_capabilities.json" || {
        echo -e "${RED}✗ Failed to setup config on $host${NC}"
        echo -e "${RED}  Make sure user has sudo privileges${NC}"
        continue
    }

    # Verify
    echo -e "${GREEN}✓ Deployed to $host${NC}"
    echo "  Verifying..."
    ssh "${SSH_USER}@${host}" "cat /etc/celery/worker_capabilities.json" | head -n 5
    echo ""
done

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo "1. Start/restart Celery workers on each host:"
echo "   ssh user@host 'cd /path/to/airflow-docker && ./scripts/start_worker.sh'"
echo ""
echo "2. Verify workers registered with correct queues:"
echo "   celery -A mycelery.system_worker inspect active_queues"
echo ""
