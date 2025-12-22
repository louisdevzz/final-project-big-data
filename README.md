# Credit Card Fraud Detection - Big Data Pipeline

A distributed big data system for real-time credit card fraud detection using Apache Airflow, Spark, Hadoop HDFS, and Kafka.

## 🎯 Project Overview

This project implements an end-to-end machine learning pipeline for detecting fraudulent credit card transactions using big data technologies. The system orchestrates data preparation, model training, and real-time streaming predictions across a distributed cluster.

**Key Features:**
- Distributed task orchestration with Apache Airflow and Celery
- Scalable data processing using Apache Spark
- Distributed storage with Hadoop HDFS
- Real-time streaming with Apache Kafka
- Random Forest ML model for fraud detection
- Worker capability-based task routing

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Scheduler                         │
│              (DAG management & task scheduling)              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Redis Broker                              │
│              (Message queue for Celery)                      │
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
```

## 📁 Project Structure

```
.
├── airflow/                    # Airflow orchestration system
│   ├── dags/                   # DAG definitions
│   ├── mycelery/               # Custom Celery workers
│   ├── worker_configs/         # Worker capability definitions
│   └── scripts/                # Deployment scripts
│
└── tasks/                      # ML pipeline tasks
    ├── exes/                   # Executable shell scripts
    └── scripts/                # Python task implementations
```

## 🚀 Quick Start

### 1. Set Up Airflow System

Navigate to the Airflow directory and follow the setup instructions:

```bash
cd airflow
# See airflow/README.md for detailed setup
```

### 2. Prepare and Run ML Pipeline

Navigate to the tasks directory:

```bash
cd tasks/exes

# Set execution permissions
bash setup.sh

# Run the complete pipeline
bash prepare.sh   # Prepare data
bash train.sh     # Train model
bash predict.sh   # Start prediction service
bash producer.sh  # Stream test data (in another terminal)
```

## 📊 ML Pipeline Workflow

1. **Data Preparation** (`prepare.sh`)
   - Downloads Credit Card Fraud dataset from Kaggle
   - Samples 2,000 transactions
   - Splits into train/test sets (70/30)
   - Saves to HDFS in Parquet format

2. **Model Training** (`train.sh`)
   - Trains Random Forest classifier (100 trees, depth 10)
   - Uses Spark MLlib for distributed training
   - Saves model to HDFS

3. **Real-time Prediction** (`predict.sh`)
   - Loads trained model from HDFS
   - Consumes streaming data from Kafka
   - Produces predictions to output topic

4. **Data Streaming** (`producer.sh`)
   - Streams test data to Kafka (2-second intervals)
   - Enables real-time fraud detection

## 🔧 Configuration

### Default Endpoints

- **Hadoop HDFS**: `hdfs://192.168.80.57:9000`
- **Spark Master**: `spark://192.168.80.55:7077`
- **Kafka Broker**: `192.168.80.57:9093`
- **Airflow Web UI**: `http://localhost:9090` (default)

### Kafka Topics

- **Input Topic**: `input_data` (transaction data)
- **Output Topic**: `predictions` (fraud predictions)

## 📚 Documentation

- [Airflow System Setup](airflow/README.md) - Detailed Airflow configuration and deployment
- [Tasks Documentation](tasks/README.md) - ML pipeline scripts and usage
- [Worker Configurations](airflow/worker_configs/README.md) - Worker capability definitions

## 🛠️ Technologies Used

- **Orchestration**: Apache Airflow, Celery
- **Processing**: Apache Spark, PySpark
- **Storage**: Hadoop HDFS
- **Streaming**: Apache Kafka
- **ML Framework**: Spark MLlib, Scikit-learn
- **Database**: PostgreSQL, Redis
- **Languages**: Python, Bash

## 📝 Dataset

This project uses the [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) dataset from Kaggle, which contains transactions made by European cardholders in September 2013.

**Features:**
- 284,807 transactions
- 30 features (Time, Amount, V1-V28 PCA components)
- Highly imbalanced: 492 frauds (0.172%)
- Project uses a stratified sample of 2,000 transactions
