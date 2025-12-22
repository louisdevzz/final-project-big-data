# Tasks Directory

This directory contains all the data processing, model training, and streaming tasks for the Credit Card Fraud Detection project using Apache Spark and Kafka.

## Overview

The tasks pipeline implements a complete machine learning workflow:
1. **Data Preparation** - Download and prepare credit card fraud dataset
2. **Model Training** - Train a Random Forest classifier using Spark MLlib
3. **Data Streaming** - Stream test data to Kafka for real-time processing
4. **Real-time Prediction** - Consume Kafka streams and make fraud predictions

## Directory Structure

```
tasks/
├── exes/              # Executable shell scripts for running tasks
├── scripts/           # Python scripts containing task logic
└── README.md          # This file
```

## Prerequisites

- Apache Spark (configured with `spark-submit` in `~/spark/bin/`)
- Apache Hadoop HDFS (default: `hdfs://192.168.80.57:9000`)
- Apache Kafka (default: `192.168.80.57:9093`)
- Spark Master (default: `spark://192.168.80.55:7077`)

## Shell Scripts (`exes/`)

### 1. setup.sh
Sets execution permissions for all task scripts.

```bash
bash setup.sh
```

### 2. prepare.sh
Downloads the Credit Card Fraud Detection dataset from Kaggle and prepares training/test data.

**Configuration:**
- TRAIN_PATH: `hdfs://192.168.80.57:9000/data/credit_train`
- TEST_PATH: `hdfs://192.168.80.57:9000/data/credit_test`
- SPARK_MASTER: `spark://192.168.80.55:7077`

**Execution:**
```bash
bash prepare.sh
```

**Output:**
- Training data saved to HDFS (70% of 2,000 samples)
- Test data saved to HDFS (30% of 2,000 samples)
- Local parquet file for streaming: `/home/donghuynh0/bd/fp_pr_tasks/test_data/credit_test_streaming.parquet`

### 3. train.sh
Trains a Random Forest model on the prepared training data.

**Configuration:**
- TRAIN_PATH: `hdfs://192.168.80.57:9000/data/credit_train`
- MODEL_PATH: `hdfs://192.168.80.57:9000/data/credit_model`
- SPARK_MASTER: `spark://192.168.80.55:7077`

**Model Parameters:**
- Algorithm: Random Forest Classifier
- Trees: 100
- Max Depth: 10
- Label Column: `Class` (0=Normal, 1=Fraud)

**Execution:**
```bash
bash train.sh
```

**Output:**
- Trained model saved to HDFS at MODEL_PATH

### 4. producer.sh
Streams test data from HDFS to a Kafka topic for real-time prediction.

**Configuration:**
- TEST_PATH: `hdfs://192.168.80.57:9000/data/credit_test`
- KAFKA_BOOTSTRAP_SERVERS: `192.168.80.57:9093`
- KAFKA_INPUT_TOPIC: `input_data`
- Delay: 2 seconds between messages

**Execution:**
```bash
bash producer.sh
```

**Note:** There's also a non-Spark version (commented out) that reads from local parquet file.

### 5. predict.sh
Runs real-time fraud prediction on Kafka streaming data.

**Configuration:**
- MODEL_PATH: `hdfs://192.168.80.57:9000/data/credit_model`
- KAFKA_BOOTSTRAP_SERVERS: `192.168.80.57:9093`
- KAFKA_INPUT_TOPIC: `input_data`
- KAFKA_OUTPUT_TOPIC: `predictions`

**Execution:**
```bash
bash predict.sh
```

**Output Format:**
```json
{
  "Time": 123.45,
  "Amount": 67.89,
  "actual_label": 0,
  "predicted_label": 0
}
```

Stop streaming with `Ctrl+C`.

## Python Scripts (`scripts/`)

### prepare_data.py
Downloads and preprocesses the credit card fraud dataset.

**Features:**
- Downloads dataset from Kaggle using `kagglehub`
- Samples 2,000 transactions (stratified by fraud class)
- Splits into 70/30 train/test sets
- Saves to HDFS in Parquet format
- Creates local copy for streaming

**Arguments:**
1. `train_output` - HDFS path for training data
2. `test_output` - HDFS path for test data

### train_model.py
Trains a Random Forest classifier for fraud detection.

**Pipeline Stages:**
1. VectorAssembler - Combines all features (except `Class`) into feature vector
2. RandomForestClassifier - Trains on `Class` label

**Arguments:**
1. `train_path` - HDFS path to training data
2. `model_path` - HDFS path to save trained model

### producer.py
Streams test data to Kafka topic with configurable delay.

**Features:**
- Reads test data from HDFS
- Converts DataFrame rows to JSON format
- Publishes to Kafka topic with 2-second intervals
- Progress tracking

**Arguments:**
1. `hadoop_namenode` - HDFS namenode URL
2. `test_path` - HDFS path to test data
3. `kafka_bootstrap_servers` - Kafka broker address
4. `kafka_topic` - Target Kafka topic

### predict.py
Real-time fraud prediction using Spark Structured Streaming.

**Features:**
- Loads trained ML model from HDFS
- Consumes from Kafka input topic
- Parses JSON messages using predefined schema
- Applies ML model for predictions
- Publishes predictions to output Kafka topic

**Arguments:**
1. `hadoop_namenode` - HDFS namenode URL
2. `model_path` - HDFS path to trained model
3. `kafka_bootstrap_servers` - Kafka broker address
4. `kafka_input_topic` - Input topic name
5. `kafka_output_topic` - Output topic name

### schema.py
Defines the Spark schema for credit card transaction data.

**Schema:**
- `Time` - Transaction time (DoubleType)
- `V1-V28` - PCA transformed features (DoubleType)
- `Amount` - Transaction amount (DoubleType)
- `Class` - Fraud label: 0=Normal, 1=Fraud (IntegerType)

## Typical Workflow

1. **First-time Setup:**
   ```bash
   cd exes
   bash setup.sh
   ```

2. **Prepare Data:**
   ```bash
   bash prepare.sh
   ```

3. **Train Model:**
   ```bash
   bash train.sh
   ```

4. **Start Prediction Service:**
   ```bash
   bash predict.sh
   ```

5. **Stream Test Data (in another terminal):**
   ```bash
   bash producer.sh
   ```

- **Checkpoint Location:** Kafka streaming uses `/tmp/kafka_checkpoint` for fault tolerance

