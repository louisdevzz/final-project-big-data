# ===== version with spark =====

SPARK_MASTER="spark://192.168.80.55:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

HADOOP_NAMENODE="hdfs://192.168.80.57:9000"
TEST_PATH="hdfs://192.168.80.57:9000/data/credit_test"
KAFKA_BOOTSTRAP_SERVERS="192.168.80.57:9093"
KAFKA_INPUT_TOPIC="input_data"

PATH_FILE="/home/donghuynh0/bd/fp_pr_tasks/credit_card/scripts/producer.py"


# command
~/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --packages $KAFKA_PACKAGE \
    --conf spark.blockManager.port=40200 \
    --conf spark.shuffle.io.port=40100 \
    --conf spark.driver.port=40300 \
    --conf spark.shuffle.io.connectionTimeout=600s \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=120s \
    $PATH_FILE \
    $HADOOP_NAMENODE \
    $TEST_PATH \
    $KAFKA_BOOTSTRAP_SERVERS \
    $KAFKA_INPUT_TOPIC


# ===== version without spark =====

# PARQUET_PATH="/home/donghuynh0/bd/fp_pr_tasks/test_data/credit_test_streaming.parquet"
# KAFKA_BOOTSTRAP_SERVERS="192.168.80.57:9093"
# KAFKA_INPUT_TOPIC="input_data"
# PATH_FILE="/home/donghuynh0/bd/fp_pr_tasks/credit_card/scripts/producer.py"

# python3 ${PATH_FILE} \
#   ${PARQUET_PATH} \
#   ${KAFKA_BOOTSTRAP_SERVERS} \
#   ${KAFKA_INPUT_TOPIC}