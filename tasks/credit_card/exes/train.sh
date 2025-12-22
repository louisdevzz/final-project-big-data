SPARK_MASTER="spark://192.168.80.55:7077"
TRAIN_PATH="hdfs://192.168.80.57:9000/data/credit_train"
MODEL_PATH="hdfs://192.168.80.57:9000/data/credit_model"
PATH_FILE="/home/donghuynh0/bd/fp_pr_tasks/credit_card/scripts/train_model.py"


# command
~/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.blockManager.port=40200 \
    --conf spark.shuffle.io.port=40100 \
    --conf spark.driver.port=40300 \
    --conf spark.shuffle.io.connectionTimeout=600s \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=120s \
    $PATH_FILE \
    $TRAIN_PATH \
    $MODEL_PATH
