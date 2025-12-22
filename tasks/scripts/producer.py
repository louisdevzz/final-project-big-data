
# =================== version with spark =================

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
import sys
import time

HADOOP_NAMENODE = sys.argv[1]
TEST_PATH = sys.argv[2]
KAFKA_BOOTSTRAP_SERVERS = sys.argv[3]
KAFKA_TOPIC = sys.argv[4]

spark = SparkSession.builder \
    .appName("Streaming Test Dataset") \
    .config("spark.hadoop.fs.defaultFS", HADOOP_NAMENODE) \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.cores.max", "8") \
    .getOrCreate()

df = spark.read.parquet(TEST_PATH)

all_cols = [col(c) for c in df.columns]
kafka_ready = df.select(to_json(struct(*all_cols)).alias("value"))

# Collect all rows
rows = kafka_ready.collect()

# Send each row with 2-second delay
for i, row in enumerate(rows):
    # Create a DataFrame with a single row
    single_row_df = spark.createDataFrame([row])
    
    single_row_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .save()
    
    print(f"Sent row {i+1}/{len(rows)}")
    
    if i < len(rows) - 1:
        time.sleep(2)

spark.stop()


# ======================= version without spark ===========================

# read data from locally to stream to kafka
# import sys
# import time
# import json
# import pyarrow.parquet as pq
# from kafka import KafkaProducer

# PARQUET_PATH = sys.argv[1]
# KAFKA_BOOTSTRAP_SERVERS = sys.argv[2]
# KAFKA_TOPIC = sys.argv[3]

# # Read parquet
# table = pq.read_table(PARQUET_PATH)
# rows = table.to_pylist()

# # Create producer
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # Send data
# for i, row in enumerate(rows):
#     producer.send(KAFKA_TOPIC, value=row)
#     producer.flush()
#     print(f"{row}")
#     time.sleep(2)
# producer.close()
