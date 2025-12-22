from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col, to_json, struct
from schema import CREDIT_CARD_SCHEMA
import sys

HADOOP_NAMENODE = sys.argv[1]
MODEL_PATH = sys.argv[2]
KAFKA_BOOTSTRAP_SERVERS = sys.argv[3]
KAFKA_INPUT_TOPIC = sys.argv[4]
KAFKA_OUTPUT_TOPIC = sys.argv[5]

spark = SparkSession.builder \
    .appName("CreditCard_Streaming_Prediction") \
    .getOrCreate()


model = PipelineModel.load(MODEL_PATH)
print("\n\tModel loaded successfully\n")

print(f"⏳ Starting streaming from Kafka topic '{KAFKA_INPUT_TOPIC}'...")
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_stream = stream_df.select(
    from_json(col("value").cast("string"), CREDIT_CARD_SCHEMA).alias("data")
).select("data.*")

predictions_stream = model.transform(parsed_stream)

# Prepare output format
output_stream = predictions_stream.select(
    to_json(struct(
        col("Time"),
        col("Amount"),
        col("Class").alias("actual_label"),
        col("prediction").alias("predicted_label")
    )).alias("value")
)

print("\nStreaming predictions to Kafka...\n")
query = output_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", KAFKA_OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .outputMode("append") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n✓ Stopping stream...")
    query.stop()
    spark.stop()
    print("✓ Stream stopped successfully!")
