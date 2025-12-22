from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import sys

spark = SparkSession.builder \
    .appName("Train_CreditCard_Model") \
    .getOrCreate()

train_path = sys.argv[1]
train_data = spark.read.parquet(train_path)

print("\n\tData loaded!\n")

feature_cols = [col for col in train_data.columns if col != 'Class']

# Vectorize features
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

rf = RandomForestClassifier(
    labelCol="Class", 
    featuresCol="features", 
    numTrees=100,
    maxDepth=10,
    seed=42
)

pipeline = Pipeline(stages=[assembler, rf])

model = pipeline.fit(train_data)

model.write().overwrite().save(sys.argv[2])

print("\nModel trained and saved\n")
spark.stop()