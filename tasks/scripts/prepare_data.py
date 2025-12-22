import pandas as pd
from sklearn.model_selection import train_test_split
from pyspark.sql import SparkSession
import kagglehub
import sys
import os

spark = SparkSession.builder \
    .appName("Prepare_CreditCard_Data") \
    .getOrCreate()

# Download dataset
dataset_path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
csv_file = os.path.join(dataset_path, "creditcard.csv")

df = pd.read_csv(csv_file)

# Take only 2,000 samples
df_2000, _ = train_test_split(
    df,
    train_size=2000,
    stratify=df["Class"],
    random_state=42
)

train_df, test_df = train_test_split(
    df_2000,
    test_size=0.3,
    stratify=df_2000["Class"],
    random_state=42
)

train_spark = spark.createDataFrame(train_df)
test_spark = spark.createDataFrame(test_df)

train_output = sys.argv[1]
test_output = sys.argv[2]

train_spark.write.mode("overwrite").parquet(train_output)
test_spark.write.mode("overwrite").parquet(test_output)


# save test locally for streaming without spark
local_test_path = "/home/donghuynh0/bd/fp_pr_tasks/test_data/credit_test_streaming.parquet"
test_df.to_parquet(local_test_path, engine='pyarrow', index=False)

spark.stop()
