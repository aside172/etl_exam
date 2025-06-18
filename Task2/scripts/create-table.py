from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("transactions-cleanup").getOrCreate()

input_path = "s3a://final-bucket/2025/06/17/transactions_v2.csv"
output_path = "s3a://final-pyspark/clean_transactions"

columns = [
    "customer_id", "name", "surname", "gender",
    "birthdate", "transaction_amount", "date",
    "merchant_name", "category"
]


df = spark.read.csv(input_path, header=False, inferSchema=True)
df = df.toDF(*columns)

clean_df = df.dropna(how="any")

clean_df.write.mode("overwrite").csv(output_path, header=True)