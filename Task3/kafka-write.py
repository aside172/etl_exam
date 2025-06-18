import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, rand

def main():
    spark = SparkSession.builder.appName("csv-to-kafka-loop-json").getOrCreate()

    csv_path = os.getenv("CSV_PATH", "s3a://final-pyspark/clean_transactions/")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "rc1b-bqeqjs0dq3h9q6qu.mdb.yandexcloud.net:9091")
    kafka_topic = os.getenv("KAFKA_TOPIC", "dataproc-kafka-topic")
    kafka_user = os.getenv("KAFKA_USER", "user1")
    kafka_password = os.getenv("KAFKA_PASSWORD", "password1")
    print("Загрузка данных из Kafka...")
    columns = [
        "customer_id", "name", "surname", "gender",
        "birthdate", "transaction_amount", "date",
        "merchant_name", "category"
    ]
    df = spark.read.csv(csv_path, header=False, inferSchema=True)
    df = df.toDF(*columns)
    total = df.count()
    print(f"Загружено {total} строк")
    while True:
        batch_df = df.orderBy(rand()).limit(10)
        kafka_df = batch_df.select(to_json(struct([col(c) for c in batch_df.columns])).alias("value"))
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("topic", kafka_topic) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_user}" password="{kafka_password}";') \
            .save()
        print("Отправлено 10 сообщений в формате JSON в Kafka")
        time.sleep(30)

    spark.stop()


if __name__ == "__main__":
    main()
