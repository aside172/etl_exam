from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

def main():
    spark = (
        SparkSession.builder
            .appName("kafka-to-postgres-transactions")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
    )

    schema = (
        StructType()
            .add("customer_id", IntegerType())
            .add("name", StringType())
            .add("surname", StringType())
            .add("gender", StringType())
            .add("birthdate", StringType())
            .add("transaction_amount", DoubleType())
            .add("date", StringType())
            .add("merchant_name", StringType())
            .add("category", StringType())
    )

    kafka_df = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "rc1b-bqeqjs0dq3h9q6qu.mdb.yandexcloud.net:9091")
            .option("subscribe", "dataproc-kafka-topic")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=\"user1\" password=\"password1\";"
            )
            .option("startingOffsets", "latest")
            .load()
    )

    parsed_df = (
        kafka_df
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
            .withColumn("birthdate", to_date(col("birthdate"), "yyyy-MM-dd"))
            .withColumn(
                "date",
                to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss Z 'UTC'")
            )
    )

    def write_to_postgres(batch_df, batch_id):
        (
            batch_df.write
                .format("jdbc")
                .option("url", "jdbc:postgresql://rc1b-4sesacni7ap44kq8.mdb.yandexcloud.net:6432/transactions")
                .option("dbtable", "transactions_stream")
                .option("user", "user1")
                .option("password", "password1")
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
        )

    query = (
        parsed_df.writeStream
            .foreachBatch(write_to_postgres)
            .trigger(processingTime="10 seconds")
            .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
