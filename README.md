# Отчет по итоговому заданию ETL

## Структура проекта

```
etl_exam/
├── README.md
├── screenshots/
├── Task1/
│   └── create_table.sql
├── Task2/
│   ├── dags/
│   │   └── Data-Processing-DAG.py
│   └── scripts/
│       └── create-table.py
└── Task3/
    ├── kafka-write.py
    └── kafka-read-stream.py
```

---

## Task1. Создание таблицы в YDB и загрузка данных

**Файл:** `Task1/create_table.sql`
<details>
<summary><i>SQL</i></summary>

```sql
    CREATE TABLE transactions_v2 (
        customer_id        BIGINT,
        name               VARCHAR(64),
        surname            VARCHAR(64),
        gender             VARCHAR(10),
        birthdate          DATE,
        transaction_amount DECIMAL(12,2),
        date               DATE,
        merchant_name      VARCHAR(128),
        category           VARCHAR(64),
        PRIMARY KEY (customer_id)
);
```

</details> 

> После создания таблицы CSV-файл `transactions_v2.csv` загружается в YDB.

---

## Task2. Автоматизация обработки в Airflow + Dataproc

### 1) DAG для создания кластера и запуска PySpark

**Файл:** `Task2/dags/Data-Processing-DAG.py`
<details>
<summary><i>Скрипт</i></summary>

```python
import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Параметры Yandex Cloud
YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAA... your_key_here ...'
YC_DP_SUBNET_ID = 'e9bghbto17oiscpeqdts'
YC_DP_SA_ID = 'ajen3jt84q5s3qkl6o13'
YC_DP_METASTORE_URI = '10.128.0.22'
YC_SOURCE_BUCKET = 'enter-pyspark'
YC_DP_LOGS_BUCKET = 'logs-pyspark'

with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id='create-cluster',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,
        computenode_count=1,
        computenode_max_hosts_count=3,
        services=['YARN', 'SPARK'],
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id='run-cleanup-script',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/create-table.py',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete-cluster',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark >> delete_cluster
```
</details> 

### 2) PySpark-скрипт очистки

**Файл:** `Task2/scripts/create-table.py`

<details>
<summary><i>Скрипт</i></summary>

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("transactions-cleanup") \
    .getOrCreate()

input_path  = "s3a://final-bucket/2025/06/17/transactions_v2.csv"
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

spark.stop()
```
</details>


## Task3. Потоковая работа с Kafka и запись в PostgreSQL

### kafka-write.py

<details>
<summary><i>Скрипт</i></summary>

```python
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, rand

def main():
    spark = SparkSession.builder \
        .appName("csv-to-kafka-loop-json") \
        .getOrCreate()

    csv_path       = os.getenv("CSV_PATH", "s3a://final-pyspark/clean_transactions/")
    kafka_servers  = os.getenv("KAFKA_BOOTSTRAP", "rc1b-bqeqjs0dq3h9q6qu.mdb.yandexcloud.net:9091")
    kafka_topic    = os.getenv("KAFKA_TOPIC", "dataproc-kafka-topic")
    kafka_user     = os.getenv("KAFKA_USER", "user1")
    kafka_password = os.getenv("KAFKA_PASSWORD", "password1")

    columns = [
        "customer_id", "name", "surname", "gender",
        "birthdate", "transaction_amount", "date",
        "merchant_name", "category"
    ]

    df = spark.read.csv(csv_path, header=False, inferSchema=True).toDF(*columns)
    total = df.count()

    while True:
        batch_df = df.orderBy(rand()).limit(10)
        kafka_df = batch_df.select(to_json(struct(*columns)).alias("value"))
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("topic", kafka_topic) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_user}" password="{kafka_password}";') \
            .save()
        time.sleep(1)

    spark.stop()

if __name__ == "__main__":
    main()
```
</details>

### kafka-read-stream.py
<details>
<summary><i>Скрипт</i></summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder \
        .appName("kafka-to-postgres-transactions") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    schema = StructType() \
        .add("customer_id", IntegerType()) \
        .add("name", StringType()) \
        .add("surname", StringType()) \
        .add("gender", StringType()) \
        .add("birthdate", StringType()) \
        .add("transaction_amount", DoubleType()) \
        .add("date", StringType()) \
        .add("merchant_name", StringType()) \
        .add("category", StringType())

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-bqeqjs0dq3h9q6qu.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"password1\";") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("birthdate", to_date(col("birthdate"), "yyyy-MM-dd")) \
        .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss Z 'UTC'"))

    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://rc1b-4sesacni7ap44kq8.mdb.yandexcloud.net:6432/transactions") \
            .option("dbtable", "transactions_stream") \
            .option("user", "user1") \
            .option("password", "password1") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
```
</details>


## Результаты

* CSV успешно очищен и сохранён в CSV формате
* Kafka-топик получает данные в реальном времени
* PostgreSQL-таблица обновляется стримингом из Kafka
