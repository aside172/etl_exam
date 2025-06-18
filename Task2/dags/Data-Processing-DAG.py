import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC90x/+eFpPM7f06h+sb+69R3MkiVesVp7PF4YSXWZ/ixJ8fZ1K+HVw5TtV1/lqiNE4upCnifulM30h56b8HH2xe6mBfIo9YqUAxIHupiMWjUVI0qFcacuqRoA22hiohFcdNogJN/Q0UnUz4LR5X2YGWRowd7dUababhFPwWYxmsxhbfIHgHhhNPpNe3528xhDzCT7pquOEN1slty+9UeV16CbxgoTwSjQ39PrF85GLckudVHPm/v4N2GcsSxyMVXNlD/p0cJfhMC9yXK63vS3frF++upniZD4ezr3LfQtA8ip3PkGFEs+4VdiLD0+XVn/0SyWW2i6XDBm3A+FbgJTRw/Ux5jYepG7E/++diRsJKODOODT6eiB47jTa0g5asOZFadmyD0wTC/vIk8sJbXgv0/xviQOZJfkq96YMLuZj5BrBJH3kKX5OY8CHnIMKFNeXpkR7aA0E+N/bjJMU9mKGD4G92CaFKUKmn0iVoaOBYgFNNA6bK3ds9S5IZRoNjXU= danya@MacBook-Air-Daniil.local'
YC_DP_SUBNET_ID = 'e9bghbto17oiscpeqdts'
YC_DP_SA_ID = 'ajen3jt84q5s3qkl6o13'
YC_DP_METASTORE_URI = '10.128.0.22'
YC_SOURCE_BUCKET = 'enter-pyspark'
YC_DP_LOGS_BUCKET = 'logs-pyspark'

with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    # 1 этап: создание кластера Yandex Data Proc
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
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
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/create-table.py',
    )

    # 3 этап: удаление кластера Yandex Data Processing
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
