# EОтчет по итоговому заданию ETL

Этот репозиторий содержит код и конфигурацию полного ETL-процесса: загрузка CSV-файлов, очистка и трансформация с помощью PySpark, стриминг через Kafka и загрузка в PostgreSQL.

## Структура репозитория

```
├── airflow-dags/                # DAG файлы для Airflow
│   └── data_pipeline_dag.py     # Оркестрация создания и удаления Dataproc-кластера
├── pyspark-scripts/             # PySpark-приложения
│   ├── clean_transactions.py    # Чистка CSV и сохранение результата
│   ├── kafka_producer.py        # Отправка данных в Kafka
│   └── kafka_consumer_stream.py # Чтение из Kafka и запись в PostgreSQL
├── README.md                    # Этот файл
└── requirements.txt             # Python-зависимости
```

## 1. Очистка данных (clean\_transactions.py)

**Описание:**

* Читаем CSV без заголовка, автоматически определяем схему.
* Задаём имена колонок:
  `customer_id, name, surname, gender, birthdate, transaction_amount, date, merchant_name, category`.
* Удаляем строки с любыми NULL.
* Записываем результат в CSV с заголовком.

**Запуск:**

```bash
spark-submit pyspark-scripts/clean_transactions.py \
  --input s3a://ваш-бакет/raw/transactions_v2.csv \
  --output s3a://ваш-бакет/processed/clean_transactions/
```

## 2. Производитель в Kafka (kafka\_producer.py)

**Описание:**

* Читаем очищенный CSV.
* Каждую секунду выбираем случайные 10 строк.
* Преобразуем в JSON и публикуем в топик `transactions-topic`.

**Запуск:**

```bash
spark-submit pyspark-scripts/kafka_producer.py \
  --csv-path s3a://ваш-бакет/processed/clean_transactions/ \
  --kafka-bootstrap хост:9091
```

## 3. Потребитель стрима (kafka\_consumer\_stream.py)

**Описание:**

* Непрерывно читаем JSON-сообщения из Kafka.
* Парсим поля в нужные типы (даты, числовые).
* Дописываем каждую микропорцию (micro-batch) в таблицу `transactions_stream` в PostgreSQL.

**Запуск:**

```bash
spark-submit pyspark-scripts/kafka_consumer_stream.py
```

## 4. Оркестрация в Airflow

Файл `airflow-dags/data_pipeline_dag.py` автоматизирует:

1. Создание Dataproc-кластера.
2. Запуск трёх PySpark-скриптов в порядке: очистка, продюсер, консьюмер.
3. Удаление кластера после завершения.

Настройте параметры подключений, расписание и пути под своё окружение.

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Результат

* Очищенные данные в Object Storage.
* Топик Kafka с живыми JSON-транзакциями.
* Таблица PostgreSQL постоянно пополняется новыми записями.
