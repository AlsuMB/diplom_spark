import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

from src.hub_department import insert_data

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'
postgresql_driver_path = "../postgresql-42.7.2.jar"

# Создание SparkSession
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars", postgresql_driver_path) \
    .config("spark.driver.extraClassPath", postgresql_driver_path) \
    .getOrCreate()

# Чтение данных из Kafka
kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "158.160.90.238:9092")
            .option("subscribe", "stream_topic")
            .option("startingOffsets", "latest")
            .load())

# Преобразование данных (например, если данные в формате JSON)
schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("hub_load_dts", TimestampType(), True),
    StructField("hub_rec_src", StringType(), True),
])

value_df = kafka_df.selectExpr("CAST(value AS STRING)")
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")


# Функция для записи данных в PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/diplom_spark") \
        .option("dbtable", "hub_department_insert") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()


# value_df.show()
# Использование foreachBatch для записи в PostgreSQL
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()
query.awaitTermination()
