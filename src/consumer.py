import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

from src.hub_department import insert_data

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("ProduceConsoleApp") \
    .getOrCreate()

source = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "158.160.90.238:9092")
          .option("subscribe", "stream_topic")
          .option("startingOffsets", "earliest")
          .load()
          )
# печать схемы - необязательна
source.printSchema()
# работа над получаемыми DataFrame
kafka_df = (source.selectExpr('CAST(value AS STRING)', 'offset'))
# блок записи полученных результатов после действий над DataFrame
schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("hub_load_dts", TimestampType(), True),
    StructField("hub_rec_src", StringType(), True),
])

# Преобразование данных в JSON (если данные в формате JSON)
json_df = kafka_df.withColumn("value", from_json(col("value"), schema))

# Обработка данных
processed_df = json_df.select(col("value.*"))

insert_data(processed_df)
# Вывод данных в консоль
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Ожидание завершения выполнения
query.awaitTermination()
