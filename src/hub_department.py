# from datetime import date
#
# from pyspark import Row
# from pyspark.sql import SparkSession
#
#
# def insert_data(ds):
#     DB_HOST = "localhost"
#     DB_NAME = "diplom_spark"
#     DB_USER = "airflow"
#     DB_PASS = "airflow"
#
#     jdbc_url = "jdbc:postgresql://localhost:5432/diplom_spark"
#     connection_properties = {
#         "user": "airflow",
#         "password": "airflow",
#         "driver": "org.postgresql.Driver"
#     }
#
#     spark_postgres = SparkSession.builder \
#         .appName("PostgreSQLApp") \
#         .config("spark.jars", "/Users/alsu/Desktop/Projects/diplom/diplom_spark/src/postgresql-42.7.2.jar") \
#         .getOrCreate()
#
#     # data = [
#     #     Row(department_id=1, hub_rec_src="John Doe", hub_load_dts=date(2024, month=1, day=1)),
#         # Row(id=2, name="Jane Doe", age=25),
#         # Row(id=3, name="Sam Brown", age=35)
#     # ]
#     # df = spark_postgres.createDataFrame(data)
#
#     # Запись данных в таблицу PostgreSQL
#     df.write.jdbc(url=jdbc_url, table="hub_department_insert", mode="append", properties=connection_properties)
#
#     # Завершение работы Spark сессии
#     spark_postgres.stop()
#     # ds.write.jdbc(url=jdbc_url, table="hub_department_insert", mode="append", properties=connection_properties)
#
#
# insert_data('')
from pyspark.sql import SparkSession


def write_to_postgres(batch_df, batch_id):
    try:
        print(f"Processing batch {batch_id}")
        url = "jdbc:postgresql://localhost:5432/diplom_spark"
        properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        batch_df.show()  # Для отладки, показываем данные
        batch_df.write.jdbc(url=url, table="hub_department_insert", mode="append", properties=properties)
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")


def insert_data(df):
    spark = SparkSession.builder \
        .appName("Spark Streaming to PostgreSQL") \
        .config("spark.jars", "postgresql-42.7.2.jar") \
        .getOrCreate()
    df = spark.createDataFrame(df)
    query = df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()
    query.awaitTermination()
    # df.writeStream \
    #     .format("jdbc") \
    #     .option("url", url) \
    #     .option("driver", 'org.postgresql.Driver') \
    #     .option("dbtable", "public") \
    #     .option("user", "airflow") \
    #     .option("password", "airflow") \
    #     .start()
    # df.writeStream \
    #     .outputMode("Append") \
    #     .option("checkpointLocation", "s3a://bucket/check") \
    #     .start()
    # df.wri.jdbc(url=url, table="hub_department_insert", mode="append", properties=properties)
