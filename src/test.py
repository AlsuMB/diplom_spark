import psycopg2
from psycopg2 import sql

# Настройки подключения
host = "localhost"  # Обычно это 'localhost'
dbname = "diplom_spark"
user = "airflow"
password = "airflow"
port = "5432"  # По умолчанию это 5432

try:
    # Создаем соединение
    connection = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )

    # Создаем курсор
    cursor = connection.cursor()

    # Выполняем SQL-запрос
    cursor.execute("select * from hub_department_insert")

    # Получаем результат
    db_version = cursor.fetchone()
    print(f"Вы подключились к - {db_version}")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении к PostgreSQL", error)
finally:
    # Закрываем соединение
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")

