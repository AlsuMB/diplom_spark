import csv
import time
from time import sleep

import psycopg2


def get_count():
    conn = psycopg2.connect(database="diplom_spark",
                            user="airflow",
                            host='localhost',
                            password="airflow",
                            port=5432)

    cur = conn.cursor()
    cur.execute('SELECT count(*) FROM hub_department;')
    rows = cur.fetchall()
    conn.commit()
    for row in rows:
        return row[0]
        # print(row)


for i in range(20):
    start = time.time()
    count_rows = get_count()
    sleep(10)
    end = time.time()
    with open('insert_time.csv', mode='a') as insert_time:
        insert_time_writer = csv.writer(insert_time, delimiter=';')

        insert_time_writer.writerow([count_rows, (end - start) * 1000])
