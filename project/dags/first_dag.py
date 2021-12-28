import json
import time

try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.hooks.postgres_hook import PostgresHook
    from datetime import datetime
    import requests
    import csv
    import psycopg2

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


# first_task_to_make_request_and_create_csv_file
def first_task_to_make_request_and_create_csv_file():
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    states = \
        ["Bihar"]
         # "West Bengal", "Sikkim", "Assam", "Arunachal Pradesh", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
         # "Tripura"]
    datafile = open('weather.csv', 'w')
    csv_writer = csv.writer(datafile)
    header = \
        ["ID", "state", "description", "temperature", "feels_like_temperature", "min_temperature", "max_temperature",
         "humidity", "clouds"]
    csv_writer.writerow(header)
    id = 1
    for state in states:
        time.sleep(5)
        query_state = state + ",India"
        querystring = {"q": query_state, "lat": "0", "lon": "0", "callback": state, "id": "2172797",
                       "lang": "null", "units": "metric", "mode": "application/json"}
        headers = {
            'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
            'x-rapidapi-key': "f05f7d42c0msh5dd8e0b3890a836p17a3d0jsnc81f300765e1"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        start_index = response.text.index('(')
        end_index = response.text.index(')')
        json_response = json.loads(response.text[start_index + 1: end_index])
        print(json_response)
        row_data = [id, state, json_response['weather'][0]['description'], json_response['main']['temp'],
                    json_response['main']['feels_like'], json_response['main']['temp_min'],
                    json_response['main']['temp_max'], json_response['main']['humidity'],
                    json_response['clouds']['all']]
        id += 1
        print(row_data)
        csv_writer.writerow(row_data)


def second_task_conn_db_create_table():
    conn = psycopg2.connect(host="localhost",
                            port="5432",
                            database="weather",
                            user="postgres",
                            password="admin")
    # pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
    # conn = pg_hook.get_conn()
    cursor = conn.cursor()
    create_table_query = '''CREATE TABLE IF NOT EXISTS Weather
                 (ID INT PRIMARY KEY     NOT NULL,
                 state           TEXT    NOT NULL,
                 description     TEXT  NOT NULL,
                 temperature REAL,
                 feels_like_temperature REAL,
                 min_temperature REAL,
                 max_temperature REAL,
                 humidity INT,
                 clouds TEXT); '''
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully in PostgreSQL ")


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:
    first_task_to_make_request_and_create_csv_file = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_task_to_make_request_and_create_csv_file
    )
    second_task_conn_db_create_table = PythonOperator(
        task_id="second_task_conn_db_create_table",
        python_callable=second_task_conn_db_create_table
    )

first_task_to_make_request_and_create_csv_file >> second_task_conn_db_create_table

