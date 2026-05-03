from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

# ------------------ DEFAULT ARGS ------------------
default_args = {
    "owner": "ana",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

# ------------------ EXTRACT ------------------
def extract():
    url = "https://api.tvmaze.com/shows"
    response = requests.get(url)
    response.raise_for_status()   # ensures API success

    data = response.json()

    # limit data to avoid large XCom
    return data[:5]


# ------------------ TRANSFORM ------------------
def transform(ti):
    data = ti.xcom_pull(task_ids='extract_task')

    cleaned_data = []

    for show in data:
        cleaned_data.append({
            "name": show.get("name"),
            "rating": show.get("rating", {}).get("average") or 0,
            "language": show.get("language")
        })

    return cleaned_data


# ------------------ LOAD ------------------
def load(ti):
    data = ti.xcom_pull(task_ids='transform_task')

    conn = psycopg2.connect(
        host="postgres",      # use "localhost" if NOT using docker
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            name TEXT,
            rating FLOAT,
            language TEXT
        )
    """)

    # optional: clear old data (prevents duplicates)
    cur.execute("DELETE FROM movies")

    # insert data
    for movie in data:
        cur.execute(
            "INSERT INTO movies (name, rating, language) VALUES (%s, %s, %s)",
            (movie["name"], movie["rating"], movie["language"])
        )

    conn.commit()
    cur.close()
    conn.close()


# ------------------ DAG ------------------
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    # task dependencies
    extract_task >> transform_task >> load_task