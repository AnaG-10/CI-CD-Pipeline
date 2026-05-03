import requests
import psycopg2

# ------------------ EXTRACT ------------------
url = "https://api.tvmaze.com/shows"
response = requests.get(url, timeout=10)
response.raise_for_status()
data = response.json()[:5]

# ------------------ TRANSFORM ------------------
cleaned_data = []
for show in data:
    cleaned_data.append({
        "name": show.get("name"),
        "rating": show.get("rating", {}).get("average") or 0,
        "language": show.get("language")
    })

# ------------------ LOAD ------------------
conn = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow"
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        name TEXT,
        rating FLOAT,
        language TEXT
    )
""")

# avoid duplicates for demo
cur.execute("DELETE FROM movies")

for movie in cleaned_data:
    cur.execute(
        "INSERT INTO movies (name, rating, language) VALUES (%s, %s, %s)",
        (movie["name"], movie["rating"], movie["language"])
    )

conn.commit()
cur.close()
conn.close()

print("ETL Pipeline Executed Successfully")