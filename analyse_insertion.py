import configparser
import psycopg2
from sql_queries import analyze_queries


def analyze_tables_queries(cur):
    print("Starting to analyze some queries")
    for query in analyze_queries:
        cur.execute(query)
        rows = cur.fetchall()
        print(f"--------------------- SQL ----------------------------")
        print(f"{query}")
        for row in rows:
            print(f"--------------------- ROW INFO -----------------------")
            print(f"{row}")

def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    HOST = config.get("CLUSTER", "HOST")
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")
    conn = psycopg2.connect(
        host=HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        # keepalives=1,
        # keepalives_idle=30,
        # keepalives_interval=10,
        # keepalives_count=5
    )
    cur = conn.cursor()

    analyze_tables_queries(cur)

    conn.close()


if __name__ == "__main__":
    main()
