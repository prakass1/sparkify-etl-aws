import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loads the data into the staging through the COPY command to copy the data from S3 storage and insert for each of the copy table queries list.
    
    Args:
    cur (obj): Database cursor obtained from the connection object.
    conn (obj): Database Connection object obtained.
    """
    print("Starting to load the data")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Data loading is completed for query -- ", query)


def insert_tables(cur, conn):
    """ Inserts the data into the fact and dimension tables by utilizing the staging tables.
    
    Args:
    cur (obj): Database cursor obtained from the connection object.
    conn (obj): Database Connection object obtained.
    """
    print("Starting to insert the data")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Data insertion is completed for the query -- ", query.split(" ")[2])


def main():
    """ Acts as a wrapper which connects to the redshift cluster, then performs:
    1. Extraction of the data from the S3 storage and staging the data into the staging tables.
    2. Insertion of the data into the fact and dimension tables.
    """
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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
