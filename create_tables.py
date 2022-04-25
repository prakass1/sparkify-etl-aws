import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drops the tables if they exist.
    
    Args:
    cur (obj): Database cursor obtained from the connection object.
    conn (obj): Database Connection object obtained.
    """
    for query in drop_table_queries:
        print(f"Query --> {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates the table if not exists based on the queries provided in the sql_queries.
    
    Args:
    cur (obj): Database cursor obtained from the connection object.
    conn (obj): Database Connection object obtained.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Table Created --> {query.split(' ')[5]}")


def main():
    """ Acts as a wrapper which connects to the redshift cluster, then drops the tables and recreates them.
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
