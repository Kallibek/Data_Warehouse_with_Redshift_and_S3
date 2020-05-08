import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies user activity and music metadata from S3 to the tables of the Redshift database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from the two copied tables into other tables of the database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Create a connection to the existing Redshift database;
    
    2. Copies data from S3 to the tables of the Redshift database;
    
    3. Inserts data from the two tables into other tables of the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
