import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Load staging tables into Redshift from data stored in S3,
Uses queries defined in 'sql_queries.py' file.
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
Inserts data from staging tables in Redshift into the analytic tables.
"""       
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
"""
- Connect to Redshift cluster. 
- Load staging tables
- Inserts data into analytic tables in Redshift.
"""
def main():
    # Open and Read config file to retrieve Cluster and DWH details required to connect
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
