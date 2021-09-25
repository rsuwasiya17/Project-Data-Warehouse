"""
Using config file to store AWS credentials and cluster details
Using configparser library to extract the config details to be used within ETL process 
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
Drops each table using the queries mentioned in the 'drop_table_queries' list.
"""

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
"""
Creates staging, fact, and dimensional tables using the 'create_table_queries' list in sql_queries.py. 
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
"""
- Read DB credential from config file
- Dropping staging and final tables if exists
- Creating staging and final tables
"""

def main():
    # Open and Read config file to retrieve Cluster and DWH details required to connect
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
