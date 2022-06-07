import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: 
        Load data on AWS to staging tables using the queries in 'copy_table_queries' list.

    Arguments:
        cur: the cursor object. 
        conn: the connection of the database.
        
    Returns:
        None
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: 
        Insert data of staging tables to the analytics tables using the queries in 'copy_table_queries' list.

    Arguments:
        cur: the cursor object. 
        conn: the connection of the database.

    Returns:
        None
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishe connection with the  database that on AWS and gets
    cursor to it.  
    
    - Load the data on AWS to staging tables.  
    
    - Insert data of staging tables to the analytics tables. 
    
    - Finally, close the connection.     
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