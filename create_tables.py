import time
import argparse
import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries
from sql_queries import staging_events_table_create, staging_songs_table_create
from sql_queries import staging_events_table_drop, staging_songs_table_drop


def drop_tables(cur, conn):
    """Drop tables specified in the drop_table_queries variable from database

    Args:
        cur: the cursor variable
        conn: the conection to the database
    """
    for query in drop_table_queries:
        print("EXECUTING: {}".format(query))
        tic = time.perf_counter()
        cur.execute(query)
        conn.commit()
        toc = time.perf_counter()
        print(f"EXECUTION DONE IN {toc - tic:0.4f} seconds.")

def execute_query(cur, conn,query):
    """Executes a given query in the database

    Args:
        cur: the cursor variable
        conn: the conection to the database
        query (str): SQL query to be run on database
    """
    print("EXECUTING: {}".format(query))
    tic = time.perf_counter()
    cur.execute(query)
    conn.commit()
    toc = time.perf_counter()
    print(f"EXECUTION DONE IN {toc - tic:0.4f} seconds.")

def create_tables(cur, conn):
    """Creates tables,  specified in the create_table_queries variable, in the database

    Args:
        cur: the cursor variable
        conn: the conection to the database
    """
    for query in create_table_queries:
        print("EXECUTING: {}".format(query))
        tic = time.perf_counter()
        cur.execute(query)
        conn.commit()
        toc = time.perf_counter()
        print(f"EXECUTION DONE IN {toc - tic:0.4f} seconds.")

def main():
    parser = argparse.ArgumentParser(description='Create Tables for Project 3')
    parser.add_argument('--staging-events', dest='staging_events', action='store_true', help='Declares that the event staging table must be loaded.')
    parser.add_argument('--no-staging-events', dest='staging_events', action='store_false', help='Declares that the event staging table must not be loaded.')
    parser.add_argument('--staging-songs', dest='staging_songs', action='store_true', help='Declares that the songs staging table must be loaded.')
    parser.add_argument('--no-staging-songs', dest='staging_songs', action='store_false', help='Declares that the songs staging table must not be loaded.')
    parser.set_defaults(staging_events=False,staging_songs=False)
    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    if args.staging_events:
        execute_query(cur, conn,staging_events_table_drop)
        execute_query(cur, conn,staging_events_table_create)
    if args.staging_songs:
        execute_query(cur, conn,staging_songs_table_drop)
        execute_query(cur, conn,staging_songs_table_create)        
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()