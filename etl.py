import time
import argparse
import psycopg2
import configparser
from sql_queries import staging_events_copy,staging_songs_copy, insert_table_queries



def load_staging_table(cur, conn, query):
    print("EXECUTING: {}".format(query))
    tic = time.perf_counter()
    cur.execute(query)
    conn.commit()
    toc = time.perf_counter()
    print(f"EXECUTION DONE IN {toc - tic:0.4f} seconds.")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("EXECUTING: {}".format(query))
        tic = time.perf_counter()
        cur.execute(query)
        conn.commit()
        toc = time.perf_counter()
        print(f"EXECUTION DONE IN {toc - tic:0.4f} seconds.")


def main():
    parser = argparse.ArgumentParser(description='ETL for Project 3')
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
        load_staging_table(cur, conn,staging_events_copy)
    if args.staging_songs:
        load_staging_table(cur, conn,staging_songs_copy)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()