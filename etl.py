import configparser
import psycopg2

from utils import timer_wrap
from sql_queries import copy_table_queries, insert_table_queries


@timer_wrap
def load_staging_tables(cur, conn):
    print('Loading staging tables...')
    timed_execute = timer_wrap(cur.execute)
    for query in copy_table_queries:
        timer_wrap(timed_execute(query))
        conn.commit()


@timer_wrap
def insert_tables(cur, conn):
    print('Inserting into tables...')
    timed_execute = timer_wrap(cur.execute)
    for query in insert_table_queries:
        timer_wrap(timed_execute(query))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()