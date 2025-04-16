import configparser
import psycopg2

from utils import timer_wrap
from sql_queries import create_table_queries, drop_table_queries


@timer_wrap
def drop_tables(cur, conn):
    print('Dropping tables...')
    timed_execute = timer_wrap(cur.execute)
    for query in drop_table_queries:
        timer_wrap(timed_execute(query))
        conn.commit()


@timer_wrap
def create_tables(cur, conn):
    print('Creating tables...')
    timed_execute = timer_wrap(cur.execute)
    for query in create_table_queries:
        timer_wrap(timed_execute(query))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()