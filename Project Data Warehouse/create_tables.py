import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all existing tables from the database.
    """
    print("Starting to drop tables...")
    for query in drop_table_queries:
        try:
            print(f"Executing drop query: {query[:50]}...")
            cur.execute(query)
            conn.commit()
            print("Drop query executed successfully")
        except Exception as e:
            print(f"Error executing drop query: {str(e)}")
            raise e


def create_tables(cur, conn):
    """
    Creates the staging and analytics tables in the database.
    """
    print("Starting to create tables...")
    for query in create_table_queries:
        try:
            print(f"Executing create query: {query[:50]}...")
            cur.execute(query)
            conn.commit()
            print("Create query executed successfully")
        except Exception as e:
            print(f"Error executing create query: {str(e)}")
            raise e


def main():
    """
    Sets up the database schema for the data warehouse.
    """
    try:
        print("Reading configuration...")
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        print("Connecting to Redshift...")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
        cur = conn.cursor()
        print("Connected successfully!")

        drop_tables(cur, conn)
        create_tables(cur, conn)

        print("All operations completed successfully!")
        conn.close()
        print("Connection closed.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e


if __name__ == "__main__":
    main()