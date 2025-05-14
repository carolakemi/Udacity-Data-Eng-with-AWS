import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("\n=== Loading Staging Tables ===")
    for i, query in enumerate(copy_table_queries):
        print(f'\nExecuting COPY command {i+1}:')
        print(f"Query: {query[:100]}...") 
        try:
            cur.execute(query)
            conn.commit()
            print('Successfully executed COPY command')
        except Exception as e:
            print(f'Error executing COPY command: {str(e)}')
            raise e


def insert_tables(cur, conn):
    print("\n=== Inserting Data into Final Tables ===")
    for i, query in enumerate(insert_table_queries):
        print(f'\nExecuting INSERT command {i+1}:')
        print(f"Query: {query[:100]}...") 
        try:
            cur.execute(query)
            conn.commit()
            print('Successfully executed INSERT command')
        except Exception as e:
            print(f'Error executing INSERT command: {str(e)}')
            raise e


def main():
    try:
        print("Reading configuration...")
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        print("Connecting to Redshift...")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
        cur = conn.cursor()
        print("Connected successfully!")

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        print("\nAll ETL operations completed successfully!")
        conn.close()
        print("Connection closed.")

    except Exception as e:
        print(f"\nAn error occurred during ETL: {str(e)}")
        raise e


if __name__ == "__main__":
    main()