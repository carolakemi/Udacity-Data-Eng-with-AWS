import configparser
import psycopg2
import pandas as pd
from sql_queries import analytical_queries

def run_analytics():
    try:
        # Read config file
        print('Reading configuration...')
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        # Connect to Redshift
        print('Connecting to Redshift...')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
        cur = conn.cursor()
        print('Connected successfully!')

        print('\nRunning Analytics Queries...\n')
        print('='*80)

        # Run each analytical query
        for title, query in analytical_queries.items():
            print(f'\n{title}')
            print('-'*len(title))
            
            try:
                # Execute query
                cur.execute(query)
                results = cur.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in cur.description]
                
                # Create DataFrame and display results
                df = pd.DataFrame(results, columns=column_names)
                print(df.to_string(index=False))
                
            except Exception as e:
                print(f"Error executing query: {str(e)}")
            
            print('='*80)

        # Close connection
        conn.close()
        print('\nAnalytics complete!')

    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    run_analytics() 