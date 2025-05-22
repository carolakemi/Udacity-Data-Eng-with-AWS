from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'akemi',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='akemi-data-pipeline',
        s3_key='log_data',
        json_path='s3://akemi-data-pipeline/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='akemi-data-pipeline',
        s3_key='song_data',
        json_path='s3://akemi-data-pipeline/song_data/A/A/A/*.json'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_conn_id='redshift',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_conn_id='redshift',
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        redshift_conn_id='redshift',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        redshift_conn_id='redshift',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        redshift_conn_id='redshift',
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks', 
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

final_project_dag = final_project()
