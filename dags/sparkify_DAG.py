from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from stage_redshift import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

BUCKET_NAME = os.environ.get('BUCKET_NAME')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket=BUCKET_NAME,
        s3_key='log_data',
        json_path='auto'
)

stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=BUCKET_NAME,
        s3_key='song_data',
        json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_statement=SqlQueries.user_table_insert,
    truncate_table=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_statement=SqlQueries.song_table_insert,
    truncate_table=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='arists',
    sql_statement=SqlQueries.artist_table_insert,
    truncate_table=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_statement=SqlQueries.time_table_insert,
    truncate_table=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_quality_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.arists WHERE artistid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE playid IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE songid IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE userid IS NULL', 'expected_result': 0 },
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_artist_dimension_table, load_song_dimension_table, load_user_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator