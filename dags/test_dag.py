from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Mihlos',
    'start_date': datetime(2018, 11, 10),
    'end_date': datetime(2018, 11, 15),
    'depends_on_past': False,
    'retries': 1, # 3
    'retry_delay': timedelta(minutes=1), # 5
    'catchup': False,
    'email_on_failure':False
}

dag = DAG('test_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily' # '0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  
                               dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    append_only=True,
    sql= SqlQueries.staging_events_table_create,
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day}-events.json',
    formated='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    append_only=False,
    sql= SqlQueries.staging_songs_table_create,
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    append_only = True,
    sql_create=SqlQueries.songplay_table_create,
    sql_insert=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    append_only = False,
    sql_create=SqlQueries.user_table_create,
    sql_insert=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    append_only = False,
    sql_create=SqlQueries.song_table_create,
    sql_insert=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    append_only = False,
    sql_create=SqlQueries.artist_table_create,
    sql_insert=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    append_only = False,
    sql_create=SqlQueries.time_table_create,
    sql_insert=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Execution flow
# Load STG tables
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
# Load fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# Load dimensionals
load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table
# Check data quality
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
# Finishing
run_quality_checks >> end_operator
