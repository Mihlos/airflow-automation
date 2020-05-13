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
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  
                               dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    sql= """
    CREATE TABLE public.staging_events (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
    );
"""
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    sql_create= """
    CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
    );
    """
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Execution flow
# Load STG tables
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
# Load dimensionals
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
# Check data quality
run_quality_checks >> end_operator

