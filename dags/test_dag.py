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
    sql= SqlQueries.staging_songs_table_create,
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    append_only = False,
    sql_create=SqlQueries.songplay_table_create,
    sql_insert=SqlQueries.songplay_table_insert
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='users',
#     sql_create='''CREATE TABLE public.users (
# 	userid int4 NOT NULL,
# 	first_name varchar(256),
# 	last_name varchar(256),
# 	gender varchar(256),
# 	"level" varchar(256),
# 	CONSTRAINT users_pkey PRIMARY KEY (userid)
#     );
#     ''',
#     sql_insert=SqlQueries.user_table_insert
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='songs',
#     sql_create='''CREATE TABLE public.songs (
# 	songid varchar(256) NOT NULL,
# 	title varchar(256),
# 	artistid varchar(256),
# 	"year" int4,
# 	duration numeric(18,0),
# 	CONSTRAINT songs_pkey PRIMARY KEY (songid)
#     );
#     ''',
#     sql_insert=SqlQueries.song_table_insert
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='artists',
#     sql_create='''CREATE TABLE public.artists (
# 	artistid varchar(256) NOT NULL,
# 	name varchar(256),
# 	location varchar(256),
# 	lattitude numeric(18,0),
# 	longitude numeric(18,0)
#     );
#     ''',
#     sql_insert=SqlQueries.artist_table_insert
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='time',
#     sql_create='''CREATE TABLE public.time (
#     start_time timestamp NOT NULL,
#     hour int4 NOT NULL,
#     day int4 NOT NULL,
#     week int4 NOT NULL,
#     month int4 NOT NULL,
#     year int4 NOT NULL,
#     weekday int4 NOT NULL,
#     CONSTRAINT time_pkey PRIMARY KEY (start_time)
#     );
#     ''',
#     sql_insert=SqlQueries.time_table_insert
# )


# Execution flow
# Load STG tables
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
# Load fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# Load dimensionals
# load_songplays_table >> load_song_dimension_table 
# load_songplays_table >> load_user_dimension_table 
# load_songplays_table >> load_artist_dimension_table 
# load_songplays_table >> load_time_dimension_table
