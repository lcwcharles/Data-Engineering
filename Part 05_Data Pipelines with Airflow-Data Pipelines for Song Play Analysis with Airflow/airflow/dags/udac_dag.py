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
    'owner': 'Congwei',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
#     s3_key='log_data/2018/11/2018-11-01-events.json',
    s3_key='log_data',
    json='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
#     s3_key='song_data/A/A/A/',
    s3_key='song_data',
    json='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    table_columns = "(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    redshift_conn_id="redshift",
    sql_load_fact = SqlQueries.songplay_table_insert,
    append_data=True,
    provide_context=True

)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    table_columns = "(user_id, firstname, lastname, gender, level)",
    redshift_conn_id="redshift",
    sql_load_dim = SqlQueries.user_table_insert,
    append_data=False,
    provide_context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    table_columns = "(song_id, title, artist_id, year, duration)",
    redshift_conn_id="redshift",
    sql_load_dim = SqlQueries.song_table_insert,
    append_data=False,
    provide_context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    table_columns = "(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)",
    redshift_conn_id="redshift",
    sql_load_dim = SqlQueries.artist_table_insert,
    append_data=False,
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    table_columns = "(start_time, hour, day, week, month, year, weekday)",
    redshift_conn_id="redshift",
    sql_load_dim = SqlQueries.time_table_insert,
    append_data=False,
    provide_context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
		{'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result':0} 
    ],
    table=('songplays', 'songs', 'users', 'artists', 'time'),
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator