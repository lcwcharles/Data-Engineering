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
    'owner': 'CharlesLee',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2021, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('udacity_capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='00 07 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_immigration_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration_events',
    dag=dag,
    table='immigration_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/immigration.parq',
    fformat='FORMAT AS PARQUET'
)

stage_immigrant_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigrant',
    dag=dag,
    table='immigrant',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/immigrant.parq',
    fformat='FORMAT AS PARQUET'
)

stage_i94cit_res_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94cit_res',
    dag=dag,
    table='i94cit_res',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/df_i94cit_res.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_i94port_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94port',
    dag=dag,
    table='i94port',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/df_i94port.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_i94mode_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94mode',
    dag=dag,
    table='i94mode',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/df_i94mode.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_i94addr_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94addr',
    dag=dag,
    table='i94addr',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/df_i94addr.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)


stage_i94visa_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94visa',
    dag=dag,
    table='i94visa',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/df_i94visa.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport',
    dag=dag,
    table='airport',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/us_airport.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperature',
    dag=dag,
    table='temperature',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/temperature_table_month.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographic',
    dag=dag,
    table='demographic',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='lcw-udacity-capstone-project',
    s3_key='transformed/demographic_table.csv',
    fformat='ignoreheader 1 FORMAT AS CSV'
)


load_immigration_fact_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    table="immigration",
    table_columns = "(cicid, year, month, airport_id, state_id, city, visa_id, admnum, longitude, latitude)",
    redshift_conn_id="redshift",
    sql_load_fact = SqlQueries.immigration_table_insert,
    append_data=True,
    provide_context=True

)

# load_immigrant_dimension_table = LoadDimensionOperator(
#     task_id='Load_immigrant_dim_table',
#     dag=dag,
#     table="immigrant",
#     table_columns = "(admnum, i94bir, gender, biryear, visatype)",
#     redshift_conn_id="redshift",
#     sql_load_dim = SqlQueries.immigrant_table_insert,
#     append_data=False,
#     provide_context=True
# )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM immigration WHERE cicid is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM immigrant WHERE admnum is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM i94cit_res WHERE i94cit_res is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM i94port WHERE i94port is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM i94mode WHERE i94mode is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM i94addr WHERE i94addr is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM i94visa WHERE i94visa is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM airport WHERE local_code is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM temperature WHERE averagetemperature is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM demographic WHERE total_population is null", 'expected_result':0}
    ],
    table=('immigration', 'immigrant', 'i94cit_res', 'i94port', 'i94mode', 'i94addr', 'i94visa', 'airport', 'temperature', 'demographic'),
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_immigration_events_to_redshift >> load_immigration_fact_table
start_operator >> stage_immigrant_to_redshift >> load_immigration_fact_table 
start_operator >> stage_i94cit_res_to_redshift >> load_immigration_fact_table
start_operator >> stage_i94port_to_redshift >> load_immigration_fact_table
start_operator >> stage_i94mode_to_redshift >> load_immigration_fact_table
start_operator >> stage_i94addr_to_redshift >> load_immigration_fact_table
start_operator >> stage_i94visa_to_redshift >> load_immigration_fact_table
start_operator >> stage_airport_to_redshift >> load_immigration_fact_table
start_operator >> stage_temperature_to_redshift >> load_immigration_fact_table
start_operator >> stage_demographic_to_redshift >> load_immigration_fact_table
load_immigration_fact_table >> run_quality_checks >> end_operator