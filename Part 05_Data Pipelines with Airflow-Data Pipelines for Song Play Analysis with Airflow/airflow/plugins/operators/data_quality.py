from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table=[],
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
#         for table in self.table:
#             records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
#             if len(records) < 1 or len(records[0]) < 1:
#                 raise ValueError(f"Data quality check failed. {table} returned no results")
#             num_records = records[0][0]
#             if num_records < 1:
#                 raise ValueError(f"Data quality check failed. {table} contained 0 rows")
#             logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        error_count=0
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql)[0]
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
            logging.info(f"Data quality on sql {sql} check passed with {records[0]} error")
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        self.log.info('DataQualityOperator implemented')