from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_columns="",
                 sql_load_dim="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.table_columns = table_columns
        self.sql_load_dim = sql_load_dim
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            self.log.info("Inserting data to fact table")
            sql_dim = 'INSERT INTO %s %s %s' % (self.table, self.table_columns, self.sql_load_dim)
            redshift.run(sql_dim)
        else:
            self.log.info("Clearing data from destination Redshift table")
            sql_dim = 'DELETE FROM %s' % self.table
            redshift.run(sql_dim)

            self.log.info("Inserting data to dimension table")
            sql_dim = 'INSERT INTO %s %s %s' % (self.table, self.table_columns, self.sql_load_dim)
            redshift.run(sql_dim)

        self.log.info('LoadDimensionOperator implemented.')
