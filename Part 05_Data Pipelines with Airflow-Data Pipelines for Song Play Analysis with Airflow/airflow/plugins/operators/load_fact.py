from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import sql
from sqlalchemy.sql.selectable import tablesample

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_columns="",
                 sql_load_fact="",
                 append_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.table_columns = table_columns
        self.sql_load_fact = sql_load_fact
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            self.log.info("Inserting data to fact table")
            sql_fact = 'INSERT INTO %s %s %s' % (self.table, self.table_columns, self.sql_load_fact)
            redshift.run(sql_fact)
        else:
            self.log.info("Clearing data from destination Redshift table")
            sql_fact = 'DELETE FROM %s' % self.table
            redshift.run(sql_fact)

            self.log.info("Inserting data to fact table")
            sql_fact = 'INSERT INTO %s %s %s' % (self.table, self.table_columns, self.sql_load_fact)
            redshift.run(sql_fact)
        self.log.info('LoadFactOperator implemented.')