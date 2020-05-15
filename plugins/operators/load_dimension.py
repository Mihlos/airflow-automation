from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql ="",
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f'DROP TABLE IF EXISTS {self.table};')

        format_sql = f'''
        CREATE TABLE {self.table} AS {self.sql}
        '''
        
        redshift.run(format_sql)
        self.log.info(f'LoadDimensionOperator {self.table} done')