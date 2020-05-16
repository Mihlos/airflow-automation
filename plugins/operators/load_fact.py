from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_create ="",
                 sql_insert ="",
                 redshift_conn_id="",
                 table="",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_create = sql_create
        self.sql_insert = sql_insert
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         redshift.run(f'DROP TABLE IF EXISTS {self.table};')
        redshift.run(self.sql_create)
        
        if self.append_only is False:
                self.log.info("Delete {} table".format(self.table))
                redshift.run("DELETE FROM {}".format(self.table))
        
        redshift.run(self.sql_insert)
        self.log.info('LoadFactOperator done')