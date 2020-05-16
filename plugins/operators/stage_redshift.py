from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
        REGION 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 aws_conn_id ='',
                 table='',
                 sql='',
                 s3_bucket="",
                 s3_key="",
                 formated='auto',
                 append_only=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.append_only = append_only
        self.formated=formated

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         redshift.run(f'DROP TABLE IF EXISTS {self.table};')
        redshift.run(self.sql)
    
        if self.append_only is False:
            self.log.info("Delete {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        format_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.formated
        )
        self.log.info("Executing copy")
        redshift.run(format_sql)

        self.log.info('StageToRedshiftOperator tables copied.')