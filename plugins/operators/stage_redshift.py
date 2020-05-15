from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json 'auto' 
        REGION 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 conn_id ='',
                 table='',
                 sql='',
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.s3_bucket=s3_bucket,
        self.s3_key=s3_key,

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f'DROP TABLE IF EXISTS {self.table};')
        redshift.run(self.sql)

        self.log.info("Copying data from S3 to Redshift")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        format_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.credentials.access_key,
            self.credentials.secret_key,
        )
        redshift.run(format_sql)

        self.log.info('StageToRedshiftOperator tables copied.')