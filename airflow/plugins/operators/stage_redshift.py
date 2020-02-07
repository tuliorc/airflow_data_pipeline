from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
              COPY {}
              FROM '{}'
              ACCESS_KEY_ID '{}'
              SECRET_ACCESS_KEY '{}'
              IGNOREHEADER '{}'
              """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = "redshift"
        self.aws_credentials_id="aws_credentials"
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Clearing data from the Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from S3 to the Redshift table {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        credentials = aws_hook.get_credentials()
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers, 
        )
        
        redshift.run(formatted_sql)
