from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            aws_credentials_id="",
            table="",
            s3_bucket="",
            s3_key="",
            json_path="auto",
            region="us-east-1",
            *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info('Starting stage load')

        aws_hook = BaseHook.get_connection(self.aws_credentials_id)

        credentials = aws_hook

        redshift = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        redshift.run(f"DELETE FROM {self.table}")

        copy_sql = f"""
        COPY {self.table}
        FROM 's3://{self.s3_bucket}/{self.s3_key}'
        ACCESS_KEY_ID '{credentials.login}'
        SECRET_ACCESS_KEY '{credentials.password}'
        REGION '{self.region}'
        FORMAT AS JSON '{self.json_path}'
        """

        self.log.info(
            f"Loading data from s3://{self.s3_bucket}/{self.s3_key}"
        )

        redshift.run(copy_sql)

        self.log.info("Stage load complete")






