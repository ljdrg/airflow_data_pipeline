from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 s3_path = "",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table           
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Preparing to stage data from S3 to Redshift')
        
        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(self.table,
                           self.s3_path,
                           credentials.access_key,
                           credentials.secret_key,
                           self.json_path,
                           self.execution_date)

        redshift_hook.run(copy_query)






