from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        
        for dq_check in self.dq_checks:
            check_sql = dq_check.get("check_sql")
            expected_result = dq_check.get("expected_result")
            records = redshift.get_records(check_sql)[0]  
            
            if records[0] == expected_result:
                self.log.info("Data quality check passed.")
            else: 
                self.log.info("Data quality check failed.")
                
        self.log.info("Data quality checks done.")