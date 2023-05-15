from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.data_quality_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            self.log.info(f"Running data quality check: {sql}")
            records = redshift.get_records(sql)

            if not records or not len(records[0]) or records[0][0] != expected_result:
                raise ValueError(f"Data quality check failed. {sql} returned {records[0][0]}, expected {expected_result}")

            self.log.info(f"Data quality check passed: {sql}")
