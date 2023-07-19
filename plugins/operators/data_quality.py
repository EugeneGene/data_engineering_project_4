from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        tables = self.params.get("tables", [])
        checks = self.params.get("checks", [])

        for table in tables:
            for i, check in enumerate(checks):
                test_sql = check.get("test_sql").format(table)
                expected_result = check.get("expected_result")
                comparison = check.get("comparison")
                explain_fail = check.get("explain_fail").format(table)


                if i == 0:
                    records = redshift.get_records(test_sql)
                    if len(records) < expected_result or len(records[0]) < expected_result:
                        raise ValueError("Data quality check failed. {}".format(explain_fail))
                    else:
                        self.log.info(f"Data quality check {i} on table {table} passed.")
                if i == 1:
                    records = redshift.get_records(test_sql)
                    num_records = records[0][0]
                    if num_records >= expected_result:
                        raise ValueError("Data quality check failed. {}".format(explain_fail))
                    else:
                        self.log.info(f"Data quality check {i} on table {table} passed with {records[0][0]} records")
