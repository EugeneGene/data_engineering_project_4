from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_delete="",
                 sql_create="",
                 sql_insert="",
                 *args, **kwargs):
                 

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_delete=sql_delete
        self.sql_create=sql_create
        self.sql_insert=sql_insert

    def execute(self, context):
        table = self.params.get("table")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Create {table}...")
        redshift.run(self.sql_create)
        self.log.info(f"Insert data into {table}")
        redshift.run(self.sql_insert)
