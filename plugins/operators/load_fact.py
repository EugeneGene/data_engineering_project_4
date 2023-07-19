from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_create="",
                 sql_insert="",
                 *args, **kwargs):
                 

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.sql_create=sql_create
        self.sql_insert=sql_insert

    def execute(self, context):
        # table == factSongPlays
        table = self.params.get("table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Create {table}')
    
        redshift.run(self.sql_create)
        
        self.log.info("CREATE factSongPlays...")
        redshift.run(self.sql_create)

        self.log.info(f'Insert data into {table}...')
        redshift.run(self.sql_insert)

