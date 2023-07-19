from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    # copy_sql = """
    #     COPY {}
    #     FROM '{}'
    #     ACCESS_KEY_ID '{}'
    #     SECRET_ACCESS_KEY '{}'
    #     IGNOREHEADER {}
    #     DELIMITER '{}'
    # """
    # staging_events_copy = ("""
    # COPY song_data
    # FROM 's3://udacity-dend/song_data/A/A'
    # CREDENTIALS 'aws_iam_role={}'
    # FORMAT AS JSON 'auto';""").format(ARN)

    # staging_songs_copy = ("""
    # COPY log_data
    # FROM 's3://udacity-dend/log_data'
    # CREDENTIALS 'aws_iam_role={}'
    # FORMAT AS JSON 's3://udacity-dend/log_json_path.json';""").format(ARN)

    # staging_copy = ("""
    # COPY {}
    # FROM '{}'
    # CREDENTIALS 'aws_iam_role={}'
    # FORMAT AS JSON '{}';""")


    staging_copy = ("""
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON '{}';""")
    


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 redshift_conn_id="",
                 ARN="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 s3_location="",
                 json_format="",
                 sql_create="",               
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.ARN = ARN
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_location = s3_location
        self.json_format = json_format
        self.sql_create = sql_create


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)



        #redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        self.log.info("Create staging {}.".format(self.table))

        self.log.info("Clearing staging {} from destination Redshift table".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Create {self.table} staging table")
        redshift.run(self.sql_create)

        self.log.info("Copying {} from S3 to Redshift".format(self.table))


        formatted_sql = StageToRedshiftOperator.staging_copy.format(
            self.table, 
            self.s3_location, 
            credentials.access_key, 
            credentials.secret_key, 
            self.json_format
        )        

        redshift.run(formatted_sql)






