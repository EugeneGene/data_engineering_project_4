from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'Reginald',
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'start_date': pendulum.now(), 
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup': False, # Catchup is turned off
    'email_on_retry': False # Do not email on retry
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="log_data",
        redshift_conn_id="redshift",
        ARN="arn:aws:iam::101621241983:role/dwhRole", 
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        s3_location="s3://udacity-dend/log_data",
        json_format="s3://udacity-dend/log_json_path.json",
        sql_create=SqlQueries.staging_events_table_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="song_data",
        redshift_conn_id="redshift",
        ARN="arn:aws:iam::101621241983:role/dwhRole", 
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A",
        s3_location="s3://udacity-dend/song_data/A/A",
        json_format = "auto",
        sql_create=SqlQueries.staging_songs_table_create
    )


    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        params={
            'table': "factSongPlays"
        },
        sql_create=SqlQueries.songplay_table_create,
        sql_insert=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        params={
            'table': "dimUsers"
        },
        sql_delete=SqlQueries.user_table_delete,
        sql_create=SqlQueries.user_table_create,
        sql_insert=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        params={
            'table': "dimSongs"
        },
        sql_delete=SqlQueries.song_table_delete,
        sql_create=SqlQueries.song_table_create,
        sql_insert=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        params={
            'table': "dimArtists"
        },
        sql_delete=SqlQueries.artist_table_delete,
        sql_create=SqlQueries.artist_table_create,
        sql_insert=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        params={
            'table': "dimTime"
        },
        sql_delete=SqlQueries.time_table_delete,
        sql_create=SqlQueries.time_table_create,
        sql_insert=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        params={
            'tables': ["log_data", "song_data",  "factSongPlays", 
                        "dimArtists", "dimTime", "dimUsers", "dimSongs"]
            
        }

    )


    start_operator >> stage_events_to_redshift

    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table


    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator




final_project_dag = final_project()