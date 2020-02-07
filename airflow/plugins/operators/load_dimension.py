from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 should_truncate="",
                 destination_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = "redshift"
        self.destination_table = destination_table
        self.should_truncate = should_truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Running ETL for dim table {self.destination_table}")
        tables = {"users": SqlQueries.user_table_insert, 
                  "songs": SqlQueries.song_table_insert,
                  "artists": SqlQueries.artist_table_insert,
                  "time": SqlQueries.time_table_insert, 
        }
        if self.should_truncate:
            redshift.run("DELETE FROM {}".format(self.table))
        
        query = f"INSERT INTO {self.destination_table} {tables.get(self.destination_table)}"
        redshift.run(query)
