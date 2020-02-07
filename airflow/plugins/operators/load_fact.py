from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 should_truncate="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = "redshift"
        self.destination_table = destination_table
        self.should_truncate = should_truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Running ETL for fact table {self.destination_table}")
        
        if self.should_truncate:
            redshift.run("DELETE FROM {}".format(self.table))
        query = f"INSERT INTO {self.destination_table} {SqlQueries.songplay_table_insert}"
        redshift.run(query)
        