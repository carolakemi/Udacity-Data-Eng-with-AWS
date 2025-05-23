from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info(f"Truncating {self.table} table")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Truncated {self.table} table")
        
        self.log.info(f"Loading data into {self.table} table")
        redshift_hook.run(self.sql)
        self.log.info(f"Loaded {self.table} table")
            
            
