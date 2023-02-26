from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class RedshiftCheckTables(BaseOperator):

    @apply_defaults
    def __init__(self,
            redshift_conn_id: str,
            tables: list,
            **kwargs):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Ensure there is records for each table
        for table in self.tables:
            query = 'SELECT COUNT(*) FROM %s' % table
            result = redshift_hook.get_first(query)
            count = result[0]

            if count == 0:
                raise ValueError('Table: %s has not any records, Data quality check failed' % table)

        self.log.info("Data quality check passed")
