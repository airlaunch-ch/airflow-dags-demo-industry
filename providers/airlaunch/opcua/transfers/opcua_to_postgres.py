from io import BytesIO
from typing import Dict

from providers.airlaunch.opcua.transfers.opcua_transfer_base import OPCUATransferBaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

class OPCUAToPostgresOperator(OPCUATransferBaseOperator):

    template_fields = ('opcua_node', 'opcua_startdate', 'opcua_enddate')

    def __init__(
        self,
        *,
        opcua_node: str,
        opcua_startdate: str,
        opcua_enddate: str,
        postgres_table: str,
        opcua_conn_id: str = 'opcua_default',
        opcua_numvalues: int = 0,
        postgres_conn_id: str = 'postgres_default',
        if_exists: str = 'fail',
        dtype: Dict or None = None,
        static_columns: Dict or None = None,
        **kwargs,
    ):
        super().__init__(
            opcua_node=opcua_node,
            opcua_startdate=opcua_startdate,
            opcua_enddate=opcua_enddate,
            opcua_conn_id=opcua_conn_id,
            opcua_numvalues=opcua_numvalues,
            upload_format='json',
            **kwargs)
        self.postgres_table = postgres_table
        self.postgres_conn_id = postgres_conn_id
        self.if_exists = if_exists
        self.static_columns = static_columns
        self.dtype = dtype

    def _upload(self, data: BytesIO):
        self.pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        df = pd.read_json(data)
        if self.static_columns is not None:
            for c in self.static_columns:
                df[c] = self.static_columns[c]

        df.to_sql(self.postgres_table, self.pg_hook.get_sqlalchemy_engine(), if_exists=self.if_exists, index=False, dtype=self.dtype)
        self.log.info(f'File loaded into {self.postgres_table}')

