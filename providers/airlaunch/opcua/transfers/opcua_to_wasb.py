from io import BytesIO

from providers.airlaunch.opcua.transfers.opcua_transfer_base import OPCUATransferBaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class OPCUAToWasbOperator(OPCUATransferBaseOperator):
  
    template_fields = ('opcua_node', 'wasb_container', 'wasb_blob')

    def __init__(
        self,
        *,
        opcua_node: str,
        opcua_startdate: str,
        opcua_enddate: str,
        wasb_container: str,
        wasb_blob: str,
        upload_format: str = "json",
        opcua_conn_id: str = 'opcua_default',
        opcua_numvalues: int = 0,
        wasb_conn_id: str = 'wasb_default',
        **kwargs,
    ):
        super().__init__(
            opcua_node=opcua_node,
            opcua_startdate=opcua_startdate,
            opcua_enddate=opcua_enddate,
            opcua_conn_id=opcua_conn_id,
            opcua_numvalues=opcua_numvalues,
            upload_format=upload_format,
            **kwargs)
        self.wasb_container = wasb_container
        self.wasb_blob = wasb_blob
        self.wasb_conn_id = wasb_conn_id
        self.wasb_hook = None

    def _upload(self, data: BytesIO):
        self.wasb_hook = WasbHook(self.wasb_conn_id)
        self.wasb_hook.upload(
                container_name = self.wasb_container, 
                blob_name = self.wasb_blob, 
                data=data
            )
        self.log.info(f'File uploaded to {self.wasb_container}/{self.wasb_blob}')