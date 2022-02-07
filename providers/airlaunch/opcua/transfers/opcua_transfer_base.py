from datetime import datetime
from dateutil import parser
import json
from io import BytesIO

from abc import ABC, abstractmethod

from airflow.models import BaseOperator
from providers.airlaunch.opcua.hooks.opc import OPCUAHook

from airflow.exceptions import AirflowBadRequest

class OPCUATransferBaseOperator(BaseOperator, ABC):
    """
    This operator enables the transfer of files from an OPCUA server to S3.
    :param opcua_node: The ID of the OPC UA node.
    :type opcua_node: str
    :param opcua_conn_id: The opcua connection id. The name or identifier for
        establishing a connection to the OPCUA server.
    """

    template_fields = ('opcua_node')

    def __init__(
        self,
        *,
        opcua_node: str,
        opcua_startdate: str,
        opcua_enddate: str,
        opcua_conn_id: str = 'opcua_default',
        opcua_numvalues: int = 0,
        upload_format='json',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.opcua_node = opcua_node
        self.opcua_startdate = opcua_startdate
        self.opcua_enddate = opcua_enddate
        self.opcua_numvalues = opcua_numvalues
        self.opcua_conn_id = opcua_conn_id
        self.upload_format = upload_format
        self.opcua_hook = None

    def execute(self, context):

        startdatetime = parser.parse(self.opcua_startdate)
        enddatetime = parser.parse(self.opcua_enddate)

        self.opcua_hook = OPCUAHook(opcua_conn_id=self.opcua_conn_id)
        history = self.opcua_hook.get_history(node=self.opcua_node, starttime=startdatetime, endtime=enddatetime, numvalues=self.opcua_numvalues)

        bBuffer = BytesIO()
        if self.upload_format == "json":
            bBuffer.write(json.dumps(history, cls=DateTimeAwareEncoder).encode())
        else:
            raise AirflowBadRequest("file format '{self.upload_format}' not supported")
        bBuffer.seek(0)
        self._upload(bBuffer)

    @abstractmethod
    def _upload(self, data: BytesIO):
        pass

class DateTimeAwareEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)