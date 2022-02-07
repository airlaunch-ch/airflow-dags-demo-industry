from datetime import datetime, timedelta
from dateutil import parser
from typing import List, Optional, Union
import json
from io import BytesIO

from providers.airlaunch.opcua.transfers.opcua_transfer_base import OPCUATransferBaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.exceptions import AirflowBadRequest

class OPCUAToS3Operator(OPCUATransferBaseOperator):

    template_fields = ('opcua_node', 's3_bucket', 's3_key')

    def __init__(
        self,
        *,
        opcua_node: str,
        opcua_startdate: str,
        opcua_enddate: str,
        s3_bucket: str,
        s3_key: str,
        opcua_conn_id: str = 'opcua_default',
        opcua_numvalues: int = 0,
        aws_conn_id: str = 'aws_default',
        upload_format: str = "json",
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str = None,
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
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.acl_policy = acl_policy
        self.s3_hook = None

    def _upload(self, data: BytesIO):
        self.s3_hook = S3Hook(self.aws_conn_id)
        self.s3_hook.load_file_obj(
            file_obj=data,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace,
            encrypt=self.encrypt,
            acl_policy=self.acl_policy)
        
        self.log.info(f'File uploaded to {self.s3_key}')
