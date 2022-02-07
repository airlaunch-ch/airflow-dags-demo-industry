#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from copy import deepcopy
from typing import Dict, Optional

from asyncua import Client

from airflow.hooks.dbapi import BaseHook
from airflow.models.connection import Connection

import asyncio

class OPCUAHook(BaseHook):
    """
    Interact with OPC UA Server.

    :param opcua_conn_id: The
        reference to a specific opc ua server instance.
    :type opcua_conn_id: str
    """

    conn_name_attr = 'opcua_conn_id'
    default_conn_name = 'opcua_default'
    conn_type = 'opcua'
    hook_name = 'Opcua'

    def __init__(self, opcua_conn_id="opcua_default", *args, **kwargs) -> None:
        super().__init__()
        self.connection: Optional[Connection] = kwargs.pop("connection", None)
        self.conn: Client = kwargs.pop("conn", None)
        setattr(self, self.conn_name_attr, opcua_conn_id)

    def get_conn(self) -> Client:
        """Establishes a connection to an OPC UA server."""
        if self.conn == None:
            connection = self.connection
            if connection == None:
                conn_id = getattr(self, self.conn_name_attr)
                connection = self.get_connection(conn_id)
            
            self.conn = Client(url=connection.host)
        return self.conn

    def get_history(self, node:str, starttime:datetime, endtime:datetime, numvalues:int) -> Dict:
        return asyncio.run(self._get_history_async(node=node, starttime=starttime, endtime=endtime, numvalues=numvalues))

    async def _get_history_async(self, node:str, starttime:datetime, endtime:datetime, numvalues:int) -> Dict:
        conn = self.get_conn()
        async with conn as c:
            node_obj = c.get_node(node)
            history = await node_obj.read_raw_history(starttime=starttime, endtime=endtime, numvalues=numvalues)
            values = [{'timestamp':v.ServerTimestamp,'value':v.Value.Value, 'status':v.StatusCode.name, 'status_code':v.StatusCode.value} for v in history if not v.Value.is_array]
        return values