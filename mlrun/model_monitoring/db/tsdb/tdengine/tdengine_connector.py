# Copyright 2024 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import datetime

import pandas as pd
import v3io_frames.client
import v3io_frames.errors
from v3io.dataplane import Client as V3IOClient
from v3io_frames.frames_pb2 import IGNORE

import mlrun.common.model_monitoring
import mlrun.common.schemas.model_monitoring as mm_constants
import mlrun.feature_store.steps
import mlrun.model_monitoring.db
import mlrun.model_monitoring.db.tsdb.v3io.stream_graph_steps

from mlrun.utils import logger




class TDEngineConnector(mlrun.model_monitoring.db.TSDBConnector):
    """
    Handles the TSDB operations when the TSDB connector is of type TDEngine.
    """
    def __init__(
        self,
        project: str,
        access_key: str = None,
        container: str = "users",
        v3io_framesd: str = None,
        create_table: bool = False,
    ):
        super().__init__(project=project)
        self.access_key = access_key or mlrun.mlconf.get_v3io_access_key()

        self.container = container

        self.v3io_framesd = v3io_framesd or mlrun.mlconf.v3io_framesd
        self._frames_client: v3io_frames.client.ClientBase = (
            self._get_v3io_frames_client(self.container)
        )
        self._v3io_client: V3IOClient = mlrun.utils.v3io_clients.get_v3io_client(
            endpoint=mlrun.mlconf.v3io_api,
        )

        self._init_tables_path()

        if create_table:
            self.create_tsdb_application_tables()
