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

from mlrun.model_monitoring.stores.tsdb.tsdb import TSDBstore
from mlrun.common.schemas.model_monitoring import (
    EventFieldType,
    EventKeyMetrics,
    AppResultEvent,
    WriterEvent,
)
from .stream_steps import ProcessBeforeTSDB, FilterAndUnpackKeys
import os
import datetime
import pandas as pd
from mlrun.utils import logger
from v3io_frames.errors import Error as V3IOFramesError
from v3io_frames.client import ClientBase as V3IOFramesClient
import mlrun.utils.v3io_clients

_TSDB_BE = "tsdb"
_TSDB_RATE = "1/s"
_TSDB_TABLE = "app-results"


class V3IOTSDBstore(TSDBstore):
    def __init__(self, project: str, access_key: str = None, path: str = None, container: str = None):
        super().__init__(project=project)
        # Initialize a V3IO client instance
        self.access_key = access_key or os.environ.get("V3IO_ACCESS_KEY")
        self.path = path
        self.container = container
        self._tsdb_client: V3IOFramesClient = self._get_v3io_frames_client(self.container)

    @staticmethod
    def _get_v3io_frames_client(v3io_container: str) -> V3IOFramesClient:
        return mlrun.utils.v3io_clients.get_frames_client(
            address=mlrun.mlconf.v3io_framesd,
            container=v3io_container,
        )

    def apply_monitoring_stream_steps(
        self,
        graph,
        frames,
        tsdb_batching_max_events: int = 10,
        tsdb_batching_timeout_secs: int = 300,
    ):
        # Step 12 - Before writing data to TSDB, create dictionary of 2-3 dictionaries that contains
        # stats and details about the events

        print("[EYAL]: going to apply monitoring steps!")

        def apply_process_before_tsdb():
            graph.add_step(
                "ProcessBeforeTSDB", name="ProcessBeforeTSDB", after="sample"
            )

        apply_process_before_tsdb()

        # Steps 13-19: - Unpacked keys from each dictionary and write to TSDB target
        def apply_filter_and_unpacked_keys(name, keys):
            graph.add_step(
                "FilterAndUnpackKeys",
                name=name,
                after="ProcessBeforeTSDB",
                keys=[keys],
            )

        def apply_tsdb_target(name, after):
            graph.add_step(
                "storey.TSDBTarget",
                name=name,
                after=after,
                path=self.path,
                rate="10/m",
                time_col=EventFieldType.TIMESTAMP,
                container=self.container,
                access_key=self.access_key,
                v3io_frames=frames,
                infer_columns_from_data=True,
                index_cols=[
                    EventFieldType.ENDPOINT_ID,
                    EventFieldType.RECORD_TYPE,
                    EventFieldType.ENDPOINT_TYPE,
                ],
                max_events=tsdb_batching_max_events,
                flush_after_seconds=tsdb_batching_timeout_secs,
                key=EventFieldType.ENDPOINT_ID,
            )

        # Steps 13-14 - unpacked base_metrics dictionary
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys1",
            keys=EventKeyMetrics.BASE_METRICS,
        )
        apply_tsdb_target(name="tsdb1", after="FilterAndUnpackKeys1")

        # Steps 15-16 - unpacked endpoint_features dictionary
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys2",
            keys=EventKeyMetrics.ENDPOINT_FEATURES,
        )
        apply_tsdb_target(name="tsdb2", after="FilterAndUnpackKeys2")

        # Steps 17-19 - unpacked custom_metrics dictionary. In addition, use storey.Filter remove none values
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys3",
            keys=EventKeyMetrics.CUSTOM_METRICS,
        )

        def apply_storey_filter():
            graph.add_step(
                "storey.Filter",
                "FilterNotNone",
                after="FilterAndUnpackKeys3",
                _fn="(event is not None)",
            )

        apply_storey_filter()
        apply_tsdb_target(name="tsdb3", after="FilterNotNone")

    def write_application_event(self, event: AppResultEvent):
        event = AppResultEvent(event.copy())
        event[WriterEvent.END_INFER_TIME] = datetime.datetime.fromisoformat(
            event[WriterEvent.END_INFER_TIME]
        )
        del event[WriterEvent.RESULT_EXTRA_DATA]
        try:
            self._tsdb_client.write(
                backend=_TSDB_BE,
                table=_TSDB_TABLE,
                dfs=pd.DataFrame.from_records([event]),
                index_cols=[
                    WriterEvent.END_INFER_TIME,
                    WriterEvent.ENDPOINT_ID,
                    WriterEvent.APPLICATION_NAME,
                    WriterEvent.RESULT_NAME,
                ],
            )
            logger.info("Updated V3IO TSDB successfully", table=_TSDB_TABLE)
        except V3IOFramesError as err:
            logger.warn(
                "Could not write drift measures to TSDB",
                err=err,
                table=_TSDB_TABLE,
                event=event,
            )
