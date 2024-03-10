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


from mlrun.model_monitoring.stores.tsdb import TSDBstore
import mlrun.model_monitoring.stores.tsdb.v3io.stream_graph_steps
from mlrun.common.schemas.model_monitoring import (
    AppResultEvent,
    EventFieldType,
    EventKeyMetrics,
    WriterEvent,
)
import mlrun.feature_store.steps
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


class InlfuxDBstore(TSDBstore):
    """
    Handles the TSDB operations when the TSDB target is from type V3IO. To manage these operations we use V3IO Frames
    Client that provides API for executing commands on the V3IO TSDB table.
    """

    def __init__(
        self,
        project: str,
        url: str = "http://192.168.224.154:8086/",
        token: str = "Cgy7baTD5C6JFjOnqotp_lfTxn3NieT88TxXv3QitTdaejUzVeyZCGZ41s4VusTIIiUI3UgZyck_mQe_l7t-pw==",
        org: str = "org_test",
        bucket: str = "bucket_test",
        # create_table: bool = False,
    ):
        super().__init__(project=project)
        self.token = token

        self.url = url
        self.org = org
        self.bucket = bucket
        self.client = influxdb_client.InfluxDBClient(
            url=self.url, token=self.token, org=self.org
        )
        if self.client.ping:
            print("[EYAL]: client connected!")
        # self.container = container
        #
        # self.v3io_framesd = v3io_framesd or mlrun.mlconf.v3io_framesd
        # self._frames_client: v3io_frames.client.ClientBase = (
        #     self._get_v3io_frames_client(self.container)
        # )
        # self._v3io_client: V3IOClient = mlrun.utils.v3io_clients.get_v3io_client(
        #     endpoint=mlrun.mlconf.v3io_api,
        # )
        #
        # if create_table:
        #     self._create_tsdb_table()

    def apply_monitoring_stream_steps(
        self,
        graph,
        tsdb_batching_max_events: int = 10,
        tsdb_batching_timeout_secs: int = 300,
    ):
        """
        Apply TSDB steps on the provided monitoring graph. Throughout these steps, the graph stores live data of
        different key metric dictionaries in TSDB target. This data is being used by the monitoring dashboards in
        grafana. Results can be found under  v3io:///users/pipelines/project-name/model-endpoints/events/.
        In that case, we generate 3 different key  metric dictionaries:
        - base_metrics (average latency and predictions over time)
        - endpoint_features (Prediction and feature names and values)
        - custom_metrics (user-defined metrics
        """

        # Step 12 - Before writing data to TSDB, create dictionary of 2-3 dictionaries that contains
        # stats and details about the events

        def apply_process_before_tsdb():
            graph.add_step(
                "mlrun.model_monitoring.stores.tsdb.v3io.stream_graph_steps.ProcessBeforeTSDB",
                name="ProcessBeforeTSDB",
                after="sample",
            )

        apply_process_before_tsdb()

        # Steps 13-19: - Unpacked keys from each dictionary and write to TSDB target
        def apply_filter_and_unpacked_keys(name, keys):
            graph.add_step(
                "mlrun.model_monitoring.stores.tsdb.v3io.stream_graph_steps.FilterAndUnpackKeys",
                name=name,
                after="ProcessBeforeTSDB",
                keys=[keys],
            )

        # def apply_tsdb_target(name, after):
        #     graph.add_step(
        #         "storey.TSDBTarget",
        #         name=name,
        #         after=after,
        #         path=self.table,
        #         rate="10/m",
        #         time_col=EventFieldType.TIMESTAMP,
        #         container=self.container,
        #         access_key=self.access_key,
        #         v3io_frames=self.v3io_framesd,
        #         infer_columns_from_data=True,
        #         index_cols=[
        #             EventFieldType.ENDPOINT_ID,
        #             EventFieldType.RECORD_TYPE,
        #             EventFieldType.ENDPOINT_TYPE,
        #         ],
        #         max_events=tsdb_batching_max_events,
        #         flush_after_seconds=tsdb_batching_timeout_secs,
        #         key=EventFieldType.ENDPOINT_ID,
        #     )
        def apply_influx_db_target(name, after):
            graph.add_step(
                "WriteToInflux",
                name=name,
                after=after,
                bucket=self.bucket,
                org=self.org,
                influx_client=self.client,
            )

        # Steps 13-14 - unpacked base_metrics dictionary
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys1",
            keys=EventKeyMetrics.BASE_METRICS,
        )
        # apply_tsdb_target(name="tsdb1", after="FilterAndUnpackKeys1")
        apply_influx_db_target(name="influxdb1", after="FilterAndUnpackKeys1")
        # Steps 15-16 - unpacked endpoint_features dictionary
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys2",
            keys=EventKeyMetrics.ENDPOINT_FEATURES,
        )
        # apply_tsdb_target(name="tsdb2", after="FilterAndUnpackKeys2")
        apply_influx_db_target(name="influxdb2", after="FilterAndUnpackKeys2")

        # Steps 17-19 - unpacked custom_metrics dictionary. In addition, use storey.Filter remove none values
        apply_filter_and_unpacked_keys(
            name="FilterAndUnpackKeys3",
            keys=EventKeyMetrics.CUSTOM_METRICS,
        )
        apply_influx_db_target(name="influxdb3", after="FilterAndUnpackKeys3")

        def apply_storey_filter():
            graph.add_step(
                "storey.Filter",
                "FilterNotNone",
                after="FilterAndUnpackKeys3",
                _fn="(event is not None)",
            )

        apply_storey_filter()
        # apply_tsdb_target(name="tsdb3", after="FilterNotNone")


class WriteToInflux(mlrun.feature_store.steps.MapClass):
    def __init__(self, bucket, org, influx_client, **kwargs):
        """
        Process the data before writing to TSDB. This step creates a dictionary that includes 3 different dictionaries
        that each one of them contains important details and stats about the events:
        1. base_metrics: stats about the average latency and the amount of predictions over time. It is based on
           storey.AggregateByKey which was executed in step 5.
        2. endpoint_features: feature names and values along with the prediction names and value.
        3. custom_metric (opt): optional metrics provided by the user.
        :returns: Dictionary of 2-3 dictionaries that contains stats and details about the events.
        """
        super().__init__(**kwargs)
        self.bucket = bucket
        self.org = org
        self.influx_client = influx_client
        # self.influx_client = influx_db_store.client

    def do(self, event):
        print("[EYAL]: event in the start of writeToInflux: ", event)
        write_api = self.influx_client.client.write_api(write_options=SYNCHRONOUS)
        print("[EYAL]: created write api obk")
        data = self.influx_client.Point(
            event[EventFieldType.RECORD_TYPE]
            .tag(event[EventFieldType.ENDPOINT_ID])
            .field("eyal_field", 2)
        )
        write_api.write(bucket=self.bucket, org=self.org, record=data)
        print("[EYAL]: write completed!")
        return event
