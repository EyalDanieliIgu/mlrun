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
import taosws

import mlrun.common.schemas.model_monitoring as mm_constants

import mlrun.model_monitoring.db

import pandas as pd
import typing
import json



class TDEngineConnector(mlrun.model_monitoring.db.TSDBConnector):
    """
    Handles the TSDB operations when the TSDB connector is of type TDEngine.
    """
    def __init__(
            self,
            project: str,
            secret_provider: typing.Callable = None,
    ):
        super().__init__(project=project)
        # self._tdengine_connection_string = (
        #     mlrun.model_monitoring.helpers.get_connection_string(
        #         secret_provider=secret_provider
        #     )
        # )
        self._tdengine_connection_string = "taosws://root:taosdata@192.168.224.154:31033"
        self._connection = self._create_connection()

    def _create_connection(self, db = "mlrun_model_monitoring"):
        conn = taosws.connect(self._tdengine_connection_string)
        try:
            conn.execute(f"CREATE DATABASE {db}")
        except taosws.QueryError:
            # Database already exists
            pass
        conn.execute(f"USE {db}")
        return conn

    def create_tables(self):
        """
        Create the TSDB tables.
        """
        print('[EYAL]: now in create_tables')

        # create the relevant super tables
        self._connection.execute("""
            CREATE STABLE if not exists app_results 
            (end_infer_time TIMESTAMP, 
            start_infer_time TIMESTAMP, 
            result_value FLOAT, 
            result_status INT, 
            result_kind BINARY(40), 
            current_stats BINARY(10000)) 
            TAGS 
            (project BINARY(64), 
            endpoint_id BINARY(64), 
            application_name BINARY(64), 
            result_name BINARY(64))
            """
                                 )

        self._connection.execute("""
            CREATE STABLE if not exists metrics 
            (end_infer_time TIMESTAMP, 
            start_infer_time TIMESTAMP, 
            metric_value FLOAT )
            TAGS 
            (project BINARY(64), 
            endpoint_id BINARY(64), 
            application_name BINARY(64), 
            metric_name BINARY(64))
            """
                                 )

        self._connection.predictions("""
            CREATE STABLE if not exists metrics 
            (end_infer_time TIMESTAMP, 
            start_infer_time TIMESTAMP, 
            metric_value FLOAT,  
            TAGS 
            (project BINARY(64), 
            endpoint_id BINARY(64), 
            application_name BINARY(64), 
            metric_name BINARY(64))
            """
                                     )

        self._connection.execute("""
            CREATE STABLE if not exists predictions 
            (time TIMESTAMP, 
            prediction_counter INT, 
            latency FLOAT,
            custom_metrics BINARY(10000))
            TAGS 
            (project BINARY(64), 
            endpoint_id BINARY(64))
            """
                                    )



    def write_application_event(
            self,
            event: dict,
            kind: mm_constants.WriterEventKind = mm_constants.WriterEventKind.RESULT,
    ):
        """
        Write a single result or metric to TSDB.
        """

        table_name = (f"{self.project}_"
                      f"{event[mm_constants.WriterEvent.ENDPOINT_ID]}_"
                      f"{event[mm_constants.WriterEvent.APPLICATION_NAME]}_")

        print('[EYAL]: current kind: ', kind)
        if kind == mm_constants.WriterEventKind.RESULT:

            table_name = ((f"{table_name}_"
                           f"{event[mm_constants.ResultData.RESULT_NAME]}")
                          .replace("-", "_"))

            print('[EYAL]: going to write application event: ', table_name)

            self._connection.execute(
                f"create table if not exists {table_name} using app_results tags("
                f"'{self.project}', "
                f"'{event[mm_constants.WriterEvent.ENDPOINT_ID]}', "
                f"'{event[mm_constants.WriterEvent.APPLICATION_NAME]}', "
                f"'{event[mm_constants.ResultData.RESULT_NAME]}')")


            # Insert a new result
            self._connection.execute(
                f"insert into {table_name} values "
                f"('{event['end_infer_time'][:-6]}', "
                f"'{event['start_infer_time'][:-6]}', "
                f"{event['result_value']}, "
                f"{event['result_status']}, "
                f"{event['result_kind']}, "
                f"{json.dumps(event['current_stats'])})")

        else:
            # Write a new metric
            table_name = ((f"{table_name}_"
                           f"{event[mm_constants.MetricData.METRIC_NAME]}")
                          .replace("-", "_"))

            self._connection.execute(
                f"create table if not exists {table_name} using metrics tags("
                f"'{self.project}', "
                f"'{event[mm_constants.WriterEvent.ENDPOINT_ID]}', "
                f"'{event[mm_constants.WriterEvent.APPLICATION_NAME]}', "
                f"'{event[mm_constants.MetricData.METRIC_NAME]}')")

            print('[EYAL]: going to write METRIC event: ', table_name)
            # Insert a new result
            self._connection.execute(
                f"insert into {table_name} values "
                f"('{event['end_infer_time'][:-6]}', "
                f"'{event['start_infer_time'][:-6]}', "
                f"{event['metric_value']})")

        # event[mm_constants.WriterEvent.END_INFER_TIME] = (
        #     datetime.datetime.fromisoformat(
        #         event[mm_constants.WriterEvent.END_INFER_TIME]
        #     )
        # )
        #
        # if kind == mm_constants.WriterEventKind.METRIC:
        #     # TODO : Implement the logic for writing metrics to V3IO TSDB
        #     return
        #
        # del event[mm_constants.ResultData.RESULT_EXTRA_DATA]
        # try:
        #     self._frames_client.write(
        #         backend=_TSDB_BE,
        #         table=self.tables[mm_constants.MonitoringTSDBTables.APP_RESULTS],
        #         dfs=pd.DataFrame.from_records([event]),
        #         index_cols=[
        #             mm_constants.WriterEvent.END_INFER_TIME,
        #             mm_constants.WriterEvent.ENDPOINT_ID,
        #             mm_constants.WriterEvent.APPLICATION_NAME,
        #             mm_constants.ResultData.RESULT_NAME,
        #         ],
        #     )
        #     logger.info(
        #         "Updated V3IO TSDB successfully",
        #         table=self.tables[mm_constants.MonitoringTSDBTables.APP_RESULTS],
        #     )
        # except v3io_frames.errors.Error as err:
        #     logger.warn(
        #         "Could not write drift measures to TSDB",
        #         err=err,
        #         table=self.tables[mm_constants.MonitoringTSDBTables.APP_RESULTS],
        #         event=event,
        #     )
        #
        #     raise mlrun.errors.MLRunInvalidArgumentError(
        #         f"Failed to write application result to TSDB: {err}"
        #     )

    def apply_monitoring_stream_steps(self, graph):
        """
        Apply TSDB steps on the provided monitoring graph. Throughout these steps, the graph stores live data of
        different key metric dictionaries. This data is being used by the monitoring dashboards in
        grafana.
        There are 3 different key metric dictionaries that are being generated throughout these steps:
        - base_metrics (average latency and predictions over time)
        - endpoint_features (Prediction and feature names and values)
        - custom_metrics (user-defined metrics)
        """
        print('[EYAL]: now in apply_monitoring_stream_steps')

        # def apply_tdengine_target(name, after, columns):
        #     graph.add_step(
        #         "storey.TDEngineTarget",
        #         name=name,
        #         after=after,
        #         url=self._tdengine_connection_string,
        #
        #         table="predictions",
        #         time_col=mm_constants.EventFieldType.TIMESTAMP,
        #         database="mlrun_model_monitoring",
        #         columns=columns,
        #     )
        #
        #
        #
        #
        # apply_tdengine_target(
        #     name="_prediction_counter",
        #     after="Rename,
        #     table="metrics_table",
        #     columns=[
        #         mm_constants.EventFieldType.TIMESTAMP,
        #         mm_constants.EventFieldType.ENDPOINT_ID,
        #         mm_constants.EventFieldType.APPLICATION_NAME,
        #         mm_constants.MetricData.METRIC_NAME,
        #         mm_constants.MetricData.METRIC_VALUE,
        #     ],
        # )
        pass

    def delete_tsdb_resources(self):
        """
        Delete all project resources in the TSDB connector, such as model endpoints data and drift results.
        """

        pass

    def get_model_endpoint_real_time_metrics(
            self,
            endpoint_id: str,
            metrics: list[str],
            start: str = "now-1h",
            end: str = "now",
    ) -> dict[str, list[tuple[str, float]]]:
        """
        Getting real time metrics from the TSDB. There are pre-defined metrics for model endpoints such as
        `predictions_per_second` and `latency_avg_5m` but also custom metrics defined by the user. Note that these
        metrics are being calculated by the model monitoring stream pod.
        :param endpoint_id:      The unique id of the model endpoint.
        :param metrics:          A list of real-time metrics to return for the model endpoint.
        :param start:            The start time of the metrics.
        :param end:              The end time of the metrics.
        :return: A dictionary of metrics in which the key is a metric name and the value is a list of tuples that
                 includes timestamps and the values.
        """
        pass

    def get_records(
            self,
            table: str,
            columns: list[str] = None,
            filter_query: str = "",
            start: str = "now-1h",
            end: str = "now",
    ) -> pd.DataFrame:
        """
        Getting records from TSDB data collection.
        :param table:            Table name, e.g. 'metrics', 'app_results'.
        :param columns:          Columns to include in the result.
        :param filter_query:     Optional filter expression as a string. The filter structure depends on the TSDB
                                 connector type.
        :param start:            The start time of the metrics.
        :param end:              The end time of the metrics.

        :return: DataFrame with the provided attributes from the data collection.
        :raise:  MLRunInvalidArgumentError if the provided table wasn't found.
        """
        pass





