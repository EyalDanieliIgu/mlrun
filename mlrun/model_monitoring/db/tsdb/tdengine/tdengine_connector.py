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
import json
import typing

import pandas as pd
import taosws

import mlrun.common.schemas.model_monitoring as mm_constants
import mlrun.model_monitoring.db
from mlrun.model_monitoring.db.tsdb.tdengine.schemas import TDEngineSchema
import mlrun.model_monitoring.db.tsdb.tdengine.stream_graph_steps
_MODEL_MONITORING_DATABASE = "mlrun_model_monitoring"

class TDEngineConnector(mlrun.model_monitoring.db.TSDBConnector):
    """
    Handles the TSDB operations when the TSDB connector is of type TDEngine.
    """

    def __init__(
        self,
        project: str,
        secret_provider: typing.Callable = None,
        database: str = _MODEL_MONITORING_DATABASE
    ):
        super().__init__(project=project)
        self._tdengine_connection_string = "taosws://root:taosdata@192.168.224.154:31033"
        # print("[EYAL]: secret_provider: ", secret_provider)
        # if not secret_provider:
        #     self._tdengine_connection_string = (
        #         "taosws://root:taosdata@192.168.224.154:31033"
        #     )
        # self._tdengine_connection_string = (
        #     mlrun.model_monitoring.helpers.get_tsdb_connection_string(
        #         secret_provider=secret_provider
        #     )
        # )
        self.database = database
        print("[EYAL]: connection: ", self._tdengine_connection_string)
        # self._tdengine_connection_string = "taosws://root:taosdata@192.168.224.154:31033"
        self._connection = self._create_connection()
        self._init_super_tables()

    def _create_connection(self):
        """Establish a connection to the TSDB server."""
        conn = taosws.connect(self._tdengine_connection_string)
        try:
            conn.execute(f"CREATE DATABASE {self.database}")
        except taosws.QueryError:
            # Database already exists
            pass
        conn.execute(f"USE {self.database}")
        return conn

    def _init_super_tables(self):
        """Initialize the super tables for the TSDB."""
        self.tables = {
            mm_constants.TDEngineSuperTables.APP_RESULTS: mlrun.model_monitoring.db.tsdb.tdengine.schemas.AppResultTable(),
            mm_constants.TDEngineSuperTables.METRICS: mlrun.model_monitoring.db.tsdb.tdengine.schemas.Metrics(),
            mm_constants.TDEngineSuperTables.PREDICTIONS: mlrun.model_monitoring.db.tsdb.tdengine.schemas.Predictions(),
        }

    def create_tables(self):
        """Create TDEngine supertables."""
        print("[EYAL]: now in create_tables")
        for table in self.tables:
            create_table_query = self.tables[table]._create_super_table_query()
            self._connection.execute(create_table_query)


    #
    #
    # def _create_app_results_table(self):
    #
    #
    #     app_result = mlrun.model_monitoring.db.tsdb.tdengine.schemas.AppResultTable()
    #     self._connection.execute(app_result._create_super_table_query())
    #
    #     # self._connection.execute("""
    #     #     CREATE STABLE if not exists app_results
    #     #     (end_infer_time TIMESTAMP,
    #     #     start_infer_time TIMESTAMP,
    #     #     result_value FLOAT,
    #     #     result_status INT,
    #     #     result_kind BINARY(40),
    #     #     current_stats BINARY(10000))
    #     #     TAGS
    #     #     (project BINARY(64),
    #     #     endpoint_id BINARY(64),
    #     #     application_name BINARY(64),
    #     #     result_name BINARY(64))
    #     #     """)
    #
    # def _create_metrics_table(self):
    #     self._connection.execute("""
    #         CREATE STABLE if not exists metrics
    #         (end_infer_time TIMESTAMP,
    #         start_infer_time TIMESTAMP,
    #         metric_value FLOAT )
    #         TAGS
    #         (project BINARY(64),
    #         endpoint_id BINARY(64),
    #         application_name BINARY(64),
    #         metric_name BINARY(64))
    #         """)
    #
    # def _create_prediction_metrics_table(self):
    #     self._connection.execute("""
    #         CREATE STABLE if not exists prediction_metrics
    #         (time TIMESTAMP,
    #         latency FLOAT,
    #         custom_metrics BINARY(10000))
    #         TAGS
    #         (project BINARY(64),
    #         endpoint_id BINARY(64))
    #         """)

    def write_application_event(
        self,
        event: dict,
        kind: mm_constants.WriterEventKind = mm_constants.WriterEventKind.RESULT,
    ):
        """
        Write a single result or metric to TSDB.
        """

        table_name = (
            f"{self.project}_"
            f"{event[mm_constants.WriterEvent.ENDPOINT_ID]}_"
            f"{event[mm_constants.WriterEvent.APPLICATION_NAME]}_"
        )

        event[mm_constants.WriterEvent.END_INFER_TIME] = event[mm_constants.WriterEvent.END_INFER_TIME][:-6]
        event[mm_constants.WriterEvent.START_INFER_TIME] = event[mm_constants.WriterEvent.START_INFER_TIME][:-6]
        event[mm_constants.EventFieldType.PROJECT] = self.project
        print("[EYAL]: current kind: ", kind)
        if kind == mm_constants.WriterEventKind.RESULT:
            # Write a new result
            table = self.tables[mm_constants.TDEngineSuperTables.APP_RESULTS]
            table_name = (
                f"{table_name}_" f"{event[mm_constants.ResultData.RESULT_NAME]}"
            ).replace("-", "_")
            # self.write_application_result_record(table_name=table_name, event=event)

        else:
            # Write a new metric
            table = self.tables[mm_constants.TDEngineSuperTables.METRICS]
            table_name = (
                f"{table_name}_" f"{event[mm_constants.MetricData.METRIC_NAME]}"
            ).replace("-", "_")
            # self.write_metric_record(table_name=table_name, event=event)

        create_table_query = table._create_subtable_query(subtable=table_name, values=event)
        self._connection.execute(create_table_query)
        insert_table_query = table._insert_subtable_query(subtable=table_name, values=event)
        self._connection.execute(insert_table_query)

    # def write_application_result_record(self, table_name, event):
    #     table_name = (
    #         f"{table_name}_" f"{event[mm_constants.ResultData.RESULT_NAME]}"
    #     ).replace("-", "_")
    #
    #
    #     create_table_query = self.tables[mm_constants.TDEngineSuperTables.APP_RESULTS]._create_subtable_query(
    #         subtable=table_name,
    #         values=event
    #     )
    #
    #     self._connection.execute(create_table_query)
    #
    #     # self._connection.execute(
    #     #     f"create table if not exists {table_name} using app_results tags("
    #     #     f"'{self.project}', "
    #     #     f"'{event[mm_constants.WriterEvent.ENDPOINT_ID]}', "
    #     #     f"'{event[mm_constants.WriterEvent.APPLICATION_NAME]}', "
    #     #     f"'{event[mm_constants.ResultData.RESULT_NAME]}')"
    #     # )
    #
    #     # Insert a new result
    #
    #     insert_table_query = self.tables[mm_constants.TDEngineSuperTables.APP_RESULTS]._insert_subtable_query(
    #         subtable=table_name,
    #         values={
    #             mm_constants.WriterEvent.END_INFER_TIME: event[mm_constants.WriterEvent.END_INFER_TIME][:-6],
    #             mm_constants.WriterEvent.START_INFER_TIME: event[mm_constants.WriterEvent.START_INFER_TIME][:-6],
    #             mm_constants.ResultData.RESULT_VALUE: event[mm_constants.ResultData.RESULT_VALUE],
    #             mm_constants.ResultData.RESULT_STATUS: event[mm_constants.ResultData.RESULT_STATUS],
    #             mm_constants.ResultData.RESULT_KIND: event[mm_constants.ResultData.RESULT_KIND],
    #             mm_constants.ResultData.CURRENT_STATS: json.dumps(event[mm_constants.ResultData.CURRENT_STATS]),
    #         }
    #     )
    #
    #     self._connection.execute(insert_table_query)
    #
    #     # self._connection.execute(
    #     #     f"insert into {table_name} values "
    #     #     f"('{event['end_infer_time'][:-6]}', "
    #     #     f"'{event['start_infer_time'][:-6]}', "
    #     #     f"{event['result_value']}, "
    #     #     f"{event['result_status']}, "
    #     #     f"{event['result_kind']}, "
    #     #     f"{json.dumps(event['current_stats'])})"
    #     # )
    #
    # def write_metric_record(self, table_name, event):
    #     # Write a new metric
    #     table_name = (
    #         f"{table_name}_" f"{event[mm_constants.MetricData.METRIC_NAME]}"
    #     ).replace("-", "_")
    #
    #     self._connection.execute(
    #         f"create table if not exists {table_name} using metrics tags("
    #         f"'{self.project}', "
    #         f"'{event[mm_constants.WriterEvent.ENDPOINT_ID]}', "
    #         f"'{event[mm_constants.WriterEvent.APPLICATION_NAME]}', "
    #         f"'{event[mm_constants.MetricData.METRIC_NAME]}')"
    #     )
    #
    #     print("[EYAL]: going to write METRIC event: ", table_name)
    #     # Insert a new result
    #     self._connection.execute(
    #         f"insert into {table_name} values "
    #         f"('{event['end_infer_time'][:-6]}', "
    #         f"'{event['start_infer_time'][:-6]}', "
    #         f"{event['metric_value']})"
    #     )

    def apply_monitoring_stream_steps(self, graph):
        """
        Apply TSDB steps on the provided monitoring graph. Throughout these steps, the graph stores live data of
        different key metric dictionaries. This data is being used by the monitoring dashboards in
        grafana. At the moment, we store two types of data:
        - prediction latency.
        - custom metrics.
        """
        print("[EYAL]: now in apply_monitoring_stream_steps")


        def apply_process_before_tsdb():
            graph.add_step(
                "mlrun.model_monitoring.db.tsdb.tdengine.stream_graph_steps.ProcessBeforeTDEngine",
                name="ProcessBeforeTDEngine",
                after="MapFeatureNames",
            )

        apply_process_before_tsdb()

        def apply_tdengine_target(name, after):
            graph.add_step(
                "storey.TDEngineTarget",
                name=name,
                after=after,
                url=self._tdengine_connection_string,
                supertable=mm_constants.TDEngineSuperTables.PREDICTIONS,
                table_col=mm_constants.EventFieldType.TABLE_COLUMN,
                time_col=mm_constants.EventFieldType.TIME,
                database=self.database,
                columns=[mm_constants.EventFieldType.LATENCY, mm_constants.EventKeyMetrics.CUSTOM_METRICS],
                tag_cols=[mm_constants.EventFieldType.PROJECT, mm_constants.EventFieldType.ENDPOINT_ID],
                # drop_key_field=False,
            )

        apply_tdengine_target(
            name="TDEngineTarget",
            after="ProcessBeforeTDEngine",
        )

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
        table: mm_constants.TDEngineSuperTables,
        columns: list[str] = None,
        filter_query: str = "",
        start: str = datetime.datetime.now().astimezone() - datetime.timedelta(hours=1),
        end: str = datetime.datetime.now().astimezone(),
        timestamp_column: str = mm_constants.EventFieldType.TIME,
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

        # full_query = "select "
        # if columns:
        #     full_query += ", ".join(columns)
        # else:
        #     full_query += "*"
        # full_query += f" from {self.database}.{table}"
        #
        # if any([filter_query, start, end]):
        #     full_query += " where "
        #     if filter_query:
        #         full_query += filter_query + " and "
        #     if start:
        #         full_query += f" {timestamp_column} >= '{start}'" + " and "
        #     if end:
        #         full_query += f" {timestamp_column} <= '{end}'"
        #     if full_query.endswith(" and "):
        #         full_query = full_query[:-5]
        full_query = self.tables[table]._get_records_query(
            subtable=table,
            columns_to_filter=columns,
            filter_query=filter_query,
            start=start,
            end=end,
            timestamp_column=timestamp_column,
        )
        try:
            query_result = self._connection.query(full_query)
        except taosws.QueryError as e:
            raise mlrun.errors.MLRunInvalidArgumentError(
                f"Failed to query table {table} in database {self.database}, {str(e)}"
            )
        columns = []
        for column in query_result.fields:
            columns.append(column.name())

        return pd.DataFrame(query_result, columns=columns)