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


from abc import ABC

import pandas as pd


class TSDBConnector(ABC):
    def __init__(self, project: str):
        """
        Initialize a new TSDB connector.
        :param project: the name of the project.
        """
        self.project = project

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
        pass

    def write_application_event(self, event: dict):
        """
        Write a single application result event to TSDB.
        """
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
        Getting metrics from the TSDB. There are pre-defined metrics for model endpoints such as
        `predictions_per_second` and `latency_avg_5m` but also custom metrics defined by the user.
        :param endpoint_id:      The unique id of the model endpoint.
        :param metrics:          A list of real-time metrics to return for the model endpoint.
        :param start:            The start time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
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
        :param table:            Path to the collection to query.
        :param columns:          Columns to include in the result.
        :param filter_query:     Filter expression.
        :param start:            The start time of the metrics.
        :param end:              The end time of the metrics.

        :return: DataFrame with the provided attributes from the data collection.
        """
        pass

    def create_tsdb_application_tables(self):
        """
        Create the application tables using the TSDB connector. At the moment we support 2 types of application tables:
        - app_results: a detailed result that includes status, kind, extra data, etc.
        - metrics: a basic key value that represents a numeric metric.
        """
        pass