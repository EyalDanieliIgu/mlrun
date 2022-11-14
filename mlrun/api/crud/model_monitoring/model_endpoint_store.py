# Copyright 2018 Iguazio
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

import enum
import json
import typing
from abc import ABC, abstractmethod

import pandas as pd
import v3io.dataplane
import v3io_frames

import mlrun
import mlrun.api.schemas
import mlrun.model_monitoring.constants as model_monitoring_constants
import mlrun.utils.model_monitoring
import mlrun.utils.v3io_clients
from mlrun.utils import logger
import datetime


class _ModelEndpointStore(ABC):
    """
    An abstract class to handle the model endpoint in the DB target.
    """

    def __init__(self, project: str):
        """
        Initialize a new model endpoint target.

        :param project:             The name of the project.
        """
        self.project = project

    @abstractmethod
    def write_model_endpoint(self, endpoint: mlrun.api.schemas.ModelEndpoint):
        """
        Create a new endpoint record in the DB table.

        :param endpoint: ModelEndpoint object that will be written into the DB.
        """
        pass

    @abstractmethod
    def update_model_endpoint(self, endpoint_id: str, attributes: dict):
        """
        Update a model endpoint record with a given attributes.

        :param endpoint_id: The unique id of the model endpoint.
        :param attributes: Dictionary of attributes that will be used for update the model endpoint. Note that the keys
                           of the attributes dictionary should exist in the KV table.

        """
        pass

    @abstractmethod
    def delete_model_endpoint(self, endpoint_id: str):
        """
        Deletes the record of a given model endpoint id.

        :param endpoint_id: The unique id of the model endpoint.
        """
        pass

    @abstractmethod
    def delete_model_endpoints_resources(
        self, endpoints: mlrun.api.schemas.model_endpoints.ModelEndpointList
    ):
        """
        Delete all model endpoints resources.

        :param endpoints: An object of ModelEndpointList which is literally a list of model endpoints along with some
                          metadata. To get a standard list of model endpoints use ModelEndpointList.endpoints.
        """
        pass

    @abstractmethod
    def get_model_endpoint(
        self,
        metrics: typing.List[str] = None,
        start: str = "now-1h",
        end: str = "now",
        feature_analysis: bool = False,
        endpoint_id: str = None,
        convert_to_endpoint_object: bool = True,
    ):
        """
        Get a single model endpoint object. You can apply different time series metrics that will be added to the
           result.

        :param endpoint_id:      The unique id of the model endpoint.
        :param start:            The start time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param metrics:          A list of metrics to return for the model endpoint. There are pre-defined metrics for
                                 model endpoints such as predictions_per_second and latency_avg_5m but also custom
                                 metrics defined by the user. Please note that these metrics are stored in the time
                                 series DB and the results will be appeared under model_endpoint.spec.metrics.
        :param feature_analysis: When True, the base feature statistics and current feature statistics will be added to
                                 the output of the resulting object.
        :param convert_to_endpoint_object: A boolean that indicates whether to convert the model endpoint dictionary
                                           into a ModelEndpoint or not. True by default.

        :return: A ModelEndpoint object.
        """
        pass

    @abstractmethod
    def list_model_endpoints(
        self, model: str= None, function: str= None, labels: typing.List= None, top_level: bool = None
    ):
        """
        Returns a list of endpoint unique ids, supports filtering by model, function,
        labels or top level. By default, when no filters are applied, all available endpoint ids for the given project
        will be listed.

        :param model:           The name of the model to filter by.
        :param function:        The name of the function to filter by.
        :param labels:          A list of labels to filter by. Label filters work by either filtering a specific value
                                of a label (i.e. list("key==value")) or by looking for the existence of a given
                                key (i.e. "key").
        :param top_level:       If True will return only routers and endpoint that are NOT children of any router.

        :return: List of model endpoints unique ids.
        """
        pass

    @staticmethod
    def get_params(endpoint: mlrun.api.schemas.ModelEndpoint) -> typing.Dict:
        """
        Retrieving the relevant attributes from the model endpoint object.

        :param endpoint: ModelEndpoint object that will be used for getting the attributes.

        :return: A flat dictionary of attributes.
        """

        # Prepare the data for the attributes dictionary
        labels = endpoint.metadata.labels or {}
        searchable_labels = {f"_{k}": v for k, v in labels.items()}
        feature_names = endpoint.spec.feature_names or []
        label_names = endpoint.spec.label_names or []
        feature_stats = endpoint.status.feature_stats or {}
        current_stats = endpoint.status.current_stats or {}
        children = endpoint.status.children or []
        endpoint_type = endpoint.status.endpoint_type or None
        children_uids = endpoint.status.children_uids or []
        predictions_per_second = endpoint.status.predictions_per_second or None
        latency_avg_1h = endpoint.status.latency_avg_1h or None

        # Fill the data. Note that because it is a flat dictionary, we use json.dumps() for encoding hierarchies
        # such as current_stats or label_names
        attributes = {
            "endpoint_id": endpoint.metadata.uid,
            "project": endpoint.metadata.project,
            "function_uri": endpoint.spec.function_uri,
            "model": endpoint.spec.model,
            "model_class": endpoint.spec.model_class or "",
            "labels": json.dumps(labels),
            "model_uri": endpoint.spec.model_uri or "",
            "stream_path": endpoint.spec.stream_path or "",
            "active": endpoint.spec.active or "",
            "monitoring_feature_set_uri": endpoint.status.monitoring_feature_set_uri
            or "",
            "monitoring_mode": endpoint.spec.monitoring_mode or "",
            "state": endpoint.status.state or "",
            "feature_stats": json.dumps(feature_stats),
            "current_stats": json.dumps(current_stats),
            "predictions_per_second": json.dumps(predictions_per_second),
            "latency_avg_1h": json.dumps(latency_avg_1h),
            "feature_names": json.dumps(feature_names),
            "children": json.dumps(children),
            "label_names": json.dumps(label_names),
            "endpoint_type": json.dumps(endpoint_type),
            "children_uids": json.dumps(children_uids),

            **searchable_labels,
        }
        return attributes

    @staticmethod
    def _json_loads_if_not_none(field: typing.Any) -> typing.Any:
        return json.loads(field) if field and field != 'null' and field is not None else None

    @staticmethod
    def get_endpoint_features(
        feature_names: typing.List[str],
        feature_stats: dict = None,
        current_stats: dict = None,
    ) -> typing.List[mlrun.api.schemas.Features]:
        """
        Getting a new list of features that exist in feature_names along with their expected (feature_stats) and
        actual (current_stats) stats. The expected stats were calculated during the creation of the model endpoint,
        usually based on the data from the Model Artifact. The actual stats are based on the results from the latest
        model monitoring batch job.

        param feature_names: List of feature names.
        param feature_stats: Dictionary of feature stats that were stored during the creation of the model endpoint
                             object.
        param current_stats: Dictionary of the latest stats that were stored during the last run of the model monitoring
                             batch job.

        return: List of feature objects. Each feature has a name, weight, expected values, and actual values. More info
                can be found under mlrun.api.schemas.Features.
        """

        # Initialize feature and current stats dictionaries
        safe_feature_stats = feature_stats or {}
        safe_current_stats = current_stats or {}

        # Create feature object and add it to a general features list
        features = []
        for name in feature_names:
            if feature_stats is not None and name not in feature_stats:
                logger.warn("Feature missing from 'feature_stats'", name=name)
            if current_stats is not None and name not in current_stats:
                logger.warn("Feature missing from 'current_stats'", name=name)
            f = mlrun.api.schemas.Features.new(
                name, safe_feature_stats.get(name), safe_current_stats.get(name)
            )
            features.append(f)
        return features

    def _convert_into_model_endpoint_object(
        self, endpoint: typing.Dict, feature_analysis: bool = False
    ):
        """
        Create a ModelEndpoint object according to a provided model endpoint dictionary.

        :param endpoint:         DB record of model endpoint which need to be converted into a valid ModelEndpoint
                                 object.
        :param feature_analysis: When True, the base feature statistics and current feature statistics will be added to
                                 the output of the resulting object.

        :return: A ModelEndpoint object.
        """

        print('[EYAL]: endpoint in _convert_into_model_endpoint_object: ', endpoint)

        # Parse JSON values into a dictionary
        feature_names = self._json_loads_if_not_none(endpoint.get("feature_names"))
        label_names = self._json_loads_if_not_none(endpoint.get("label_names"))
        feature_stats = self._json_loads_if_not_none(endpoint.get("feature_stats"))
        current_stats = self._json_loads_if_not_none(endpoint.get("current_stats"))
        children = self._json_loads_if_not_none(endpoint.get("children"))
        monitor_configuration = self._json_loads_if_not_none(
            endpoint.get("monitor_configuration")
        )
        endpoint_type = self._json_loads_if_not_none(endpoint.get("endpoint_type"))
        children_uids = self._json_loads_if_not_none(endpoint.get("children_uids"))
        labels = self._json_loads_if_not_none(endpoint.get("labels"))
        # predictions_per_second = self._json_loads_if_not_none(endpoint.get("predictions_per_second"))
        # latency_avg_1h = self._json_loads_if_not_none(endpoint.get("latency_avg_1h"))

        # Convert into model endpoint object
        endpoint_obj = mlrun.api.schemas.ModelEndpoint(
            metadata=mlrun.api.schemas.ModelEndpointMetadata(
                project=endpoint.get("project"),
                labels=labels,
                uid=endpoint.get("endpoint_id"),
            ),
            spec=mlrun.api.schemas.ModelEndpointSpec(
                function_uri=endpoint.get("function_uri"),
                model=endpoint.get("model"),
                model_class=endpoint.get("model_class"),
                model_uri=endpoint.get("model_uri"),
                feature_names=feature_names or None,
                label_names=label_names or None,
                stream_path=endpoint.get("stream_path"),
                algorithm=endpoint.get("algorithm"),
                monitor_configuration=monitor_configuration or None,
                active=endpoint.get("active"),
                monitoring_mode=endpoint.get("monitoring_mode"),
            ),
            status=mlrun.api.schemas.ModelEndpointStatus(
                state=endpoint.get("state") or None,
                feature_stats=feature_stats or None,
                current_stats=current_stats or None,
                children=children or None,
                first_request=endpoint.get("first_request"),
                last_request=endpoint.get("last_request"),
                accuracy=endpoint.get("accuracy"),
                error_count=endpoint.get("error_count"),
                drift_status=endpoint.get("drift_status"),
                endpoint_type=endpoint_type or None,
                children_uids=children_uids or None,
                monitoring_feature_set_uri=endpoint.get("monitoring_feature_set_uri")
                or None,
                predictions_per_second=endpoint.get("predictions_per_second") if endpoint.get("predictions_per_second") != 'null'
                else None,
                latency_avg_1h=endpoint.get("latency_avg_1h") if endpoint.get("latency_avg_1h") != 'null' else None,
            ),
        )

        # If feature analysis was applied, add feature stats and current stats to the model endpoint result
        if feature_analysis and feature_names:
            endpoint_features = self.get_endpoint_features(
                feature_names=feature_names,
                feature_stats=feature_stats,
                current_stats=current_stats,
            )
            if endpoint_features:
                endpoint_obj.status.features = endpoint_features
                # Add the latest drift measures results (calculated by the model monitoring batch)
                drift_measures = self._json_loads_if_not_none(
                    endpoint.get("drift_measures")
                )
                endpoint_obj.status.drift_measures = drift_measures

        return endpoint_obj


class _ModelEndpointKVStore(_ModelEndpointStore):
    """
    Handles the DB operations when the DB target is from type KV. For the KV operations, we use an instance of V3IO
    client and usually the KV table can be found under v3io:///users/pipelines/project-name/model-endpoints/endpoints/.
    """

    def __init__(self, access_key: str, project: str):
        super().__init__(project=project)
        # Initialize a V3IO client instance
        self.access_key = access_key
        self.client = mlrun.utils.v3io_clients.get_v3io_client(
            endpoint=mlrun.mlconf.v3io_api, access_key=self.access_key
        )
        # Get the KV table path and container
        self.path, self.container = self._get_path_and_container()

    def write_model_endpoint(self, endpoint: mlrun.api.schemas.ModelEndpoint):
        """
        Create a new endpoint record in the KV table.

        :param endpoint: ModelEndpoint object that will be written into the DB.
        """

        # Retrieving the relevant attributes from the model endpoint object
        attributes = self.get_params(endpoint)
        # Create or update the model endpoint record
        self.client.kv.put(
            container=self.container,
            table_path=self.path,
            key=endpoint.metadata.uid,
            attributes=attributes,
        )

    def update_model_endpoint(self, endpoint_id: str, attributes: dict):
        """
        Update a model endpoint record with a given attributes.

        :param endpoint_id: The unique id of the model endpoint.
        :param attributes: Dictionary of attributes that will be used for update the model endpoint. Note that the keys
                           of the attributes dictionary should exist in the KV table.

        """

        self.client.kv.update(
            container=self.container,
            table_path=self.path,
            key=endpoint_id,
            attributes=attributes,
        )

        logger.info("Model endpoint table updated", endpoint_id=endpoint_id)

    def delete_model_endpoint(
        self,
        endpoint_id: str,
    ):
        """
        Deletes the KV record of a given model endpoint id.

        :param endpoint_id: The unique id of the model endpoint.
        """

        self.client.kv.delete(
            container=self.container,
            table_path=self.path,
            key=endpoint_id,
        )

        logger.info("Model endpoint table cleared", endpoint_id=endpoint_id)

    def get_model_endpoint(
        self,
        endpoint_id: str = None,
        start: str = "now-1h",
        end: str = "now",
        metrics: typing.List[str] = None,
        feature_analysis: bool = False,
        convert_to_endpoint_object: bool = True,
    ):
        """
        Get a single model endpoint object. You can apply different time series metrics that will be added to the
        result.

        :param endpoint_id:      The unique id of the model endpoint.
        :param start:            The start time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param metrics:          A list of metrics to return for the model endpoint. There are pre-defined metrics for
                                 model endpoints such as predictions_per_second and latency_avg_5m but also custom
                                 metrics defined by the user. Please note that these metrics are stored in the time
                                 series DB and the results will be appeared under model_endpoint.spec.metrics.
        :param feature_analysis: When True, the base feature statistics and current feature statistics will be added to
                                 the output of the resulting object.
        :param convert_to_endpoint_object: A boolean that indicates whether to convert the model endpoint dictionary
                                           into a ModelEndpoint or not. True by default.

        :return: A ModelEndpoint object.
        """
        logger.info(
            "Getting model endpoint record from kv",
            endpoint_id=endpoint_id,
        )

        # Getting the raw data from the KV table
        endpoint = self.client.kv.get(
            container=self.container,
            table_path=self.path,
            key=endpoint_id,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
            access_key=self.access_key,
        )
        endpoint = endpoint.output.item

        if not endpoint:
            raise mlrun.errors.MLRunNotFoundError(f"Endpoint {endpoint_id} not found")

        # Generate a model endpoint object from the model endpoint KV record
        if convert_to_endpoint_object:
            endpoint = self._convert_into_model_endpoint_object(
                endpoint=endpoint, feature_analysis=feature_analysis
            )

            # If time metrics were provided, retrieve the results from the time series DB
            if metrics:
                endpoint_metrics = self.get_endpoint_metrics(
                    endpoint_id=endpoint_id,
                    start=start,
                    end=end,
                    metrics=metrics,
                )
                if endpoint_metrics:
                    endpoint.status.metrics = endpoint_metrics

        return endpoint

    # def _convert_into_model_endpoint_object(
    #     self, endpoint, start, end, metrics, feature_analysis
    # ):
    #     """
    #     Create a ModelEndpoint object according to a provided endpoint record from the DB.
    #
    #     :param endpoint:         KV record of model endpoint which need to be converted into a valid ModelEndpoint
    #                              object.
    #     :param start:            The start time of the metrics. Can be represented by a string containing an RFC 3339
    #                              time, a Unix timestamp in milliseconds, a relative time (`'now'` or
    #                              `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
    #                              earliest time.
    #     :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
    #                              time, a Unix timestamp in milliseconds, a relative time (`'now'` or
    #                              `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
    #                              earliest time.
    #     :param metrics:          A list of metrics to return for the model endpoint. There are pre-defined metrics for
    #                              model endpoints such as predictions_per_second and latency_avg_5m but also custom
    #                              metrics defined by the user. Please note that these metrics are stored in the time
    #                              series DB and the results will be appeared under model_endpoint.spec.metrics.
    #     :param feature_analysis: When True, the base feature statistics and current feature statistics will be added to
    #                              the output of the resulting object.
    #
    #     :return: A ModelEndpoint object.
    #     """
    #
    #     # Parse JSON values into a dictionary
    #     feature_names = self._json_loads_if_not_none(endpoint.get("feature_names"))
    #     label_names = self._json_loads_if_not_none(endpoint.get("label_names"))
    #     feature_stats = self._json_loads_if_not_none(endpoint.get("feature_stats"))
    #     current_stats = self._json_loads_if_not_none(endpoint.get("current_stats"))
    #     children = self._json_loads_if_not_none(endpoint.get("children"))
    #     monitor_configuration = self._json_loads_if_not_none(
    #         endpoint.get("monitor_configuration")
    #     )
    #     endpoint_type = self._json_loads_if_not_none(endpoint.get("endpoint_type"))
    #     children_uids = self._json_loads_if_not_none(endpoint.get("children_uids"))
    #     labels = self._json_loads_if_not_none(endpoint.get("labels"))
    #
    #     # Convert into model endpoint object
    #     endpoint_obj = mlrun.api.schemas.ModelEndpoint(
    #         metadata=mlrun.api.schemas.ModelEndpointMetadata(
    #             project=endpoint.get("project"),
    #             labels=labels,
    #             uid=endpoint.get("endpoint_id"),
    #         ),
    #         spec=mlrun.api.schemas.ModelEndpointSpec(
    #             function_uri=endpoint.get("function_uri"),
    #             model=endpoint.get("model"),
    #             model_class=endpoint.get("model_class"),
    #             model_uri=endpoint.get("model_uri"),
    #             feature_names=feature_names or None,
    #             label_names=label_names or None,
    #             stream_path=endpoint.get("stream_path"),
    #             algorithm=endpoint.get("algorithm"),
    #             monitor_configuration=monitor_configuration or None,
    #             active=endpoint.get("active"),
    #             monitoring_mode=endpoint.get("monitoring_mode"),
    #         ),
    #         status=mlrun.api.schemas.ModelEndpointStatus(
    #             state=endpoint.get("state") or None,
    #             feature_stats=feature_stats or None,
    #             current_stats=current_stats or None,
    #             children=children or None,
    #             first_request=endpoint.get("first_request"),
    #             last_request=endpoint.get("last_request"),
    #             accuracy=endpoint.get("accuracy"),
    #             error_count=endpoint.get("error_count"),
    #             drift_status=endpoint.get("drift_status"),
    #             endpoint_type=endpoint_type or None,
    #             children_uids=children_uids or None,
    #             monitoring_feature_set_uri=endpoint.get("monitoring_feature_set_uri")
    #             or None,
    #         ),
    #     )
    #
    #     # If feature analysis was applied, add feature stats and current stats to the model endpoint result
    #     if feature_analysis and feature_names:
    #         endpoint_features = self.get_endpoint_features(
    #             feature_names=feature_names,
    #             feature_stats=feature_stats,
    #             current_stats=current_stats,
    #         )
    #         if endpoint_features:
    #             endpoint_obj.status.features = endpoint_features
    #             # Add the latest drift measures results (calculated by the model monitoring batch)
    #             drift_measures = self._json_loads_if_not_none(
    #                 endpoint.get("drift_measures")
    #             )
    #             endpoint_obj.status.drift_measures = drift_measures
    #
    #     # If time metrics were provided, retrieve the results from the time series DB
    #     if metrics:
    #         endpoint_metrics = self.get_endpoint_metrics(
    #             endpoint_id=endpoint_obj.metadata.uid,
    #             start=start,
    #             end=end,
    #             metrics=metrics,
    #         )
    #         if endpoint_metrics:
    #             endpoint_obj.status.metrics = endpoint_metrics
    #
    #     return endpoint_obj

    def _get_path_and_container(self):
        """Getting path and container based on the model monitoring configurations"""
        path = mlrun.mlconf.model_endpoint_monitoring.store_prefixes.default.format(
            project=self.project,
            kind=mlrun.api.schemas.ModelMonitoringStoreKinds.ENDPOINTS,
        )
        (
            _,
            container,
            path,
        ) = mlrun.utils.model_monitoring.parse_model_endpoint_store_prefix(path)
        return path, container

    def list_model_endpoints(
        self, model: str= None, function: str= None, labels: typing.List= None, top_level: bool = None,
            metrics: typing.List[str] = None,
            start: str = "now-1h",
            end: str = "now",
    ):
        """
        Returns a list of endpoint unique ids, supports filtering by model, function,
        labels or top level. By default, when no filters are applied, all available endpoint ids for the given project
        will be listed.

        :param model:           The name of the model to filter by.
        :param function:        The name of the function to filter by.
        :param labels:          A list of labels to filter by. Label filters work by either filtering a specific value
                                of a label (i.e. list("key==value")) or by looking for the existence of a given
                                key (i.e. "key").
        :param top_level:       If True will return only routers and endpoint that are NOT children of any router.
        :param metrics:          A list of metrics to return for the model endpoint. There are pre-defined
                                 metrics for model endpoints such as predictions_per_second and
                                 latency_avg_5m but also custom metrics defined by the user. Please note that
                                 these metrics are stored in the time series DB and the results will be
                                 appeared under model_endpoint.spec.metrics.
        :param start:            The start time of the metrics. Can be represented by a string containing an
                                 RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or
                                 0 for the earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an
                                 RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days),
                                 or 0 for the earliest time.


        :return: List of model endpoints unique ids.
        """

        # Initialize an empty model endpoints list
        endpoint_list = mlrun.api.schemas.model_endpoints.ModelEndpointList(
            endpoints=[]
        )

        # Retrieve the raw data from the KV table and get the endpoint ids
        cursor = self.client.kv.new_cursor(
            container=self.container,
            table_path=self.path,
            filter_expression=self.build_kv_cursor_filter_expression(
                self.project,
                function,
                model,
                labels,
                top_level,
            ),
            attribute_names=["endpoint_id"],
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
        )
        try:
            items = cursor.all()
        except Exception:
            return []

        print('[EYAL]: now in kv list endpoints: ', items)

        # Create a list of model endpoints unique ids
        uids = [item["endpoint_id"] for item in items]

        # Add each relevant model endpoint to the model endpoints list
        for endpoint_id in uids:
            endpoint = self.get_model_endpoint(
                metrics=metrics,
                endpoint_id=endpoint_id,
                start=start,
                end=end,
            )
            endpoint_list.endpoints.append(endpoint)



        return endpoint_list


    # def list_model_endpoints(
    #     self, model: str= None, function: str= None, labels: typing.List= None, top_level: bool = None
    # ):
    #     """
    #     Returns a list of endpoint unique ids, supports filtering by model, function,
    #     labels or top level. By default, when no filters are applied, all available endpoint ids for the given project
    #     will be listed.
    #
    #     :param model:           The name of the model to filter by.
    #     :param function:        The name of the function to filter by.
    #     :param labels:          A list of labels to filter by. Label filters work by either filtering a specific value
    #                             of a label (i.e. list("key==value")) or by looking for the existence of a given
    #                             key (i.e. "key").
    #     :param top_level:       If True will return only routers and endpoint that are NOT children of any router.
    #
    #     :return: List of model endpoints unique ids.
    #     """
    #
    #     # Retrieve the raw data from the KV table and get the endpoint ids
    #     cursor = self.client.kv.new_cursor(
    #         container=self.container,
    #         table_path=self.path,
    #         filter_expression=self.build_kv_cursor_filter_expression(
    #             self.project,
    #             function,
    #             model,
    #             labels,
    #             top_level,
    #         ),
    #         attribute_names=["endpoint_id"],
    #         raise_for_status=v3io.dataplane.RaiseForStatus.never,
    #     )
    #     try:
    #         items = cursor.all()
    #     except Exception:
    #         return []
    #
    #     # Create a list of model endpoints unique ids
    #     uids = [item["endpoint_id"] for item in items]
    #
    #     return uids

    def delete_model_endpoints_resources(
        self, endpoints: mlrun.api.schemas.model_endpoints.ModelEndpointList
    ):
        """
        Delete all model endpoints resources in both KV and the time series DB.

        :param endpoints: An object of ModelEndpointList which is literally a list of model endpoints along with some
                          metadata. To get a standard list of model endpoints use ModelEndpointList.endpoints.
        """

        # Delete model endpoint record from KV table
        for endpoint in endpoints.endpoints:
            self.delete_model_endpoint(
                endpoint.metadata.uid,
            )

        # Delete remain records in the KV
        all_records = self.client.kv.new_cursor(
            container=self.container,
            table_path=self.path,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
        ).all()

        all_records = [r["__name"] for r in all_records]

        # Cleanup KV
        for record in all_records:
            self.client.kv.delete(
                container=self.container,
                table_path=self.path,
                key=record,
                raise_for_status=v3io.dataplane.RaiseForStatus.never,
            )

        # Cleanup TSDB
        frames = mlrun.utils.v3io_clients.get_frames_client(
            token=self.access_key,
            address=mlrun.mlconf.v3io_framesd,
            container=self.container,
        )

        # Getting the path for the time series DB
        events_path = (
            mlrun.mlconf.model_endpoint_monitoring.store_prefixes.default.format(
                project=self.project,
                kind=mlrun.api.schemas.ModelMonitoringStoreKinds.EVENTS,
            )
        )
        (
            _,
            _,
            events_path,
        ) = mlrun.utils.model_monitoring.parse_model_endpoint_store_prefix(events_path)

        # Delete time series DB resources
        try:
            frames.delete(
                backend=model_monitoring_constants.TimeSeriesTarget.TSDB,
                table=events_path,
                if_missing=v3io_frames.frames_pb2.IGNORE,
            )
        except v3io_frames.errors.CreateError:
            # Frames might raise an exception if schema file does not exist.
            pass

        # Final cleanup of tsdb path
        events_path.replace("://u", ":///u")
        store, _ = mlrun.store_manager.get_or_create_store(events_path)
        store.rm(events_path, recursive=True)

    @staticmethod
    def build_kv_cursor_filter_expression(
        project: str,
        function: str = None,
        model: str = None,
        labels: typing.List[str] = None,
        top_level: bool = False,
    ) -> str:
        """
        Convert the provided filters into a valid filter expression. The expected filter expression includes different
        conditions, divided by ' AND '.

        :param project:    The name of the project.
        :param model:      The name of the model to filter by.
        :param function:   The name of the function to filter by.
        :param labels:     A list of labels to filter by. Label filters work by either filtering a specific value of
                           a label (i.e. list("key==value")) or by looking for the existence of a given
                           key (i.e. "key").
        :param top_level:  If True will return only routers and endpoint that are NOT children of any router.

        :return: A valid filter expression as a string.
        """

        if not project:
            raise mlrun.errors.MLRunInvalidArgumentError("project can't be empty")

        # Add project filter
        filter_expression = [f"project=='{project}'"]

        # Add function and model filters
        if function:
            filter_expression.append(f"function=='{function}'")
        if model:
            filter_expression.append(f"model=='{model}'")

        # Add labels filters
        if labels:
            for label in labels:
                print("[EYA:]: label: ", label)
                if not label.startswith("_"):
                    label = f"_{label}"

                if "=" in label:
                    lbl, value = list(map(lambda x: x.strip(), label.split("=")))
                    filter_expression.append(f"{lbl}=='{value}'")
                else:
                    filter_expression.append(f"exists({label})")
                print("[EYAL]: filter expression: ", filter_expression)

        # Apply top_level filter (remove endpoints that considered a child of a router)
        if top_level:
            filter_expression.append(
                f"(endpoint_type=='{str(mlrun.utils.model_monitoring.EndpointType.NODE_EP.value)}' "
                f"OR  endpoint_type=='{str(mlrun.utils.model_monitoring.EndpointType.ROUTER.value)}')"
            )

        return " AND ".join(filter_expression)

    def get_endpoint_metrics(
        self,
        endpoint_id: str,
        metrics: typing.List[str],
        start: str = "now-1h",
        end: str = "now",
    ) -> typing.Dict[str, mlrun.api.schemas.Metric]:
        """
        Getting metrics from the time series DB. There are pre-defined metrics for model endpoints such as
        predictions_per_second and latency_avg_5m but also custom metrics defined by the user.

        :param endpoint_id:      The unique id of the model endpoint.
        :param metrics:          A list of metrics to return for the model endpoint.
        :param start:            The start time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.

        :return: A dictionary of metrics in which the key is a metric name and the value is a Metric object that also
                 includes the relevant timestamp. More details about the Metric object can be found under
                 mlrun.api.schemas.Metric.
        """

        if not metrics:
            raise mlrun.errors.MLRunInvalidArgumentError(
                "Metric names must be provided"
            )

        # Initialize metrics mapping dictionary
        metrics_mapping = {}

        # Getting the path for the time series DB
        events_path = (
            mlrun.mlconf.model_endpoint_monitoring.store_prefixes.default.format(
                project=self.project,
                kind=mlrun.api.schemas.ModelMonitoringStoreKinds.EVENTS,
            )
        )
        (
            _,
            _,
            events_path,
        ) = mlrun.utils.model_monitoring.parse_model_endpoint_store_prefix(events_path)

        # Retrieve the raw data from the time series DB based on the provided metrics and time ranges
        frames_client = mlrun.utils.v3io_clients.get_frames_client(
            token=self.access_key,
            address=mlrun.mlconf.v3io_framesd,
            container=self.container,
        )

        try:
            data = frames_client.read(
                backend=model_monitoring_constants.TimeSeriesTarget.TSDB,
                table=events_path,
                columns=["endpoint_id", *metrics],
                filter=f"endpoint_id=='{endpoint_id}'",
                start=start,
                end=end,
            )

            # Fill the metrics mapping dictionary with the metric name and values
            data_dict = data.to_dict()
            for metric in metrics:
                metric_data = data_dict.get(metric)
                if metric_data is None:
                    continue

                values = [
                    (str(timestamp), value) for timestamp, value in metric_data.items()
                ]
                metrics_mapping[metric] = mlrun.api.schemas.Metric(
                    name=metric, values=values
                )
        except v3io_frames.errors.ReadError:
            logger.warn("Failed to read tsdb", endpoint=endpoint_id)
        return metrics_mapping


class _ModelEndpointSQLStore(_ModelEndpointStore):
    """
    Handles the DB operations when the DB target is from type SQL. For the SQL operations, we use SQLAlchemy, a Python
    SQL toolkit that handles the communication with the database. Please note that for writing a new model endpoint
    record in the SQL database, we use an instance of SQLtarget from mlrun datastore objects.
    When using SQL for storing the model endpoints record, the user have to provide a valid path for the database.
    """

    def __init__(
        self,
        project: str,
        connection_string: str = None,
    ):
        """
        Initialize SQL store target object. Includes the import of SQLAlchemy toolkit and the required details for
        handling the SQL operations.
        :param project: The name of the project.
        :param connection_string: Valid connection string or a path to SQL database with model endpoints table.
        """
        import sqlalchemy as db
        from sqlalchemy.orm import sessionmaker

        super().__init__(project=project)
        self.connection_string = connection_string
        self.db = db
        self.sessionmaker = sessionmaker
        self.table_name = model_monitoring_constants.EventFieldType.MODEL_ENDPOINTS

    def write_model_endpoint(self, endpoint):
        """
        Create a new endpoint record in the SQL table using SQLTarget object from datastore.

        :param endpoint: ModelEndpoint object that will be written into the DB.
        """
        print("[EYAL]: try to connect db")
        engine = self.db.create_engine(
            self.connection_string
        )

        with engine.connect():
            # Define schema and key for the model endpoints table as required by the SQL table structure
            metadata = self.db.MetaData()
            self._get_table(self.table_name, metadata)
            metadata.create_all(engine)

            # Retrieving the relevant attributes from the model endpoint object
            endpoint_dict = self.get_params(endpoint=endpoint)
            endpoint_dict['predictions_per_second'] = None
            endpoint_dict['latency_avg_1h'] = None
            # need to add schema missing columns
            print("[EYAL]: endpoint_dict: ", endpoint_dict)
            # Convert the result into pandas Dataframe and write it into the database using the SQLTarget object
            endpoint_df = pd.DataFrame([endpoint_dict])
            endpoint_df.to_sql(
                self.table_name, con=engine, index=False, if_exists="append"
            )


        print("[EYAL]: SQL endpoint created!")

    def update_model_endpoint(self, endpoint_id, attributes):
        """
        Update a model endpoint record with a given attributes.

        :param endpoint_id: The unique id of the model endpoint.
        :param attributes: Dictionary of attributes that will be used for update the model endpoint. Note that the keys
                           of the attributes dictionary should exist in the SQL table.

        """
        print("[EYAL]: going to update SQL db TARGET: ", attributes)

        engine = self.db.create_engine(self.connection_string)
        with engine.connect():

            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            # Define and execute the query with the given attributes and the related model endpoint id
            update_query = (
                self.db.update(model_endpoints_table)
                .values(attributes)
                .where(model_endpoints_table.c[model_monitoring_constants.EventFieldType.ENDPOINT_ID] == endpoint_id)
            )
            engine.execute(update_query)

        print("[EYAL]: model endpoint has been updated!")

    def delete_model_endpoint(self, endpoint_id):
        """
        Deletes the SQL record of a given model endpoint id.

        :param endpoint_id: The unique id of the model endpoint.
        """
        engine = self.db.create_engine(self.connection_string)
        with engine.connect():

            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            print("[EYAL]: going to delete model endpoint!")
            # Delete the model endpoint record using sqlalchemy ORM
            session = self.sessionmaker(bind=engine)()
            session.query(model_endpoints_table).filter_by(
                endpoint_id=endpoint_id
            ).delete()
            session.commit()
            session.close()

            print("[EYAL]: model endpoint has been deleted!")

    def get_model_endpoint(
        self,
        metrics: typing.List[str] = None,
        start: str = "now-1h",
        end: str = "now",
        feature_analysis: bool = False,
        endpoint_id: str = None,
        convert_to_endpoint_object: bool = True,
    ):
        """
        Get a single model endpoint object. You can apply different time series metrics that will be added to the
        result.

        :param endpoint_id:                The unique id of the model endpoint.
        :param start:                      The start time of the metrics. Can be represented by a string containing an
                                           RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                           `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or
                                           0 for the earliest time.
        :param end:                        The end time of the metrics. Can be represented by a string containing an
                                           RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                           `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days),
                                           or 0 for the earliest time.
        :param metrics:                    A list of metrics to return for the model endpoint. There are pre-defined
                                           metrics for model endpoints such as predictions_per_second and
                                           latency_avg_5m but also custom metrics defined by the user. Please note that
                                           these metrics are stored in the time series DB and the results will be
                                           appeared under model_endpoint.spec.metrics.
        :param feature_analysis:           When True, the base feature statistics and current feature statistics will
                                           be added to the output of the resulting object.
        :param convert_to_endpoint_object: A boolean that indicates whether to convert the model endpoint dictionary
                                           into a ModelEndpoint or not. True by default.

        :return: A ModelEndpoint object.
        """
        logger.info(
            "Getting model endpoint record from SQL",
            endpoint_id=endpoint_id,
        )

        engine = self.db.create_engine(self.connection_string)

        # Validate that the model endpoints table exists
        if not engine.has_table(self.table_name):
            raise mlrun.errors.MLRunNotFoundError(f"Table {self.table_name} not found")

        with engine.connect():

            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            # Get the model endpoint record using sqlalchemy ORM
            from sqlalchemy.orm import sessionmaker

            session = sessionmaker(bind=engine)()

            columns = model_endpoints_table.columns.keys()
            values = (
                session.query(model_endpoints_table)
                .filter_by(endpoint_id=endpoint_id).filter_by()
                .all()
            )
            session.close()

        if len(values) == 0:
            raise mlrun.errors.MLRunNotFoundError(f"Endpoint {endpoint_id} not found")

        # Convert the database values and the table columns into a python dictionary
        endpoint = dict(zip(columns, values[0]))

        if convert_to_endpoint_object:
            # Convert the model endpoint dictionary into a ModelEndpont object
            endpoint = self._convert_into_model_endpoint_object(
                endpoint=endpoint, feature_analysis=feature_analysis
            )

        return endpoint

    # def list_model_endpoints(
    #     self, model: str= None, function: str= None, labels: typing.List= None, top_level: bool= None
    # ):
    #     engine = self.db.create_engine(self.connection_string)
    #     with engine.connect():
    #         metadata = self.db.MetaData()
    #         model_endpoints_table = self.db.Table(
    #             self.table_name, metadata, autoload=True, autoload_with=engine
    #         )
    #
    #         from sqlalchemy.orm import sessionmaker
    #
    #         session = sessionmaker(bind=engine)()
    #
    #         columns = model_endpoints_table.columns.keys()
    #         # values = session.query(model_endpoints_table.c["endpoint_id"]).filter_by(project=self.project)
    #         values = session.query(model_endpoints_table).filter_by(project=self.project)
    #
    #         print("[EYAL]: columns: ", columns)
    #         print("[EYAL]: values: ", values)
    #         for endpoint_values in values.all():
    #             endpoint_dict = dict(zip(columns, endpoint_values))
    #         # endpoint_dict = dict(zip(columns, values[0]))
    #
    #         if model:
    #             values = self._filter_values(
    #                 values, model_endpoints_table, "model", [model]
    #             )
    #         if function:
    #             values = self._filter_values(
    #                 values, model_endpoints_table, "function", [function]
    #             )
    #         if top_level:
    #             node_ep = str(mlrun.utils.model_monitoring.EndpointType.NODE_EP.value)
    #             router_ep = str(mlrun.utils.model_monitoring.EndpointType.ROUTER.value)
    #             endpoint_types = [node_ep, router_ep]
    #             values = self._filter_values(
    #                 values,
    #                 model_endpoints_table,
    #                 "endpoint_type",
    #                 [endpoint_types],
    #                 combined=False,
    #             )
    #         if labels:
    #             pass
    #
    #     # Convert list of tuples of endpoint ids into a single list with endpoint ids
    #     uids = [
    #         endpoint_id
    #         for endpoint_id_tuple in values.all()
    #         for endpoint_id in endpoint_id_tuple
    #     ]
    #
    #     return uids

    def list_model_endpoints(
        self, model: str= None, function: str= None, labels: typing.List= None, top_level: bool= None
    ):
        engine = self.db.create_engine(self.connection_string)
        endpoint_list = mlrun.api.schemas.model_endpoints.ModelEndpointList(
            endpoints=[]
        )
        with engine.connect():
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            from sqlalchemy.orm import sessionmaker

            session = sessionmaker(bind=engine)()

            columns = model_endpoints_table.columns.keys()
            # values = session.query(model_endpoints_table.c["endpoint_id"]).filter_by(project=self.project)
            values = session.query(model_endpoints_table).filter_by(project=self.project)



            if model:
                values = self._filter_values(
                    values, model_endpoints_table, "model", [model]
                )
            if function:
                values = self._filter_values(
                    values, model_endpoints_table, "function", [function]
                )
            if top_level:
                node_ep = str(mlrun.utils.model_monitoring.EndpointType.NODE_EP.value)
                router_ep = str(mlrun.utils.model_monitoring.EndpointType.ROUTER.value)
                endpoint_types = [node_ep, router_ep]
                values = self._filter_values(
                    values,
                    model_endpoints_table,
                    "endpoint_type",
                    [endpoint_types],
                    combined=False,
                )
            if labels:
                pass

            print("[EYAL]: columns: ", columns)
            print("[EYAL]: values: ", values)
            # Initialize an empty model endpoints list

            for endpoint_values in values.all():
                endpoint_dict = dict(zip(columns, endpoint_values))
                endpoint_obj = self._convert_into_model_endpoint_object(endpoint_dict)
                endpoint_list.endpoints.append(endpoint_obj)
            # endpoint_dict = dict(zip(columns, values[0]))

        # Convert list of tuples of endpoint ids into a single list with endpoint ids
        # uids = [
        #     endpoint_id
        #     for endpoint_id_tuple in values.all()
        #     for endpoint_id in endpoint_id_tuple
        # ]

        return endpoint_list

    def _filter_values(
        self, values, model_endpoints_table, key_filter, filtered_values, combined=True
    ):
        if len(filtered_values) == 1:
            return values.filter(model_endpoints_table.c[key_filter] == filtered_values)
        if combined:
            pass
        else:
            # Create a filter query and take into account at least one of the filtered values
            filter_query = ()
            for filter in filtered_values:
                filter_query += model_endpoints_table.c[key_filter] == filter
            return values.filter(filter_query).all()

    def _get_schema(self):
        return {
            "endpoint_id": str,
            "state": str,
            "project": str,
            "function_uri": str,
            "model": str,
            "model_class": str,
            "labels": str,
            "model_uri": str,
            "stream_path": str,
            "active": bool,
            "monitoring_mode": str,
            "feature_stats": str,
            "current_stats": str,
            "feature_names": str,
            "children": str,
            "label_names": str,
            "timestamp": datetime.datetime,
            "endpoint_type": str,
            "children_uids": str,
            "drift_measures": str,
            "drift_status": str,
            "monitor_configuration": str,
            "monitoring_feature_set_uri": str,
            "latency_avg_5m": float,
            "latency_avg_1h": float,
            "predictions_per_second": float,
            "predictions_count_5m": float,
            "predictions_count_1h": float,
            "first_request": str,
            "last_request": str,
            "error_count": int,
        }

    def _get_table(self, table_name, metadata):
        self.db.Table(
            table_name,
            metadata,
            self.db.Column("endpoint_id", self.db.String(40), primary_key=True),
            self.db.Column("state", self.db.String(10)),
            self.db.Column("project", self.db.String(40)),
            self.db.Column("function_uri", self.db.String(255)),
            self.db.Column("model", self.db.String(255)),
            self.db.Column("model_class", self.db.String(255)),
            self.db.Column("labels", self.db.Text),
            self.db.Column("model_uri", self.db.String(255)),
            self.db.Column("stream_path", self.db.Text),
            self.db.Column("active", self.db.Boolean),
            self.db.Column("monitoring_mode", self.db.String(10)),
            self.db.Column("feature_stats", self.db.Text),
            self.db.Column("current_stats", self.db.Text),
            self.db.Column("feature_names", self.db.Text),
            self.db.Column("children", self.db.Text),
            self.db.Column("label_names", self.db.Text),
            self.db.Column("timestamp", self.db.DateTime),
            self.db.Column("endpoint_type", self.db.String(10)),
            self.db.Column("children_uids", self.db.Text),
            self.db.Column("drift_measures", self.db.Text),
            self.db.Column("drift_status", self.db.String(40)),
            self.db.Column("monitor_configuration", self.db.Text),
            self.db.Column("monitoring_feature_set_uri", self.db.String(255)),
            self.db.Column("latency_avg_5m", self.db.Float),
            self.db.Column("latency_avg_1h", self.db.Float),
            self.db.Column("predictions_per_second", self.db.Float),
            self.db.Column("predictions_count_5m", self.db.Float),
            self.db.Column("predictions_count_1h", self.db.Float),
            self.db.Column("first_request", self.db.String(40)),
            self.db.Column("last_request", self.db.String(40)),
            self.db.Column("error_count", self.db.Integer),
        )

    def delete_model_endpoints_resources(
        self, endpoints: mlrun.api.schemas.model_endpoints.ModelEndpointList
    ):
        """
        Delete all model endpoints resources in both SQL and the time series DB.

        :param endpoints: An object of ModelEndpointList which is literally a list of model endpoints along with some
                          metadata. To get a standard list of model endpoints use ModelEndpointList.endpoints.
        """

        # Delete model endpoint record from SQL table
        for endpoint in endpoints.endpoints:
            self.delete_model_endpoint(
                endpoint.metadata.uid,
            )


class ModelEndpointStoreType(enum.Enum):
    """Enum class to handle the different store type values for saving a model endpoint record."""

    kv = "kv"
    sql = "sql"

    def to_endpoint_target(
        self,
        project: str,
        access_key: str = None,
        connection_string: str = None,
    ) -> _ModelEndpointStore:
        """
        Return a ModelEndpointStore object based on the provided enum value.

        :param project:           The name of the project.
        :param access_key:        Access key with permission to the DB table. Note that if access key is None and the
                                  endpoint target is from type KV then the access key will be retrieved from the
                                  environment variable.
        :param connection_string: A valid connection string for SQL target. Contains several key-value pairs that
                                  required for the database connection.
                                  e.g. A root user with password 1234, tries to connect a schema called mlrun within a
                                  local MySQL DB instance: 'mysql+pymysql://root:1234@localhost:3306/mlrun'.

        :return: ModelEndpointStore object.

        """

        if self.value == ModelEndpointStoreType.kv.value:

            # Get V3IO access key from env
            access_key = (
                mlrun.mlconf.get_v3io_access_key() if access_key is None else access_key
            )

            return _ModelEndpointKVStore(project=project, access_key=access_key)

        # Assuming SQL store target if store type is not KV.
        # Update these lines once there are more than two store target types.
        sql_connection_string = (
            connection_string
            if connection_string is not None
            else mlrun.mlconf.model_endpoint_monitoring.connection_string
        )
        return _ModelEndpointSQLStore(
            project=project, connection_string=sql_connection_string
        )

    @classmethod
    def _missing_(cls, value: typing.Any):
        """A lookup function to handle an invalid value.
        :param value: Provided enum (invalid) value.
        """
        valid_values = list(cls.__members__.keys())
        raise mlrun.errors.MLRunInvalidArgumentError(
            "%r is not a valid %s, please choose a valid value: %s."
            % (value, cls.__name__, valid_values)
        )


def get_model_endpoint_target(
    project: str, access_key: str = None
) -> _ModelEndpointStore:
    """
    Getting the DB target type based on mlrun.config.model_endpoint_monitoring.store_type.

    :param project:    The name of the project.
    :param access_key: Access key with permission to the DB table.

    :return: ModelEndpointStore object. Using this object, the user can apply different operations on the
             model endpoint record such as write, update, get and delete.
    """

    # Get store type value from ModelEndpointStoreType enum class
    model_endpoint_store_type = ModelEndpointStoreType(
        mlrun.mlconf.model_endpoint_monitoring.store_type
    )

    # Convert into model endpoint store target object
    return model_endpoint_store_type.to_endpoint_target(project, access_key)


# from sqlalchemy.orm import declarative_base
# from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEX
# Base = declarative_base()
#
# class ModelEndpointsSQLtable(Base):
#     __tablename__ = "model_endpoints"
#
#     endpoint_id = Column(String, primary_key=True)
#     state = Column(String)
#     project= Column(String)
#     function_uri= Column(String)
#     model= Column(String)
#     model_class= Column(String)
#     labels= Column(String)
#     model_uri= Column(String)
#     stream_path= Column(String)
#     active= Column(Boolean)
#     monitoring_mode= Column(String)
#     feature_stats= Column(String)
#     current_stats= Column(String)
#     feature_names= Column(String)
#     children= Column(String)
#     label_names= Column(String)
#     timestamp= Column(DateTime)
#     endpoint_type= Column(String)
#     children_uids= Column(String)
#     drift_measures= Column(String)
#     drift_status= Column(String)
#     monitor_configuration= Column(String)
#     monitoring_feature_set_uri= Column(String)
#     latency_avg_5m= Column(Float)
#     latency_avg_1h= Column(Float)
#     predictions_per_second= Column(Float)
#     predictions_count_5m= Column(Float)
#     predictions_count_1h= Column(Float)
#     first_request= Column(String)
#     last_request= Column(String)
#     error_count= Column(Integer)
#
# ModelEndpointsSQLtable.__tablename__.
