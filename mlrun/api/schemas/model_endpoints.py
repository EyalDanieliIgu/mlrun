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
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field


from mlrun.api.schemas.object import ObjectKind
from mlrun.utils.model_monitoring import EndpointType, create_model_endpoint_id
import mlrun.model
import mlrun.model_monitoring.constants as model_monitoring_constants


class ModelMonitoringStoreKinds:
    ENDPOINTS = "endpoints"
    EVENTS = "events"


class ModelEndpointMetadata(mlrun.model.ModelObj):
    def __init__(self, project: Optional[str] = "",
    labels: Optional[dict] = None,
    uid: Optional[str] = ""):
        self.project: Optional[str] = project
        self.labels: Optional[dict] = labels or {}
        self.uid: Optional[str] = uid

class ModelMonitoringMode(str, enum.Enum):
    enabled = "enabled"
    disabled = "disabled"


class ModelEndpointSpec(mlrun.model.ModelObj):
    def __init__(self,    function_uri: Optional[str] = None,  # <project_name>/<function_name>:<tag>
    model: Optional[str]= "",  # <model_name>:<version>
    model_class: Optional[str]= "",
    model_uri: Optional[str]= "",
    feature_names: Optional[List[str]]= None,
    label_names: Optional[List[str]]= None,
    stream_path: Optional[str]= "",
    algorithm: Optional[str]= "",
    monitor_configuration: Optional[dict]= None,
    active: Optional[bool]= True,
    monitoring_mode: Optional[str] = ModelMonitoringMode.disabled):
        self.function_uri = function_uri
        self.model = model
        self.model_class = model_class
        self.model_uri = model_uri
        self.feature_names = feature_names or []
        self.label_names = label_names or []
        self.stream_path = stream_path
        self.algorithm = algorithm
        self.monitor_configuration = monitor_configuration or {}
        self.active = active
        self.monitoring_mode = monitoring_mode


class Histogram(mlrun.model.ModelObj):
    def __init__(self,     buckets: List[float] = None,
    counts: List[int] = None):
        self.buckets = buckets
        self.counts = counts


class FeatureValues(mlrun.model.ModelObj):
    def __init__(self, min: float = None,
                 mean: float = None,
                 max: float = None,
                 histogram: Histogram = None):
        self.min = min
        self.mean = mean
        self.max = max
        self.histogram = histogram

    @classmethod
    def from_dict(cls, stats: Optional[dict]):
        if stats:
            return FeatureValues(
                min=stats["min"],
                mean=stats["mean"],
                max=stats["max"],
                histogram=Histogram(buckets=stats["hist"][1], counts=stats["hist"][0]),
            )
        else:
            return None


class Features(mlrun.model.ModelObj):
    def __init__(self,     name: str = None,
    weight: float = None,
    expected: Optional[FeatureValues] = None,
    actual: Optional[FeatureValues] = None,):
        self.name = name
        self.weight = weight
        self.expected = expected
        self.actual = actual


    @classmethod
    def new(
        cls,
        feature_name: str,
        feature_stats: Optional[dict],
        current_stats: Optional[dict],
    ):
        return cls(
            name=feature_name,
            weight=-1.0,
            expected=FeatureValues.from_dict(feature_stats),
            actual=FeatureValues.from_dict(current_stats),
        )


class ModelEndpointStatus(mlrun.model.ModelObj):
    def __init__(self,
    feature_stats: Optional[dict] = None,
    current_stats: Optional[dict]= None,
    first_request: Optional[str]= "",
    last_request: Optional[str]= "",
    accuracy: Optional[float]= 0,
    error_count: Optional[int]= 0,
    drift_status: Optional[str]= "",
    drift_measures: Optional[dict]= None,
    metrics: Optional[Dict[str, Dict[str, Any]]]= None,
    features: Optional[List[Features]]= None,
    children: Optional[List[str]]= None,
    children_uids: Optional[List[str]]= None,
    endpoint_type: Optional[EndpointType]= EndpointType.NODE_EP,
    monitoring_feature_set_uri: Optional[str]= "",
                state: Optional[str] = ""):
        self.feature_stats = feature_stats or {}
        self.current_stats = current_stats or {}
        self.first_request = first_request
        self.last_request = last_request
        self.accuracy = accuracy
        self.error_count = error_count
        self.drift_status = drift_status
        self.drift_measures = drift_measures or {}
        self.metrics = metrics or {model_monitoring_constants.EventKeyMetrics.GENERIC : {
                model_monitoring_constants.EventLiveStats.LATENCY_AVG_1H: 0,
                model_monitoring_constants.EventLiveStats.PREDICTIONS_PER_SECOND: 0,
            }}
        self.features = features or []
        self.children = children or []
        self.children_uids = children_uids or []
        self.endpoint_type = endpoint_type
        self.monitoring_feature_set_uri = monitoring_feature_set_uri
        self.state = state


class ModelEndpoint(mlrun.model.ModelObj):
    def __init__(self,
    kind: ObjectKind = Field(ObjectKind.model_endpoint, const=True),
    metadata: ModelEndpointMetadata = ModelEndpointMetadata(),
    spec: ModelEndpointSpec = ModelEndpointSpec(),
    status: ModelEndpointStatus = ModelEndpointStatus(),**data: Any):
        print('here')
        self.kind = kind
        self.metadata = metadata
        self.spec = spec
        self.status = status

#     class Config:
#         extra = Extra.allow
        super().__init__(**data)
        if self.metadata.uid is None:
            uid = create_model_endpoint_id(
                function_uri=self.spec.function_uri,
                versioned_model=self.spec.model,
            )
            self.metadata.uid = str(uid)


class ModelEndpointList(mlrun.model.ModelObj):
    def __init__(self, endpoints: List[ModelEndpoint]):
        self.endpoints = endpoints


class GrafanaColumn(BaseModel):
    text: str
    type: str


class GrafanaNumberColumn(GrafanaColumn):
    text: str
    type: str = "number"


class GrafanaStringColumn(GrafanaColumn):
    text: str
    type: str = "string"


class GrafanaTable(BaseModel):
    columns: List[GrafanaColumn]
    rows: List[List[Optional[Union[float, int, str]]]] = []
    type: str = "table"

    def add_row(self, *args):
        self.rows.append(list(args))


class GrafanaDataPoint(BaseModel):
    value: float
    timestamp: int  # Unix timestamp in milliseconds


class GrafanaTimeSeriesTarget(BaseModel):
    target: str
    datapoints: List[Tuple[float, int]] = []

    def add_data_point(self, data_point: GrafanaDataPoint):
        self.datapoints.append((data_point.value, data_point.timestamp))
