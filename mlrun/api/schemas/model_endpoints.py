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
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field
from pydantic.main import Extra

from mlrun.api.schemas.object import ObjectKind, ObjectSpec, ObjectStatus
from mlrun.utils.model_monitoring import EndpointType, create_model_endpoint_id
import mlrun.model_monitoring.constants as model_monitoring_constants

class ModelMonitoringStoreKinds:
    ENDPOINTS = "endpoints"
    EVENTS = "events"


class ModelEndpointMetadata(BaseModel):
    project: Optional[str] = ""
    labels: Optional[dict] = {}
    uid: Optional[str] = ""

    class Config:
        extra = Extra.allow

    @classmethod
    def from_dict(cls, endpoint_dict, json_parse_values=None):
        if json_parse_values is None:
            json_parse_values = ['labels']
        if 'endpoint_id' in endpoint_dict:
            endpoint_dict['uid'] = endpoint_dict.pop('endpoint_id')
        return _mapping_parse(cls, flatted_dictionary=endpoint_dict, json_parse_values=json_parse_values)


class ModelMonitoringMode(str, enum.Enum):
    enabled = "enabled"
    disabled = "disabled"


class ModelEndpointSpec(ObjectSpec):
    function_uri: Optional[str] = ""  # <project_name>/<function_name>:<tag>
    model: Optional[str] = ""  # <model_name>:<version>
    model_class: Optional[str] = ""
    model_uri: Optional[str] = ""
    feature_names: Optional[List[str]] = []
    label_names: Optional[List[str]] = []
    stream_path: Optional[str] = ""
    algorithm: Optional[str] = ""
    monitor_configuration: Optional[dict] = {}
    active: Optional[bool] = True
    monitoring_mode: Optional[str] = ModelMonitoringMode.disabled

    @classmethod
    def from_dict(self, endpoint_dict, json_parse_values=None):
        if json_parse_values is None:
            json_parse_values = ['feature_names', 'label_names', 'monitor_configuration']
        return _mapping_parse(self, flatted_dictionary=endpoint_dict, json_parse_values=json_parse_values)


class Histogram(BaseModel):
    buckets: List[float]
    counts: List[int]


class FeatureValues(BaseModel):
    min: float
    mean: float
    max: float
    histogram: Histogram

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


class Features(BaseModel):
    name: str
    weight: float
    expected: Optional[FeatureValues]
    actual: Optional[FeatureValues]

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


class ModelEndpointStatus(ObjectStatus):
    feature_stats: Optional[dict] = {}
    current_stats: Optional[dict] = {}
    first_request: Optional[str] = ""
    last_request: Optional[str] = ""
    accuracy: Optional[float] = 0
    error_count: Optional[int] = 0
    drift_status: Optional[str] = ""
    drift_measures: Optional[dict] = {}
    metrics: Optional[Dict[str, Dict[str, Any]]] = {model_monitoring_constants.EventKeyMetrics.GENERIC : {
                model_monitoring_constants.EventLiveStats.LATENCY_AVG_1H: 0,
                model_monitoring_constants.EventLiveStats.PREDICTIONS_PER_SECOND: 0,
            }}
    features: Optional[List[Features]] = []
    children: Optional[List[str]] = []
    children_uids: Optional[List[str]] = []
    endpoint_type: Optional[EndpointType] = EndpointType.NODE_EP
    monitoring_feature_set_uri: Optional[str] = ""

    class Config:
        extra = Extra.allow

    @classmethod
    def from_dict(cls, endpoint_dict, json_parse_values=None):
        if json_parse_values is None:
            json_parse_values = ['feature_stats', 'current_stats', 'drift_measures', 'metrics', 'features', 'children',
                                 'children_uids', 'endpoint_type']
        return _mapping_parse(cls, flatted_dictionary=endpoint_dict, json_parse_values=json_parse_values)


class ModelEndpoint(BaseModel):
    kind: ObjectKind = Field(ObjectKind.model_endpoint, const=True)
    metadata: ModelEndpointMetadata = ModelEndpointMetadata()
    spec: ModelEndpointSpec = ModelEndpointSpec()
    status: ModelEndpointStatus = ModelEndpointStatus()

    class Config:
        extra = Extra.allow

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.metadata.uid is None:
            uid = create_model_endpoint_id(
                function_uri=self.spec.function_uri,
                versioned_model=self.spec.model,
            )
            self.metadata.uid = str(uid)

    def flat_dict(self):
        model_endpoint_dictionary = self.dict(exclude={"kind"})
        flatten_dict = {}
        for k_object in model_endpoint_dictionary:
            for key in model_endpoint_dictionary[k_object]:
                if not isinstance(model_endpoint_dictionary[k_object][key], (str, bool)):
                    flatten_dict[key] = json.dumps(model_endpoint_dictionary[k_object][key])
                else:
                    flatten_dict[key] = model_endpoint_dictionary[k_object][key]
        flatten_dict['endpoint_id'] = flatten_dict.pop('uid')
        return flatten_dict

    @classmethod
    def from_dict(cls, endpoint_dict):
        return cls(metadata=ModelEndpointMetadata.from_dict(endpoint_dict=endpoint_dict),
                   spec=ModelEndpointSpec.from_dict(endpoint_dict=endpoint_dict),
                   status=ModelEndpointStatus.from_dict(endpoint_dict=endpoint_dict))




class ModelEndpointList(BaseModel):
    endpoints: List[ModelEndpoint] = []


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

def _mapping_parse(base_model, flatted_dictionary, json_parse_values = []):
    wanted_keys = base_model.__fields__.keys()
    dict_to_parse  = {}
    for field_key in wanted_keys:
        if field_key in flatted_dictionary:
            if field_key in json_parse_values:
                dict_to_parse[field_key] = _json_loads_if_not_none(flatted_dictionary[field_key])
            else:
                dict_to_parse[field_key] = flatted_dictionary[field_key]
    print('[EYAL]: dict_to_parse: ', dict_to_parse)
    return base_model.parse_obj(dict_to_parse)

def _json_loads_if_not_none(field: Any) -> Any:
    return (
        json.loads(field)
        if field and field != "null" and field is not None
        else None
    )