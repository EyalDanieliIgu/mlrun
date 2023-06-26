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
import typing

import prometheus_client

_registry_path = "/tmp/prom-reg.txt"
_registry: prometheus_client.CollectorRegistry = prometheus_client.CollectorRegistry()
_prediction_counter: prometheus_client.Counter = prometheus_client.Counter(name="predictions_total", documentation="Counter for total predictions", registry=_registry, labelnames=['project', 'endpoint_id', 'model', 'endpoint_type'])
_model_latency: prometheus_client.Summary = prometheus_client.Summary(name="model_latency_seconds", documentation="Summary for for model latency", registry=_registry, labelnames=['project', 'endpoint_id', 'model', 'endpoint_type'])
_batch_metrics: prometheus_client.Gauge = prometheus_client.Gauge(name='drift_metrics', documentation='Results from the batch drift analysis', registry=_registry, labelnames=['project', 'endpoint_id', 'metric'])
_income_features: prometheus_client.Gauge = prometheus_client.Gauge(name='income_features', documentation='Samples of features and predictions', registry=_registry, labelnames=['project', 'endpoint_id', 'metric'])
_error_counter: prometheus_client.Counter = prometheus_client.Counter(name="errors_total", documentation="Counter for total errors", registry=_registry, labelnames=['project', 'endpoint_id', 'model'])
_drift_status: prometheus_client.Enum = prometheus_client.Enum(name="drift_status", documentation='Drift status of the model endpoint', registry=_registry, states=['NO_DRIFT', 'DRIFT_DETECTED', 'POSSIBLE_DRIFT'], labelnames=['project', 'endpoint_id'])

def _write_registry(func):
    def wrapper(*args, **kwargs):
        global _registry
        """A wrapper function"""
        # Extend some capabilities of func
        print('now in wrapper')
        func(*args, **kwargs)
        print('finish in wrapper')
        prometheus_client.write_to_textfile(path=_registry_path, registry=_registry)
    return wrapper

@_write_registry
def write_predictions_and_latency_metrics(project: str,
endpoint_id: str, latency: int, model_name: str, endpoint_type: int):
    """
    Update the prediction counter and the latency value of the provided model endpoint within Prometheus registry.

    :param project: Project name
    :endpoint id:   Model endpoint unique id
    :latency:       Latency time (microsecond) in which the event has been processed through the model server.
    """

    global _prediction_counter
    print('[EYAL]: now in ince counter iwthin model endpoints!')
    # Increase the prediction counter by 1
    _prediction_counter.labels(project=project, endpoint_id=endpoint_id, model=model_name, endpoint_type=endpoint_type).inc(1)

    # Update latency value
    _model_latency.labels(project=project, endpoint_id=endpoint_id, model=model_name, endpoint_type=endpoint_type).observe(latency)

    # _write_registry()

@_write_registry
def write_drift_metrics(project: str, endpoint_id: str, metric: str, value: float):
    """Update metrics within Prometheus registry. At the moment"""
    global _batch_metrics

    # Set the provided value
    _batch_metrics.labels(project=project, endpoint_id=endpoint_id, metric=metric).set(value=value)
    # _write_registry()

@_write_registry
def write_income_features(project: str, endpoint_id: str, features: typing.Dict[str, float]):
    """Update income and predictions


    :param project: Project name
    :endpoint id:   Model endpoint unique id
    :features:


    """
    global _income_features

    for metric in features:
        _income_features.labels(project=project, endpoint_id=endpoint_id, metric=metric).set(value=features[metric])
    # Set the provided value

    # _write_registry()

@_write_registry
def write_drift_status(project: str,
endpoint_id: str, drift_status: str):
    """
    Update model endpoint drift status.

    :param project: Project name
    :endpoint id:   Model endpoint unique id
    :drift_status:  Drift status value, can be one of the following: 'NO_DRIFT', 'DRIFT_DETECTED', or 'POSSIBLE_DRIFT'.
    """

    global _drift_status
    print('[EYAL]: now in drift status counter iwthin model endpoints!')
    # Increase the prediction counter by 1
    _drift_status.labels(project=project, endpoint_id=endpoint_id).state(drift_status)


@_write_registry
def write_errors(project: str, endpoint_id: str,model_name: str):
    global _error_counter
    _error_counter.labels(project=project, endpoint_id=endpoint_id, model=model_name).inc(1)

def get_registry():

    f = open(_registry_path)  # opening a file
    lines = f.read()  # reading a file

    f.close()
    res = lines.encode(encoding = 'UTF-8', errors = 'strict')
    print('[EYAL]: lines before return: ', res)

    return lines


