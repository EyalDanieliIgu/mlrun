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

import prometheus_client

_registry: prometheus_client.CollectorRegistry = prometheus_client.CollectorRegistry()
_prediction_counter: prometheus_client.Counter = prometheus_client.Counter(name="predictions_total", documentation="Counter for total predictions", registry=_registry, labelnames=['endpoint_id'])
_model_latency: prometheus_client.Summary = prometheus_client.Summary(name="model_latency_seconds", documentation="Summary for for model latency", registry=_registry, labelnames=['endpoint_id'])
_batch_metrics: prometheus_client.Gauge = prometheus_client.Gauge(name='drift_metrics', documentation='Results from the batch drift analysis', registry=_registry, labelnames=['endpoint_id', 'metric'])
def update_prometheus_metrics(
endpoint_id: str, latency: int):
    global _prediction_counter
    print('[EYAL]: now in ince counter iwthin model endpoints!')

    _prediction_counter.labels(f"{endpoint_id}").inc(1)
    _model_latency.labels(f"{endpoint_id}").observe(latency)

    write_registry()

def update_batch_metrics(endpoint_id: str, metric: str, value: float):
    global _batch_metrics
    _batch_metrics.labels(endpoint_id=endpoint_id, metric=metric).set(value=value)
    write_registry()

def write_registry():
    global _registry
    print('[EYAL]: going to write to registry')

    print('[EYAL]: our regisytty: ', _registry)

    prometheus_client.write_to_textfile('/tmp/eyal-raid.txt', _registry)
    print('[EYAL]: done to write to registry')

def get_registry():


    f = open('/tmp/eyal-raid.txt')  # opening a file
    lines = f.read()  # reading a file

    f.close()
    res = lines.encode(encoding = 'UTF-8', errors = 'strict')
    print('[EYAL]: lines before return: ', res)

    return lines

