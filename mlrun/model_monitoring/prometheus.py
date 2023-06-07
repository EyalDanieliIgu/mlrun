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
import typing
_counters: typing.Dict[str, prometheus_client.Counter] = {}
_registry = prometheus_client.CollectorRegistry()

def get_counter(
endpoint_id: str,):
    global _counters
    global _registry
    # if _registry is None:
    #     print('[EYAL]: generating a new registry')
    #     _registry = prometheus_client.CollectorRegistry()
    if endpoint_id not in _counters:
        print('[EYAL]: create counter in dictionary for name: ', endpoint_id)
        _counters[endpoint_id] = prometheus_client.Counter(name=f"endpoint_predictions_{endpoint_id}", documentation=f"Counter for {endpoint_id}", registry=_registry)
    print('[EYAL]: counters dictioanry: ', _counters)
    return _counters[endpoint_id]

def inc_counter(
endpoint_id: str):
    print('[EYAL]: now in ince counter iwthin model endpoints!')
    counter = get_counter(endpoint_id)
    counter.inc(1)
    print('[EYAL]: counter was increased: ', counter._value.get())
    write_registry()


def write_registry():
    global _registry
    print('[EYAL]: going to write to registry')

    print('[EYAL]: our regisytty: ', _registry)
    # g = prometheus_client.Gauge('eyal_status', '1 if raid array is okay', registry=_registry)
    # g.set(1)
    prometheus_client.write_to_textfile('/tmp/eyal-raid.txt', _registry)
    print('[EYAL]: done to write to registry')

def get_registry():
    # global _registry
    # res = prometheus_client.generate_latest(registry=_registry)
    # print('[EYAL]: registry before return: ', res)

    f = open('/tmp/eyal-raid.txt')  # opening a file
    lines = f.read()  # reading a file

    f.close()
    res = lines.encode(encoding = 'UTF-8', errors = 'strict')
    print('[EYAL]: lines before return: ', res)
    # event.body = {
    #     "id": event.id,
    #     "body": res,
    # }
    # print('[EYAL]: event result: ', event)
    return lines

    # return res