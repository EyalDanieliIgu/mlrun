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

def get_counter(
endpoint_id: str,):
    global _counters
    if endpoint_id not in _counters:
        print('[EYAL]: create counter in dictionary')
        _counters[endpoint_id] = prometheus_client.Counter(name=endpoint_id, documentation=f"Counter for {endpoint_id}", )
    print('[EYAL]: counters dictioanry: ', _counters)
    return _counters[endpoint_id]

def inc_counter(
endpoint_id: str):
    print('[EYAL]: now in ince counter iwthin model endpoints!')
    counter = get_counter(endpoint_id)
    counter.inc(1)

    print('[EYAL]: counter was increased: ', counter._value.get())