# Copyright 2023 Iguazio
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
from services.api.tests.unit.runtime_handlers.test_kubejob import (
    TestKubejobRuntimeHandler,
)


class TestRemoteSparkjobRuntimeHandler(TestKubejobRuntimeHandler):
    """
    Remote Spark runtime behaving pretty much like the kubejob runtime just with few modifications (several automations
    we want to do for the user) so we're simply running the same tests as the ones of the job runtime
    """

    def _get_class_name(self):
        return "remote-spark"
