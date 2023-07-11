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
import mlrun
import mlrun.common.schemas
import mlrun.utils.singleton


class ClusterizationSpec(
    metaclass=mlrun.utils.singleton.Singleton,
):
    @staticmethod
    def get_clusterization_spec():
        is_chief = mlrun.mlconf.httpdb.clusterization.role == "chief"
        return mlrun.common.schemas.ClusterizationSpec(
            chief_api_state=mlrun.mlconf.httpdb.state if is_chief else None,
            chief_version=mlrun.mlconf.version if is_chief else None,
        )
