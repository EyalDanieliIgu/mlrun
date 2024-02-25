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

# flake8: noqa  - this is until we take care of the F401 violations with respect to __all__ & sphinx

import enum
import typing

import mlrun.common.schemas.secret
import mlrun.errors

from .tsdb import TSDBstore


class TSDBstoreType(enum.Enum):
    """Enum class to handle the different TSDB store type values for storing real time metrics"""

    v3io_tsdb = "v3io-tsdb"
    Prometheus = "prometheus"

    def to_tsdb_store(
        self,
        project: str,
        **kwargs
        # access_key: str = None,
    ) -> TSDBstore:
        """
        Return a TSDBstore object based on the provided enum value.

        :param project:                    The name of the project.
        :param access_key:                 Access key with permission to the TSDB.

        :return: `TSDBstore` object.

        """

        if self.value == TSDBstoreType.v3io_tsdb.value:
            from .v3io.v3io_tsdb import V3IOTSDBstore


            return V3IOTSDBstore(project=project, **kwargs)



    @classmethod
    def _missing_(cls, value: typing.Any):
        """A lookup function to handle an invalid value.
        :param value: Provided enum (invalid) value.
        """
        valid_values = list(cls.__members__.keys())
        raise mlrun.errors.MLRunInvalidArgumentError(
            f"{value} is not a valid tsdb store, please choose a valid value: %{valid_values}."
        )

def get_tsdb_store(
    project: str,
    # access_key: str = None,
    # secret_provider: typing.Callable = None,
    **kwargs
) -> TSDBstore:
    """
    Getting the DB target type based on mlrun.config.model_endpoint_monitoring.store_type.

    :param project:         The name of the project.
    :param access_key:      Access key with permission to the DB table.

    :return: `ModelEndpointStore` object. Using this object, the user can apply different operations on the
             model endpoint record such as write, update, get and delete.
    """

    # Get store type value from ModelEndpointStoreType enum class
    tsdb_store_type = TSDBstoreType(
        mlrun.mlconf.model_endpoint_monitoring.tsdb_store_type
    )

    # Convert into model endpoint store target object
    return tsdb_store_type.to_tsdb_store(
        project=project, **kwargs
    )


