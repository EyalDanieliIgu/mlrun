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

import typing
from abc import ABC, abstractmethod


class ApplicationResult(ABC):
    """
    An abstract class to handle the applications result data.
    """

    def __init__(self, project: str):
        """
        Initialize a new application result target.

        :param project:             The name of the project.
        """
        self.project = project

    @abstractmethod
    def write_application_result(self, event: typing.Dict[str, typing.Any]):
        """
        Write a new application result record to the DB table.

        :param event: Event dictionary that represents application result. In general, the dictionary structure
                      expected to be corresponded to the .
        """
        pass

