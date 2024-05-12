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

import enum

import enum
class ValueType(str, enum.Enum):
    """TDEngine value type. Used to define data types in TDEngine."""

    TIMESTAMP = "TIMESTAMP"
    INT = "INT"
    FLOAT = "FLOAT"
    BINARY = "BINARY"




class _TDEngineSuperTableSchema:
    def __init__(self, table_name: str, columns: dict[str, str], tags: dict[str, str]):
        self.table_name = table_name
        self.columns = columns
        self.tags = tags

    def create_super_table_query(self, db_prefix: str = "") -> str:
        columns = ", ".join(f"{col} {val}" for col, val in self.columns.items())
        tags = ", ".join(f"{col} {val}" for col, val in self.tags.items())
        return f"CREATE TABLE {db_prefix}{self.table_name} ({columns}) TAGS ({tags});"
    def drop_table(self, db_prefix: str = "") -> str:
        return f"DROP TABLE {db_prefix}{self.table_name};"
