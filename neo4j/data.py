# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO: 6.0 - remove this file


from ._data import (
    DataTransformer,
    Record,
    RecordExporter,
    RecordTableRowExporter,
)
from ._meta import deprecation_warn


map_type = type(map(str, range(0)))

__all__ = [
    "map_type",
    "Record",
    "DataTransformer",
    "RecordExporter",
    "RecordTableRowExporter",
]

deprecation_warn(
    "The module 'neo4j.data' was made internal and will "
    "no longer be available for import in future versions. "
    "`neo4j.data.Record` should be imported directly from `neo4j`.",
    stack_level=2
)
