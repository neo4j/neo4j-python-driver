# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

# * *date*: 2025-10-27
# * *id*: `deserialize-vector`


from __future__ import annotations

from typing import TYPE_CHECKING

from ._data_vector import DATA
from .util import (
    benchmark,
    deserialization_template,
)


if TYPE_CHECKING:
    from .util import Bencher


@benchmark
def deserialize_vector(bencher: Bencher) -> None:
    setup_query = (
        "CREATE (n:Test)\n"
        "WITH *, "
        "reduce(acc = [], "
        "i IN RANGE(0, size($data) - 1) | acc + [[i, $data[i]]]) AS data\n"
        "UNWIND data AS row\n"
        "WITH *, row[0] AS i, row[1] AS data\n"
        "SET n[toString(i)] = data"
    )
    query = (
        "MATCH (n:Test)\n"
        "LIMIT 1\n"
        "RETURN reduce(acc = [], key IN keys(n) | acc + [n[key]]) AS data"
    )
    deserialization_template(
        bencher,
        query,
        setup_query=setup_query,
        setup_data=DATA,
    )
