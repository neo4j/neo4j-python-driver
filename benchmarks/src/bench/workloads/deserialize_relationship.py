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
# * *id*: `deserialize-relationship`


from __future__ import annotations

from typing import TYPE_CHECKING

from .util import (
    benchmark,
    deserialization_template,
)


if TYPE_CHECKING:
    from .util import Bencher


@benchmark
def deserialize_relationship(bencher: Bencher) -> None:
    setup_query = (
        "UNWIND range(1, 50) AS nodeId\n"
        "UNWIND range(1, 5) AS relId\n"
        "CREATE (s:TestStart {id: nodeId})\n"
        "CREATE (e:TestEnd {id: nodeId})\n"
        "CREATE (s)-[r:TEST {id: relId}]->(e)"
    )
    query = "MATCH (s:TestStart)-[r:TEST]-()\nRETURN s, collect(r)"
    deserialization_template(bencher, query, setup_query=setup_query)
