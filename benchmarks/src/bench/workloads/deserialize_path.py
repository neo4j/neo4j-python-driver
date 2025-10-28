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
# * *id*: `deserialize-path`


from __future__ import annotations

from typing import TYPE_CHECKING

from .util import (
    benchmark,
    deserialization_template,
)


if TYPE_CHECKING:
    from .util import Bencher


@benchmark
def deserialize_path(bencher: Bencher) -> None:
    setup_query = (
        "CREATE (center:TestCenter)\n"
        "WITH center\n"
        "CREATE (a1:Test {id: 1})-[:IN {id: 1}]->(center)"
        "-[:OUT {id: 125}]->(a125:Test {id: 125})\n"
        "WITH center\n"
        "UNWIND range(2, 124) AS id_\n"
        "CREATE (a:Test {id: id_})-[:IN {id: id_}]->(center)"
        "-[:OUT {id: id_}]->(a)"
    )
    query = (
        "MATCH p=(\n"
        "  (:Test {id: 1}) ((a:Test)-[:IN]->(:TestCenter)-[:OUT]->(b:Test) "
        "WHERE a.id + 1 = b.id)* (:Test {id: 125})\n"
        ")\n"
        "RETURN p"
    )
    deserialization_template(bencher, query, setup_query=setup_query)
