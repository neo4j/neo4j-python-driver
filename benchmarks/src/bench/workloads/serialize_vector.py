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
# * *id*: `serialize-vector`


from __future__ import annotations

from typing import TYPE_CHECKING

from ._data_vector import DATA
from .util import benchmark


if TYPE_CHECKING:
    from typing import Any

    import neo4j

    from .util import Bencher


@benchmark
def serialize_vector(bencher: Bencher) -> None:
    with bencher.ctx.new_driver() as driver:
        parameters = {"data": DATA}

        bencher.timed_loop(_work, driver, parameters)


def _work(driver: neo4j.Driver, parameters: dict[str, Any]) -> None:
    driver.execute_query(
        "CALL db.ping()",
        parameters_=parameters,
        database_="system",
    )
