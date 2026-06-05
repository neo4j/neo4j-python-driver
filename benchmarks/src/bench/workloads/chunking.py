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
# * *id*: `chunking`


from __future__ import annotations

from typing import (
    Any,
    TYPE_CHECKING,
)

from .util import benchmark


if TYPE_CHECKING:
    from neo4j import Driver

    from .util import Bencher


@benchmark
def chunking(bencher: Bencher) -> None:
    with bencher.ctx.new_driver() as driver:
        data = b"\x00" * 70_000_000
        parameters = {"data": data}

        bencher.timed_loop(_work, driver, parameters, bencher.ctx.db_name)


def _work(driver: Driver, parameters: dict[str, Any], db: str) -> None:
    driver.execute_query("RETURN $data", parameters, database_=db)
