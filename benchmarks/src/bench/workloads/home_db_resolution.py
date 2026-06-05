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
# * *id*: `home-db-resolution`


from __future__ import annotations

from typing import TYPE_CHECKING

from .util import benchmark


if TYPE_CHECKING:
    import neo4j

    from .util import Bencher


@benchmark
def home_db_resolution(bencher: Bencher) -> None:
    with bencher.ctx.new_driver() as driver:
        bencher.timed_loop(_work, driver)


def _work(driver: neo4j.Driver) -> None:
    driver.execute_query("CALL db.ping()")
