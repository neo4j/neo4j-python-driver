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
# * *id*: `serialize-integer`


from __future__ import annotations

from typing import TYPE_CHECKING

from .util import benchmark
from .util.const import (
    I16_MAX,
    I16_MIN,
    I32_MAX,
    I32_MIN,
    I64_MAX,
    I64_MIN,
)


if TYPE_CHECKING:
    from typing import Any

    import neo4j

    from .util import Bencher


@benchmark
def serialize_integer(bencher: Bencher) -> None:
    with bencher.ctx.new_driver() as driver:
        # 20.5 - 49.5 kB of data
        # (depending on integer representations chosen by driver)
        data_small = [
            0,
            -16,
            127,
            -17,
            -128,
            128,
            I16_MAX,
            I16_MIN,
            I32_MAX,
            I32_MIN,
            I64_MAX,
            I64_MIN,
        ]
        data_large = data_small * 500
        parameters = {"data": data_large}

        bencher.timed_loop(_work, driver, parameters)


def _work(driver: neo4j.Driver, parameters: dict[str, Any]) -> None:
    driver.execute_query(
        "CALL db.ping()",
        parameters_=parameters,
        database_="system",
    )
