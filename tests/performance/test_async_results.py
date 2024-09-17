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


import pytest


def work(async_driver, *units_of_work):
    async def runner():
        async with async_driver.session() as session:
            for unit_of_work in units_of_work:
                await session.execute_read(unit_of_work)

    return runner


def unit_of_work_generator(record_count, record_width, value):
    async def transaction_function(tx):
        s = "UNWIND range(1, $record_count) AS _ RETURN {}".format(
            ", ".join(f"$x AS x{i}" for i in range(record_width))
        )
        p = {"record_count": record_count, "x": value}
        async for record in await tx.run(s, p):
            assert all(x == value for x in record.values())

    return transaction_function


@pytest.mark.parametrize("record_count", [1, 1000])
@pytest.mark.parametrize("record_width", [1, 10])
@pytest.mark.parametrize("value", [1, "hello, world"])
def test_async_1x1(
    async_driver, aio_benchmark, record_count, record_width, value
):
    aio_benchmark(
        work(
            async_driver,
            unit_of_work_generator(record_count, record_width, value),
        )
    )
