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


import pytest

from neo4j import GraphDatabase


def work(driver, *units_of_work):
    def runner():
        with driver.session() as session:
            for unit_of_work in units_of_work:
                session.read_transaction(unit_of_work)
    return runner


def unit_of_work(record_count, record_width, value):
    def transaction_function(tx):
        s = "UNWIND range(1, $record_count) AS _ RETURN {}".format(
            ", ".join("$x AS x{}".format(i) for i in range(record_width)))
        p = {"record_count": record_count, "x": value}
        for record in tx.run(s, p):
            assert all(x == value for x in record.values())

    return transaction_function


@pytest.mark.parametrize("record_count", [1, 1000])
@pytest.mark.parametrize("record_width", [1, 10])
@pytest.mark.parametrize("value", [1, u'hello, world'])
def test_1x1(driver, benchmark, record_count, record_width, value):
    benchmark(work(driver, unit_of_work(record_count, record_width, value)))
