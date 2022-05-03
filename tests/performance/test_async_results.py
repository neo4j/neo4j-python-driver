# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


import asyncio
from itertools import product

from pytest import mark

from neo4j import AsyncGraphDatabase

from .tools import RemoteGraphDatabaseServer


class AsyncReadWorkload(object):

    server = None
    driver = None
    loop = None

    @classmethod
    def setup_class(cls):
        cls.server = server = RemoteGraphDatabaseServer()
        server.start()
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        cls.driver = AsyncGraphDatabase.driver(server.server_uri,
                                               auth=server.auth_token,
                                               encrypted=server.encrypted)

    @classmethod
    def teardown_class(cls):
        try:
            cls.loop.run_until_complete(cls.driver.close())
            cls.server.stop()
        finally:
            cls.loop.stop()
            asyncio.set_event_loop(None)

    def work(self, *units_of_work):
        async def runner():
            async with self.driver.session() as session:
                for unit_of_work in units_of_work:
                    await session.read_transaction(unit_of_work)

        def sync_runner():
            self.loop.run_until_complete(runner())

        return sync_runner


class TestAsyncReadWorkload(AsyncReadWorkload):

    @staticmethod
    def uow(record_count, record_width, value):

        async def _(tx):
            s = "UNWIND range(1, $record_count) AS _ RETURN {}".format(
                ", ".join("$x AS x{}".format(i) for i in range(record_width)))
            p = {"record_count": record_count, "x": value}
            async for record in await tx.run(s, p):
                assert all(x == value for x in record.values())

        return _

    @mark.parametrize("record_count,record_width,value", product(
        [1, 1000],  # record count
        [1, 10],    # record width
        [1, u'hello, world'],        # value
    ))
    def test_1x1(self, benchmark, record_count, record_width, value):
        benchmark(self.work(self.uow(record_count, record_width, value)))
