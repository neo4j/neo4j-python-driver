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


import asyncio

import neo4j


def test_can_create_async_driver_outside_of_loop(uri, auth, event_loop):
    pool_size = 2
    # used to make sure the pool was full at least at some point
    counter = 0
    was_full = False

    async def return_1(tx):
        nonlocal counter, was_full
        res = await tx.run("RETURN 1")

        counter += 1
        while not was_full and counter < pool_size:
            await asyncio.sleep(0.001)
        if not was_full:
            # a little extra time to make sure a connection too many was
            # tried to be acquired from the pool
            was_full = True
            await asyncio.sleep(0.5)

        await res.consume()
        counter -= 1

    async def run(driver: neo4j.AsyncDriver):
        async with driver:
            sessions = []
            try:
                for i in range(pool_size * 4):
                    sessions.append(driver.session())
                work_loads = (session.execute_read(return_1)
                              for session in sessions)
                await asyncio.gather(*work_loads)
            finally:
                cancelled = None
                for session in sessions:
                    if not cancelled:
                        try:
                            await session.close()
                        except asyncio.CancelledError as e:
                            session.cancel()
                            cancelled = e
                    else:
                        session.cancel()
                if cancelled:
                    raise cancelled


    coro = run(
        neo4j.AsyncGraphDatabase.driver(
            uri, auth=auth, max_connection_pool_size=pool_size
        )
    )
    event_loop.run_until_complete(coro)
