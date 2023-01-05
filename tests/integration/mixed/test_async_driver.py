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
import sys

import pytest

import neo4j

from ... import env
from ..._async_compat import mark_async_test


# TODO: Python 3.9: when support gets dropped, remove this mark
@pytest.mark.xfail(
    # direct driver is not making use of `asyncio.Lock`.
    sys.version_info < (3, 10) and env.NEO4J_SCHEME == "neo4j",
    reason="asyncio's synchronization primitives can create a new event loop "
           "if instantiated while there is no running event loop. This "
           "changed with Python 3.10.",
    raises=RuntimeError,
    strict=True,
)
def test_can_create_async_driver_outside_of_loop(uri, auth):
    pool_size = 2
    # used to make sure the pool was full at least at some point
    counter = 0
    was_full = False

    async def return_1(tx: neo4j.AsyncManagedTransaction) -> None:
        nonlocal counter, was_full
        res = await tx.run("UNWIND range(1, 10000) AS x RETURN x")

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

    async def session_handler(session: neo4j.AsyncSession) -> None:
        nonlocal was_full
        try:
            async with session:
                await session.execute_read(return_1)
        except BaseException:
            # if we failed, no need to make return_1 stall any longer
            was_full = True
            raise

    async def run(driver_: neo4j.AsyncDriver):
        async with driver_:
            work_loads = (session_handler(driver_.session())
                          for _ in range(pool_size * 4))
            res = await asyncio.gather(*work_loads, return_exceptions=True)
            for r in res:
                if isinstance(r, Exception):
                    raise r

    driver = neo4j.AsyncGraphDatabase.driver(
        uri, auth=auth, max_connection_pool_size=pool_size
    )
    coro = run(driver)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()


@mark_async_test
async def test_cancel_driver_close(uri, auth):
    class Signal:
        queried = False
        released = False

    async def fill_pool(driver_: neo4j.AsyncDriver, n=10):
        signals = [Signal() for _ in range(n)]
        await asyncio.gather(
            *(handle_session(driver_.session(), signals[i]) for i in range(n)),
            handle_signals(signals),
            return_exceptions=True,
        )

    async def handle_signals(signals):
        while any(not signal.queried for signal in signals):
            await asyncio.sleep(0.001)
        await asyncio.sleep(0.1)
        for signal in signals:
            signal.released = True

    async def handle_session(session, signal):
        async with session:
            await session.execute_read(work, signal)

    async def work(tx: neo4j.AsyncManagedTransaction, signal: Signal) -> None:
        res = await tx.run("UNWIND range(1, 10000) AS x RETURN x")
        signal.queried = True
        while not signal.released:
            await asyncio.sleep(0.001)
        await res.consume()

    def connection_count(driver_):
        return sum(len(v) for v in driver_._pool.connections.values())

    driver = neo4j.AsyncGraphDatabase.driver(uri, auth=auth)
    await fill_pool(driver)
    # sanity check, there should be some connections
    assert connection_count(driver) >= 10

    # start the close and give it some event loop iterations to kick off
    fut = asyncio.ensure_future(driver.close())
    await asyncio.sleep(0)

    # cancel in the middle of closing connections
    fut.cancel()
    # give the driver a chance to close connections forcefully
    await asyncio.sleep(0)
    # driver should be marked as closed to not emmit a ResourceWarning later
    assert driver._closed == True
