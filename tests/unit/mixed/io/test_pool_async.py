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
from asyncio import Condition

from ...async_.fixtures import *  # fixtures necessary for pytest
from ...async_.io.test_neo4j_pool import *
from ._common import AsyncMultiEvent


@pytest.mark.asyncio
async def test_force_new_auth_blocks(opener):
    count = 0
    done = False
    condition = Condition()
    event = AsyncMultiEvent()

    async def auth_provider():
        nonlocal done, count
        count += 1
        if count == 1:
            return "user1", "pass1"
        await event.increment()
        async with condition:
            await event.wait(2)
            await condition.wait()
        await asyncio.sleep(0.1)  # block
        done = True
        return "user", "password"

    config = PoolConfig()
    config.auth = auth_provider
    pool = AsyncNeo4jPool(
        opener, config, WorkspaceConfig(), ROUTER1_ADDRESS
    )

    assert count == 0
    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx)
    assert count == 1

    async def task1():
        assert count == 1
        await pool.force_new_auth()
        assert count == 2

    async def task2():
        await event.increment()
        await event.wait(2)
        async with condition:
            condition.notify()
        cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
        assert done  # assert waited for blocking auth provider
        await pool.release(cx)

    await asyncio.gather(task1(), task2())
