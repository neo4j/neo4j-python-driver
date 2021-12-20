# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio

import pytest

from neo4j._async_compat.concurrency import AsyncRLock


@pytest.mark.asyncio
async def test_async_r_lock():
    counter = 1
    lock = AsyncRLock()

    async def worker():
        nonlocal counter
        async with lock:
            counter_ = counter
            counter += 1
            await asyncio.sleep(0)
            # assert no one else touched the counter
            assert counter == counter_ + 1

    assert not lock.locked()
    await asyncio.gather(worker(), worker(), worker())
    assert not lock.locked()


@pytest.mark.asyncio
async def test_async_r_lock_is_reentrant():
    lock = AsyncRLock()

    async def worker():
        async with lock:
            assert lock._count == 1
            async with lock:
                assert lock._count == 2
                assert lock.locked()

    assert not lock.locked()
    await asyncio.gather(worker(), worker(), worker())
    assert not lock.locked()


@pytest.mark.asyncio
async def test_async_r_lock_acquire_timeout_blocked():
    lock = AsyncRLock()

    async def blocker():
        await lock.acquire()

    async def waiter():
        # make sure blocker has a chance to acquire the lock
        await asyncio.sleep(0)
        await lock.acquire(0.1)

    assert not lock.locked()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.gather(blocker(), waiter())


@pytest.mark.asyncio
async def test_async_r_lock_acquire_timeout_released():
    lock = AsyncRLock()

    async def blocker():
        await lock.acquire()
        await asyncio.sleep(0)
        # waiter: lock.acquire(0.1)
        lock.release()

    async def waiter():
        await asyncio.sleep(0)
        # blocker: lock.acquire()
        await lock.acquire(0.1)
        # blocker: lock.release()

    assert not lock.locked()
    await asyncio.gather(blocker(), waiter())
    assert lock.locked()  # waiter still owns it!


@pytest.mark.asyncio
async def test_async_r_lock_acquire_timeout_reentrant():
    lock = AsyncRLock()
    assert not lock.locked()

    await lock.acquire()
    assert lock._count == 1
    await lock.acquire()
    assert lock._count == 2
    await lock.acquire(0.1)
    assert lock._count == 3
    await lock.acquire(0.1)
    assert lock._count == 4
    for _ in range(4):
        lock.release()

    assert not lock.locked()
