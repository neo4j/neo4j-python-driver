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
        assert await lock.acquire()

    async def waiter():
        # make sure blocker has a chance to acquire the lock
        await asyncio.sleep(0)
        assert not await lock.acquire(timeout=0.1)

    assert not lock.locked()
    await asyncio.gather(blocker(), waiter())
    assert lock.locked()  # blocker still owns it!


@pytest.mark.asyncio
async def test_async_r_lock_acquire_timeout_released():
    lock = AsyncRLock()

    async def blocker():
        assert await lock.acquire()
        await asyncio.sleep(0)
        # waiter: lock.acquire(timeout=0.1)
        lock.release()

    async def waiter():
        await asyncio.sleep(0)
        # blocker: lock.acquire()
        assert await lock.acquire(timeout=0.1)
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
    await lock.acquire(timeout=0.1)
    assert lock._count == 3
    await lock.acquire(timeout=0.1)
    assert lock._count == 4
    for _ in range(4):
        lock.release()

    assert not lock.locked()


@pytest.mark.asyncio
async def test_async_r_lock_acquire_non_blocking():
    lock = AsyncRLock()
    assert not lock.locked()
    awaits = 0

    async def blocker():
        nonlocal awaits
        assert awaits == 0
        awaits += 1
        assert await lock.acquire()
        assert awaits == 1
        awaits += 1
        await asyncio.sleep(0)
        assert awaits == 4
        awaits += 1  # not really, but ok...
        lock.release()

    async def waiter_non_blocking():
        nonlocal awaits
        assert awaits == 2
        awaits += 1
        await asyncio.sleep(0)
        assert awaits == 5
        awaits += 1
        assert not await lock.acquire(blocking=False)
        assert awaits == 7
        awaits += 1
        assert not await lock.acquire(blocking=False)
        assert awaits == 9
        awaits += 1
        assert await lock.acquire(blocking=False)

    async def waiter():
        nonlocal awaits
        assert awaits == 3
        awaits += 1
        assert await lock.acquire()
        assert awaits == 6
        awaits += 1
        await asyncio.sleep(0)
        assert awaits == 8
        awaits += 1
        await asyncio.sleep(0)
        assert awaits == 10
        awaits += 1
        lock.release()

    assert not lock.locked()
    await asyncio.gather(blocker(), waiter_non_blocking(), waiter())
    assert lock.locked()  # waiter_non_blocking still owns it!


@pytest.mark.parametrize("waits", range(1, 10))
@pytest.mark.asyncio
async def test_async_r_lock_acquire_cancellation(waits):
    lock = AsyncRLock()

    async def acquire_task():
        while True:
            count = lock._count
            try:
                await lock.acquire(timeout=0.1)
                assert lock._count == count + 1
            except asyncio.CancelledError:
                assert lock._count == count
                raise
            try:
                # we're also ok with a deferred cancellation
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            assert count < 50  # safety guard, we shouldn't ever get there!

    fut = asyncio.ensure_future(acquire_task())
    for _ in range(waits):
        await asyncio.sleep(0)
    fut.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fut
