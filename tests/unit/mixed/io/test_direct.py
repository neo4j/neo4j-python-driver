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
from asyncio import (
    Condition as AsyncCondition,
    Event as AsyncEvent,
    Lock as AsyncLock,
)
from threading import (
    Condition,
    Event,
    Lock,
    Thread,
)
import time

import pytest

from neo4j.exceptions import ClientError

from ...async_.io.test_direct import AsyncFakeBoltPool
from ...sync.io.test_direct import FakeBoltPool


class MultiEvent:
    # Adopted from threading.Event

    def __init__(self):
        super().__init__()
        self._cond = Condition(Lock())
        self._counter = 0

    def _reset_internal_locks(self):
        # private!  called by Thread._reset_internal_locks by _after_fork()
        self._cond.__init__(Lock())

    def counter(self):
        return self._counter

    def increment(self):
        with self._cond:
            self._counter += 1
            self._cond.notify_all()

    def decrement(self):
        with self._cond:
            self._counter -= 1
            self._cond.notify_all()

    def clear(self):
        with self._cond:
            self._counter = 0
            self._cond.notify_all()

    def wait(self, value=0, timeout=None):
        with self._cond:
            t_start = time.time()
            while True:
                if value == self._counter:
                    return True
                if timeout is None:
                    time_left = None
                else:
                    time_left = timeout - (time.time() - t_start)
                    if time_left <= 0:
                        return False
                if not self._cond.wait(time_left):
                    return False


class AsyncMultiEvent:
    # Adopted from threading.Event

    def __init__(self):
        super().__init__()
        self._cond = AsyncCondition()
        self._counter = 0

    def _reset_internal_locks(self):
        # private!  called by Thread._reset_internal_locks by _after_fork()
        self._cond.__init__(AsyncLock())

    def counter(self):
        return self._counter

    async def increment(self):
        async with self._cond:
            self._counter += 1
            self._cond.notify_all()

    async def decrement(self):
        async with self._cond:
            self._counter -= 1
            self._cond.notify_all()

    async def clear(self):
        async with self._cond:
            self._counter = 0
            self._cond.notify_all()

    async def wait(self, value=0, timeout=None):
        async with self._cond:
            t_start = time.time()
            while True:
                if value == self._counter:
                    return True
                if timeout is None:
                    time_left = None
                else:
                    time_left = timeout - (time.time() - t_start)
                    if time_left <= 0:
                        return False
                try:
                    await asyncio.wait_for(self._cond.wait(), time_left)
                except asyncio.TimeoutError:
                    return False


class TestMixedConnectionPoolTestCase:
    def assert_pool_size(self, address, expected_active, expected_inactive,
                         pool):
        try:
            connections = pool.connections[address]
        except KeyError:
            assert 0 == expected_active
            assert 0 == expected_inactive
        else:
            assert (expected_active
                    == len([cx for cx in connections if cx.in_use]))
            assert (expected_inactive
                    == len([cx for cx in connections if not cx.in_use]))

    def test_multithread(self):
        def acquire_release_conn(pool_, address_, acquired_counter_,
                                 release_event_):
            conn = pool_._acquire(address_, 3, None)
            acquired_counter_.increment()
            release_event_.wait()
            pool_.release(conn)

        with FakeBoltPool((), max_connection_pool_size=5) as pool:
            address = ("127.0.0.1", 7687)
            acquired_counter = MultiEvent()
            release_event = Event()

            # start 10 threads competing for connections from a pool of size 5
            threads = []
            for i in range(10):
                t = Thread(
                    target=acquire_release_conn,
                    args=(pool, address, acquired_counter, release_event),
                    daemon=True
                )
                t.start()
                threads.append(t)

            if not acquired_counter.wait(5, timeout=1):
                raise RuntimeError("Acquire threads not fast enough")
            # The pool size should be 5, all are in-use
            self.assert_pool_size(address, 5, 0, pool)
            # Now we allow the threads to release connections they obtained
            # from the pool
            release_event.set()

            # wait for all threads to release connections back to pool
            for t in threads:
                t.join(timeout=1)
            # The pool size is still 5, but all are free
            self.assert_pool_size(address, 0, 5, pool)

    @pytest.mark.asyncio
    async def test_multi_coroutine(self):
        async def acquire_release_conn(pool_, address_, acquired_counter_,
                                       release_event_):
            conn = await pool_._acquire(address_, 3, None)
            await acquired_counter_.increment()
            await release_event_.wait()
            await pool_.release(conn)

        async def waiter(pool_, acquired_counter_, release_event_):
            if not await acquired_counter_.wait(5, timeout=1):
                raise RuntimeError("Acquire coroutines not fast enough")
            # The pool size should be 5, all are in-use
            self.assert_pool_size(address, 5, 0, pool_)
            # Now we allow the coroutines to release connections they obtained
            # from the pool
            release_event_.set()

            # wait for all coroutines to release connections back to pool
            if not await acquired_counter_.wait(10, timeout=5):
                raise RuntimeError("Acquire coroutines not fast enough")
            # The pool size is still 5, but all are free
            self.assert_pool_size(address, 0, 5, pool_)

        async with AsyncFakeBoltPool((), max_connection_pool_size=5) as pool:
            address = ("127.0.0.1", 7687)
            acquired_counter = AsyncMultiEvent()
            release_event = AsyncEvent()

            # start 10 coroutines competing for connections from a pool of size
            # 5
            coroutines = [
                acquire_release_conn(
                    pool, address, acquired_counter, release_event
                ) for _ in range(10)
            ]
            await asyncio.gather(
                waiter(pool, acquired_counter, release_event),
                *coroutines
            )
