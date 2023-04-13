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
from asyncio import Event as AsyncEvent
from threading import (
    Event,
    Lock,
    Thread,
)

import pytest

from neo4j._deadline import Deadline

from ...async_.io.test_direct import AsyncFakeBoltPool
from ...sync.io.test_direct import FakeBoltPool
from ._common import (
    AsyncMultiEvent,
    MultiEvent,
)


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

    @pytest.mark.parametrize("pre_populated", (0, 3, 5))
    def test_multithread(self, pre_populated):
        connections_lock = Lock()
        connections = []
        pre_populated_connections = []

        def acquire_release_conn(pool_, address_, acquired_counter_,
                                 release_event_):
            nonlocal connections, connections_lock
            conn_ = pool_._acquire(address_, None, Deadline(3), None)
            with connections_lock:
                if connections is not None:
                    connections.append(conn_)
            acquired_counter_.increment()
            release_event_.wait()
            pool_.release(conn_)

        with FakeBoltPool((), max_connection_pool_size=5) as pool:
            address = ("127.0.0.1", 7687)
            acquired_counter = MultiEvent()
            release_event = Event()

            # pre-populate the pool with connections
            for _ in range(pre_populated):
                conn = pool._acquire(address, None, Deadline(3), None)
                pre_populated_connections.append(conn)
            for conn in pre_populated_connections:
                pool.release(conn)
            assert len(set(pre_populated_connections)) == pre_populated
            self.assert_pool_size(address, 0, pre_populated, pool)

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
            with connections_lock:
                assert set(pre_populated_connections).issubset(
                    set(connections)
                )
                connections = pre_populated_connections = None
            # Now we allow the threads to release connections they obtained
            # from the pool
            release_event.set()

            # wait for all threads to release connections back to pool
            for t in threads:
                t.join(timeout=1)
            # The pool size is still 5, but all are free
            self.assert_pool_size(address, 0, 5, pool)

    @pytest.mark.parametrize("pre_populated", (0, 3, 5))
    @pytest.mark.asyncio
    async def test_multi_coroutine(self, pre_populated):
        connections = []
        pre_populated_connections = []

        async def acquire_release_conn(pool_, address_, acquired_counter_,
                                       release_event_):
            nonlocal connections
            conn_ = await pool_._acquire(address_, None, Deadline(3), None)
            if connections is not None:
                connections.append(conn_)
            await acquired_counter_.increment()
            await release_event_.wait()
            await pool_.release(conn_)

        async def waiter(pool_, acquired_counter_, release_event_):
            nonlocal pre_populated_connections, connections

            if not await acquired_counter_.wait(5, timeout=1):
                raise RuntimeError("Acquire coroutines not fast enough")
            # The pool size should be 5, all are in-use
            self.assert_pool_size(address, 5, 0, pool_)
            assert set(pre_populated_connections).issubset(set(connections))
            connections = pre_populated_connections = None

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

            # pre-populate the pool with connections
            for _ in range(pre_populated):
                conn = await pool._acquire(address, None, Deadline(3), None)
                pre_populated_connections.append(conn)
            for conn in pre_populated_connections:
                await pool.release(conn)
            assert len(set(pre_populated_connections)) == pre_populated
            self.assert_pool_size(address, 0, pre_populated, pool)

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
