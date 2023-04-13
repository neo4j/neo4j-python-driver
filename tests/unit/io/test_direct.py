#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from unittest import (
    mock,
    TestCase,
)
import pytest
from threading import (
    Condition,
    Event,
    Lock,
    Thread,
)
import time

from neo4j import (
    Config,
    PoolConfig,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j.io import (
    Bolt,
    IOPool
)
from neo4j.exceptions import (
    ClientError,
    ServiceUnavailable,
)


class FakeSocket:
    def __init__(self, address):
        self.address = address

    def getpeername(self):
        return self.address

    def sendall(self, data):
        return

    def close(self):
        return


class QuickConnection:

    def __init__(self, socket):
        self.socket = socket
        self.address = socket.getpeername()

    @property
    def is_reset(self):
        return True

    def stale(self):
        return False

    def reset(self):
        pass

    def close(self):
        self.socket.close()

    def closed(self):
        return False

    def defunct(self):
        return False

    def timedout(self):
        return False


class FakeBoltPool(IOPool):

    def __init__(self, address, *, auth=None, **config):
        self.pool_config, self.workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        if config:
            raise ValueError("Unexpected config keys: %s" % ", ".join(config.keys()))

        def opener(addr, timeout):
            return QuickConnection(FakeSocket(addr))

        super().__init__(opener, self.pool_config, self.workspace_config)
        self.address = address

    def acquire(self, access_mode, timeout, acquisition_timeout, database,
                bookmarks):
        return self._acquire(self.address, timeout)


class BoltTestCase(TestCase):

    def test_open(self):
        with pytest.raises(ServiceUnavailable):
            Bolt.open(("localhost", 9999), auth=("test", "test"))

    def test_open_timeout(self):
        with pytest.raises(ServiceUnavailable):
            Bolt.open(("localhost", 9999), auth=("test", "test"),
                      deadline=Deadline(1))

    def test_ping(self):
        protocol_version = Bolt.ping(("localhost", 9999))
        assert protocol_version is None

    def test_ping_timeout(self):
        protocol_version = Bolt.ping(("localhost", 9999), deadline=Deadline(1))
        assert protocol_version is None


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


class ConnectionPoolTestCase(TestCase):

    def setUp(self):
        self.pool = FakeBoltPool(("127.0.0.1", 7687))

    def tearDown(self):
        self.pool.close()

    def assert_pool_size(self, address, expected_active, expected_inactive, pool=None):
        if pool is None:
            pool = self.pool
        try:
            connections = pool.connections[address]
        except KeyError:
            self.assertEqual(0, expected_active)
            self.assertEqual(0, expected_inactive)
        else:
            self.assertEqual(expected_active, len([cx for cx in connections if cx.in_use]))
            self.assertEqual(expected_inactive, len([cx for cx in connections if not cx.in_use]))

    def test_can_acquire(self):
        address = ("127.0.0.1", 7687)
        connection = self.pool._acquire(address, Deadline(3))
        assert connection.address == address
        self.assert_pool_size(address, 1, 0)

    def test_can_acquire_twice(self):
        address = ("127.0.0.1", 7687)
        connection_1 = self.pool._acquire(address, Deadline(3))
        connection_2 = self.pool._acquire(address, Deadline(3))
        assert connection_1.address == address
        assert connection_2.address == address
        assert connection_1 is not connection_2
        self.assert_pool_size(address, 2, 0)

    def test_can_acquire_two_addresses(self):
        address_1 = ("127.0.0.1", 7687)
        address_2 = ("127.0.0.1", 7474)
        connection_1 = self.pool._acquire(address_1, Deadline(3))
        connection_2 = self.pool._acquire(address_2, Deadline(3))
        assert connection_1.address == address_1
        assert connection_2.address == address_2
        self.assert_pool_size(address_1, 1, 0)
        self.assert_pool_size(address_2, 1, 0)

    def test_can_acquire_and_release(self):
        address = ("127.0.0.1", 7687)
        connection = self.pool._acquire(address, Deadline(3))
        self.assert_pool_size(address, 1, 0)
        self.pool.release(connection)
        self.assert_pool_size(address, 0, 1)

    def test_releasing_twice(self):
        address = ("127.0.0.1", 7687)
        connection = self.pool._acquire(address, Deadline(3))
        self.pool.release(connection)
        self.assert_pool_size(address, 0, 1)
        self.pool.release(connection)
        self.assert_pool_size(address, 0, 1)

    def test_in_use_count(self):
        address = ("127.0.0.1", 7687)
        self.assertEqual(self.pool.in_use_connection_count(address), 0)
        connection = self.pool._acquire(address, Deadline(3))
        self.assertEqual(self.pool.in_use_connection_count(address), 1)
        self.pool.release(connection)
        self.assertEqual(self.pool.in_use_connection_count(address), 0)

    def test_max_conn_pool_size(self):
        with FakeBoltPool((), max_connection_pool_size=1) as pool:
            address = ("127.0.0.1", 7687)
            pool._acquire(address, Deadline(0))
            self.assertEqual(pool.in_use_connection_count(address), 1)
            with self.assertRaises(ClientError):
                pool._acquire(address, Deadline(0))
            self.assertEqual(pool.in_use_connection_count(address), 1)

    def test_multithread(self):
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

            if not acquired_counter.wait(5, 1):
                raise RuntimeError("Acquire threads not fast enough")
            # The pool size should be 5, all are in-use
            self.assert_pool_size(address, 5, 0, pool)
            # Now we allow thread to release connections they obtained from pool
            release_event.set()

            # wait for all threads to release connections back to pool
            for t in threads:
                t.join(1)
            # The pool size is still 5, but all are free
            self.assert_pool_size(address, 0, 5, pool)

    def test_reset_when_released(self):
        def test(is_reset):
            with mock.patch(__name__ + ".QuickConnection.is_reset",
                            new_callable=mock.PropertyMock) as is_reset_mock:
                with mock.patch(__name__ + ".QuickConnection.reset",
                                new_callable=mock.MagicMock) as reset_mock:
                    is_reset_mock.return_value = is_reset
                    connection = self.pool._acquire(
                        address, Deadline(3)
                    )
                    self.assertIsInstance(connection, QuickConnection)
                    self.assertEqual(is_reset_mock.call_count, 0)
                    self.assertEqual(reset_mock.call_count, 0)
                    self.pool.release(connection)
                    self.assertEqual(is_reset_mock.call_count, 1)
                    self.assertEqual(reset_mock.call_count, int(not is_reset))

        address = ("127.0.0.1", 7687)
        for is_reset in (True, False):
            with self.subTest():
                test(is_reset)


def acquire_release_conn(pool, address, acquired_counter, release_event):
    conn = pool._acquire(address, Deadline(3))
    acquired_counter.increment()
    release_event.wait()
    pool.release(conn)
