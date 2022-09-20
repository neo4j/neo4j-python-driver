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

from neo4j._async.io import AsyncBolt
from neo4j._async.io._pool import AsyncIOPool
from neo4j._conf import (
    Config,
    PoolConfig,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j.exceptions import (
    ClientError,
    ServiceUnavailable,
)

from ...._async_compat import mark_async_test


class AsyncFakeSocket:
    def __init__(self, address):
        self.address = address

    def getpeername(self):
        return self.address

    async def sendall(self, data):
        return

    def close(self):
        return


class AsyncQuickConnection:
    def __init__(self, socket):
        self.socket = socket
        self.address = socket.getpeername()
        self.local_port = self.address[1]

    @property
    def is_reset(self):
        return True

    def stale(self):
        return False

    async def reset(self):
        pass

    def close(self):
        self.socket.close()

    def closed(self):
        return False

    def defunct(self):
        return False

    def timedout(self):
        return False


class AsyncFakeBoltPool(AsyncIOPool):

    def __init__(self, address, *, auth=None, **config):
        self.pool_config, self.workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        if config:
            raise ValueError("Unexpected config keys: %s" % ", ".join(config.keys()))

        async def opener(addr, timeout):
            return AsyncQuickConnection(AsyncFakeSocket(addr))

        super().__init__(opener, self.pool_config, self.workspace_config)
        self.address = address

    async def acquire(
        self, access_mode, timeout, database, bookmarks, liveness_check_timeout
    ):
        return await self._acquire(
            self.address, timeout, liveness_check_timeout
        )


@mark_async_test
async def test_bolt_connection_open():
    with pytest.raises(ServiceUnavailable):
        await AsyncBolt.open(("localhost", 9999), auth=("test", "test"))


@mark_async_test
async def test_bolt_connection_open_timeout():
    with pytest.raises(ServiceUnavailable):
        await AsyncBolt.open(("localhost", 9999), auth=("test", "test"),
                             timeout=1)


@mark_async_test
async def test_bolt_connection_ping():
    protocol_version = await AsyncBolt.ping(("localhost", 9999))
    assert protocol_version is None


@mark_async_test
async def test_bolt_connection_ping_timeout():
    protocol_version = await AsyncBolt.ping(("localhost", 9999), timeout=1)
    assert protocol_version is None


@pytest.fixture
async def pool():
    async with AsyncFakeBoltPool(("127.0.0.1", 7687)) as pool:
        yield pool


def assert_pool_size( address, expected_active, expected_inactive, pool):
    try:
        connections = pool.connections[address]
    except KeyError:
        assert 0 == expected_active
        assert 0 == expected_inactive
    else:
        assert expected_active == len([cx for cx in connections if cx.in_use])
        assert (expected_inactive
                == len([cx for cx in connections if not cx.in_use]))


@mark_async_test
async def test_pool_can_acquire(pool):
    address = ("127.0.0.1", 7687)
    connection = await pool._acquire(address, Deadline(3), None)
    assert connection.address == address
    assert_pool_size(address, 1, 0, pool)


@mark_async_test
async def test_pool_can_acquire_twice(pool):
    address = ("127.0.0.1", 7687)
    connection_1 = await pool._acquire(address, Deadline(3), None)
    connection_2 = await pool._acquire(address, Deadline(3), None)
    assert connection_1.address == address
    assert connection_2.address == address
    assert connection_1 is not connection_2
    assert_pool_size(address, 2, 0, pool)


@mark_async_test
async def test_pool_can_acquire_two_addresses(pool):
    address_1 = ("127.0.0.1", 7687)
    address_2 = ("127.0.0.1", 7474)
    connection_1 = await pool._acquire(address_1, Deadline(3), None)
    connection_2 = await pool._acquire(address_2, Deadline(3), None)
    assert connection_1.address == address_1
    assert connection_2.address == address_2
    assert_pool_size(address_1, 1, 0, pool)
    assert_pool_size(address_2, 1, 0, pool)


@mark_async_test
async def test_pool_can_acquire_and_release(pool):
    address = ("127.0.0.1", 7687)
    connection = await pool._acquire(address, Deadline(3), None)
    assert_pool_size(address, 1, 0, pool)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)


@mark_async_test
async def test_pool_releasing_twice(pool):
    address = ("127.0.0.1", 7687)
    connection = await pool._acquire(address, Deadline(3), None)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)


@mark_async_test
async def test_pool_in_use_count(pool):
    address = ("127.0.0.1", 7687)
    assert pool.in_use_connection_count(address) == 0
    connection = await pool._acquire(address, Deadline(3), None)
    assert pool.in_use_connection_count(address) == 1
    await pool.release(connection)
    assert pool.in_use_connection_count(address) == 0


@mark_async_test
async def test_pool_max_conn_pool_size(pool):
    async with AsyncFakeBoltPool((), max_connection_pool_size=1) as pool:
        address = ("127.0.0.1", 7687)
        await pool._acquire(address, Deadline(0), None)
        assert pool.in_use_connection_count(address) == 1
        with pytest.raises(ClientError):
            await pool._acquire(address, Deadline(0), None)
        assert pool.in_use_connection_count(address) == 1


@pytest.mark.parametrize("is_reset", (True, False))
@mark_async_test
async def test_pool_reset_when_released(is_reset, pool, mocker):
    address = ("127.0.0.1", 7687)
    quick_connection_name = AsyncQuickConnection.__name__
    is_reset_mock = mocker.patch(
        f"{__name__}.{quick_connection_name}.is_reset",
        new_callable=mocker.PropertyMock
    )
    reset_mock = mocker.patch(
        f"{__name__}.{quick_connection_name}.reset",
        new_callable=mocker.AsyncMock
    )
    is_reset_mock.return_value = is_reset
    connection = await pool._acquire(address, Deadline(3), None)
    assert isinstance(connection, AsyncQuickConnection)
    assert is_reset_mock.call_count == 0
    assert reset_mock.call_count == 0
    await pool.release(connection)
    assert is_reset_mock.call_count == 1
    assert reset_mock.call_count == int(not is_reset)
