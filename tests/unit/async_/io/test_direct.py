# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import neo4j
from neo4j._async.config import AsyncPoolConfig
from neo4j._async.io import AsyncBolt
from neo4j._async.io._pool import AsyncIOPool
from neo4j._conf import (
    Config,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j.auth_management import AsyncAuthManagers
from neo4j.exceptions import (
    ClientError,
    ServiceUnavailable,
)

from ...._async_compat import mark_async_test


class AsyncFakeBoltPool(AsyncIOPool):
    is_direct_pool = False

    def __init__(self, connection_gen, address, *, auth=None, **config):
        self.buffered_connection_mocks = []
        config["auth"] = static_auth(None)
        self.pool_config, self.workspace_config = Config.consume_chain(
            config, AsyncPoolConfig, WorkspaceConfig
        )
        if config:
            raise ValueError("Unexpected config keys: %s" % ", ".join(config.keys()))

        async def opener(addr, auth, timeout):
            if self.buffered_connection_mocks:
                mock = self.buffered_connection_mocks.pop()
            else:
                mock = connection_gen()
                mock.address = addr
            return mock

        super().__init__(opener, self.pool_config, self.workspace_config)
        self.address = address

    async def acquire(
        self, access_mode, timeout, database, bookmarks, auth,
        liveness_check_timeout
    ):
        return await self._acquire(
            self.address, auth, timeout, liveness_check_timeout
        )


def static_auth(auth):
    return AsyncAuthManagers.static(auth)


@pytest.fixture
def auth_manager():
    static_auth(("test", "test"))


@mark_async_test
async def test_bolt_connection_open(auth_manager):
    with pytest.raises(ServiceUnavailable):
        await AsyncBolt.open(("localhost", 9999), auth_manager=auth_manager)


@mark_async_test
async def test_bolt_connection_open_timeout(auth_manager):
    with pytest.raises(ServiceUnavailable):
        await AsyncBolt.open(
            ("localhost", 9999), auth_manager=auth_manager,
            deadline=Deadline(1)
        )


@mark_async_test
async def test_bolt_connection_ping():
    protocol_version = await AsyncBolt.ping(("localhost", 9999))
    assert protocol_version is None


@mark_async_test
async def test_bolt_connection_ping_timeout():
    protocol_version = await AsyncBolt.ping(
        ("localhost", 9999), deadline=Deadline(1)
    )
    assert protocol_version is None


@pytest.fixture
async def pool(async_fake_connection_generator):
    async with AsyncFakeBoltPool(
        async_fake_connection_generator, ("127.0.0.1", 7687)
    ) as pool:
        yield pool


def assert_pool_size(address, expected_active, expected_inactive, pool):
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
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = await pool._acquire(address, None, Deadline(3), None)
    assert connection.address == address
    assert_pool_size(address, 1, 0, pool)


@mark_async_test
async def test_pool_can_acquire_twice(pool):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection_1 = await pool._acquire(address, None, Deadline(3), None)
    connection_2 = await pool._acquire(address, None, Deadline(3), None)
    assert connection_1.address == address
    assert connection_2.address == address
    assert connection_1 is not connection_2
    assert_pool_size(address, 2, 0, pool)


@mark_async_test
async def test_pool_can_acquire_two_addresses(pool):
    address_1 = neo4j.Address(("127.0.0.1", 7687))
    address_2 = neo4j.Address(("127.0.0.1", 7474))
    connection_1 = await pool._acquire(address_1, None, Deadline(3), None)
    connection_2 = await pool._acquire(address_2, None, Deadline(3), None)
    assert connection_1.address == address_1
    assert connection_2.address == address_2
    assert_pool_size(address_1, 1, 0, pool)
    assert_pool_size(address_2, 1, 0, pool)


@mark_async_test
async def test_pool_can_acquire_and_release(pool):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = await pool._acquire(address, None, Deadline(3), None)
    assert_pool_size(address, 1, 0, pool)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)


@mark_async_test
async def test_pool_releasing_twice(pool):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = await pool._acquire(address, None, Deadline(3), None)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)
    await pool.release(connection)
    assert_pool_size(address, 0, 1, pool)


@mark_async_test
async def test_pool_in_use_count(pool):
    address = neo4j.Address(("127.0.0.1", 7687))
    assert pool.in_use_connection_count(address) == 0
    connection = await pool._acquire(address, None, Deadline(3), None)
    assert pool.in_use_connection_count(address) == 1
    await pool.release(connection)
    assert pool.in_use_connection_count(address) == 0


@mark_async_test
async def test_pool_max_conn_pool_size(async_fake_connection_generator):
    async with AsyncFakeBoltPool(
        async_fake_connection_generator, (), max_connection_pool_size=1
    ) as pool:
        address = neo4j.Address(("127.0.0.1", 7687))
        await pool._acquire(address, None, Deadline(0), None)
        assert pool.in_use_connection_count(address) == 1
        with pytest.raises(ClientError):
            await pool._acquire(address, None, Deadline(0), None)
        assert pool.in_use_connection_count(address) == 1


@pytest.mark.parametrize("is_reset", (True, False))
@mark_async_test
async def test_pool_reset_when_released(
    is_reset, pool, async_fake_connection_generator
):
    connection_mock = async_fake_connection_generator()
    pool.buffered_connection_mocks.append(connection_mock)
    address = neo4j.Address(("127.0.0.1", 7687))
    is_reset_mock = connection_mock.is_reset_mock
    reset_mock = connection_mock.reset
    is_reset_mock.return_value = is_reset
    connection = await pool._acquire(address, None, Deadline(3), None)
    assert is_reset_mock.call_count == 0
    assert reset_mock.call_count == 0
    await pool.release(connection)
    assert is_reset_mock.call_count == 1
    assert reset_mock.call_count == int(not is_reset)


@pytest.mark.parametrize("config_timeout", (None, 0, 0.2, 1234))
@pytest.mark.parametrize("acquire_timeout", (None, 0, 0.2, 1234))
@mark_async_test
async def test_liveness_check(
    config_timeout, acquire_timeout, async_fake_connection_generator
):
    effective_timeout = config_timeout
    if acquire_timeout is not None:
        effective_timeout = acquire_timeout
    async with AsyncFakeBoltPool(
        async_fake_connection_generator, ("127.0.0.1", 7687),
        liveness_check_timeout=config_timeout,
    ) as pool:
        address = neo4j.Address(("127.0.0.1", 7687))
        # pre-populate pool
        cx1 = await pool._acquire(address, None, Deadline(3), None)
        await pool.release(cx1)
        cx1.reset.assert_not_called()
        cx1.is_idle_for.assert_not_called()

        # simulate just before timeout
        cx1.is_idle_for.return_value = False

        cx2 = await pool._acquire(address, None, Deadline(3), acquire_timeout)
        assert cx2 is cx1
        if effective_timeout is not None:
            cx1.is_idle_for.assert_called_once_with(effective_timeout)
        else:
            cx1.is_idle_for.assert_not_called()
        await pool.release(cx1)
        cx1.reset.assert_not_called()

        # simulate after timeout
        cx1.is_idle_for.return_value = True
        cx1.is_idle_for.reset_mock()

        cx2 = await pool._acquire(address, None, Deadline(3), acquire_timeout)
        assert cx2 is cx1
        if effective_timeout is not None:
            cx1.is_idle_for.assert_called_once_with(effective_timeout)
            cx1.reset.assert_awaited_once()
        else:
            cx1.is_idle_for.assert_not_called()
            cx1.reset.assert_not_called()
        cx1.reset.reset_mock()
        await pool.release(cx1)
        cx1.reset.assert_not_called()
