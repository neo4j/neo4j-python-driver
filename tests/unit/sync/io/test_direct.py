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
from neo4j._addressing import ResolvedAddress
from neo4j._conf import (
    Config,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j._sync.config import PoolConfig
from neo4j._sync.io import (
    AcquisitionDatabase,
    Bolt,
)
from neo4j._sync.io._pool import DirectBoltPool
from neo4j.api import READ_ACCESS
from neo4j.auth_management import AuthManagers
from neo4j.exceptions import (
    ConnectionAcquisitionTimeoutError,
    ServiceUnavailable,
)

from ...._async_compat import (
    fixture,
    mark_sync_test,
)


TEST_DB1 = AcquisitionDatabase("test_db1")
ADDRESS1 = ResolvedAddress(("127.0.0.1", 7687), host_name="localhost")
ADDRESS2 = ResolvedAddress(("127.0.0.1", 7474), host_name="host")


class FakeBoltPool(DirectBoltPool):
    is_direct_pool = False

    def __init__(self, connection_gen, address, *, auth=None, **config):
        self.buffered_connection_mocks = []
        config["auth"] = static_auth(None)
        self.pool_config, self.workspace_config = Config.consume_chain(
            config, PoolConfig, WorkspaceConfig
        )
        if config:
            raise ValueError(
                f"Unexpected config keys: {', '.join(config.keys())}"
            )

        def opener(addr, auth, timeout):
            if self.buffered_connection_mocks:
                mock = self.buffered_connection_mocks.pop()
            else:
                mock = connection_gen()
                mock.address = addr
            return mock

        super().__init__(
            opener, self.pool_config, self.workspace_config, address
        )

    @classmethod
    def open(cls, *args, **kwargs):
        raise NotImplementedError


def static_auth(auth):
    return AuthManagers.static(auth)


@pytest.fixture
def auth_manager():
    static_auth(("test", "test"))


@mark_sync_test
def test_bolt_connection_open(auth_manager):
    with pytest.raises(ServiceUnavailable):
        Bolt.open(
            neo4j.Address(("localhost", 9999)),
            auth_manager=auth_manager,
            deadline=Deadline(None),
            routing_context=None,
            pool_config=None,
        )


@mark_sync_test
def test_bolt_connection_open_timeout(auth_manager):
    with pytest.raises(ServiceUnavailable):
        Bolt.open(
            neo4j.Address(("localhost", 999)),
            auth_manager=auth_manager,
            deadline=Deadline(1),
            routing_context=None,
            pool_config=None,
        )


@mark_sync_test
def test_bolt_connection_ping():
    protocol_version = Bolt.ping(("localhost", 9999))
    assert protocol_version is None


@mark_sync_test
def test_bolt_connection_ping_timeout():
    protocol_version = Bolt.ping(
        ("localhost", 9999), deadline=Deadline(1)
    )
    assert protocol_version is None


@fixture
def pool(fake_connection_generator):
    with FakeBoltPool(
        fake_connection_generator, ADDRESS1
    ) as pool:
        yield pool


def assert_pool_size(address, expected_active, expected_inactive, pool):
    try:
        connections = pool.connections[address]
    except KeyError:
        assert expected_active == 0
        assert expected_inactive == 0
    else:
        assert expected_active == len([cx for cx in connections if cx.in_use])
        assert expected_inactive == len(
            [cx for cx in connections if not cx.in_use]
        )


@mark_sync_test
def test_pool_can_acquire(pool):
    connection = pool._acquire(ADDRESS1, None, Deadline(3), None)
    assert connection.address == ADDRESS1
    assert_pool_size(ADDRESS1, 1, 0, pool)


@mark_sync_test
def test_pool_can_acquire_twice(pool):
    connection_1 = pool._acquire(ADDRESS1, None, Deadline(3), None)
    connection_2 = pool._acquire(ADDRESS1, None, Deadline(3), None)
    assert connection_1.address == ADDRESS1
    assert connection_2.address == ADDRESS1
    assert connection_1 is not connection_2
    assert_pool_size(ADDRESS1, 2, 0, pool)


@mark_sync_test
def test_pool_can_acquire_two_addresses(pool):
    connection_1 = pool._acquire(ADDRESS1, None, Deadline(3), None)
    connection_2 = pool._acquire(ADDRESS2, None, Deadline(3), None)
    assert connection_1.address == ADDRESS1
    assert connection_2.address == ADDRESS2
    assert_pool_size(ADDRESS1, 1, 0, pool)
    assert_pool_size(ADDRESS2, 1, 0, pool)


@mark_sync_test
def test_pool_can_acquire_and_release(pool):
    connection = pool._acquire(ADDRESS1, None, Deadline(3), None)
    assert_pool_size(ADDRESS1, 1, 0, pool)
    pool.release(connection)
    assert_pool_size(ADDRESS1, 0, 1, pool)


@mark_sync_test
def test_pool_releasing_twice(pool):
    connection = pool._acquire(ADDRESS1, None, Deadline(3), None)
    pool.release(connection)
    assert_pool_size(ADDRESS1, 0, 1, pool)
    pool.release(connection)
    assert_pool_size(ADDRESS1, 0, 1, pool)


@mark_sync_test
def test_pool_in_use_count(pool):
    assert pool.in_use_connection_count(ADDRESS1) == 0
    connection = pool._acquire(ADDRESS1, None, Deadline(3), None)
    assert pool.in_use_connection_count(ADDRESS1) == 1
    pool.release(connection)
    assert pool.in_use_connection_count(ADDRESS1) == 0


@mark_sync_test
def test_pool_max_conn_pool_size(fake_connection_generator):
    with FakeBoltPool(
        fake_connection_generator, (), max_connection_pool_size=1
    ) as pool:
        pool._acquire(ADDRESS1, None, Deadline(float("inf")), None)
        assert pool.in_use_connection_count(ADDRESS1) == 1
        with pytest.raises(ConnectionAcquisitionTimeoutError):
            pool._acquire(ADDRESS1, None, Deadline(0), None)
        assert pool.in_use_connection_count(ADDRESS1) == 1


@pytest.mark.parametrize("is_reset", (True, False))
@mark_sync_test
def test_pool_reset_when_released(
    is_reset, pool, fake_connection_generator
):
    connection_mock = fake_connection_generator()
    pool.buffered_connection_mocks.append(connection_mock)
    is_reset_mock = connection_mock.is_reset_mock
    reset_mock = connection_mock.reset
    is_reset_mock.return_value = is_reset
    connection = pool._acquire(ADDRESS1, None, Deadline(3), None)
    assert is_reset_mock.call_count == 0
    assert reset_mock.call_count == 0
    pool.release(connection)
    assert is_reset_mock.call_count == 1
    assert reset_mock.call_count == int(not is_reset)


@pytest.mark.parametrize("config_timeout", (None, 0, 0.2, 1234))
@pytest.mark.parametrize("acquire_timeout", (None, 0, 0.2, 1234))
@mark_sync_test
def test_liveness_check(
    config_timeout, acquire_timeout, fake_connection_generator
):
    effective_timeout = config_timeout
    if acquire_timeout is not None:
        effective_timeout = acquire_timeout
    with FakeBoltPool(
        fake_connection_generator,
        ADDRESS1,
        liveness_check_timeout=config_timeout,
    ) as pool:
        # pre-populate pool
        cx1 = pool._acquire(ADDRESS1, None, Deadline(3), None)
        pool.release(cx1)
        cx1.reset.assert_not_called()
        cx1.is_idle_for.assert_not_called()

        # simulate just before timeout
        cx1.is_idle_for.return_value = False

        cx2 = pool._acquire(ADDRESS1, None, Deadline(3), acquire_timeout)
        assert cx2 is cx1
        if effective_timeout is not None:
            cx1.is_idle_for.assert_called_once_with(effective_timeout)
        else:
            cx1.is_idle_for.assert_not_called()
        pool.release(cx1)
        cx1.liveness_check.assert_not_called()

        # simulate after timeout
        cx1.is_idle_for.return_value = True
        cx1.is_idle_for.reset_mock()

        cx2 = pool._acquire(ADDRESS1, None, Deadline(3), acquire_timeout)
        assert cx2 is cx1
        if effective_timeout is not None:
            cx1.is_idle_for.assert_called_once_with(effective_timeout)
            cx1.liveness_check.assert_called_once()
        else:
            cx1.is_idle_for.assert_not_called()
            cx1.liveness_check.assert_not_called()
        cx1.liveness_check.reset_mock()
        pool.release(cx1)
        cx1.liveness_check.assert_not_called()


@pytest.mark.parametrize("unprepared", (True, False, None))
@mark_sync_test
def test_reauth(fake_connection_generator, unprepared):
    with FakeBoltPool(
        fake_connection_generator,
        ADDRESS1,
    ) as pool:
        # pre-populate pool
        cx = pool._acquire(ADDRESS1, None, Deadline(3), None)
        pool.release(cx)
        cx.reset_mock()

        kwargs = {}
        if unprepared is not None:
            kwargs["unprepared"] = unprepared
        cx = pool._acquire(ADDRESS1, None, Deadline(3), None, **kwargs)
        if unprepared:
            cx.re_auth.assert_not_called()
        else:
            cx.re_auth.assert_called_once()

        pool.release(cx)


@mark_sync_test
def test_connection_acquisition_timeout(fake_connection_generator):
    pool_max_size = 5

    with FakeBoltPool(
        fake_connection_generator,
        ADDRESS1,
        max_connection_pool_size=pool_max_size,
    ) as pool:
        connections = []
        for _ in range(pool_max_size):
            connection = pool.acquire(
                READ_ACCESS, 0.5, TEST_DB1, None, None, None
            )
            connections.append(connection)

        with pytest.raises(ConnectionAcquisitionTimeoutError):
            pool.acquire(READ_ACCESS, 0.5, TEST_DB1, None, None, None)

        assert set(pool.connections.keys()) == {ADDRESS1}
        assert len(pool.connections[ADDRESS1]) == pool_max_size
