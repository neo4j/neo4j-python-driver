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


import inspect

import pytest

from neo4j import (
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j._async.io import AsyncNeo4jPool
from neo4j._conf import (
    PoolConfig,
    RoutingConfig,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j.addressing import ResolvedAddress
from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
)

from ...._async_compat import mark_async_test
from ..work import async_fake_connection_generator  # needed as fixture


ROUTER_ADDRESS = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
READER_ADDRESS = ResolvedAddress(("1.2.3.1", 9002), host_name="host")
WRITER_ADDRESS = ResolvedAddress(("1.2.3.1", 9003), host_name="host")


@pytest.fixture
def opener(async_fake_connection_generator, mocker):
    async def open_(addr, timeout):
        connection = async_fake_connection_generator()
        connection.addr = addr
        connection.timeout = timeout
        route_mock = mocker.AsyncMock()
        route_mock.return_value = [{
            "ttl": 1000,
            "servers": [
                {"addresses": [str(ROUTER_ADDRESS)], "role": "ROUTE"},
                {"addresses": [str(READER_ADDRESS)], "role": "READ"},
                {"addresses": [str(WRITER_ADDRESS)], "role": "WRITE"},
            ],
        }]
        connection.attach_mock(route_mock, "route")
        opener_.connections.append(connection)
        return connection

    opener_ = mocker.AsyncMock()
    opener_.connections = []
    opener_.side_effect = open_
    return opener_


@mark_async_test
async def test_acquires_new_routing_table_if_deleted(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")

    del pool.routing_tables["test_db"]

    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")


@mark_async_test
async def test_acquires_new_routing_table_if_stale(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")

    old_value = pool.routing_tables["test_db"].last_updated_time
    pool.routing_tables["test_db"].ttl = 0

    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx)
    assert pool.routing_tables["test_db"].last_updated_time > old_value


@mark_async_test
async def test_removes_old_routing_table(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx = await pool.acquire(READ_ACCESS, 30, "test_db1", None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db1")
    cx = await pool.acquire(READ_ACCESS, 30, "test_db2", None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db2")

    old_value = pool.routing_tables["test_db1"].last_updated_time
    pool.routing_tables["test_db1"].ttl = 0
    pool.routing_tables["test_db2"].ttl = \
        -RoutingConfig.routing_table_purge_delay

    cx = await pool.acquire(READ_ACCESS, 30, "test_db1", None, None)
    await pool.release(cx)
    assert pool.routing_tables["test_db1"].last_updated_time > old_value
    assert "test_db2" not in pool.routing_tables


@pytest.mark.parametrize("type_", ("r", "w"))
@mark_async_test
async def test_chooses_right_connection_type(opener, type_):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(
        READ_ACCESS if type_ == "r" else WRITE_ACCESS,
        30, "test_db", None, None
    )
    await pool.release(cx1)
    if type_ == "r":
        assert cx1.addr == READER_ADDRESS
    else:
        assert cx1.addr == WRITER_ADDRESS


@mark_async_test
async def test_reuses_connection(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx1)
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    assert cx1 is cx2


@pytest.mark.parametrize("break_on_close", (True, False))
@mark_async_test
async def test_closes_stale_connections(opener, break_on_close):
    async def break_connection():
        await pool.deactivate(cx1.addr)

        if cx_close_mock_side_effect:
            res = cx_close_mock_side_effect()
            if inspect.isawaitable(res):
                return await res

    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx1)
    assert cx1 in pool.connections[cx1.addr]
    # simulate connection going stale (e.g. exceeding) and then breaking when
    # the pool tries to close the connection
    cx1.stale.return_value = True
    cx_close_mock = cx1.close
    if break_on_close:
        cx_close_mock_side_effect = cx_close_mock.side_effect
        cx_close_mock.side_effect = break_connection
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx2)
    if break_on_close:
        cx1.close.assert_called()
    else:
        cx1.close.assert_called_once()
    assert cx2 is not cx1
    assert cx2.addr == cx1.addr
    assert cx1 not in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx2.addr]


@mark_async_test
async def test_does_not_close_stale_connections_in_use(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    assert cx1 in pool.connections[cx1.addr]
    # simulate connection going stale (e.g. exceeding) while being in use
    cx1.stale.return_value = True
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx2)
    cx1.close.assert_not_called()
    assert cx2 is not cx1
    assert cx2.addr == cx1.addr
    assert cx1 in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx2.addr]

    await pool.release(cx1)
    # now that cx1 is back in the pool and still stale,
    # it should be closed when trying to acquire the next connection
    cx1.close.assert_not_called()

    cx3 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx3)
    cx1.close.assert_called_once()
    assert cx2 is cx3
    assert cx3.addr == cx1.addr
    assert cx1 not in pool.connections[cx1.addr]
    assert cx3 in pool.connections[cx2.addr]


@mark_async_test
async def test_release_resets_connections(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    cx1.is_reset_mock.return_value = False
    cx1.is_reset_mock.reset_mock()
    await pool.release(cx1)
    cx1.is_reset_mock.assert_called_once()
    cx1.reset.assert_called_once()


@mark_async_test
async def test_release_does_not_resets_closed_connections(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    cx1.closed.return_value = True
    cx1.closed.reset_mock()
    cx1.is_reset_mock.reset_mock()
    await pool.release(cx1)
    cx1.closed.assert_called_once()
    cx1.is_reset_mock.assert_not_called()
    cx1.reset.assert_not_called()


@mark_async_test
async def test_release_does_not_resets_defunct_connections(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    cx1.defunct.return_value = True
    cx1.defunct.reset_mock()
    cx1.is_reset_mock.reset_mock()
    await pool.release(cx1)
    cx1.defunct.assert_called_once()
    cx1.is_reset_mock.assert_not_called()
    cx1.reset.assert_not_called()


@pytest.mark.parametrize("liveness_timeout", (0, 1, 2))
@mark_async_test
async def test_acquire_performs_no_liveness_check_on_fresh_connection(
    opener, liveness_timeout
):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)
    assert cx1.addr == READER_ADDRESS
    cx1.reset.assert_not_called()


@pytest.mark.parametrize("liveness_timeout", (0, 1, 2))
@mark_async_test
async def test_acquire_performs_liveness_check_on_existing_connection(
    opener, liveness_timeout
):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    # populate the pool with a connection
    cx1 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)

    # make sure we assume the right state
    assert cx1.addr == READER_ADDRESS
    cx1.is_idle_for.assert_not_called()
    cx1.reset.assert_not_called()

    cx1.is_idle_for.return_value = True

    # release the connection
    await pool.release(cx1)
    cx1.reset.assert_not_called()

    # then acquire it again and assert the liveness check was performed
    cx2 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)
    assert cx1 is cx2
    cx1.is_idle_for.assert_called_once_with(liveness_timeout)
    cx2.reset.assert_awaited_once()


@pytest.mark.parametrize("liveness_error",
                         (OSError, ServiceUnavailable, SessionExpired))
@mark_async_test
async def test_acquire_creates_connection_on_failed_liveness_check(
    opener, liveness_error
):
    def liveness_side_effect(*args, **kwargs):
        raise liveness_error("liveness check failed")

    liveness_timeout = 1
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    # populate the pool with a connection
    cx1 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)

    # make sure we assume the right state
    assert cx1.addr == READER_ADDRESS
    cx1.is_idle_for.assert_not_called()
    cx1.reset.assert_not_called()

    cx1.is_idle_for.return_value = True
    # simulate cx1 failing liveness check
    cx1.reset.side_effect = liveness_side_effect

    # release the connection
    await pool.release(cx1)
    cx1.reset.assert_not_called()

    # then acquire it again and assert the liveness check was performed
    cx2 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)
    assert cx1 is not cx2
    assert cx1.addr == cx2.addr
    cx1.is_idle_for.assert_called_once_with(liveness_timeout)
    cx2.reset.assert_not_called()
    assert cx1 not in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx1.addr]


@pytest.mark.parametrize("liveness_error",
                         (OSError, ServiceUnavailable, SessionExpired))
@mark_async_test
async def test_acquire_returns_other_connection_on_failed_liveness_check(
    opener, liveness_error
):
    def liveness_side_effect(*args, **kwargs):
        raise liveness_error("liveness check failed")

    liveness_timeout = 1
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    # populate the pool with a connection
    cx1 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)
    cx2 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)

    # make sure we assume the right state
    assert cx1.addr == READER_ADDRESS
    assert cx2.addr == READER_ADDRESS
    assert cx1 is not cx2
    cx1.is_idle_for.assert_not_called()
    cx2.is_idle_for.assert_not_called()
    cx1.reset.assert_not_called()

    cx1.is_idle_for.return_value = True
    cx2.is_idle_for.return_value = True
    # simulate cx1 failing liveness check
    cx1.reset.side_effect = liveness_side_effect

    # release the connection
    await pool.release(cx1)
    await pool.release(cx2)
    cx1.reset.assert_not_called()
    cx2.reset.assert_not_called()

    # then acquire it again and assert the liveness check was performed
    cx3 = await pool._acquire(READER_ADDRESS, Deadline(30), liveness_timeout)
    assert cx3 is cx2
    cx1.is_idle_for.assert_called_once_with(liveness_timeout)
    cx1.reset.assert_awaited_once()
    cx3.is_idle_for.assert_called_once_with(liveness_timeout)
    cx3.reset.assert_awaited_once()
    assert cx1 not in pool.connections[cx1.addr]
    assert cx3 in pool.connections[cx1.addr]


@mark_async_test
async def test_multiple_broken_connections_on_close(opener, mocker):
    def mock_connection_breaks_on_close(cx):
        async def close_side_effect():
            cx.closed.return_value = True
            cx.defunct.return_value = True
            await pool.deactivate(READER_ADDRESS)

        cx.attach_mock(mocker.AsyncMock(side_effect=close_side_effect),
                       "close")

    # create pool with 2 idle connections
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    await pool.release(cx1)
    await pool.release(cx2)

    # both will loose connection
    mock_connection_breaks_on_close(cx1)
    mock_connection_breaks_on_close(cx2)

    # force pool to close cx1, which will make it realize that the server is
    # unreachable
    cx1.stale.return_value = True

    cx3 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)

    assert cx3 is not cx1
    assert cx3 is not cx2


@mark_async_test
async def test_failing_opener_leaves_connections_in_use_alone(opener):
    pool = AsyncNeo4jPool(
        opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None)

    opener.side_effect = ServiceUnavailable("Server overloaded")
    with pytest.raises((ServiceUnavailable, SessionExpired)):
        await pool.acquire(READ_ACCESS, 30, "test_db", None, None)
    assert not cx1.closed()
