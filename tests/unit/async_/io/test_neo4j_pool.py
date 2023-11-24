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


import inspect
from collections import defaultdict

import pytest

from neo4j import (
    PreviewWarning,
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j._async.io import (
    AsyncBolt,
    AsyncNeo4jPool,
)
from neo4j._async_compat.util import AsyncUtil
from neo4j._conf import (
    PoolConfig,
    RoutingConfig,
    WorkspaceConfig,
)
from neo4j._deadline import Deadline
from neo4j.addressing import ResolvedAddress
from neo4j.auth_management import AsyncAuthManagers
from neo4j.exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
)

from ...._async_compat import mark_async_test


ROUTER1_ADDRESS = ResolvedAddress(("1.2.3.1", 9000), host_name="host")
ROUTER2_ADDRESS = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
ROUTER3_ADDRESS = ResolvedAddress(("1.2.3.1", 9002), host_name="host")
READER1_ADDRESS = ResolvedAddress(("1.2.3.1", 9010), host_name="host")
READER2_ADDRESS = ResolvedAddress(("1.2.3.1", 9011), host_name="host")
READER3_ADDRESS = ResolvedAddress(("1.2.3.1", 9012), host_name="host")
WRITER1_ADDRESS = ResolvedAddress(("1.2.3.1", 9020), host_name="host")


@pytest.fixture
def custom_routing_opener(async_fake_connection_generator, mocker):
    def make_opener(failures=None, get_readers=None):
        def routing_side_effect(*args, **kwargs):
            nonlocal failures
            res = next(failures, None)
            if res is None:
                if get_readers is not None:
                    readers = get_readers(kwargs.get("database"))
                else:
                    readers = [str(READER1_ADDRESS)]
                return [{
                    "ttl": 1000,
                    "servers": [
                        {"addresses": [str(ROUTER1_ADDRESS),
                                       str(ROUTER2_ADDRESS),
                                       str(ROUTER3_ADDRESS)],
                         "role": "ROUTE"},
                        {"addresses": readers, "role": "READ"},
                        {"addresses": [str(WRITER1_ADDRESS)], "role": "WRITE"},
                    ],
                }]
            raise res

        async def open_(addr, auth, timeout):
            connection = async_fake_connection_generator()
            connection.unresolved_address = addr
            connection.timeout = timeout
            connection.auth = auth
            route_mock = mocker.AsyncMock()

            route_mock.side_effect = routing_side_effect
            connection.attach_mock(route_mock, "route")
            opener_.connections.append(connection)
            return connection

        failures = iter(failures or [])
        opener_ = mocker.AsyncMock()
        opener_.connections = []
        opener_.side_effect = open_
        return opener_

    return make_opener


@pytest.fixture
def opener(custom_routing_opener):
    return custom_routing_opener()


def _pool_config():
    pool_config = PoolConfig()
    pool_config.auth = _auth_manager(("user", "pass"))
    return pool_config


def _auth_manager(auth):
    return AsyncAuthManagers.static(auth)


def _simple_pool(opener) -> AsyncNeo4jPool:
    return AsyncNeo4jPool(
        opener, _pool_config(), WorkspaceConfig(), ROUTER1_ADDRESS
    )


@mark_async_test
async def test_acquires_new_routing_table_if_deleted(opener):
    pool = _simple_pool(opener)
    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")

    del pool.routing_tables["test_db"]

    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")


@mark_async_test
async def test_acquires_new_routing_table_if_stale(opener):
    pool = _simple_pool(opener)
    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db")

    old_value = pool.routing_tables["test_db"].last_updated_time
    pool.routing_tables["test_db"].ttl = 0

    cx = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables["test_db"].last_updated_time > old_value


@mark_async_test
async def test_removes_old_routing_table(opener):
    pool = _simple_pool(opener)
    cx = await pool.acquire(READ_ACCESS, 30, "test_db1", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db1")
    cx = await pool.acquire(READ_ACCESS, 30, "test_db2", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables.get("test_db2")

    old_value = pool.routing_tables["test_db1"].last_updated_time
    pool.routing_tables["test_db1"].ttl = 0
    pool.routing_tables["test_db2"].ttl = \
        -RoutingConfig.routing_table_purge_delay

    cx = await pool.acquire(READ_ACCESS, 30, "test_db1", None, None, None)
    await pool.release(cx)
    assert pool.routing_tables["test_db1"].last_updated_time > old_value
    assert "test_db2" not in pool.routing_tables


@pytest.mark.parametrize("type_", ("r", "w"))
@mark_async_test
async def test_chooses_right_connection_type(opener, type_):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(
        READ_ACCESS if type_ == "r" else WRITE_ACCESS,
        30, "test_db", None, None, None
    )
    await pool.release(cx1)
    if type_ == "r":
        assert cx1.unresolved_address == READER1_ADDRESS
    else:
        assert cx1.unresolved_address == WRITER1_ADDRESS


@mark_async_test
async def test_reuses_connection(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx1)
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    assert cx1 is cx2


@pytest.mark.parametrize("break_on_close", (True, False))
@mark_async_test
async def test_closes_stale_connections(opener, break_on_close):
    async def break_connection():
        await pool.deactivate(cx1.unresolved_address)

        if cx_close_mock_side_effect:
            res = cx_close_mock_side_effect()
            if inspect.isawaitable(res):
                return await res

    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx1)
    assert cx1 in pool.connections[cx1.unresolved_address]
    # simulate connection going stale (e.g. exceeding idle timeout) and then
    # breaking when the pool tries to close the connection
    cx1.stale.return_value = True
    cx_close_mock = cx1.close
    if break_on_close:
        cx_close_mock_side_effect = cx_close_mock.side_effect
        cx_close_mock.side_effect = break_connection
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx2)
    if break_on_close:
        cx1.close.assert_called()
    else:
        cx1.close.assert_called_once()
    assert cx2 is not cx1
    assert cx2.unresolved_address == cx1.unresolved_address
    assert cx1 not in pool.connections[cx1.unresolved_address]
    assert cx2 in pool.connections[cx2.unresolved_address]


@mark_async_test
async def test_does_not_close_stale_connections_in_use(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    assert cx1 in pool.connections[cx1.unresolved_address]
    # simulate connection going stale (e.g. exceeding idle timeout) while being
    # in use
    cx1.stale.return_value = True
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx2)
    cx1.close.assert_not_called()
    assert cx2 is not cx1
    assert cx2.unresolved_address == cx1.unresolved_address
    assert cx1 in pool.connections[cx1.unresolved_address]
    assert cx2 in pool.connections[cx2.unresolved_address]

    await pool.release(cx1)
    # now that cx1 is back in the pool and still stale,
    # it should be closed when trying to acquire the next connection
    cx1.close.assert_not_called()

    cx3 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx3)
    cx1.close.assert_called_once()
    assert cx2 is cx3
    assert cx3.unresolved_address == cx1.unresolved_address
    assert cx1 not in pool.connections[cx1.unresolved_address]
    assert cx3 in pool.connections[cx2.unresolved_address]


@mark_async_test
async def test_release_resets_connections(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    cx1.is_reset_mock.return_value = False
    cx1.is_reset_mock.reset_mock()
    await pool.release(cx1)
    cx1.is_reset_mock.assert_called_once()
    cx1.reset.assert_called_once()


@mark_async_test
async def test_release_does_not_resets_closed_connections(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    cx1.closed.return_value = True
    cx1.closed.reset_mock()
    cx1.is_reset_mock.reset_mock()
    await pool.release(cx1)
    cx1.closed.assert_called_once()
    cx1.is_reset_mock.assert_not_called()
    cx1.reset.assert_not_called()


@mark_async_test
async def test_release_does_not_resets_defunct_connections(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
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
    pool = _simple_pool(opener)
    cx1 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)
    assert cx1.unresolved_address == READER1_ADDRESS
    cx1.reset.assert_not_called()


@pytest.mark.parametrize("liveness_timeout", (0, 1, 2))
@mark_async_test
async def test_acquire_performs_liveness_check_on_existing_connection(
    opener, liveness_timeout
):
    pool = _simple_pool(opener)
    # populate the pool with a connection
    cx1 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)

    # make sure we assume the right state
    assert cx1.unresolved_address == READER1_ADDRESS
    cx1.is_idle_for.assert_not_called()
    cx1.reset.assert_not_called()

    cx1.is_idle_for.return_value = True

    # release the connection
    await pool.release(cx1)
    cx1.reset.assert_not_called()

    # then acquire it again and assert the liveness check was performed
    cx2 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)
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
    pool = _simple_pool(opener)
    # populate the pool with a connection
    cx1 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)

    # make sure we assume the right state
    assert cx1.unresolved_address == READER1_ADDRESS
    cx1.is_idle_for.assert_not_called()
    cx1.reset.assert_not_called()

    cx1.is_idle_for.return_value = True
    # simulate cx1 failing liveness check
    cx1.reset.side_effect = liveness_side_effect

    # release the connection
    await pool.release(cx1)
    cx1.reset.assert_not_called()

    # then acquire it again and assert the liveness check was performed
    cx2 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)
    assert cx1 is not cx2
    assert cx1.unresolved_address == cx2.unresolved_address
    cx1.is_idle_for.assert_called_once_with(liveness_timeout)
    cx2.reset.assert_not_called()
    assert cx1 not in pool.connections[cx1.unresolved_address]
    assert cx2 in pool.connections[cx1.unresolved_address]


@pytest.mark.parametrize("liveness_error",
                         (OSError, ServiceUnavailable, SessionExpired))
@mark_async_test
async def test_acquire_returns_other_connection_on_failed_liveness_check(
    opener, liveness_error
):
    def liveness_side_effect(*args, **kwargs):
        raise liveness_error("liveness check failed")

    liveness_timeout = 1
    pool = _simple_pool(opener)
    # populate the pool with a connection
    cx1 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)
    cx2 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)

    # make sure we assume the right state
    assert cx1.unresolved_address == READER1_ADDRESS
    assert cx2.unresolved_address == READER1_ADDRESS
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
    cx3 = await pool._acquire(READER1_ADDRESS, None, Deadline(30),
                              liveness_timeout)
    assert cx3 is cx2
    cx1.is_idle_for.assert_called_once_with(liveness_timeout)
    cx1.reset.assert_awaited_once()
    cx3.is_idle_for.assert_called_once_with(liveness_timeout)
    cx3.reset.assert_awaited_once()
    assert cx1 not in pool.connections[cx1.unresolved_address]
    assert cx3 in pool.connections[cx1.unresolved_address]


@mark_async_test
async def test_multiple_broken_connections_on_close(opener, mocker):
    def mock_connection_breaks_on_close(cx):
        async def close_side_effect():
            cx.closed.return_value = True
            cx.defunct.return_value = True
            await pool.deactivate(READER1_ADDRESS)

        cx.attach_mock(mocker.AsyncMock(side_effect=close_side_effect),
                       "close")

    # create pool with 2 idle connections
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx1)
    await pool.release(cx2)

    # both will loose connection
    mock_connection_breaks_on_close(cx1)
    mock_connection_breaks_on_close(cx2)

    # force pool to close cx1, which will make it realize that the server is
    # unreachable
    cx1.stale.return_value = True

    cx3 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)

    assert cx3 is not cx1
    assert cx3 is not cx2


@mark_async_test
async def test_failing_opener_leaves_connections_in_use_alone(opener):
    pool = _simple_pool(opener)
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)

    opener.side_effect = ServiceUnavailable("Server overloaded")
    with pytest.raises((ServiceUnavailable, SessionExpired)):
        await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    assert not cx1.closed()


@mark_async_test
async def test__acquire_new_later_with_room(opener):
    config = _pool_config()
    config.max_connection_pool_size = 1
    pool = AsyncNeo4jPool(
        opener, config, WorkspaceConfig(), ROUTER1_ADDRESS
    )
    assert pool.connections_reservations[READER1_ADDRESS] == 0
    creator = pool._acquire_new_later(READER1_ADDRESS, None, Deadline(1))
    assert pool.connections_reservations[READER1_ADDRESS] == 1
    assert callable(creator)
    if AsyncUtil.is_async_code:
        assert inspect.iscoroutinefunction(creator)


@mark_async_test
async def test__acquire_new_later_without_room(opener):
    config = _pool_config()
    config.max_connection_pool_size = 1
    pool = AsyncNeo4jPool(
        opener, config, WorkspaceConfig(), ROUTER1_ADDRESS
    )
    _ = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    # pool is full now
    assert pool.connections_reservations[READER1_ADDRESS] == 0
    creator = pool._acquire_new_later(READER1_ADDRESS, None, Deadline(1))
    assert pool.connections_reservations[READER1_ADDRESS] == 0
    assert creator is None


@mark_async_test
async def test_passes_pool_config_to_connection(mocker):
    bolt_mock = mocker.patch.object(AsyncBolt, "open", autospec=True)

    pool_config = PoolConfig()
    workspace_config = WorkspaceConfig()
    pool = AsyncNeo4jPool.open(
        mocker.Mock, pool_config=pool_config, workspace_config=workspace_config
    )

    _ = await pool._acquire(
        mocker.Mock, None, Deadline.from_timeout_or_deadline(30), None
    )

    bolt_mock.assert_awaited_once()
    assert bolt_mock.call_args.kwargs["pool_config"] is pool_config



@pytest.mark.parametrize("error", (
    ServiceUnavailable(),
    Neo4jError.hydrate("message", "Neo.ClientError.Statement.EntityNotFound"),
    Neo4jError.hydrate("message",
                       "Neo.ClientError.Security.AuthorizationExpired"),
))
@mark_async_test
async def test_discovery_is_retried(custom_routing_opener, error):
    opener = custom_routing_opener([
        None,  # first call to router for seeding the RT with more routers
        error,  # will be retried
    ])
    pool = AsyncNeo4jPool(
        opener, _pool_config(), WorkspaceConfig(),
        ResolvedAddress(("1.2.3.1", 9999), host_name="host")
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx1)
    pool.routing_tables.get("test_db").ttl = 0

    cx2 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx2)
    assert pool.routing_tables.get("test_db")

    assert cx1 is cx2

    # initial router
    # reader
    # failed router
    # successful router
    # same reader again
    assert len(opener.connections) == 4


@pytest.mark.parametrize("error", map(
    lambda args: Neo4jError.hydrate(*args), (
        ("message", "Neo.ClientError.Database.DatabaseNotFound"),
        ("message", "Neo.ClientError.Transaction.InvalidBookmark"),
        ("message", "Neo.ClientError.Transaction.InvalidBookmarkMixture"),
        ("message", "Neo.ClientError.Statement.TypeError"),
        ("message", "Neo.ClientError.Statement.ArgumentError"),
        ("message", "Neo.ClientError.Request.Invalid"),
        ("message", "Neo.ClientError.Security.AuthenticationRateLimit"),
        ("message", "Neo.ClientError.Security.CredentialsExpired"),
        ("message", "Neo.ClientError.Security.Forbidden"),
        ("message", "Neo.ClientError.Security.TokenExpired"),
        ("message", "Neo.ClientError.Security.Unauthorized"),
        ("message", "Neo.ClientError.Security.MadeUpError"),
    )
))
@mark_async_test
async def test_fast_failing_discovery(custom_routing_opener, error):
    opener = custom_routing_opener([
        None,  # first call to router for seeding the RT with more routers
        error,  # will be retried
    ])
    pool = AsyncNeo4jPool(
        opener, _pool_config(), WorkspaceConfig(),
        ResolvedAddress(("1.2.3.1", 9999), host_name="host")
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    await pool.release(cx1)
    pool.routing_tables.get("test_db").ttl = 0

    with pytest.raises(error.__class__) as exc:
        await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)

    assert exc.value is error

    # initial router
    # reader
    # failed router
    assert len(opener.connections) == 3


@pytest.mark.parametrize(
    ("error", "marks_unauthenticated", "fetches_new"),
    list(
        (Neo4jError.hydrate("message", args[0]), *args[1:])
        for args in (
            ("Neo.ClientError.Database.DatabaseNotFound", False, False),
            ("Neo.ClientError.Statement.TypeError", False, False),
            ("Neo.ClientError.Statement.ArgumentError", False, False),
            ("Neo.ClientError.Request.Invalid", False, False),
            ("Neo.ClientError.Security.AuthenticationRateLimit", False, True),
            ("Neo.ClientError.Security.CredentialsExpired", False, True),
            ("Neo.ClientError.Security.Forbidden", False, True),
            ("Neo.ClientError.Security.Unauthorized", False, True),
            ("Neo.ClientError.Security.MadeUpError", False, True),
            ("Neo.ClientError.Security.TokenExpired", False, True),
            ("Neo.ClientError.Security.AuthorizationExpired", True, True),
        )
    )[4:5]
)
@mark_async_test
async def test_connection_error_callback(
    opener, error, marks_unauthenticated, fetches_new, mocker
):
    config = _pool_config()
    auth_manager = _auth_manager(("user", "auth"))
    handle_exc_mock = mocker.patch.object(
        auth_manager, "handle_security_exception", autospec=True
    )
    config.auth = auth_manager
    pool = AsyncNeo4jPool(
        opener, config, WorkspaceConfig(), ROUTER1_ADDRESS
    )
    cxs_read = [
        await pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
        for _ in range(5)
    ]
    cxs_write = [
        await pool.acquire(WRITE_ACCESS, 30, "test_db", None, None, None)
        for _ in range(5)
    ]

    handle_exc_mock.assert_not_called()
    for cx in cxs_read + cxs_write:
        cx.mark_unauthenticated.assert_not_called()

    await pool.on_neo4j_error(error, cxs_read[0])

    if fetches_new:
        cx = cxs_read[0]
        cx.auth_manager.handle_security_exception.assert_awaited_once()
    else:
        handle_exc_mock.assert_not_called()
        for cx in cxs_read:
            cx.auth_manager.handle_security_exception.assert_not_called()

    for cx in cxs_read:
        if marks_unauthenticated:
            cx.mark_unauthenticated.assert_called_once()
        else:
            cx.mark_unauthenticated.assert_not_called()
    for cx in cxs_write:
        cx.mark_unauthenticated.assert_not_called()


@mark_async_test
async def test_pool_closes_connections_dropped_from_rt(custom_routing_opener):
    readers = {"db1": [str(READER1_ADDRESS)]}

    def get_readers(database):
        return readers[database]

    opener = custom_routing_opener(get_readers=get_readers)

    pool = AsyncNeo4jPool(
        opener, _pool_config(), WorkspaceConfig(), ROUTER1_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "db1", None, None, None)
    assert cx1.unresolved_address == READER1_ADDRESS
    await pool.release(cx1)

    cx1.close.assert_not_called()
    assert len(pool.connections[READER1_ADDRESS]) == 1

    # force RT refresh, returning a different reader
    del pool.routing_tables["db1"]
    readers["db1"] = [str(READER2_ADDRESS)]

    cx2 = await pool.acquire(READ_ACCESS, 30, "db1", None, None, None)
    assert cx2.unresolved_address == READER2_ADDRESS

    cx1.close.assert_awaited_once()
    assert len(pool.connections[READER1_ADDRESS]) == 0

    await pool.release(cx2)
    assert len(pool.connections[READER2_ADDRESS]) == 1


@mark_async_test
async def test_pool_does_not_close_connections_dropped_from_rt_for_other_server(
    custom_routing_opener
):
    readers = {
        "db1": [str(READER1_ADDRESS), str(READER2_ADDRESS)],
        "db2": [str(READER1_ADDRESS)]
    }

    def get_readers(database):
        return readers[database]

    opener = custom_routing_opener(get_readers=get_readers)

    pool = AsyncNeo4jPool(
        opener, _pool_config(), WorkspaceConfig(), ROUTER1_ADDRESS
    )
    cx1 = await pool.acquire(READ_ACCESS, 30, "db1", None, None, None)
    await pool.release(cx1)
    assert cx1.unresolved_address in (READER1_ADDRESS, READER2_ADDRESS)
    reader1_connection_count = len(pool.connections[READER1_ADDRESS])
    reader2_connection_count = len(pool.connections[READER2_ADDRESS])
    assert reader1_connection_count + reader2_connection_count == 1

    cx2 = await pool.acquire(READ_ACCESS, 30, "db2", None, None, None)
    await pool.release(cx2)
    assert cx2.unresolved_address == READER1_ADDRESS
    cx1.close.assert_not_called()
    cx2.close.assert_not_called()
    assert len(pool.connections[READER1_ADDRESS]) == 1
    assert len(pool.connections[READER2_ADDRESS]) == reader2_connection_count


    # force RT refresh, returning a different reader
    del pool.routing_tables["db2"]
    readers["db2"] = [str(READER3_ADDRESS)]

    cx3 = await pool.acquire(READ_ACCESS, 30, "db2", None, None, None)
    await pool.release(cx3)
    assert cx3.unresolved_address == READER3_ADDRESS

    cx1.close.assert_not_called()
    cx2.close.assert_not_called()
    cx3.close.assert_not_called()
    assert len(pool.connections[READER1_ADDRESS]) == 1
    assert len(pool.connections[READER2_ADDRESS]) == reader2_connection_count
    assert len(pool.connections[READER3_ADDRESS]) == 1
