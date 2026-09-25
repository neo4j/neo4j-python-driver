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

from neo4j import READ_ACCESS
from neo4j._addressing import ResolvedAddress
from neo4j._async.config import AsyncPoolConfig
from neo4j._async.io import (
    AcquisitionDatabase,
    AsyncHttpV2Pool,
)
from neo4j._conf import WorkspaceConfig
from neo4j.auth_management import AsyncAuthManagers
from neo4j.exceptions import ConnectionAcquisitionTimeoutError

from ...._async_compat import (
    async_fixture,
    mark_async_test,
)
from ...._optional_deps import mark_skip_if_http_dependencies_missing


ADDRESS = ResolvedAddress(("1.2.3.1", 9000), host_name="host")


@pytest.fixture
def custom_routing_opener(async_fake_connection_generator, mocker):
    def make_opener(on_open=None):
        async def open_(addr, auth, timeout):
            connection = async_fake_connection_generator()
            connection.unresolved_address = addr
            connection.timeout = timeout
            connection.auth = auth
            opener_.connections.append(connection)

            if callable(on_open):
                on_open(connection)

            return connection

        opener_ = mocker.AsyncMock()
        opener_.connections = []
        opener_.route_requests = []
        opener_.side_effect = open_
        return opener_

    return make_opener


@pytest.fixture
def opener(custom_routing_opener):
    return custom_routing_opener()


def _pool_config():
    pool_config = AsyncPoolConfig()
    pool_config.auth = _auth_manager(("user", "pass"))
    return pool_config


def _auth_manager(auth):
    return AsyncAuthManagers.static(auth)


@async_fixture
async def simple_pool_factory(mocker):
    from neo4j._async.io._http import AsyncHttpConnectionFactory
    from neo4j._async_compat.network._http_query_api import (
        AsyncHTTPQueryAPIFactory,
    )

    pools = []

    def factory(opener, pool_config=None):
        nonlocal pools

        api_factory = mocker.AsyncMock(spec=AsyncHTTPQueryAPIFactory)
        connection_factory = mocker.AsyncMock(spec=AsyncHttpConnectionFactory)
        pool = AsyncHttpV2Pool(
            opener,
            _pool_config() if pool_config is None else pool_config,
            WorkspaceConfig(),
            ADDRESS,
            async_http_query_api_factory=api_factory,
            async_http_connection_factory=connection_factory,
        )
        pools.append(pool)
        return pool

    try:
        yield factory
    finally:
        for pool in pools:
            await pool.close()


TEST_DB = AcquisitionDatabase("test_db")


@mark_async_test
@mark_skip_if_http_dependencies_missing
async def test_connection_acquisition_timeout(simple_pool_factory, opener):
    pool_max_size = 5

    pool_config = _pool_config()
    pool_config.max_connection_pool_size = pool_max_size
    pool = simple_pool_factory(opener, pool_config)

    connections = []
    for _ in range(pool_max_size):
        connection = await pool.acquire(
            READ_ACCESS, 0.5, TEST_DB, None, None, None
        )
        connections.append(connection)

    with pytest.raises(ConnectionAcquisitionTimeoutError):
        await pool.acquire(READ_ACCESS, 0.5, TEST_DB, None, None, None)

    await pool.release(*connections)
