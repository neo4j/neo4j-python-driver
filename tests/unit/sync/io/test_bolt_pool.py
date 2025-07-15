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


import contextlib

import pytest

from neo4j import READ_ACCESS
from neo4j._addressing import ResolvedAddress
from neo4j._conf import WorkspaceConfig
from neo4j._sync.config import PoolConfig
from neo4j._sync.io import (
    AcquisitionDatabase,
    BoltPool,
)
from neo4j.auth_management import AuthManagers
from neo4j.exceptions import ConnectionAcquisitionTimeoutError

from ...._async_compat import mark_sync_test


SERVER1_ADDRESS = ResolvedAddress(("1.2.3.1", 9000), host_name="host")


def make_home_db_resolve(home_db):
    def _home_db_resolve(db):
        return db or home_db

    return _home_db_resolve


_default_db_resolve = make_home_db_resolve("neo4j")


@pytest.fixture
def custom_opener(fake_connection_generator, mocker):
    def make_opener(
        failures=None,
        db_resolve=_default_db_resolve,
        on_open=None,
    ):
        def routing_side_effect(*args, **kwargs):
            nonlocal failures
            opener_.route_requests.append(kwargs.get("database"))
            res = next(failures, None)
            if res is None:
                routers = readers = writers = [str(SERVER1_ADDRESS)]
                rt = {
                    "ttl": 1000,
                    "servers": [
                        {"addresses": routers, "role": "ROUTE"},
                        {"addresses": readers, "role": "READ"},
                        {"addresses": writers, "role": "WRITE"},
                    ],
                }
                db = db_resolve(kwargs.get("database"))
                if db is not ...:
                    rt["db"] = db
                return [rt]
            raise res

        def open_(addr, auth, timeout):
            connection = fake_connection_generator()
            connection.unresolved_address = addr
            connection.timeout = timeout
            connection.auth = auth
            route_mock = mocker.MagicMock()

            route_mock.side_effect = routing_side_effect
            connection.attach_mock(route_mock, "route")
            opener_.connections.append(connection)

            if callable(on_open):
                on_open(connection)

            return connection

        failures = iter(failures or [])
        opener_ = mocker.MagicMock()
        opener_.connections = []
        opener_.route_requests = []
        opener_.side_effect = open_
        return opener_

    return make_opener


@pytest.fixture
def opener(custom_opener):
    return custom_opener()


def _pool_config():
    pool_config = PoolConfig()
    pool_config.auth = _auth_manager(("user", "pass"))
    return pool_config


def _auth_manager(auth):
    return AuthManagers.static(auth)


def _simple_pool(opener) -> BoltPool:
    return BoltPool(
        opener, _pool_config(), WorkspaceConfig(), SERVER1_ADDRESS
    )


TEST_DB1 = AcquisitionDatabase("test_db1")


@pytest.mark.parametrize(
    ("timeout", "expected_error"),
    (
        (1, None),
        (2 ^ 128, None),
        (0.000000001, None),
        (float("inf"), None),
        (-1, ValueError),
        (0, ValueError),
        (float("-inf"), ValueError),
        (float("NaN"), ValueError),
        (float("-NaN"), ValueError),
        ("1", TypeError),
        (None, TypeError),
        ([1], TypeError),
    ),
)
@mark_sync_test
def test_invalid_acquisition_timeouts(opener, timeout, expected_error):
    pool = _simple_pool(opener)

    def call():
        with contextlib.suppress(ConnectionAcquisitionTimeoutError):
            pool.acquire(
                READ_ACCESS, timeout, TEST_DB1, None, None, None
            )

    if expected_error is None:
        call()
    else:
        with pytest.raises(expected_error):
            call()
