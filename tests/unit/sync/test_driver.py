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


import pytest

from neo4j import (
    BoltDriver,
    GraphDatabase,
    Neo4jDriver,
)
from neo4j.api import WRITE_ACCESS
from neo4j.exceptions import ConfigurationError

from ..._async_compat import (
    mark_sync_test,
    mock,
)


@pytest.mark.parametrize("protocol", ("bolt://", "bolt+s://", "bolt+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
def test_direct_driver_constructor(protocol, host, port, auth_token):
    uri = protocol + host + port
    driver = GraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, BoltDriver)


@pytest.mark.parametrize("protocol", ("neo4j://", "neo4j+s://", "neo4j+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
def test_routing_driver_constructor(protocol, host, port, auth_token):
    uri = protocol + host + port
    driver = GraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, Neo4jDriver)


@pytest.mark.parametrize("test_uri", (
    "bolt+ssc://127.0.0.1:9001",
    "bolt+s://127.0.0.1:9001",
    "bolt://127.0.0.1:9001",
    "neo4j+ssc://127.0.0.1:9001",
    "neo4j+s://127.0.0.1:9001",
    "neo4j://127.0.0.1:9001",
))
@pytest.mark.parametrize(
    ("test_config", "expected_failure", "expected_failure_message"),
    (
        ({"encrypted": False}, ConfigurationError, "The config settings"),
        ({"encrypted": True}, ConfigurationError, "The config settings"),
        (
            {"encrypted": True, "trusted_certificates": []},
            ConfigurationError, "The config settings"
        ),
        (
            {"trusted_certificates": []},
            ConfigurationError, "The config settings"
        ),
        (
            {"trusted_certificates": None},
            ConfigurationError, "The config settings"
        ),
    )
)
def test_driver_config_error(
    test_uri, test_config, expected_failure, expected_failure_message
):
    if "+" in test_uri:
        # `+s` and `+ssc` are short hand syntax for not having to configure the
        # encryption behavior of the driver. Specifying both is invalid.
        with pytest.raises(expected_failure, match=expected_failure_message):
            GraphDatabase.driver(test_uri, **test_config)
    else:
        GraphDatabase.driver(test_uri, **test_config)


@pytest.mark.parametrize("test_uri", (
    "http://localhost:9001",
    "ftp://localhost:9001",
    "x://localhost:9001",
))
def test_invalid_protocol(test_uri):
    with pytest.raises(ConfigurationError, match="scheme"):
        GraphDatabase.driver(test_uri)


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_sync_test
def test_driver_opens_write_session_by_default(uri, mocker):
    driver = GraphDatabase.driver(uri)
    from neo4j import Transaction

    # we set a specific db, because else the driver would try to fetch a RT
    # to get hold of the actual home database (which won't work in this
    # unittest)
    with driver.session(database="foobar") as session:
        with mock.patch.object(
            session._pool, "acquire", autospec=True
        ) as acquire_mock:
            with mock.patch.object(
                Transaction, "_begin", autospec=True
            ) as tx_begin_mock:
                tx = session.begin_transaction()
        acquire_mock.assert_called_once_with(
            access_mode=WRITE_ACCESS,
            timeout=mocker.ANY,
            database=mocker.ANY,
            bookmarks=mocker.ANY
        )
        tx_begin_mock.assert_called_once_with(
            tx,
            mocker.ANY,
            mocker.ANY,
            mocker.ANY,
            WRITE_ACCESS,
            mocker.ANY,
            mocker.ANY
        )
