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


import logging
from itertools import permutations

import pytest

import neo4j
from neo4j._conf import PoolConfig
from neo4j._sync.io._bolt4 import Bolt4x1
from neo4j.exceptions import ConfigurationError

from ...._async_compat import mark_sync_test


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 0
    connection = Bolt4x1(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = -1
    connection = Bolt4x1(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 999999999
    connection = Bolt4x1(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@mark_sync_test
def test_db_extra_in_begin(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.begin(db="something")
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x11"
    assert len(fields) == 1
    assert fields[0] == {"db": "something"}


@mark_sync_test
def test_db_extra_in_run(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.run("", {}, db="something")
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x10"
    assert len(fields) == 3
    assert fields[0] == ""
    assert fields[1] == {}
    assert fields[2] == {"db": "something"}


@mark_sync_test
def test_n_extra_in_discard(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.discard(n=666)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666}


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (666, {"n": -1, "qid": 666}),
        (-1, {"n": -1}),
    ]
)
@mark_sync_test
def test_qid_extra_in_discard(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.discard(qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (777, {"n": 666, "qid": 777}),
        (-1, {"n": 666}),
    ]
)
@mark_sync_test
def test_n_and_qid_extras_in_discard(fake_socket, test_input, expected):
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_n_and_qid_extras_in_discard
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.discard(n=666, qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (666, {"n": 666}),
        (-1, {"n": -1}),
    ]
)
@mark_sync_test
def test_n_extra_in_pull(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(n=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (777, {"n": -1, "qid": 777}),
        (-1, {"n": -1}),
    ]
)
@mark_sync_test
def test_qid_extra_in_pull(fake_socket, test_input, expected):
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_qid_extra_in_pull
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


@mark_sync_test
def test_n_and_qid_extras_in_pull(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt4x1.UNPACKER_CLS)
    connection = Bolt4x1(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(n=666, qid=777)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}


@mark_sync_test
def test_hello_passes_routing_metadata(fake_socket_pair):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=Bolt4x1.PACKER_CLS,
                               unpacker_cls=Bolt4x1.UNPACKER_CLS)
    sockets.server.send_message(b"\x70", {"server": "Neo4j/4.1.0"})
    connection = Bolt4x1(
        address, sockets.client, PoolConfig.max_connection_lifetime,
        routing_context={"foo": "bar"}
    )
    connection.hello()
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x01"
    assert len(fields) == 1
    assert fields[0]["routing"] == {"foo": "bar"}


@pytest.mark.parametrize("recv_timeout", (1, -1))
@mark_sync_test
def test_hint_recv_timeout_seconds_gets_ignored(
    fake_socket_pair, recv_timeout, mocker
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=Bolt4x1.PACKER_CLS,
                               unpacker_cls=Bolt4x1.UNPACKER_CLS)
    sockets.client.settimeout = mocker.Mock()
    sockets.server.send_message(b"\x70", {
        "server": "Neo4j/4.1.0",
        "hints": {"connection.recv_timeout_seconds": recv_timeout},
    })
    connection = Bolt4x1(address, sockets.client,
                         PoolConfig.max_connection_lifetime)
    connection.hello()
    sockets.client.settimeout.assert_not_called()


CREDENTIALS = "+++super-secret-sauce+++"


@pytest.mark.parametrize("auth", (
    ("user", CREDENTIALS),
    neo4j.basic_auth("user", CREDENTIALS),
    neo4j.kerberos_auth(CREDENTIALS),
    neo4j.bearer_auth(CREDENTIALS),
    neo4j.custom_auth("user", CREDENTIALS, "realm", "scheme"),
    neo4j.Auth("scheme", "principal", CREDENTIALS, "realm", foo="bar"),
))
@mark_sync_test
def test_credentials_are_not_logged(
    auth, fake_socket_pair, mocker, caplog
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=Bolt4x1.PACKER_CLS,
                               unpacker_cls=Bolt4x1.UNPACKER_CLS)
    sockets.client.settimeout = mocker.Mock()
    sockets.server.send_message(b"\x70", {"server": "Neo4j/4.3.4"})
    connection = Bolt4x1(
        address, sockets.client, PoolConfig.max_connection_lifetime, auth=auth
    )
    with caplog.at_level(logging.DEBUG):
        connection.hello()

    if isinstance(auth, tuple):
        auth = neo4j.basic_auth(*auth)
    for field in ("scheme", "principal", "realm", "parameters"):
        value = getattr(auth, field, None)
        if value:
            assert repr(value) in caplog.text
    assert CREDENTIALS not in caplog.text


@pytest.mark.parametrize("message", ("logon", "logoff"))
def test_auth_message_raises_configuration_error(message, fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = Bolt4x1(address, fake_socket(address),
                              PoolConfig.max_connection_lifetime)
    with pytest.raises(ConfigurationError,
                       match="Session level authentication is not supported"):
        getattr(connection, message)()


@pytest.mark.parametrize("auth", (
    None,
    neo4j.Auth("scheme", "principal", "credentials", "realm"),
    ("user", "password"),
))
@mark_sync_test
def test_re_auth_noop(auth, fake_socket, mocker):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = Bolt4x1(address, fake_socket(address),
                              PoolConfig.max_connection_lifetime, auth=auth)
    logon_spy = mocker.spy(connection, "logon")
    logoff_spy = mocker.spy(connection, "logoff")
    res = connection.re_auth(auth, None)

    assert res is False
    logon_spy.assert_not_called()
    logoff_spy.assert_not_called()


@pytest.mark.parametrize(
    ("auth1", "auth2"),
    permutations(
        (
            None,
            neo4j.Auth("scheme", "principal", "credentials", "realm"),
            ("user", "password"),
        ),
        2
    )
)
@mark_sync_test
def test_re_auth(auth1, auth2, fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = Bolt4x1(address, fake_socket(address),
                              PoolConfig.max_connection_lifetime, auth=auth1)
    with pytest.raises(ConfigurationError,
                       match="Session level authentication is not supported"):
        connection.re_auth(auth2, None)
