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


import itertools
import logging

import pytest

import neo4j
from neo4j._api import TelemetryAPI
from neo4j._async.config import AsyncPoolConfig
from neo4j._async.io._bolt4 import AsyncBolt4x4
from neo4j._meta import USER_AGENT
from neo4j.exceptions import ConfigurationError

from ...._async_compat import mark_async_test


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 0
    connection = AsyncBolt4x4(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = -1
    connection = AsyncBolt4x4(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 999999999
    connection = AsyncBolt4x4(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize(("args", "kwargs", "expected_fields"), (
    (("", {}), {"db": "something"}, ({"db": "something"},)),
    (("", {}), {"imp_user": "imposter"}, ({"imp_user": "imposter"},)),
    (
        ("", {}),
        {"db": "something", "imp_user": "imposter"},
        ({"db": "something", "imp_user": "imposter"},)
    ),
))
@mark_async_test
async def test_extra_in_begin(fake_socket, args, kwargs, expected_fields):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.begin(*args, **kwargs)
    await connection.send_all()
    tag, is_fields = await socket.pop_message()
    assert tag == b"\x11"
    assert tuple(is_fields) == expected_fields


@pytest.mark.parametrize(("args", "kwargs", "expected_fields"), (
    (("", {}), {"db": "something"}, ("", {}, {"db": "something"})),
    (("", {}), {"imp_user": "imposter"}, ("", {}, {"imp_user": "imposter"})),
    (
        ("", {}),
        {"db": "something", "imp_user": "imposter"},
        ("", {}, {"db": "something", "imp_user": "imposter"})
    ),
))
@mark_async_test
async def test_extra_in_run(fake_socket, args, kwargs, expected_fields):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.run(*args, **kwargs)
    await connection.send_all()
    tag, is_fields = await socket.pop_message()
    assert tag == b"\x10"
    assert tuple(is_fields) == expected_fields


@mark_async_test
async def test_n_extra_in_discard(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.discard(n=666)
    await connection.send_all()
    tag, fields = await socket.pop_message()
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
@mark_async_test
async def test_qid_extra_in_discard(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.discard(qid=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
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
@mark_async_test
async def test_n_and_qid_extras_in_discard(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.discard(n=666, qid=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
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
@mark_async_test
async def test_n_extra_in_pull(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.pull(n=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
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
@mark_async_test
async def test_qid_extra_in_pull(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.pull(qid=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


@mark_async_test
async def test_n_and_qid_extras_in_pull(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket, AsyncPoolConfig.max_connection_lifetime)
    connection.pull(n=666, qid=777)
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}


@mark_async_test
async def test_hello_passes_routing_metadata(fake_socket_pair):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/4.4.0"})
    connection = AsyncBolt4x4(
        address, sockets.client, AsyncPoolConfig.max_connection_lifetime,
        routing_context={"foo": "bar"}
    )
    await connection.hello()
    tag, fields = await sockets.server.pop_message()
    assert tag == b"\x01"
    assert len(fields) == 1
    assert fields[0]["routing"] == {"foo": "bar"}


@pytest.mark.parametrize("api", TelemetryAPI)
@pytest.mark.parametrize("serv_enabled", (True, False))
@pytest.mark.parametrize("driver_disabled", (True, False))
@mark_async_test
async def test_telemetry_message(
    fake_socket, api, serv_enabled, driver_disabled
):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(
        address, socket, AsyncPoolConfig.max_connection_lifetime,
        telemetry_disabled=driver_disabled
    )
    if serv_enabled:
        connection.configuration_hints["telemetry.enabled"] = True
    connection.telemetry(api)
    await connection.send_all()

    with pytest.raises(OSError):
        await socket.pop_message()


@pytest.mark.parametrize(("hints", "valid"), (
    ({"connection.recv_timeout_seconds": 1}, True),
    ({"connection.recv_timeout_seconds": 42}, True),
    ({}, True),
    ({"whatever_this_is": "ignore me!"}, True),
    ({"connection.recv_timeout_seconds": -1}, False),
    ({"connection.recv_timeout_seconds": 0}, False),
    ({"connection.recv_timeout_seconds": 2.5}, False),
    ({"connection.recv_timeout_seconds": None}, False),
    ({"connection.recv_timeout_seconds": False}, False),
    ({"connection.recv_timeout_seconds": "1"}, False),
))
@mark_async_test
async def test_hint_recv_timeout_seconds(
    fake_socket_pair, hints, valid, caplog, mocker
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    sockets.client.settimeout = mocker.Mock()
    await sockets.server.send_message(
        b"\x70", {"server": "Neo4j/4.3.4", "hints": hints}
    )
    connection = AsyncBolt4x4(
        address, sockets.client, AsyncPoolConfig.max_connection_lifetime
    )
    with caplog.at_level(logging.INFO):
        await connection.hello()
    if valid:
        if "connection.recv_timeout_seconds" in hints:
            sockets.client.settimeout.assert_called_once_with(
                hints["connection.recv_timeout_seconds"]
            )
        else:
            sockets.client.settimeout.assert_not_called()
        assert not any("recv_timeout_seconds" in msg
                       and "invalid" in msg
                       for msg in caplog.messages)
    else:
        sockets.client.settimeout.assert_not_called()
        assert any(repr(hints["connection.recv_timeout_seconds"]) in msg
                   and "recv_timeout_seconds" in msg
                   and "invalid" in msg
                   for msg in caplog.messages)


CREDENTIALS = "+++super-secret-sauce+++"


@pytest.mark.parametrize("auth", (
    ("user", CREDENTIALS),
    neo4j.basic_auth("user", CREDENTIALS),
    neo4j.kerberos_auth(CREDENTIALS),
    neo4j.bearer_auth(CREDENTIALS),
    neo4j.custom_auth("user", CREDENTIALS, "realm", "scheme"),
    neo4j.Auth("scheme", "principal", CREDENTIALS, "realm", foo="bar"),
))
@mark_async_test
async def test_credentials_are_not_logged(
    auth, fake_socket_pair, mocker, caplog
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    sockets.client.settimeout = mocker.Mock()
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/4.3.4"})
    connection = AsyncBolt4x4(
        address, sockets.client, AsyncPoolConfig.max_connection_lifetime, auth=auth
    )
    with caplog.at_level(logging.DEBUG):
        await connection.hello()

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
    connection = AsyncBolt4x4(address, fake_socket(address),
                              AsyncPoolConfig.max_connection_lifetime)
    with pytest.raises(ConfigurationError,
                       match="User switching is not supported"):
        getattr(connection, message)()


@pytest.mark.parametrize("auth", (
    None,
    neo4j.Auth("scheme", "principal", "credentials", "realm"),
    ("user", "password"),
))
@mark_async_test
async def test_re_auth_noop(auth, fake_socket, mocker):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = AsyncBolt4x4(address, fake_socket(address),
                              AsyncPoolConfig.max_connection_lifetime, auth=auth)
    logon_spy = mocker.spy(connection, "logon")
    logoff_spy = mocker.spy(connection, "logoff")
    res = connection.re_auth(auth, None)

    assert res is False
    logon_spy.assert_not_called()
    logoff_spy.assert_not_called()


@pytest.mark.parametrize(
    ("auth1", "auth2"),
    itertools.permutations(
        (
            None,
            neo4j.Auth("scheme", "principal", "credentials", "realm"),
            ("user", "password"),
        ),
        2
    )
)
@mark_async_test
async def test_re_auth(auth1, auth2, fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    connection = AsyncBolt4x4(address, fake_socket(address),
                              AsyncPoolConfig.max_connection_lifetime, auth=auth1)
    with pytest.raises(ConfigurationError,
                       match="User switching is not supported"):
        connection.re_auth(auth2, None)


@pytest.mark.parametrize(("method", "args"), (
    ("run", ("RETURN 1",)),
    ("begin", ()),
))
@pytest.mark.parametrize("kwargs", (
    {"notifications_min_severity": "WARNING"},
    {"notifications_disabled_classifications": ["HINT"]},
    {"notifications_disabled_classifications": []},
    {
        "notifications_min_severity": "WARNING",
        "notifications_disabled_classifications": ["HINT"]
    },
))
def test_does_not_support_notification_filters(fake_socket, method,
                                               args, kwargs):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(address, socket,
                            AsyncPoolConfig.max_connection_lifetime)
    method = getattr(connection, method)
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        method(*args, **kwargs)


@mark_async_test
@pytest.mark.parametrize("kwargs", (
    {"notifications_min_severity": "WARNING"},
    {"notifications_disabled_classifications": ["HINT"]},
    {"notifications_disabled_classifications": []},
    {
        "notifications_min_severity": "WARNING",
        "notifications_disabled_classifications": ["HINT"]
    },
))
async def test_hello_does_not_support_notification_filters(
    fake_socket, kwargs
):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, AsyncBolt4x4.UNPACKER_CLS)
    connection = AsyncBolt4x4(
        address, socket, AsyncPoolConfig.max_connection_lifetime,
        **kwargs
    )
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        await connection.hello()


@mark_async_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", USER_AGENT)
)
async def test_user_agent(fake_socket_pair, user_agent):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt4x4(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    await connection.hello()

    tag, fields = await sockets.server.pop_message()
    extra = fields[0]
    if not user_agent:
        assert extra["user_agent"] == USER_AGENT
    else:
        assert extra["user_agent"] == user_agent


@mark_async_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", USER_AGENT)
)
async def test_does_not_send_bolt_agent(fake_socket_pair, user_agent):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt4x4(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    await connection.hello()

    tag, fields = await sockets.server.pop_message()
    extra = fields[0]
    assert "bolt_agent" not in extra


@mark_async_test
@pytest.mark.parametrize(
    ("func", "args", "extra_idx"),
    (
        ("run", ("RETURN 1",), 2),
        ("begin", (), 0),
    )
)
@pytest.mark.parametrize(
    ("timeout", "res"),
    (
        (None, None),
        (0, 0),
        (0.1, 100),
        (0.001, 1),
        (1e-15, 1),
        (0.0005, 1),
        (0.0001, 1),
        (1.0015, 1002),
        (1.000499, 1000),
        (1.0025, 1002),
        (3.0005, 3000),
        (3.456, 3456),
        (1, 1000),
        (
            -1e-15,
            ValueError("Timeout must be a positive number or 0")
        ),
        (
            "foo",
            ValueError("Timeout must be specified as a number of seconds")
        ),
        (
            [1, 2],
            TypeError("Timeout must be specified as a number of seconds")
        )
    )
)
async def test_tx_timeout(
    fake_socket_pair, func, args, extra_idx, timeout, res
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x4.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x4.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {})
    connection = AsyncBolt4x4(address, sockets.client, 0)
    func = getattr(connection, func)
    if isinstance(res, Exception):
        with pytest.raises(type(res), match=str(res)):
            func(*args, timeout=timeout)
    else:
        func(*args, timeout=timeout)
        await connection.send_all()
        tag, fields = await sockets.server.pop_message()
        extra = fields[extra_idx]
        if timeout is None:
            assert "tx_timeout" not in extra
        else:
            assert extra["tx_timeout"] == res
