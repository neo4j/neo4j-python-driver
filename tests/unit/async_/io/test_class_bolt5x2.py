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
import itertools
import logging

import pytest

from neo4j._async.io._bolt5 import AsyncBolt5x2
from neo4j._conf import PoolConfig
from neo4j.api import Auth

from ...._async_compat import mark_async_test


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 0
    connection = AsyncBolt5x2(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = -1
    connection = AsyncBolt5x2(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 999999999
    connection = AsyncBolt5x2(address, fake_socket(address), max_connection_lifetime)
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
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
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
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.run(*args, **kwargs)
    await connection.send_all()
    tag, is_fields = await socket.pop_message()
    assert tag == b"\x10"
    assert tuple(is_fields) == expected_fields


@mark_async_test
async def test_n_extra_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
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
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
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
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_n_and_qid_extras_in_discard
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
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
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
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
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_qid_extra_in_pull
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(qid=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


@mark_async_test
async def test_n_and_qid_extras_in_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(n=666, qid=777)
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}


@mark_async_test
async def test_hello_passes_routing_metadata(fake_socket_pair):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x2.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/4.4.0"})
    await sockets.server.send_message(b"\x70", {})
    connection = AsyncBolt5x2(
        address, sockets.client, PoolConfig.max_connection_lifetime,
        routing_context={"foo": "bar"}
    )
    await connection.hello()
    tag, fields = await sockets.server.pop_message()
    assert tag == b"\x01"
    assert len(fields) == 1
    assert fields[0]["routing"] == {"foo": "bar"}


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
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x2.UNPACKER_CLS)
    sockets.client.settimeout = mocker.Mock()
    await sockets.server.send_message(
        b"\x70", {"server": "Neo4j/4.3.4", "hints": hints}
    )
    await sockets.server.send_message(b"\x70", {})
    connection = AsyncBolt5x2(
        address, sockets.client, PoolConfig.max_connection_lifetime
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


def _assert_notifications_in_extra(extra, expected):
    for key in expected:
        assert key in extra
        assert extra[key] == expected[key]



@pytest.mark.parametrize(("method", "args", "extra_idx"), (
    ("run", ("RETURN 1",), 2),
    ("begin", (), 0),
))
@pytest.mark.parametrize(
    ("cls_min_sev", "method_min_sev"),
    itertools.product((None, "WARNING", "OFF"), repeat=2)
)
@pytest.mark.parametrize(
    ("cls_dis_cats", "method_dis_cats"),
    itertools.product((None, [], ["HINT"], ["HINT", "DEPRECATION"]), repeat=2)
)
@mark_async_test
async def test_supports_notification_filters(
    fake_socket, method, args, extra_idx, cls_min_sev, method_min_sev,
    cls_dis_cats, method_dis_cats
):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x2.UNPACKER_CLS)
    connection = AsyncBolt5x2(
        address, socket, PoolConfig.max_connection_lifetime,
        notifications_min_severity=cls_min_sev,
        notifications_disabled_categories=cls_dis_cats
    )
    method = getattr(connection, method)

    method(*args, notifications_min_severity=method_min_sev,
           notifications_disabled_categories=method_dis_cats)
    await connection.send_all()

    _, fields = await socket.pop_message()
    extra = fields[extra_idx]
    expected = {}
    if method_min_sev is not None:
        expected["notifications_minimum_severity"] = method_min_sev
    if method_dis_cats is not None:
        expected["notifications_disabled_categories"] = method_dis_cats
    _assert_notifications_in_extra(extra, expected)


@pytest.mark.parametrize("min_sev", (None, "WARNING", "OFF"))
@pytest.mark.parametrize("dis_cats",
                         (None, [], ["HINT"], ["HINT", "DEPRECATION"]))
@mark_async_test
async def test_hello_supports_notification_filters(
    fake_socket_pair, min_sev, dis_cats
):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x2.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    connection = AsyncBolt5x2(
        address, sockets.client, PoolConfig.max_connection_lifetime,
        notifications_min_severity=min_sev,
        notifications_disabled_categories=dis_cats
    )

    await connection.hello()

    tag, fields = await sockets.server.pop_message()
    extra = fields[0]
    expected = {}
    if min_sev is not None:
        expected["notifications_minimum_severity"] = min_sev
    if dis_cats is not None:
        expected["notifications_disabled_categories"] = dis_cats
    _assert_notifications_in_extra(extra, expected)


class HackedAuth:
    def __init__(self, dict_):
        self.__dict__ = dict_


@mark_async_test
@pytest.mark.parametrize("auth", (
    ("awesome test user", "safe p4ssw0rd"),
    Auth("super nice scheme", "awesome test user", "safe p4ssw0rd"),
    Auth("super nice scheme", "awesome test user", "safe p4ssw0rd",
         realm="super duper realm"),
    Auth("super nice scheme", "awesome test user", "safe p4ssw0rd",
         realm="super duper realm"),
    Auth("super nice scheme", "awesome test user", "safe p4ssw0rd",
         foo="bar"),
    HackedAuth({
        "scheme": "super nice scheme", "principal": "awesome test user",
        "credentials": "safe p4ssw0rd", "realm": "super duper realm",
        "parameters": {"credentials": "should be visible!"},
    })

))
async def test_hello_does_not_log_credentials(fake_socket_pair, caplog, auth):
    def items():
        if isinstance(auth, tuple):
            yield "scheme", "basic"
            yield "principal", auth[0]
            yield "credentials", auth[1]
        elif isinstance(auth, Auth):
            for key in ("scheme", "principal", "credentials", "realm",
                        "parameters"):
                value = getattr(auth, key, None)
                if value:
                    yield key, value
        elif isinstance(auth, HackedAuth):
            yield from auth.__dict__.items()
        else:
            raise TypeError(auth)

    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x2.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt5x2(address, sockets.client,
                              max_connection_lifetime, auth=auth)

    with caplog.at_level(logging.DEBUG):
        await connection.hello()

    logons = [m for m in caplog.messages if "C: LOGON " in m]
    assert len(logons) == 1
    logon = logons[0]

    for key, value in items():
        if key == "credentials":
            assert value not in logon
        else:
            assert str({key: value})[1:-1] in logon
