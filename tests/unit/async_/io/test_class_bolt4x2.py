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


import pytest

from neo4j._async.io._bolt4 import AsyncBolt4x2
from neo4j._conf import PoolConfig
from neo4j.exceptions import ConfigurationError

from ...._async_compat import mark_async_test


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 0
    connection = AsyncBolt4x2(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = -1
    connection = AsyncBolt4x2(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 999999999
    connection = AsyncBolt4x2(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@mark_async_test
async def test_db_extra_in_begin(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.begin(db="something")
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x11"
    assert len(fields) == 1
    assert fields[0] == {"db": "something"}


@mark_async_test
async def test_db_extra_in_run(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.run("", {}, db="something")
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x10"
    assert len(fields) == 3
    assert fields[0] == ""
    assert fields[1] == {}
    assert fields[2] == {"db": "something"}


@mark_async_test
async def test_n_extra_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
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
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
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
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
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
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
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
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(qid=test_input)
    await connection.send_all()
    tag, fields = await socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


@mark_async_test
async def test_n_and_qid_extras_in_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket, PoolConfig.max_connection_lifetime)
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
                               packer_cls=AsyncBolt4x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x2.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/4.2.0"})
    connection = AsyncBolt4x2(
        address, sockets.client, PoolConfig.max_connection_lifetime,
        routing_context={"foo": "bar"}
    )
    await connection.hello()
    tag, fields = await sockets.server.pop_message()
    assert tag == b"\x01"
    assert len(fields) == 1
    assert fields[0]["routing"] == {"foo": "bar"}


@pytest.mark.parametrize("recv_timeout", (1, -1))
@mark_async_test
async def test_hint_recv_timeout_seconds_gets_ignored(
    fake_socket_pair, recv_timeout, mocker
):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt4x2.PACKER_CLS,
                               unpacker_cls=AsyncBolt4x2.UNPACKER_CLS)
    sockets.client.settimeout = mocker.AsyncMock()
    await sockets.server.send_message(b"\x70", {
        "server": "Neo4j/4.2.0",
        "hints": {"connection.recv_timeout_seconds": recv_timeout},
    })
    connection = AsyncBolt4x2(
        address, sockets.client, PoolConfig.max_connection_lifetime
    )
    await connection.hello()
    sockets.client.settimeout.assert_not_called()


@pytest.mark.parametrize(("method", "args"), (
    ("run", ("RETURN 1",)),
    ("begin", ()),
))
def test_does_not_support_notification_filters(fake_socket, method, args):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(address, socket,
                              PoolConfig.max_connection_lifetime)
    method = getattr(connection, method)
    with pytest.raises(ConfigurationError, match="Notification filters"):
        method(*args, notification_filters={"*.*"})


@mark_async_test
async def test_hello_does_not_support_notification_filters(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt4x2.UNPACKER_CLS)
    connection = AsyncBolt4x2(
        address, socket, PoolConfig.max_connection_lifetime,
        notification_filters={"*.*"}
    )
    with pytest.raises(ConfigurationError, match="Notification filters"):
        await connection.hello()
