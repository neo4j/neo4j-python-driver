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

import pytest

from neo4j._async.io._bolt5 import AsyncBolt5x1
from neo4j._conf import PoolConfig
from neo4j._meta import BOLT_AGENT
from neo4j.api import Auth
from neo4j.exceptions import ConfigurationError

from ...._async_compat import mark_async_test


# TODO: proper testing should come from the re-auth ADR,
#       which properly introduces Bolt 5.1


@pytest.mark.parametrize(("method", "args"), (
    ("run", ("RETURN 1",)),
    ("begin", ()),
))
@pytest.mark.parametrize("kwargs", (
    {"notifications_min_severity": "WARNING"},
    {"notifications_disabled_categories": ["HINT"]},
    {"notifications_disabled_categories": []},
    {
        "notifications_min_severity": "WARNING",
        "notifications_disabled_categories": ["HINT"]
    },
))
def test_does_not_support_notification_filters(fake_socket, method,
                                               args, kwargs):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x1.UNPACKER_CLS)
    connection = AsyncBolt5x1(address, socket,
                            PoolConfig.max_connection_lifetime)
    method = getattr(connection, method)
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        method(*args, **kwargs)


@mark_async_test
@pytest.mark.parametrize("kwargs", (
    {"notifications_min_severity": "WARNING"},
    {"notifications_disabled_categories": ["HINT"]},
    {"notifications_disabled_categories": []},
    {
        "notifications_min_severity": "WARNING",
        "notifications_disabled_categories": ["HINT"]
    },
))
async def test_hello_does_not_support_notification_filters(
    fake_socket, kwargs
):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address, AsyncBolt5x1.UNPACKER_CLS)
    connection = AsyncBolt5x1(
        address, socket, PoolConfig.max_connection_lifetime,
        **kwargs
    )
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        await connection.hello()


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
                               packer_cls=AsyncBolt5x1.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x1.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt5x1(
        address, sockets.client, max_connection_lifetime, auth=auth
    )

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


@mark_async_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", BOLT_AGENT)
)
async def test_user_agent(fake_socket_pair, user_agent):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x1.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x1.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt5x1(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    await connection.hello()

    tag, fields = await sockets.server.pop_message()
    extra = fields[0]
    if user_agent is None:
        assert extra["user_agent"] == BOLT_AGENT
    else:
        assert extra["user_agent"] == user_agent


@mark_async_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", BOLT_AGENT)
)
async def test_does_not_send_bolt_agent(fake_socket_pair, user_agent):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address,
                               packer_cls=AsyncBolt5x1.PACKER_CLS,
                               unpacker_cls=AsyncBolt5x1.UNPACKER_CLS)
    await sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    await sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = AsyncBolt5x1(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    await connection.hello()

    tag, fields = await sockets.server.pop_message()
    extra = fields[0]
    assert "bolt_agent" not in extra
