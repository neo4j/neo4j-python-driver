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

from neo4j._sync.io._bolt5 import Bolt5x2
from neo4j.api import Auth

from ...._async_compat import mark_sync_test


# TODO: proper testing should come from the re-auth ADR,
#       which properly introduces Bolt 5.1

class HackedAuth:
    def __init__(self, dict_):
        self.__dict__ = dict_


@mark_sync_test
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
def test_hello_does_not_log_credentials(fake_socket_pair, caplog, auth):
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
                               packer_cls=Bolt5x2.PACKER_CLS,
                               unpacker_cls=Bolt5x2.UNPACKER_CLS)
    sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = Bolt5x2(address, sockets.client,
                              max_connection_lifetime, auth=auth)

    with caplog.at_level(logging.DEBUG):
        connection.hello()

    logons = [m for m in caplog.messages if "C: LOGON " in m]
    assert len(logons) == 1
    logon = logons[0]

    for key, value in items():
        if key == "credentials":
            assert value not in logon
        else:
            assert str({key: value})[1:-1] in logon
