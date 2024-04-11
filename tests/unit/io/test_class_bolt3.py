#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

import contextlib
import itertools
from unittest.mock import MagicMock

import pytest

from neo4j import Address
from neo4j.io._bolt3 import Bolt3
from neo4j.conf import PoolConfig
from neo4j.exceptions import (
    ConfigurationError,
)

# python -m pytest tests/unit/io/test_class_bolt3.py -s -v


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 0
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = -1
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 999999999
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


def test_db_extra_not_supported_in_begin(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ConfigurationError):
        connection.begin(db="something")


def test_db_extra_not_supported_in_run(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ConfigurationError):
        connection.run("", db="something")


def test_simple_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt3(address, socket, PoolConfig.max_connection_lifetime)
    connection.discard()
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 0


def test_simple_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt3(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull()
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 0


@pytest.mark.parametrize("recv_timeout", (1, -1))
def test_hint_recv_timeout_seconds_gets_ignored(fake_socket_pair, recv_timeout):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address)
    sockets.client.settimeout = MagicMock()
    sockets.server.send_message(0x70, {
        "server": "Neo4j/3.5.0",
        "hints": {"connection.recv_timeout_seconds": recv_timeout},
    })
    connection = Bolt3(address, sockets.client,
                       PoolConfig.max_connection_lifetime)
    connection.hello()
    sockets.client.settimeout.assert_not_called()


@pytest.mark.parametrize(
    "actions",
    itertools.combinations_with_replacement(
        itertools.product(
            ("run", "begin", "begin_run"),
            ("reset", "commit", "rollback"),
            (None, "some_db", "another_db"),
        ),
        2
    )
)
def test_tracks_last_database(fake_socket_pair, actions):
    address = Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(address)
    connection = Bolt3(address, sockets.client, 0)
    sockets.server.send_message(0x70, {"server": "Neo4j/1.2.3"})
    connection.hello()
    assert connection.last_database is None
    for action, finish, db in actions:
        sockets.server.send_message(0x70, {})
        if action == "run":
            with raises_if_db(db):
                connection.run("RETURN 1", db=db)
        elif action == "begin":
            with raises_if_db(db):
                connection.begin(db=db)
        elif action == "begin_run":
            with raises_if_db(db):
                connection.begin(db=db)
            assert connection.last_database is None
            sockets.server.send_message(0x70, {})
            connection.run("RETURN 1")
        else:
            raise ValueError(action)

        assert connection.last_database is None
        connection.send_all()
        connection.fetch_all()
        assert connection.last_database is None

        sockets.server.send_message(0x70, {})
        if finish == "reset":
            connection.reset()
        elif finish == "commit":
            if action == "run":
                connection.pull()
            else:
                connection.commit()
        elif finish == "rollback":
            if action == "run":
                connection.pull()
            else:
                connection.rollback()
        else:
            raise ValueError(finish)

        connection.send_all()
        connection.fetch_all()

        assert connection.last_database is None


@contextlib.contextmanager
def raises_if_db(db):
    if db is None:
        yield
    else:
        with pytest.raises(ConfigurationError,
                           match="selecting database is not supported"):
            yield
