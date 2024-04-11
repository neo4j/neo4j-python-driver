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

import itertools
from unittest.mock import MagicMock

import pytest

from neo4j import Address
from neo4j.io._bolt4 import Bolt4x0
from neo4j.conf import PoolConfig


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 0
    connection = Bolt4x0(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = -1
    connection = Bolt4x0(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 999999999
    connection = Bolt4x0(address, fake_socket(address), max_connection_lifetime)
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


def test_db_extra_in_begin(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
    connection.begin(db="something")
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x11"
    assert len(fields) == 1
    assert fields[0] == {"db": "something"}


def test_db_extra_in_run(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
    connection.run("", {}, db="something")
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x10"
    assert len(fields) == 3
    assert fields[0] == ""
    assert fields[1] == {}
    assert fields[2] == {"db": "something"}


def test_n_extra_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
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
def test_qid_extra_in_discard(fake_socket, test_input, expected):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
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
def test_n_and_qid_extras_in_discard(fake_socket, test_input, expected):
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_n_and_qid_extras_in_discard
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
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
def test_n_extra_in_pull(fake_socket, test_input, expected):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
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
def test_qid_extra_in_pull(fake_socket, test_input, expected):
    # python -m pytest tests/unit/io/test_class_bolt4x0.py -s -k test_qid_extra_in_pull
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == expected


def test_n_and_qid_extras_in_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull(n=666, qid=777)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}


@pytest.mark.parametrize("recv_timeout", (1, -1))
def test_hint_recv_timeout_seconds_gets_ignored(fake_socket_pair, recv_timeout):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address)
    sockets.client.settimeout = MagicMock()
    sockets.server.send_message(0x70, {
        "server": "Neo4j/4.0.0",
        "hints": {"connection.recv_timeout_seconds": recv_timeout},
    })
    connection = Bolt4x0(address, sockets.client,
                         PoolConfig.max_connection_lifetime)
    connection.hello()
    sockets.client.settimeout.assert_not_called()


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
            "foo",
            ValueError("Timeout must be specified as a number of seconds")
        ),
        (
            [1, 2],
            TypeError("Timeout must be specified as a number of seconds")
        )
    )
)
def test_tx_timeout(fake_socket_pair, func, args, extra_idx, timeout, res):
    address = ("127.0.0.1", 7687)
    sockets = fake_socket_pair(address)
    sockets.server.send_message(0x70, {})
    connection = Bolt4x0(address, sockets.client, 0)
    func = getattr(connection, func)
    if isinstance(res, Exception):
        with pytest.raises(type(res), match=str(res)):
            func(*args, timeout=timeout)
    else:
        func(*args, timeout=timeout)
        connection.send_all()
        tag, fields = sockets.server.pop_message()
        extra = fields[extra_idx]
        if timeout is None:
            assert "tx_timeout" not in extra
        else:
            assert extra["tx_timeout"] == res


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
    connection = Bolt4x0(address, sockets.client, 0)
    sockets.server.send_message(0x70, {"server": "Neo4j/1.2.3"})
    connection.hello()
    assert connection.last_database is None
    for action, finish, db in actions:
        sockets.server.send_message(0x70, {})
        if action == "run":
            connection.run("RETURN 1", db=db)
        elif action == "begin":
            connection.begin(db=db)
        elif action == "begin_run":
            connection.begin(db=db)
            assert connection.last_database == db
            sockets.server.send_message(0x70, {})
            connection.run("RETURN 1")
        else:
            raise ValueError(action)

        assert connection.last_database == db
        connection.send_all()
        connection.fetch_all()
        assert connection.last_database == db

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

        assert connection.last_database == db
