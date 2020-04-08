#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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

from neo4j.io._bolt3 import Bolt3
from neo4j.conf import PoolConfig
from neo4j.exceptions import (
    ConfigurationError,
)

# python -m pytest tests/unit/io/test_class_bolt3.py -s -v


def test_conn_timed_out(fake_socket):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 0
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    assert connection.timedout() is True


def test_conn_not_timed_out_if_not_enabled(fake_socket):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = -1
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    assert connection.timedout() is False


def test_conn_not_timed_out(fake_socket):
    address = ("127.0.0.1", 7687)
    max_connection_lifetime = 999999999
    connection = Bolt3(address, fake_socket(address), max_connection_lifetime)
    assert connection.timedout() is False


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


def test_n_extra_not_supported_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ValueError):
        connection.discard(n=666)


def test_qid_extra_not_supported_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ValueError):
        connection.discard(qid=666)


def test_simple_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt3(address, socket, PoolConfig.max_connection_lifetime)
    connection.pull()
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3F"
    assert len(fields) == 0


def test_n_extra_not_supported_in_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ValueError):
        connection.pull(n=666)


def test_qid_extra_not_supported_in_pull(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, fake_socket(address), PoolConfig.max_connection_lifetime)
    with pytest.raises(ValueError):
        connection.pull(qid=666)
