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


from neo4j.io._bolt4x0 import Bolt4x0


def test_conn_timed_out(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt4x0(address, fake_socket(address), max_age=0)
    assert connection.timedout() is True


def test_conn_not_timed_out_if_not_enabled(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt4x0(address, fake_socket(address), max_age=-1)
    assert connection.timedout() is False


def test_conn_not_timed_out(fake_socket):
    address = ("127.0.0.1", 7687)
    connection = Bolt4x0(address, fake_socket(address), max_age=999999999)
    assert connection.timedout() is False


def test_db_extra_in_begin(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket)
    connection.begin(db="something")
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x11"
    assert len(fields) == 1
    assert fields[0] == {"db": "something"}


def test_db_extra_in_run(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket)
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
    connection = Bolt4x0(address, socket)
    connection.discard(n=666)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": -1}


def test_qid_extra_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket)
    connection.discard(qid=666)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == {"n": -1, "qid": 666}


def test_n_and_qid_extras_in_discard(fake_socket):
    address = ("127.0.0.1", 7687)
    socket = fake_socket(address)
    connection = Bolt4x0(address, socket)
    connection.discard(n=666, qid=777)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2F"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}
