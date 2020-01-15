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


from unittest import TestCase
import pytest

from neo4j.exceptions import (
    ClientError,
    ServiceUnavailable,
)
from neo4j.io._bolt3 import Bolt3


class FakeSocket:
    def __init__(self, address):
        self.address = address

    def setblocking(self, flag):
        pass

    def getpeername(self):
        return self.address

    def sendall(self, data):
        return

    def close(self):
        return


def test_conn_timed_out():
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, FakeSocket(address), max_age=0)
    assert connection.timedout() is True


def test_conn_not_timed_out_if_not_enabled():
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, FakeSocket(address), max_age=-1)
    assert connection.timedout() is False


def test_conn_not_timed_out():
    address = ("127.0.0.1", 7687)
    connection = Bolt3(address, FakeSocket(address), max_age=999999999)
    assert connection.timedout() is False
