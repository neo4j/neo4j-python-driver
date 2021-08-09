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

import inspect
from unittest.mock import Mock

import pytest

from ..work import FakeConnection

from neo4j import READ_ACCESS
from neo4j.addressing import ResolvedAddress
from neo4j.conf import (
    PoolConfig,
    RoutingConfig,
    WorkspaceConfig
)
from neo4j.io import Neo4jPool


@pytest.fixture()
def opener():
    def open_(*_, **__):
        connection = FakeConnection()
        route_mock = Mock()
        route_mock.return_value = [{
            "ttl": 1000,
            "servers": [
                {"addresses": ["1.2.3.1:9001"], "role": "ROUTE"},
                {
                    "addresses": ["1.2.3.10:9010", "1.2.3.11:9011"],
                    "role": "READ"
                },
                {
                    "addresses": ["1.2.3.20:9020", "1.2.3.21:9021"],
                    "role": "WRITE"
                },
            ],
        }]
        connection.attach_mock(route_mock, "route")
        opener_.connections.append(connection)
        return connection

    opener_ = Mock()
    opener_.connections = []
    opener_.side_effect = open_
    return opener_


def test_acquires_new_routing_table_if_deleted(opener):
    address = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), address)
    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")

    del pool.routing_tables["test_db"]

    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")


def test_acquires_new_routing_table_if_stale(opener):
    address = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), address)
    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")

    old_value = pool.routing_tables["test_db"].last_updated_time
    pool.routing_tables["test_db"].ttl = 0

    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables["test_db"].last_updated_time > old_value


def test_removes_old_routing_table(opener):
    address = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), address)
    cx = pool.acquire(READ_ACCESS, 30, "test_db1", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db1")
    cx = pool.acquire(READ_ACCESS, 30, "test_db2", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db2")

    old_value = pool.routing_tables["test_db1"].last_updated_time
    pool.routing_tables["test_db1"].ttl = 0
    pool.routing_tables["test_db2"].ttl = \
        -RoutingConfig.routing_table_purge_delay

    cx = pool.acquire(READ_ACCESS, 30, "test_db1", None)
    pool.release(cx)
    assert pool.routing_tables["test_db1"].last_updated_time > old_value
    assert "test_db2" not in pool.routing_tables

