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


def test_import_dunder_version():
    from neo4j import __version__


def test_import_graphdatabase():
    from neo4j import GraphDatabase


def test_import_async_graphdatabase():
    from neo4j import AsyncGraphDatabase


def test_import_driver():
    from neo4j import Driver


def test_import_async_driver():
    from neo4j import AsyncDriver


def test_import_boltdriver():
    from neo4j import BoltDriver


def test_import_async_boltdriver():
    from neo4j import AsyncBoltDriver


def test_import_neo4jdriver():
    from neo4j import Neo4jDriver


def test_import_async_neo4jdriver():
    from neo4j import AsyncNeo4jDriver


def test_import_auth():
    from neo4j import Auth


def test_import_authtoken():
    from neo4j import AuthToken


def test_import_basic_auth():
    from neo4j import basic_auth


def test_import_bearer_auth():
    from neo4j import bearer_auth


def test_import_kerberos_auth():
    from neo4j import kerberos_auth


def test_import_custom_auth():
    from neo4j import custom_auth


def test_import_read_access():
    from neo4j import READ_ACCESS


def test_import_write_access():
    from neo4j import WRITE_ACCESS


def test_import_transaction():
    from neo4j import Transaction


def test_import_async_transaction():
    from neo4j import AsyncTransaction


def test_import_record():
    from neo4j import Record


def test_import_session():
    from neo4j import Session


def test_import_async_session():
    from neo4j import AsyncSession


def test_import_sessionconfig():
    with pytest.warns(DeprecationWarning):
        from neo4j import SessionConfig


def test_import_query():
    from neo4j import Query


def test_import_result():
    from neo4j import Result


def test_import_async_result():
    from neo4j import AsyncResult


def test_import_resultsummary():
    from neo4j import ResultSummary


def test_import_unit_of_work():
    from neo4j import unit_of_work


def test_import_config():
    with pytest.warns(DeprecationWarning):
        from neo4j import Config


def test_import_poolconfig():
    with pytest.warns(DeprecationWarning):
        from neo4j import PoolConfig


def test_import_graph():
    from neo4j import graph


def test_import_graph_node():
    from neo4j.graph import Node


def test_import_graph_path():
    from neo4j.graph import Path


def test_import_graph_graph():
    from neo4j.graph import Graph


def test_import_spatial():
    from neo4j import spatial


def test_import_time():
    from neo4j import time


def test_import_exceptions():
    from neo4j import exceptions
