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

# python -m pytest tests/unit/test_import_neo4j.py -s -v


def test_import_dunder_version():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_dunder_version
    from neo4j import __version__


def test_import_graphdatabase():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_graphdatabase
    from neo4j import GraphDatabase


def test_import_driver():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_driver
    from neo4j import Driver


def test_import_boltdriver():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_boltdriver
    from neo4j import BoltDriver


def test_import_neo4jdriver():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_neo4jdriver
    from neo4j import Neo4jDriver


def test_import_auth():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_auth
    from neo4j import Auth


def test_import_authtoken():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_authtoken
    from neo4j import AuthToken


def test_import_basic_auth():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_auth
    from neo4j import basic_auth


def test_import_kerberos_auth():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_kerberos_auth
    from neo4j import kerberos_auth


def test_import_custom_auth():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_custom_auth
    from neo4j import custom_auth


def test_import_read_access():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_read_access
    from neo4j import READ_ACCESS


def test_import_write_access():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_write_access
    from neo4j import WRITE_ACCESS


def test_import_transaction():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_transaction
    from neo4j import Transaction


def test_import_record():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_record
    from neo4j import Record


def test_import_session():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_session
    from neo4j import Session


def test_import_sessionconfig():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_sessionconfig
    from neo4j import SessionConfig


def test_import_query():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_query
    from neo4j import Query


def test_import_result():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_result
    from neo4j import Result


def test_import_resultsummary():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_resultsummary
    from neo4j import ResultSummary


def test_import_unit_of_work():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_unit_of_work
    from neo4j import unit_of_work


def test_import_config():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_config
    from neo4j import Config


def test_import_poolconfig():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_poolconfig
    from neo4j import PoolConfig


def test_import_graph():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_graph
    import neo4j.graph as graph


def test_import_graph_node():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_graph_node
    from neo4j.graph import Node


def test_import_graph_Path():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_graph_Path
    from neo4j.graph import Path


def test_import_graph_graph():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_graph_graph
    from neo4j.graph import Graph


def test_import_spatial():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_spatial
    import neo4j.spatial as spatial


def test_import_time():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_time
    import neo4j.time as time


def test_import_exceptions():
    # python -m pytest tests/unit/test_import_neo4j.py -s -v -k test_import_exceptions
    import neo4j.exceptions as exceptions


