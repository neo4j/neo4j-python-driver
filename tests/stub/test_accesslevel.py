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

from neo4j import GraphDatabase
from neo4j.exceptions import (
    CypherError,
    TransientError,
)

from tests.stub.conftest import StubCluster

# python -m pytest tests/stub/test_accesslevel.py -s -v


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/return_1_in_read_tx.script"),
        ("v4x0/router.script", "v4x0/tx_return_1_port_9004.script"),
    ]
)
def test_read_transaction(driver_info, test_scripts):
    # python -m pytest tests/stub/test_accesslevel.py -s -v -k test_read_transaction
    with StubCluster(*test_scripts):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                value = session.read_transaction(unit_of_work)
                assert value == 1


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/return_1_in_write_tx.script"),
        ("v4x0/router.script", "v4x0/tx_return_1_port_9006.script"),
    ]
)
def test_write_transaction(driver_info, test_scripts):
    # python -m pytest tests/stub/test_accesslevel.py -s -v -k test_write_transaction
    with StubCluster(*test_scripts):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                value = session.write_transaction(unit_of_work)
                assert value == 1


def test_read_transaction_with_error(driver_info):
    with StubCluster("v3/router.script", "v3/error_in_read_tx.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    tx.run("X")

                with pytest.raises(CypherError):
                    _ = session.read_transaction(unit_of_work)


def test_write_transaction_with_error(driver_info):
    with StubCluster("v3/router.script", "v3/error_in_write_tx.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    tx.run("X")

                with pytest.raises(CypherError):
                    _ = session.write_transaction(unit_of_work)


def test_two_subsequent_read_transactions(driver_info):
    with StubCluster("v3/router.script",
                     "v3/return_1_in_read_tx_twice.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                value = session.read_transaction(unit_of_work)
                assert value == 1
                value = session.read_transaction(unit_of_work)
                assert value == 1


def test_two_subsequent_write_transactions(driver_info):
    with StubCluster("v3/router.script",
                     "v3/return_1_in_write_tx_twice.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                value = session.write_transaction(unit_of_work)
                assert value == 1
                value = session.write_transaction(unit_of_work)
                assert value == 1


def test_read_tx_then_write_tx(driver_info):
    with StubCluster("v3/router.script",
                     "v3/return_1_in_read_tx.script",
                     "v3/return_2_in_write_tx.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work_1(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                def unit_of_work_2(tx):
                    total = 0
                    for record in tx.run("RETURN 2"):
                        total += record[0]
                    return total

                value = session.read_transaction(unit_of_work_1)
                assert session.last_bookmark() == "bookmark:1"
                assert value == 1
                value = session.write_transaction(unit_of_work_2)
                assert session.last_bookmark() == "bookmark:2"
                assert value == 2


def test_write_tx_then_read_tx(driver_info):
    with StubCluster("v3/router.script",
                     "v3/return_2_in_read_tx.script",
                     "v3/return_1_in_write_tx.script"):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:

                def unit_of_work_1(tx):
                    total = 0
                    for record in tx.run("RETURN 1"):
                        total += record[0]
                    return total

                def unit_of_work_2(tx):
                    total = 0
                    for record in tx.run("RETURN 2"):
                        total += record[0]
                    return total

                value = session.write_transaction(unit_of_work_1)
                assert value == 1
                value = session.read_transaction(unit_of_work_2)
                assert value == 2


def test_no_retry_read_on_user_canceled_tx(driver_info):
    with StubCluster("v3/router.script", "v3/user_canceled_read.script"):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:
                def unit_of_work(tx):
                    tx.run("RETURN 1")

                with pytest.raises(TransientError):
                    _ = session.read_transaction(unit_of_work)


def test_no_retry_write_on_user_canceled_tx(driver_info):
    with StubCluster("v3/router.script", "v3/user_canceled_write.script"):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:
                def unit_of_work(tx):
                    tx.run("RETURN 1")

                with pytest.raises(TransientError):
                    _ = session.write_transaction(unit_of_work)
