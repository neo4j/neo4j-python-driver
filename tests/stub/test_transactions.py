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

from neo4j import (
    GraphDatabase,
    WRITE_ACCESS,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
)
from neo4j.exceptions import ServiceUnavailable

from tests.stub.conftest import StubCluster

# python -m pytest tests/stub/test_transactions.py -s -v


driver_config = {
    "encrypted": False,
    "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    "user_agent": "test",
    "max_age": 1000,
    "max_size": 10,
    "keep_alive": False,
    "max_retry_time": 1,
    "resolver": None,
}

session_config = {
    # "fetch_size": 100,
    # "database": "default",
    # "bookmarks": ["bookmark-1", ],
    "default_access_mode": WRITE_ACCESS,
    "acquire_timeout": 1.0,
    "max_retry_time": 1.0,
    "initial_retry_delay": 1.0,
    "retry_delay_multiplier": 1.0,
    "retry_delay_jitter_factor": 0.1,
}


def create_bob(tx):
    tx.run("CREATE (n {name:'Bob'})").data()


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/connection_error_on_commit.script",
        "v4x0/tx_connection_error_on_commit.script",
    ]
)
def test_connection_error_on_explicit_commit(driver_info, test_script):
    # python -m pytest tests/stub/test_transactions.py -s -v -k test_connection_error_on_explicit_commit
    with StubCluster(test_script):
        uri = "bolt://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"], **driver_config) as driver:
            with driver.session(**session_config) as session:
                tx = session.begin_transaction()
                tx.run("CREATE (n {name:'Bob'})").data()
                with pytest.raises(ServiceUnavailable):
                    tx.commit()


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/connection_error_on_commit.script",
        "v4x0/tx_connection_error_on_commit.script",
    ]
)
def test_connection_error_on_commit(driver_info, test_script):
    # python -m pytest tests/stub/test_transactions.py -s -v -k test_connection_error_on_commit
    with StubCluster(test_script):
        uri = "bolt://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"], **driver_config) as driver:
            with driver.session(**session_config) as session:
                with pytest.raises(ServiceUnavailable):
                    session.write_transaction(create_bob)
