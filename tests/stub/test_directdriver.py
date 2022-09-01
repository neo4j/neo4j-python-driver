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


import pytest

import neo4j
from neo4j.exceptions import (
    ServiceUnavailable,
    ConfigurationError,
    UnsupportedServerProduct,
)
from neo4j._exceptions import (
    BoltHandshakeError,
    BoltSecurityError,
)

from neo4j import (
    GraphDatabase,
    BoltDriver,
    Query,
    WRITE_ACCESS,
    READ_ACCESS,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    DEFAULT_DATABASE,
    Result,
    unit_of_work,
    Transaction,
)

from tests.stub.conftest import (
    StubCluster,
)

# python -m pytest tests/stub/test_directdriver.py -s -v


driver_config = {
    "encrypted": False,
    "user_agent": "test",
    "max_connection_lifetime": 1000,
    "max_connection_pool_size": 10,
    "keep_alive": True,
    "resolver": None,
}


session_config = {
    "default_access_mode": READ_ACCESS,
    "connection_acquisition_timeout": 1.0,
    "max_transaction_retry_time": 1.0,
    "initial_retry_delay": 1.0,
    "retry_delay_multiplier": 1.0,
    "retry_delay_jitter_factor": 0.1,
    "fetch_size": -1,
}


# TODO: those tests will stay until a uniform behavior across the drivers has
#       been specified and tests are created in testkit
def test_direct_driver_with_wrong_port(driver_info):
    # python -m pytest tests/stub/test_directdriver.py -s -v -k test_direct_driver_with_wrong_port
    uri = "bolt://127.0.0.1:9002"
    with pytest.raises(ServiceUnavailable):
        driver = GraphDatabase.driver(uri, auth=driver_info["auth_token"], **driver_config)
        with pytest.warns(neo4j.ExperimentalWarning,
                          match="The configuration may change in the future."):
            driver.verify_connectivity()


@pytest.mark.parametrize(
    "test_script, test_expected",
    [
        ("v3/return_1_port_9001.script", "Neo4j/3.0.0"),
        ("v4x0/return_1_port_9001.script", "Neo4j/4.0.0"),
    ]
)
def test_direct_verify_connectivity(driver_info, test_script, test_expected):
    # python -m pytest tests/stub/test_directdriver.py -s -v -k test_direct_verify_connectivity
    with StubCluster(test_script):
        uri = "bolt://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"], **driver_config) as driver:
            assert isinstance(driver, BoltDriver)
            with pytest.warns(
                    neo4j.ExperimentalWarning,
                    match="The configuration may change in the future."
            ):
                assert driver.verify_connectivity(
                    default_access_mode=READ_ACCESS
                ) == test_expected


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/disconnect_on_run.script",
        "v4x0/disconnect_on_run.script",
    ]
)
def test_direct_verify_connectivity_disconnect_on_run(driver_info, test_script):
    # python -m pytest tests/stub/test_directdriver.py -s -v -k test_direct_verify_connectivity_disconnect_on_run
    with StubCluster(test_script):
        uri = "bolt://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"], **driver_config) as driver:
            with pytest.raises(ServiceUnavailable):
                with pytest.warns(
                        neo4j.ExperimentalWarning,
                        match="The configuration may change in the future."
                ):
                    driver.verify_connectivity(default_access_mode=READ_ACCESS)
