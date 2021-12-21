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

from neo4j import (
    GraphDatabase,
    Neo4jDriver,
)
from neo4j.exceptions import ServiceUnavailable
from tests.stub.conftest import StubCluster


# python -m pytest tests/stub/test_routingdriver.py -s -v
# TODO: those tests will stay until a uniform behavior across the drivers has
#       been specified and tests are created in testkit
@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router_port_9001_one_read_port_9004_one_write_port_9006.script",
    ]
)
def test_neo4j_driver_verify_connectivity(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_neo4j_driver_verify_connectivity
    with StubCluster(test_script):
        with GraphDatabase.driver(driver_info["uri_neo4j"], auth=driver_info["auth_token"], user_agent="test") as driver:
            assert isinstance(driver, Neo4jDriver)
            assert driver.verify_connectivity() is not None


# @pytest.mark.skip(reason="Flaky")
@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router_port_9001_one_read_port_9004_one_write_port_9006.script",
    ]
)
def test_neo4j_driver_verify_connectivity_server_down(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_neo4j_driver_verify_connectivity_server_down
    with GraphDatabase.driver(driver_info["uri_neo4j"], auth=driver_info["auth_token"], user_agent="test") as driver:
        assert isinstance(driver, Neo4jDriver)

        with pytest.raises(ServiceUnavailable):
            driver.verify_connectivity()
