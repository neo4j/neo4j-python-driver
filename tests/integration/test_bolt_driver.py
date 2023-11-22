# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


@pytest.fixture
def server_info(driver):
    """ Simple fixture to provide quick and easy access to a
    :class:`.ServerInfo` object.
    """
    with driver.session() as session:
        summary = session.run("RETURN 1").consume()
        yield summary.server


# TODO: 6.0 -
#       This test will stay as python is currently the only driver exposing
#       the connection id. This will be removed in 6.0
def test_server_connection_id(driver):
    server_info = driver.get_server_info()
    with pytest.warns(DeprecationWarning):
        cid = server_info.connection_id
    assert cid.startswith("bolt-") and cid[5:].isdigit()
