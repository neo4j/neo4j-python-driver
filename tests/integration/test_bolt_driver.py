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


from pytest import fixture


# TODO: this test will stay until a uniform behavior for `.single()` across the
#       drivers has been specified and tests are created in testkit
def test_normal_use_case(bolt_driver):
    # python -m pytest tests/integration/test_bolt_driver.py -s -v -k test_normal_use_case
    session = bolt_driver.session()
    value = session.run("RETURN 1").single().value()
    assert value == 1


# TODO: this test will stay until a uniform behavior for `.encrypted` across the
#       drivers has been specified and tests are created in testkit
def test_encrypted_set_to_false_by_default(bolt_driver):
    # python -m pytest tests/integration/test_bolt_driver.py -s -v -k test_encrypted_set_to_false_by_default
    assert bolt_driver.encrypted is False


@fixture
def server_info(driver):
    """ Simple fixture to provide quick and easy access to a
    :class:`.ServerInfo` object.
    """
    with driver.session() as session:
        summary = session.run("RETURN 1").consume()
        yield summary.server


# TODO: this test will stay asy python is currently the only driver exposing the
#       connection id. So this might change in the future.
def test_server_connection_id(server_info):
    cid = server_info.connection_id
    assert cid.startswith("bolt-") and cid[5:].isdigit()
