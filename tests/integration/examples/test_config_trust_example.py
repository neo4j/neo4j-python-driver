# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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

from tests.integration.examples import DriverSetupExample


# isort: off
# tag::config-trust-import[]
import neo4j
from neo4j import GraphDatabase
# end::config-trust-import[]
# isort: on


class ConfigTrustExample(DriverSetupExample):

    # tag::config-trust[]
    def __init__(self, uri, auth):
        # trusted_certificates:
        # neo4j.TrustSystemCAs()
        #     (default) trust certificates from system store)
        # neo4j.TrustAll()
        #     trust all certificates
        # neo4j.TrustCustomCAs("<path>", ...)
        #     specify a list of paths to certificates to trust
        self.driver = GraphDatabase.driver(
            uri, auth=auth, encrypted=True,
            trusted_certificates=neo4j.TrustAll()
        )
    # end::config-trust[]


def test_example(uri, auth):
    # TODO: re-enable when we can test with encrypted=True on Docker
    # ConfigTrustExample.test(uri, auth)
    pytest.skip("re-enable when we can test with encrypted=True on Docker")
