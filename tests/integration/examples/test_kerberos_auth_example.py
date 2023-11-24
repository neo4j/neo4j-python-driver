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

from . import DriverSetupExample


# isort: off
# tag::kerberos-auth-import[]
from neo4j import (
    GraphDatabase,
    kerberos_auth,
)
# end::kerberos-auth-import[]
# isort: on


# python -m pytest tests/integration/examples/test_kerberos_auth_example.py -s -v

class KerberosAuthExample(DriverSetupExample):
    # tag::kerberos-auth[]
    def __init__(self, uri, ticket):
        self._driver = GraphDatabase.driver(uri, auth=kerberos_auth(ticket))
    # end::kerberos-auth[]


def test_example():
    pytest.skip("Currently no way to test Kerberos auth")
