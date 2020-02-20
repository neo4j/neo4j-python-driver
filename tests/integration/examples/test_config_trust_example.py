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

# tag::config-trust-import[]
from neo4j import GraphDatabase
# end::config-trust-import[]

from tests.integration.examples import DriverSetupExample


class ConfigTrustExample(DriverSetupExample):

    # tag::config-trust[]
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth, secure=True, verify_cert=False)
    # end::config-trust[]


def test_example(uri, auth):
    # TODO: re-enable when we can test with secure=True on Docker
    # ConfigTrustExample.test(uri, auth)
    pytest.skip("re-enable when we can test with secure=True on Docker")
