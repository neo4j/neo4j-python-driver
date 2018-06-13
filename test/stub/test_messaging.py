#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from neo4j.v1 import GraphDatabase, DirectDriver, READ_ACCESS, WRITE_ACCESS, TransientError
from neo4j.exceptions import ServiceUnavailable

from test.stub.tools import StubTestCase, StubCluster


class MessagingTestCase(StubTestCase):

    def test_close_connection_on_no_successful_reset(self):
        with StubCluster({9001: "fail_to_reset.script"}):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    session.run("MATCH (n) RETURN n.name AS name").consume()
                    # reset is not sent??
