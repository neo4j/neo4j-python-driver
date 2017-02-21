#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


from neo4j.v1 import GraphDatabase, READ_ACCESS

from test.stub.tools import StubTestCase, StubCluster


class BookmarkingTestCase(StubTestCase):

    def test_should_be_no_bookmark_in_new_session(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    assert session.bookmark is None

    def test_should_be_able_to_set_bookmark(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    session.bookmark = "X"
                    assert session.bookmark == "X"

    def test_should_automatically_chain_bookmarks(self):
        with StubCluster({9001: "router.script", 9004: "bookmark_chain.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    session.bookmark = "bookmark:1"
                    with session.begin_transaction():
                        pass
                    assert session.bookmark == "bookmark:2"
                    with session.begin_transaction():
                        pass
                    assert session.bookmark == "bookmark:3"
