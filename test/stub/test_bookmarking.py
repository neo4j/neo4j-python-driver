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


from neo4j import GraphDatabase, READ_ACCESS

from test.stub.tools import StubTestCase, StubCluster


class BookmarkingTestCase(StubTestCase):

    def test_should_be_no_bookmark_in_new_session(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    assert session.last_bookmark() is None

    def test_should_be_able_to_set_bookmark(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(bookmark="X") as session:
                    assert session.next_bookmarks() == ("X",)

    def test_should_be_able_to_set_multiple_bookmarks(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(bookmarks=[":1", ":2"]) as session:
                    assert session.next_bookmarks() == (":1", ":2")

    def test_should_automatically_chain_bookmarks(self):
        with StubCluster({9001: "router.script", 9004: "bookmark_chain.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS, bookmarks=["bookmark:0", "bookmark:1"]) as session:
                    with session.begin_transaction():
                        pass
                    assert session.last_bookmark() == "bookmark:2"
                    with session.begin_transaction():
                        pass
                    assert session.last_bookmark() == "bookmark:3"

    def test_autocommit_transaction_included_in_chain(self):
        with StubCluster({9001: "router.script", 9004: "bookmark_chain_with_autocommit.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS, bookmark="bookmark:1") as session:
                    with session.begin_transaction():
                        pass
                    assert session.last_bookmark() == "bookmark:2"
                    session.run("RETURN 1").consume()
                    assert session.last_bookmark() == "bookmark:3"
                    with session.begin_transaction():
                        pass
                    assert session.last_bookmark() == "bookmark:4"
