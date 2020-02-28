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

from neo4j import (
    GraphDatabase,
)
from neo4j.api import READ_ACCESS

from tests.stub.conftest import StubCluster

# python -m pytest tests/stub/test_bookmarking.py -s -v


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router.script",
    ]
)
def test_should_be_no_bookmark_in_new_session(driver_info, test_script):
    # python -m pytest tests/stub/test_bookmarking.py -s -v -k test_should_be_no_bookmark_in_new_session
    with StubCluster(test_script):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:
                assert session.last_bookmark() is None


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router.script",
    ]
)
def test_should_be_able_to_set_bookmark(driver_info, test_script):
    # python -m pytest tests/stub/test_bookmarking.py -s -v -k test_should_be_able_to_set_bookmark
    with StubCluster(test_script):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(bookmarks=["X"]) as session:
                assert session.next_bookmarks() == ("X",)


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router.script",
    ]
)
def test_should_be_able_to_set_multiple_bookmarks(driver_info, test_script):
    # python -m pytest tests/stub/test_bookmarking.py -s -v -k test_should_be_able_to_set_multiple_bookmarks
    with StubCluster(test_script):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(bookmarks=[":1", ":2"]) as session:
                assert session.next_bookmarks() == (":1", ":2")


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/bookmark_chain.script"),
        ("v4x0/router.script", "v4x0/tx_bookmark_chain.script"),
    ]
)
def test_should_automatically_chain_bookmarks(driver_info, test_scripts):
    # python -m pytest tests/stub/test_bookmarking.py -s -v -k test_should_automatically_chain_bookmarks
    with StubCluster(*test_scripts):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS,
                                bookmarks=["bookmark:0", "bookmark:1"]) as session:
                with session.begin_transaction():
                    pass
                assert session.last_bookmark() == "bookmark:2"
                with session.begin_transaction():
                    pass
                assert session.last_bookmark() == "bookmark:3"


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/bookmark_chain_with_autocommit.script"),
        ("v4x0/router.script", "v4x0/tx_bookmark_chain_with_autocommit.script"),
    ]
)
def test_autocommit_transaction_included_in_chain(driver_info, test_scripts):
    # python -m pytest tests/stub/test_bookmarking.py -s -v -k test_autocommit_transaction_included_in_chain
    with StubCluster(*test_scripts):
        uri = "bolt+routing://localhost:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS,
                                bookmarks=["bookmark:1"]) as session:
                with session.begin_transaction():
                    pass
                assert session.last_bookmark() == "bookmark:2"
                session.run("RETURN 1").consume()
                assert session.last_bookmark() == "bookmark:3"
                with session.begin_transaction():
                    pass
                assert session.last_bookmark() == "bookmark:4"
