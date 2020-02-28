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


from uuid import uuid4

from neo4j.api import READ_ACCESS, WRITE_ACCESS
from neo4j.graph import Node


def test_can_obtain_bookmark_after_commit(bolt_driver):
    with bolt_driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run("RETURN 1")
        assert session.last_bookmark() is not None


def test_can_pass_bookmark_into_next_transaction(driver):
    unique_id = uuid4().hex

    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        with session.begin_transaction() as tx:
            tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=unique_id)
        bookmark = session.last_bookmark()

    assert bookmark is not None

    with driver.session(default_access_mode=READ_ACCESS, bookmarks=[bookmark]) as session:
        with session.begin_transaction() as tx:
            result = tx.run("MATCH (a:Thing {uuid:$uuid}) RETURN a", uuid=unique_id)
            record_list = list(result)
            assert len(record_list) == 1
            record = record_list[0]
            assert len(record) == 1
            thing = record[0]
            assert isinstance(thing, Node)
            assert thing["uuid"] == unique_id


def test_bookmark_should_be_none_after_rollback(driver):
    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        with session.begin_transaction() as tx:
            tx.run("CREATE (a)")

    assert session.last_bookmark() is not None

    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        with session.begin_transaction() as tx:
            tx.run("CREATE (a)")
            tx.rollback()

    assert session.last_bookmark() is None
