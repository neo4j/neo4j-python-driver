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
from pytest import raises

from neo4j.work.simple import Query
from neo4j.exceptions import Neo4jError, ClientError, TransientError
from neo4j.graph import Node, Relationship
from neo4j.api import Version


def test_can_run_simple_statement(session):
    result = session.run("RETURN 1 AS n")
    for record in result:
        assert record[0] == 1
        assert record["n"] == 1
        with raises(KeyError):
            _ = record["x"]
        assert record["n"] == 1
        with raises(KeyError):
            _ = record["x"]
        with raises(TypeError):
            _ = record[object()]
        assert repr(record)
        assert len(record) == 1


def test_can_run_simple_statement_with_params(session):
    count = 0
    for record in session.run("RETURN $x AS n",
                              {"x": {"abc": ["d", "e", "f"]}}):
        assert record[0] == {"abc": ["d", "e", "f"]}
        assert record["n"] == {"abc": ["d", "e", "f"]}
        assert repr(record)
        assert len(record) == 1
        count += 1
    assert count == 1


def test_autocommit_transactions_use_bookmarks(neo4j_driver):
    bookmarks = []
    # Generate an initial bookmark
    with neo4j_driver.session() as session:
        session.run("CREATE ()").consume()
        bookmark = session.last_bookmark()
        assert bookmark is not None
        bookmarks.append(bookmark)
    # Propagate into another session
    with neo4j_driver.session(bookmarks=bookmarks) as session:
        assert list(session.next_bookmarks()) == bookmarks
        session.run("CREATE ()").consume()
        bookmark = session.last_bookmark()
        assert bookmark is not None
        assert bookmark not in bookmarks


def test_fails_on_bad_syntax(session):
    with raises(Neo4jError):
        session.run("X").consume()


def test_fails_on_missing_parameter(session):
    with raises(Neo4jError):
        session.run("RETURN {x}").consume()


def test_can_run_statement_that_returns_multiple_records(session):
    count = 0
    for record in session.run("unwind(range(1, 10)) AS z RETURN z"):
        assert 1 <= record[0] <= 10
        count += 1
    assert count == 10


def test_can_use_with_to_auto_close_session(session):
    record_list = list(session.run("RETURN 1"))
    assert len(record_list) == 1
    for record in record_list:
        assert record[0] == 1


def test_can_return_node(neo4j_driver):
    with neo4j_driver.session() as session:
        record_list = list(session.run("CREATE (a:Person {name:'Alice'}) "
                                       "RETURN a"))
        assert len(record_list) == 1
        for record in record_list:
            alice = record[0]
            assert isinstance(alice, Node)
            assert alice.labels == {"Person"}
            assert dict(alice) == {"name": "Alice"}


def test_can_return_relationship(neo4j_driver):
    with neo4j_driver.session() as session:
        record_list = list(session.run("CREATE ()-[r:KNOWS {since:1999}]->() "
                                       "RETURN r"))
        assert len(record_list) == 1
        for record in record_list:
            rel = record[0]
            assert isinstance(rel, Relationship)
            assert rel.type == "KNOWS"
            assert dict(rel) == {"since": 1999}


# TODO: re-enable after server bug is fixed
# def test_can_return_path(session):
#     with self.driver.session() as session:
#         record_list = list(session.run("MERGE p=({name:'Alice'})-[:KNOWS]->"
#                                        "({name:'Bob'}) RETURN p"))
#         assert len(record_list) == 1
#         for record in record_list:
#             path = record[0]
#             assert isinstance(path, Path)
#             assert path.start_node["name"] == "Alice"
#             assert path.end_node["name"] == "Bob"
#             assert path.relationships[0].type == "KNOWS"
#             assert len(path.nodes) == 2
#             assert len(path.relationships) == 1


def test_can_handle_cypher_error(session):
    with raises(Neo4jError):
        session.run("X").consume()


def test_keys_are_available_before_and_after_stream(session):
    result = session.run("UNWIND range(1, 10) AS n RETURN n")
    assert list(result.keys()) == ["n"]
    list(result)
    assert list(result.keys()) == ["n"]


def test_keys_with_an_error(session):
    with raises(Neo4jError):
        result = session.run("X")
        list(result.keys())


def test_should_not_allow_empty_statements(session):
    with raises(ValueError):
        _ = session.run("")


def test_statement_object(session):
    value = session.run(Query("RETURN $x"), x=1).single().value()
    assert value == 1


@pytest.mark.parametrize(
    "test_input, neo4j_version",
    [
        ("CALL dbms.getTXMetaData", Version(3, 0)),
        ("CALL tx.getMetaData", Version(4, 0)),
    ]
)
def test_autocommit_transactions_should_support_metadata(session, test_input, neo4j_version):
    # python -m pytest tests/integration/test_autocommit.py -s -r fEsxX -k test_autocommit_transactions_should_support_metadata
    metadata_in = {"foo": "bar"}

    result = session.run("RETURN 1")
    value = result.single().value()
    summary = result.summary()
    server_agent = summary.server.agent

    try:
        statement = Query(test_input, metadata=metadata_in)
        result = session.run(statement)
        metadata_out = result.single().value()
    except ClientError as e:
        if e.code == "Neo.ClientError.Procedure.ProcedureNotFound":
            pytest.skip("Cannot assert correct metadata as {} does not support procedure '{}' introduced in Neo4j {}".format(server_agent, test_input, neo4j_version))
        else:
            raise
    else:
        assert metadata_in == metadata_out


def test_autocommit_transactions_should_support_timeout(neo4j_driver):
    with neo4j_driver.session() as s1:
        s1.run("CREATE (a:Node)").consume()
        with neo4j_driver.session() as s2:
            tx1 = s1.begin_transaction()
            tx1.run("MATCH (a:Node) SET a.property = 1").consume()
            with raises(TransientError):
                s2.run(Query("MATCH (a:Node) SET a.property = 2",
                             timeout=0.25)).consume()


def test_regex_in_parameter(session):
    matches = session.run("UNWIND ['A', 'B', 'C', 'A B', 'B C', 'A B C', "
                          "'A BC', 'AB C'] AS t WITH t "
                          "WHERE t =~ $re RETURN t", re=r'.*\bB\b.*').value()
    assert matches == ["B", "A B", "B C", "A B C"]


def test_regex_inline(session):
    matches = session.run(r"UNWIND ['A', 'B', 'C', 'A B', 'B C', 'A B C', "
                          r"'A BC', 'AB C'] AS t WITH t "
                          r"WHERE t =~ '.*\\bB\\b.*' RETURN t").value()
    assert matches == ["B", "A B", "B C", "A B C"]


def test_automatic_reset_after_failure(session):
    try:
        session.run("X").consume()
    except Neo4jError:
        result = session.run("RETURN 1")
        record = next(iter(result))
        assert record[0] == 1
    else:
        assert False, "A Cypher error should have occurred"


def test_large_values(bolt_driver):
    for i in range(1, 7):
        with bolt_driver.session() as session:
            session.run("RETURN '{}'".format("A" * 2 ** 20))
