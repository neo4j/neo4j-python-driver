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


def test_data_with_one_key_and_no_records(session):
    result = session.run("UNWIND range(1, 0) AS n RETURN n")
    assert result.data() == []


def test_multiple_data(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.data() == [{"x": 1, "y": 2, "z": 3},
                             {"x": 2, "y": 4, "z": 6},
                             {"x": 3, "y": 6, "z": 9}]


def test_multiple_indexed_data(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.data(2, 0) == [{"x": 1, "z": 3},
                                 {"x": 2, "z": 6},
                                 {"x": 3, "z": 9}]


def test_multiple_keyed_data(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.data("z", "x") == [{"x": 1, "z": 3},
                                     {"x": 2, "z": 6},
                                     {"x": 3, "z": 9}]


def test_single_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data() == {"x": 1, "y": 2, "z": 3}


def test_single_indexed_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data(2, 0) == {"x": 1, "z": 3}


def test_single_keyed_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data("z", "x") == {"x": 1, "z": 3}


def test_none(session):
    result = session.run("RETURN null AS x")
    assert result.data() == [{"x": None}]


def test_bool(session):
    result = session.run("RETURN true AS x, false AS y")
    assert result.data() == [{"x": True, "y": False}]


def test_int(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.data() == [{"x": 1, "y": 2, "z": 3}]


def test_float(session):
    result = session.run("RETURN 0.0 AS x, 1.0 AS y, 3.141592653589 AS z")
    assert result.data() == [{"x": 0.0, "y": 1.0, "z": 3.141592653589}]


def test_string(session):
    result = session.run("RETURN 'hello, world' AS x")
    assert result.data() == [{"x": "hello, world"}]


def test_byte_array(session):
    result = session.run("RETURN $x AS x", x=bytearray([1, 2, 3]))
    assert result.data() == [{"x": bytearray([1, 2, 3])}]


def test_list(session):
    result = session.run("RETURN [1, 2, 3] AS x")
    assert result.data() == [{"x": [1, 2, 3]}]


def test_dict(session):
    result = session.run("RETURN {one: 1, two: 2} AS x")
    assert result.data() == [{"x": {"one": 1, "two": 2}}]


def test_node(session):
    result = session.run("CREATE (x:Person {name:'Alice'}) RETURN x")
    assert result.data() == [{"x": {"name": "Alice"}}]


def test_relationship_with_pre_known_nodes(session):
    result = session.run("CREATE (a:Person {name:'Alice'})-[x:KNOWS {since:1999}]->(b:Person {name:'Bob'}) "
                         "RETURN a, b, x")
    assert result.data() == [{"a": {"name": "Alice"}, "b": {"name": "Bob"},
                              "x": ({"name": "Alice"}, "KNOWS", {"name": "Bob"})}]


def test_relationship_with_post_known_nodes(session):
    result = session.run("CREATE (a:Person {name:'Alice'})-[x:KNOWS {since:1999}]->(b:Person {name:'Bob'}) "
                         "RETURN x, a, b")
    assert result.data() == [{"x": ({"name": "Alice"}, "KNOWS", {"name": "Bob"}),
                              "a": {"name": "Alice"}, "b": {"name": "Bob"}}]


def test_relationship_with_unknown_nodes(session):
    result = session.run("CREATE (:Person {name:'Alice'})-[x:KNOWS {since:1999}]->(:Person {name:'Bob'}) "
                         "RETURN x")
    assert result.data() == [{"x": ({}, "KNOWS", {})}]


def test_path(session):
    result = session.run("CREATE x = (a:Person {name:'Alice'})-[:KNOWS {since:1999}]->(b:Person {name:'Bob'}) "
                         "RETURN x")
    assert result.data() == [{"x": [{"name": "Alice"}, "KNOWS", {"name": "Bob"}]}]
