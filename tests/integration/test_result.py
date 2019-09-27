#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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
from pytest import raises, warns

from neo4j.exceptions import CypherError
import datetime

def test_can_consume_result_immediately(session):

    def f(tx):
        result = tx.run("UNWIND range(1, 3) AS n RETURN n")
        assert [record[0] for record in result] == [1, 2, 3]

    session.read_transaction(f)


def test_can_consume_result_from_buffer(session):

    def f(tx):
        result = tx.run("UNWIND range(1, 3) AS n RETURN n")
        result.detach()
        assert [record[0] for record in result] == [1, 2, 3]

    session.read_transaction(f)


def test_can_consume_result_after_commit(session):
    tx = session.begin_transaction()
    result = tx.run("UNWIND range(1, 3) AS n RETURN n")
    tx.commit()
    assert [record[0] for record in result] == [1, 2, 3]


def test_can_consume_result_after_rollback(session):
    tx = session.begin_transaction()
    result = tx.run("UNWIND range(1, 3) AS n RETURN n")
    tx.rollback()
    assert [record[0] for record in result] == [1, 2, 3]


def test_can_consume_result_after_session_close(bolt_driver):
    with bolt_driver.session() as session:
        tx = session.begin_transaction()
        result = tx.run("UNWIND range(1, 3) AS n RETURN n")
        tx.commit()
    assert [record[0] for record in result] == [1, 2, 3]


def test_can_consume_result_after_session_reuse(bolt_driver):
    session = bolt_driver.session()
    tx = session.begin_transaction()
    result_a = tx.run("UNWIND range(1, 3) AS n RETURN n")
    tx.commit()
    session.close()
    session = bolt_driver.session()
    tx = session.begin_transaction()
    result_b = tx.run("UNWIND range(4, 6) AS n RETURN n")
    tx.commit()
    session.close()
    assert [record[0] for record in result_a] == [1, 2, 3]
    assert [record[0] for record in result_b] == [4, 5, 6]


def test_can_consume_results_after_harsh_session_death(bolt_driver):
    session = bolt_driver.session()
    result_a = session.run("UNWIND range(1, 3) AS n RETURN n")
    del session
    session = bolt_driver.session()
    result_b = session.run("UNWIND range(4, 6) AS n RETURN n")
    del session
    assert [record[0] for record in result_a] == [1, 2, 3]
    assert [record[0] for record in result_b] == [4, 5, 6]


def test_can_consume_result_after_session_with_error(bolt_driver):
    session = bolt_driver.session()
    with raises(CypherError):
        session.run("X").consume()
    session.close()
    session = bolt_driver.session()
    tx = session.begin_transaction()
    result = tx.run("UNWIND range(1, 3) AS n RETURN n")
    tx.commit()
    session.close()
    assert [record[0] for record in result] == [1, 2, 3]


def test_single_with_exactly_one_record(session):
    result = session.run("UNWIND range(1, 1) AS n RETURN n")
    record = result.single()
    assert list(record.values()) == [1]


def test_value_with_no_records(session):
    result = session.run("CREATE ()")
    assert result.value() == []


def test_values_with_no_records(session):
    result = session.run("CREATE ()")
    assert result.values() == []


def test_peek_can_look_one_ahead(session):
    result = session.run("UNWIND range(1, 3) AS n RETURN n")
    record = result.peek()
    assert list(record.values()) == [1]


def test_peek_fails_if_nothing_remains(neo4j_driver):
    with neo4j_driver.session() as session:
        result = session.run("CREATE ()")
        upcoming = result.peek()
        assert upcoming is None


def test_peek_does_not_advance_cursor(session):
    result = session.run("UNWIND range(1, 3) AS n RETURN n")
    result.peek()
    assert [record[0] for record in result] == [1, 2, 3]


def test_peek_at_different_stages(session):
    result = session.run("UNWIND range(0, 9) AS n RETURN n")
    # Peek ahead to the first record
    expected_next = 0
    upcoming = result.peek()
    assert upcoming[0] == expected_next
    # Then look through all the other records
    for expected, record in enumerate(result):
        # Check this record is as expected
        assert record[0] == expected
        # Check the upcoming record is as expected...
        if expected < 9:
            # ...when one should follow
            expected_next = expected + 1
            upcoming = result.peek()
            assert upcoming[0] == expected_next
        else:
            # ...when none should follow
            upcoming = result.peek()
            assert upcoming is None


def test_can_safely_exit_session_without_consuming_result(session):
    session.run("RETURN 1")
    assert True


def test_multiple_value(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.value() == [1, 2, 3]


def test_multiple_indexed_value(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.value(2) == [3, 6, 9]


def test_multiple_keyed_value(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.value("z") == [3, 6, 9]


def test_multiple_values(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.values() == [[1, 2, 3],
                               [2, 4, 6],
                               [3, 6, 9]]


def test_multiple_indexed_values(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.values(2, 0), [[3, 1],
                                 [6, 2],
                                 [9, 3]]


def test_multiple_keyed_values(session):
    result = session.run("UNWIND range(1, 3) AS n "
                         "RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
    assert result.values("z", "x") == [[3, 1],
                                       [6, 2],
                                       [9, 3]]


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


def test_value_with_no_keys_and_no_records(neo4j_driver):
    with neo4j_driver.session() as session:
        result = session.run("CREATE ()")
        assert result.value() == []


def test_values_with_one_key_and_no_records(session):
    result = session.run("UNWIND range(1, 0) AS n RETURN n")
    assert result.values() == []


def test_data_with_one_key_and_no_records(session):
    result = session.run("UNWIND range(1, 0) AS n RETURN n")
    assert result.data() == []


def test_single_with_no_keys_and_no_records(session):
    result = session.run("CREATE ()")
    record = result.single()
    assert record is None


def test_single_with_one_key_and_no_records(session):
    result = session.run("UNWIND range(1, 0) AS n RETURN n")
    record = result.single()
    assert record is None


def test_single_with_multiple_records(session):
    import warnings
    result = session.run("UNWIND range(1, 3) AS n RETURN n")
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        record = result.single()
        assert len(warning_list) == 1
        assert record[0] == 1


def test_single_consumes_entire_result_if_one_record(session):
    result = session.run("UNWIND range(1, 1) AS n RETURN n")
    _ = result.single()
    assert not result.session


def test_single_consumes_entire_result_if_multiple_records(session):
    result = session.run("UNWIND range(1, 3) AS n RETURN n")
    with warns(UserWarning):
        _ = result.single()
    assert not result.session


def test_single_value(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().value() == 1


def test_single_indexed_value(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().value(2) == 3


def test_single_keyed_value(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().value("z") == 3


def test_single_values(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().values() == [1, 2, 3]


def test_single_indexed_values(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().values(2, 0) == [3, 1]


def test_single_keyed_values(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().values("z", "x") == [3, 1]


def test_node_labels(session):

    result = session.run(
        "CREATE (n:Test:Node {tstamp: datetime()}) RETURN n",
    )

    row_0 = result.single()

    assert row_0["n"].labels == {"Test", "Node"}


def test_single_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data() == {"x": 1, "y": 2, "z": 3}


def test_single_indexed_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data(2, 0) == {"x": 1, "z": 3}


def test_single_keyed_data(session):
    result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
    assert result.single().data("z", "x") == {"x": 1, "z": 3}


def test_single_data_result(session):
    result = session.run("CREATE p=(a)-[ab:X]->(b:Test)-[bc:X]->(c:Test:Node) RETURN a, b, c, ab, bc, p")
    row_0 = result.single()
    assert row_0.keys() == ["a", "b", "c", "ab", "bc", "p"]
    a, b, c, ab, bc, p = row_0
    assert p.data() == {"node_id_index": {a.id:0, b.id:1, c.id:2},"nodes": [a.data(), b.data(), c.data()], "relationships": [ab.data(), bc.data()]}

    assert row_0.data() == {"a": a.data(), "b": b.data(), "c": c.data(), "ab": ab.data(), "bc": bc.data(), "p": p.data()}

    with raises(TypeError):
        assert row_0.data(["a", "b"]) == {"a": a.data(), "b": b.data()}

    with raises(TypeError):
        assert row_0.data([0, 1]) == {"a": a.data(), "b": b.data()}

    assert row_0.data("a", "b") == {"a": a.data(), "b": b.data()}
    assert row_0.data(0, 1) == {"a": a.data(), "b": b.data()}

    assert row_0.data("a", "b", "void") == {"a": a.data(), "b": b.data(), "void": None}

    with raises(IndexError):
        assert row_0.data(0, 1, 10) == {"a": a.data(), "b": b.data(), 10: None}


def test_multiple_data_result(session):

    result = session.run("CREATE p=(a)-[ab:X]->(b:Test)-[bc:X]->(c:Test:Node) RETURN a, b, c, ab, bc, p")
    rows = list(result)
    a, b, c, ab, bc, p = rows[0]

    assert result.data() == []  #Consumes the remaining result

    result = session.run(
        "MATCH p=(a)-[ab:X]->(b:Test)-[bc:X]->(c:Test:Node) WHERE ID(a) = $a AND ID(b) = $b AND ID(c) = $c RETURN a, b, c, ab, bc, p",
        a=a.id,
        b=b.id,
        c=c.id,
    )

    assert result.data() == [rows[0].data(),]

    result = session.run(
        "MATCH p=(a)-[ab:X]->(b:Test)-[bc:X]->(c:Test:Node) WHERE ID(a) = $a AND ID(b) = $b AND ID(c) = $c RETURN a, b, c, ab, bc, p",
        a=a.id,
        b=b.id,
        c=c.id,
    )

    assert result.data("a", "b") == [rows[0].data("a", "b"),]


def test_temporal_data_result(session):

    result = session.run("RETURN date() AS a")
    row_0 = result.single()
    assert isinstance(row_0.data()["a"], datetime.date)

    result = session.run("RETURN time() AS a")
    row_0 = result.single()
    assert isinstance(row_0.data()["a"], datetime.time)

    result = session.run("RETURN localtime() AS a")
    row_0 = result.single()
    assert isinstance(row_0.data()["a"], datetime.time)

    result = session.run("RETURN datetime() AS a")
    row_0 = result.single()
    assert isinstance(row_0.data()["a"], datetime.datetime)

    result = session.run("RETURN localdatetime() AS a")
    row_0 = result.single()
    assert isinstance(row_0.data()["a"], datetime.datetime)

    #result = session.run("RETURN duration("P3Y6M4DT12H30M5S") AS a")
    #row_0 = result.single()
    #assert isinstance(row_0.data()["a"], datetime.timedelta)

    # Test Node with temporal defaults
    # TODO: Duration

    result = session.run(
        "CREATE (a:Test:Node {date: date(), time: time(), localTime: localtime(), dateTime: datetime(), localDateTime: localdatetime()}) RETURN a",
    )

    row_0 = result.single()

    assert row_0.keys() == ["a",]

    a = row_0["a"]

    assert row_0.data() == {"a": a.data()}

    assert a.data() == {
        "id": a.id,
        "labels": ["Node", "Test"],
        "properties": {
            "date": a["date"].to_native(),
            "time": a["time"].to_native(),
            "localTime": a["localTime"].to_native(),
            "dateTime": a["dateTime"].to_native(),
            "localDateTime": a["localDateTime"].to_native(),
        },
    }

    result = session.run(
        "MATCH (a) WHERE ID(a) = $a RETURN a, ID(a) AS node_id",
        a=a.id,
    )

    assert result.data("a") == [row_0.data(),]


