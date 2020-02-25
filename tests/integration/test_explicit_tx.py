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


from unittest import SkipTest
from uuid import uuid4

from pytest import raises

from neo4j.work.simple import Query, TransactionError
from neo4j.exceptions import CypherSyntaxError, ClientError, TransientError


def test_can_commit_transaction(session):

    tx = session.begin_transaction()

    # Create a node
    result = tx.run("CREATE (a) RETURN id(a)")
    record = next(iter(result))
    node_id = record[0]
    assert isinstance(node_id, int)

    # Update a property
    tx.run("MATCH (a) WHERE id(a) = $n "
           "SET a.foo = $foo", {"n": node_id, "foo": "bar"})

    tx.commit()

    # Check the property value
    result = session.run("MATCH (a) WHERE id(a) = $n "
                         "RETURN a.foo", {"n": node_id})
    record = next(iter(result))
    value = record[0]
    assert value == "bar"


def test_can_rollback_transaction(session):
    tx = session.begin_transaction()

    # Create a node
    result = tx.run("CREATE (a) RETURN id(a)")
    record = next(iter(result))
    node_id = record[0]
    assert isinstance(node_id, int)

    # Update a property
    tx.run("MATCH (a) WHERE id(a) = $n "
           "SET a.foo = $foo", {"n": node_id, "foo": "bar"})

    tx.rollback()

    # Check the property value
    result = session.run("MATCH (a) WHERE id(a) = $n "
                         "RETURN a.foo", {"n": node_id})
    assert len(list(result)) == 0


def test_can_commit_transaction_using_with_block(session):
    with session.begin_transaction() as tx:
        # Create a node
        result = tx.run("CREATE (a) RETURN id(a)")
        record = next(iter(result))
        node_id = record[0]
        assert isinstance(node_id, int)

        # Update a property
        tx.run("MATCH (a) WHERE id(a) = $n "
               "SET a.foo = $foo", {"n": node_id, "foo": "bar"})

        tx.commit()

    # Check the property value
    result = session.run("MATCH (a) WHERE id(a) = $n "
                         "RETURN a.foo", {"n": node_id})
    record = next(iter(result))
    value = record[0]
    assert value == "bar"


def test_can_rollback_transaction_using_with_block(session):
    with session.begin_transaction() as tx:
        # Create a node
        result = tx.run("CREATE (a) RETURN id(a)")
        record = next(iter(result))
        node_id = record[0]
        assert isinstance(node_id, int)

        # Update a property
        tx.run("MATCH (a) WHERE id(a) = $n "
               "SET a.foo = $foo", {"n": node_id, "foo": "bar"})
        tx.rollback()

    # Check the property value
    result = session.run("MATCH (a) WHERE id(a) = $n "
                         "RETURN a.foo", {"n": node_id})
    assert len(list(result)) == 0


def test_broken_transaction_should_not_break_session(session):
    with raises(CypherSyntaxError):
        with session.begin_transaction() as tx:
            tx.run("X")
    with session.begin_transaction() as tx:
        tx.run("RETURN 1")


def test_statement_object_not_supported(session):
    with session.begin_transaction() as tx:
        with raises(ValueError):
            tx.run(Query("RETURN 1", timeout=0.25))


def test_transaction_metadata(session):
    metadata_in = {"foo": "bar"}
    with session.begin_transaction(metadata=metadata_in) as tx:
        try:
            metadata_out = tx.run("CALL dbms.getTXMetaData").single().value()
        except ClientError as e:
            if e.code == "Neo.ClientError.Procedure.ProcedureNotFound":
                raise SkipTest("Cannot assert correct metadata as Neo4j "
                               "edition does not support procedure "
                               "dbms.getTXMetaData")
            else:
                raise
        else:
            assert metadata_in == metadata_out


def test_transaction_timeout(driver):
    with driver.session() as s1:
        s1.run("CREATE (a:Node)").consume()
        with driver.session() as s2:
            tx1 = s1.begin_transaction()
            tx1.run("MATCH (a:Node) SET a.property = 1").consume()
            tx2 = s2.begin_transaction(timeout=0.25)
            with raises(TransientError):
                tx2.run("MATCH (a:Node) SET a.property = 2").consume()


# TODO: Re-enable and test when TC is available again
# def test_exit_after_explicit_close_should_be_silent(bolt_driver):
#     with bolt_driver.session() as s:
#         with s.begin_transaction() as tx:
#             assert not tx.closed()
#             tx.close()
#             assert tx.closed()
#         assert tx.closed()


def test_should_sync_after_commit(session):
    tx = session.begin_transaction()
    result = tx.run("RETURN 1")
    tx.commit()
    buffer = result._records
    assert len(buffer) == 1
    assert buffer[0][0] == 1


def test_should_sync_after_rollback(session):
    tx = session.begin_transaction()
    result = tx.run("RETURN 1")
    tx.rollback()
    buffer = result._records
    assert len(buffer) == 1
    assert buffer[0][0] == 1


def test_errors_on_run_transaction(session):
    tx = session.begin_transaction()
    with raises(TypeError):
        tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=uuid4())
    tx.rollback()


def test_error_on_using_closed_transaction(session):
    tx = session.begin_transaction()
    tx.run("RETURN 1")
    tx.commit()
    with raises(TransactionError):
        tx.run("RETURN 1")
