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
from uuid import uuid4


from neo4j.work.simple import unit_of_work
from neo4j.exceptions import ClientError

# python -m pytest tests/integration/test_tx_functions.py -s -v


def test_simple_read(session):

    def work(tx):
        return tx.run("RETURN 1").single().value()

    value = session.read_transaction(work)
    assert value == 1


def test_read_with_arg(session):

    def work(tx, x):
        return tx.run("RETURN $x", x=x).single().value()

    value = session.read_transaction(work, x=1)
    assert value == 1


def test_read_with_arg_and_metadata(session):

    # TODO: Investigate the Query object work pattern
    # from neo4j import Query
    # def work(tx, *args, **kwargs):
    #     query = Query("CALL dbms.getTXMetaData", timeout=10, metadata={"foo": "bar"})
    #     return tx.run(query).single().value()

    @unit_of_work(timeout=25, metadata={"foo": "bar"})
    def work(tx):
        return tx.run("CALL dbms.getTXMetaData").single().value()

    try:
        value = session.read_transaction(work)
    except ClientError:
        pytest.skip("Transaction metadata and timeout only supported in Neo4j EE 3.5+")
    else:
        assert value == {"foo": "bar"}


def test_simple_write(session):

    def work(tx):
        return tx.run("CREATE (a {x: 1}) RETURN a.x").single().value()

    value = session.write_transaction(work)
    assert value == 1


def test_write_with_arg(session):

    def work(tx, x):
        return tx.run("CREATE (a {x: $x}) RETURN a.x", x=x).single().value()

    value = session.write_transaction(work, x=1)
    assert value == 1


def test_write_with_arg_and_metadata(session):

    # TODO: Investigate the Query object work pattern
    # TODO: Raise TypeError you are doing it wrong dont input a Query instance use a string query.
    # from neo4j import Query
    # def work(tx, x, **kwargs):
    #     query = Query("CREATE (a {x: $x}) RETURN a.x", timeout=10, metadata={"foo": "bar"})  # Session.run(Query, )
    #     return tx.run(query, x=x).single().value()

    @unit_of_work(timeout=25, metadata={"foo": "bar"})
    def work(tx, x):
        return tx.run("CREATE (a {x: $x}) RETURN a.x", x=x).single().value()

    try:
        value = session.write_transaction(work, x=1)
    except ClientError:
        pytest.skip("Transaction metadata and timeout only supported in Neo4j EE 3.5+")
    else:
        assert value == 1


def test_error_on_write_transaction(session):

    def f(tx, uuid):
        tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=uuid), uuid4()

    with pytest.raises(TypeError):
        session.write_transaction(f)
