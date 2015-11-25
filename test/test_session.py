#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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


from unittest import TestCase

from neo4j.v1.session import GraphDatabase, CypherError
from neo4j.v1.typesystem import Node, Relationship, Path


class RunTestCase(TestCase):
    def test_must_use_valid_url_scheme(self):
        with self.assertRaises(ValueError):
            GraphDatabase.driver("x://xxx")

    def test_can_run_simple_statement(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run("RETURN 1 AS n"):
            assert record[0] == 1
            assert record["n"] == 1
            with self.assertRaises(AttributeError):
                _ = record["x"]
            assert record.n == 1
            with self.assertRaises(AttributeError):
                _ = record.x
            with self.assertRaises(TypeError):
                _ = record[object()]
            assert repr(record)
            assert len(record) == 1
            count += 1
        session.close()
        assert count == 1

    def test_can_run_simple_statement_with_params(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run("RETURN {x} AS n", {"x": {"abc": ["d", "e", "f"]}}):
            assert record[0] == {"abc": ["d", "e", "f"]}
            assert record["n"] == {"abc": ["d", "e", "f"]}
            assert repr(record)
            assert len(record) == 1
            count += 1
        session.close()
        assert count == 1

    def test_fails_on_bad_syntax(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        with self.assertRaises(CypherError):
            session.run("X").consume()

    def test_fails_on_missing_parameter(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        with self.assertRaises(CypherError):
            session.run("RETURN {x}").consume()

    def test_can_run_simple_statement_from_bytes_string(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run(b"RETURN 1 AS n"):
            assert record[0] == 1
            assert record["n"] == 1
            assert record.n == 1
            assert repr(record)
            assert len(record) == 1
            count += 1
        session.close()
        assert count == 1

    def test_can_run_statement_that_returns_multiple_records(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run("unwind(range(1, 10)) AS z RETURN z"):
            assert 1 <= record[0] <= 10
            count += 1
        session.close()
        assert count == 10

    def test_can_use_with_to_auto_close_session(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("RETURN 1")
            assert len(result) == 1
            for record in result:
                assert record[0] == 1

    def test_can_return_node(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("MERGE (a:Person {name:'Alice'}) RETURN a")
            assert len(result) == 1
            for record in result:
                alice = record[0]
                assert isinstance(alice, Node)
                assert alice.labels == {"Person"}
                assert alice.properties == {"name": "Alice"}

    def test_can_return_relationship(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("MERGE ()-[r:KNOWS {since:1999}]->() RETURN r")
            assert len(result) == 1
            for record in result:
                rel = record[0]
                assert isinstance(rel, Relationship)
                assert rel.type == "KNOWS"
                assert rel.properties == {"since": 1999}

    def test_can_return_path(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p")
            assert len(result) == 1
            for record in result:
                path = record[0]
                assert isinstance(path, Path)
                assert path.start.properties == {"name": "Alice"}
                assert path.end.properties == {"name": "Bob"}
                assert path.relationships[0].type == "KNOWS"
                assert len(path.nodes) == 2
                assert len(path.relationships) == 1

    def test_can_handle_cypher_error(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            with self.assertRaises(CypherError):
                session.run("X")

    def test_record_equality(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("unwind([1, 1]) AS a RETURN a")
            assert result[0] == result[1]
            assert result[0] != "this is not a record"

    def test_can_obtain_summary_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.summarize()
            assert summary.statement == "CREATE (n) RETURN n"
            assert summary.parameters == {}
            assert summary.statement_type == "rw"
            assert summary.statistics.nodes_created == 1

    def test_no_plan_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("CREATE (n) RETURN n")
            assert result.summarize().plan is None
            assert result.summarize().profile is None

    def test_can_obtain_plan_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("EXPLAIN CREATE (n) RETURN n")
            plan = result.summarize().plan
            assert plan.operator_type == "ProduceResults"
            assert plan.identifiers == ["n"]
            assert plan.arguments == {"planner": "COST", "EstimatedRows": 1.0, "version": "CYPHER 3.0",
                                      "KeyNames": "n", "runtime-impl": "INTERPRETED", "planner-impl": "IDP",
                                      "runtime": "INTERPRETED"}
            assert len(plan.children) == 1

    def test_can_obtain_profile_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("PROFILE CREATE (n) RETURN n")
            profile = result.summarize().profile
            assert profile.db_hits == 0
            assert profile.rows == 1
            assert profile.operator_type == "ProduceResults"
            assert profile.identifiers == ["n"]
            assert profile.arguments == {"planner": "COST", "EstimatedRows": 1.0, "version": "CYPHER 3.0",
                                         "KeyNames": "n", "runtime-impl": "INTERPRETED", "planner-impl": "IDP",
                                         "runtime": "INTERPRETED", "Rows": 1, "DbHits": 0}
            assert len(profile.children) == 1

    def test_no_notification_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("CREATE (n) RETURN n")
            notifications = result.summarize().notifications
            assert notifications == []

    def test_can_obtain_notification_info(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
            notifications = result.summarize().notifications

            assert len(notifications) == 1
            notification = notifications[0]
            assert notification.code == "Neo.ClientNotification.Statement.CartesianProduct"
            assert notification.title == "This query builds a cartesian product between disconnected patterns."
            assert notification.description == \
                   "If a part of a query contains multiple disconnected patterns, " \
                   "this will build a cartesian product between all those parts. " \
                   "This may produce a large amount of data and slow down query processing. " \
                   "While occasionally intended, it may often be possible to reformulate the query " \
                   "that avoids the use of this cross product, perhaps by adding a relationship between " \
                   "the different parts or by using OPTIONAL MATCH (identifier is: (m))"
            position = notification.position
            assert position.offset == 0
            assert position.line == 1
            assert position.column == 1


class TransactionTestCase(TestCase):
    def test_can_commit_transaction(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            tx = session.begin_transaction()

            # Create a node
            result = tx.run("CREATE (a) RETURN id(a)")
            node_id = result[0][0]
            assert isinstance(node_id, int)

            # Update a property
            tx.run("MATCH (a) WHERE id(a) = {n} "
                   "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            tx.commit()

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            foo = result[0][0]
            assert foo == "bar"

    def test_can_rollback_transaction(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            tx = session.begin_transaction()

            # Create a node
            result = tx.run("CREATE (a) RETURN id(a)")
            node_id = result[0][0]
            assert isinstance(node_id, int)

            # Update a property
            tx.run("MATCH (a) WHERE id(a) = {n} "
                   "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            tx.rollback()

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            assert len(result) == 0

    def test_can_commit_transaction_using_with_block(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            with session.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                node_id = result[0][0]
                assert isinstance(node_id, int)

                # Update a property
                tx.run("MATCH (a) WHERE id(a) = {n} "
                       "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

                tx.success = True

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            foo = result[0][0]
            assert foo == "bar"

    def test_can_rollback_transaction_using_with_block(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            with session.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                node_id = result[0][0]
                assert isinstance(node_id, int)

                # Update a property
                tx.run("MATCH (a) WHERE id(a) = {n} "
                       "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            assert len(result) == 0
