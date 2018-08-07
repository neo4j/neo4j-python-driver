#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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

from neo4j.v1 import \
    READ_ACCESS, WRITE_ACCESS, \
    CypherError, SessionError, TransactionError
from neo4j.v1.types.graph import Node, Relationship, Path
from neo4j.exceptions import CypherSyntaxError

from test.integration.tools import DirectIntegrationTestCase


class AutoCommitTransactionTestCase(DirectIntegrationTestCase):

    def test_can_run_simple_statement(self):
        session = self.driver.session()
        result = session.run("RETURN 1 AS n")
        for record in result:
            assert record[0] == 1
            assert record["n"] == 1
            with self.assertRaises(KeyError):
                _ = record["x"]
            assert record["n"] == 1
            with self.assertRaises(KeyError):
                _ = record["x"]
            with self.assertRaises(TypeError):
                _ = record[object()]
            assert repr(record)
            assert len(record) == 1
        session.close()

    def test_can_run_simple_statement_with_params(self):
        session = self.driver.session()
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
        session = self.driver.session()
        with self.assertRaises(CypherError):
            session.run("X").consume()

    def test_fails_on_missing_parameter(self):
        session = self.driver.session()
        with self.assertRaises(CypherError):
            session.run("RETURN {x}").consume()

    def test_can_run_simple_statement_from_bytes_string(self):
        session = self.driver.session()
        count = 0
        for record in session.run(b"RETURN 1 AS n"):
            assert record[0] == 1
            assert record["n"] == 1
            assert repr(record)
            assert len(record) == 1
            count += 1
        session.close()
        assert count == 1

    def test_can_run_statement_that_returns_multiple_records(self):
        session = self.driver.session()
        count = 0
        for record in session.run("unwind(range(1, 10)) AS z RETURN z"):
            assert 1 <= record[0] <= 10
            count += 1
        session.close()
        assert count == 10

    def test_can_use_with_to_auto_close_session(self):
        with self.driver.session() as session:
            record_list = list(session.run("RETURN 1"))
            assert len(record_list) == 1
            for record in record_list:
                assert record[0] == 1

    def test_can_return_node(self):
        with self.driver.session() as session:
            record_list = list(session.run("CREATE (a:Person {name:'Alice'}) RETURN a"))
            assert len(record_list) == 1
            for record in record_list:
                alice = record[0]
                assert isinstance(alice, Node)
                assert alice.labels == {"Person"}
                assert dict(alice) == {"name": "Alice"}

    def test_can_return_relationship(self):
        with self.driver.session() as session:
            record_list = list(session.run("CREATE ()-[r:KNOWS {since:1999}]->() RETURN r"))
            assert len(record_list) == 1
            for record in record_list:
                rel = record[0]
                assert isinstance(rel, Relationship)
                assert rel.type == "KNOWS"
                assert dict(rel) == {"since": 1999}

    def test_can_return_path(self):
        with self.driver.session() as session:
            record_list = list(session.run("MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p"))
            assert len(record_list) == 1
            for record in record_list:
                path = record[0]
                assert isinstance(path, Path)
                assert path.start_node["name"] == "Alice"
                assert path.end_node["name"] == "Bob"
                assert path.relationships[0].type == "KNOWS"
                assert len(path.nodes) == 2
                assert len(path.relationships) == 1

    def test_can_handle_cypher_error(self):
        with self.driver.session() as session:
            with self.assertRaises(CypherError):
                session.run("X").consume()

    def test_keys_are_available_before_and_after_stream(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 10) AS n RETURN n")
            assert list(result.keys()) == ["n"]
            list(result)
            assert list(result.keys()) == ["n"]

    def test_keys_with_an_error(self):
        with self.driver.session() as session:
            with self.assertRaises(CypherError):
                result = session.run("X")
                list(result.keys())

    def test_should_not_allow_empty_statements(self):
        with self.driver.session() as session:
            with self.assertRaises(ValueError):
                _ = session.run("")


class SummaryTestCase(DirectIntegrationTestCase):

    def test_can_obtain_summary_after_consuming_result(self):
        with self.driver.session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.summary()
            assert summary.statement == "CREATE (n) RETURN n"
            assert summary.parameters == {}
            assert summary.statement_type == "rw"
            assert summary.counters.nodes_created == 1

    def test_no_plan_info(self):
        with self.driver.session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.summary()
            assert summary.plan is None
            assert summary.profile is None

    def test_can_obtain_plan_info(self):
        with self.driver.session() as session:
            result = session.run("EXPLAIN CREATE (n) RETURN n")
            summary = result.summary()
            plan = summary.plan
            assert plan.operator_type == "ProduceResults"
            assert plan.identifiers == ["n"]
            assert len(plan.children) == 1

    def test_can_obtain_profile_info(self):
        with self.driver.session() as session:
            result = session.run("PROFILE CREATE (n) RETURN n")
            summary = result.summary()
            profile = summary.profile
            assert profile.db_hits == 0
            assert profile.rows == 1
            assert profile.operator_type == "ProduceResults"
            assert profile.identifiers == ["n"]
            assert len(profile.children) == 1

    def test_no_notification_info(self):
        with self.driver.session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.summary()
            notifications = summary.notifications
            assert notifications == []

    def test_can_obtain_notification_info(self):
        with self.driver.session() as session:
            result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
            summary = result.summary()
            notifications = summary.notifications

            assert len(notifications) == 1
            notification = notifications[0]
            assert notification.code == "Neo.ClientNotification.Statement.CartesianProductWarning"
            assert notification.title == "This query builds a cartesian product between " \
                                         "disconnected patterns."
            assert notification.severity == "WARNING"
            assert notification.description == "If a part of a query contains multiple " \
                                               "disconnected patterns, this will build a " \
                                               "cartesian product between all those parts. This " \
                                               "may produce a large amount of data and slow down " \
                                               "query processing. While occasionally intended, " \
                                               "it may often be possible to reformulate the " \
                                               "query that avoids the use of this cross product, " \
                                               "perhaps by adding a relationship between the " \
                                               "different parts or by using OPTIONAL MATCH " \
                                               "(identifier is: (m))"
            position = notification.position
            assert position

    def test_contains_time_information(self):
        if not self.at_least_server_version(3, 1):
            raise SkipTest("Execution times are not supported before server 3.1")
        with self.driver.session() as session:
            summary = session.run("UNWIND range(1,1000) AS n RETURN n AS number").consume()
            if self.protocol_version() >= 3:
                self.assertIsInstance(summary.t_first, int)
                self.assertIsInstance(summary.t_last, int)
            else:
                self.assertIsInstance(summary.result_available_after, int)
                self.assertIsInstance(summary.result_consumed_after, int)


class ResetTestCase(DirectIntegrationTestCase):

    def test_automatic_reset_after_failure(self):
        with self.driver.session() as session:
            try:
                session.run("X").consume()
            except CypherError:
                result = session.run("RETURN 1")
                record = next(iter(result))
                assert record[0] == 1
            else:
                assert False, "A Cypher error should have occurred"


class ExplicitTransactionTestCase(DirectIntegrationTestCase):

    def test_can_commit_transaction(self):

        with self.driver.session() as session:
            tx = session.begin_transaction()

            # Create a node
            result = tx.run("CREATE (a) RETURN id(a)")
            record = next(iter(result))
            node_id = record[0]
            assert isinstance(node_id, int)

            # Update a property
            tx.run("MATCH (a) WHERE id(a) = {n} "
                   "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            tx.commit()

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            record = next(iter(result))
            value = record[0]
            assert value == "bar"

    def test_can_rollback_transaction(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()

            # Create a node
            result = tx.run("CREATE (a) RETURN id(a)")
            record = next(iter(result))
            node_id = record[0]
            assert isinstance(node_id, int)

            # Update a property
            tx.run("MATCH (a) WHERE id(a) = {n} "
                   "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            tx.rollback()

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            assert len(list(result)) == 0

    def test_can_commit_transaction_using_with_block(self):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                record = next(iter(result))
                node_id = record[0]
                assert isinstance(node_id, int)

                # Update a property
                tx.run("MATCH (a) WHERE id(a) = {n} "
                       "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

                tx.success = True

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            record = next(iter(result))
            value = record[0]
            assert value == "bar"

    def test_can_rollback_transaction_using_with_block(self):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                record = next(iter(result))
                node_id = record[0]
                assert isinstance(node_id, int)

                # Update a property
                tx.run("MATCH (a) WHERE id(a) = {n} "
                       "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

                tx.success = False

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            assert len(list(result)) == 0

    def test_broken_transaction_should_not_break_session(self):
        with self.driver.session() as session:
            with self.assertRaises(CypherSyntaxError):
                with session.begin_transaction() as tx:
                    tx.run("X")
            with session.begin_transaction() as tx:
                tx.run("RETURN 1")

    def test_last_run_statement_should_be_cleared_on_failure(self):
        if not self.at_least_server_version(3, 2):
            raise SkipTest("Statement reuse is not supported before server 3.2")

        with self.driver.session() as session:
            tx = session.begin_transaction()
            tx.run("RETURN 1").consume()
            connection_1 = session._connection
            assert connection_1._last_run_statement == "RETURN 1"
            with self.assertRaises(CypherSyntaxError):
                result = tx.run("X")
                connection_2 = session._connection
                result.consume()
            # connection_2 = session._connection
            assert connection_2 is connection_1
            assert connection_2._last_run_statement is None
            tx.close()


class BookmarkingTestCase(DirectIntegrationTestCase):

    def test_can_obtain_bookmark_after_commit(self):
        if not self.at_least_server_version(3, 1):
            raise SkipTest("Bookmarking is not supported before server 3.1")
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                tx.run("RETURN 1")
            assert session.last_bookmark() is not None

    def test_can_pass_bookmark_into_next_transaction(self):
        if not self.at_least_server_version(3, 1):
            raise SkipTest("Bookmarking is not supported before server 3.1")

        unique_id = uuid4().hex

        with self.driver.session(access_mode=WRITE_ACCESS) as session:
            with session.begin_transaction() as tx:
                tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=unique_id)
            bookmark = session.last_bookmark()

        assert bookmark is not None

        with self.driver.session(access_mode=READ_ACCESS, bookmark=bookmark) as session:
            with session.begin_transaction() as tx:
                result = tx.run("MATCH (a:Thing {uuid:$uuid}) RETURN a", uuid=unique_id)
                record_list = list(result)
                assert len(record_list) == 1
                record = record_list[0]
                assert len(record) == 1
                thing = record[0]
                assert isinstance(thing, Node)
                assert thing["uuid"] == unique_id

    def test_bookmark_should_be_none_after_rollback(self):
        if not self.at_least_server_version(3, 1):
            raise SkipTest("Bookmarking is not supported before server 3.1")

        with self.driver.session(access_mode=WRITE_ACCESS) as session:
            with session.begin_transaction() as tx:
                tx.run("CREATE (a)")
        assert session.last_bookmark() is not None
        with self.driver.session(access_mode=WRITE_ACCESS) as session:
            with session.begin_transaction() as tx:
                tx.run("CREATE (a)")
                tx.success = False
        assert session.last_bookmark() is None


class SessionCompletionTestCase(DirectIntegrationTestCase):

    def test_should_sync_after_commit(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1")
            tx.commit()
            buffer = result._records
            assert len(buffer) == 1
            assert buffer[0][0] == 1

    def test_should_sync_after_rollback(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1")
            tx.rollback()
            buffer = result._records
            assert len(buffer) == 1
            assert buffer[0][0] == 1

    def test_errors_on_write_transaction(self):
        session = self.driver.session()
        with self.assertRaises(TypeError):
            session.write_transaction(lambda tx, uuid : tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=uuid), uuid4())
        session.close()

    def test_errors_on_run_transaction(self):
        session = self.driver.session()
        tx = session.begin_transaction()
        with self.assertRaises(TypeError):
            tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=uuid4())
        tx.rollback()
        session.close()

    def test_errors_on_run_session(self):
        session = self.driver.session()
        session.close()
        with self.assertRaises(SessionError):
            session.run("RETURN 1")

    def test_errors_on_begin_transaction(self):
        session = self.driver.session()
        session.close()
        with self.assertRaises(SessionError):
            session.begin_transaction()

    def test_large_values(self):
        for i in range(1, 7):
            session = self.driver.session()
            session.run("RETURN '{}'".format("A" * 2 ** 20))
            session.close()

class TransactionCommittedTestCase(DirectIntegrationTestCase):

    def setUp(self):
        super(TransactionCommittedTestCase, self).setUp()
        self.session = self.driver.session()
        self.transaction = self.session.begin_transaction()
        self.transaction.run("RETURN 1")
        self.transaction.commit()

    def test_errors_on_run(self):
        with self.assertRaises(TransactionError):
            self.transaction.run("RETURN 1")
