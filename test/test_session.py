#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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


from socket import socket
from ssl import SSLSocket

from mock import patch
from neo4j.v1.constants import TRUST_ON_FIRST_USE
from neo4j.v1.exceptions import CypherError
from neo4j.v1.session import GraphDatabase, basic_auth, Record
from neo4j.v1.types import Node, Relationship, Path

from test.util import ServerTestCase


auth_token = basic_auth("neo4j", "password")


class DriverTestCase(ServerTestCase):

    def test_healthy_session_will_be_returned_to_the_pool_on_close(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        assert len(driver.session_pool) == 0
        driver.session().close()
        assert len(driver.session_pool) == 1

    def test_unhealthy_session_will_not_be_returned_to_the_pool_on_close(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        assert len(driver.session_pool) == 0
        session = driver.session()
        session.connection.defunct = True
        session.close()
        assert len(driver.session_pool) == 0

    def session_pool_cannot_exceed_max_size(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token, max_pool_size=1)
        assert len(driver.session_pool) == 0
        driver.session().close()
        assert len(driver.session_pool) == 1
        driver.session().close()
        assert len(driver.session_pool) == 1

    def test_session_that_dies_in_the_pool_will_not_be_given_out(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session_1 = driver.session()
        session_1.close()
        assert len(driver.session_pool) == 1
        session_1.connection.close()
        session_2 = driver.session()
        assert session_2 is not session_1

    def test_must_use_valid_url_scheme(self):
        with self.assertRaises(ValueError):
            GraphDatabase.driver("x://xxx", auth=auth_token)

    def test_sessions_are_reused(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session_1 = driver.session()
        session_1.close()
        session_2 = driver.session()
        session_2.close()
        assert session_1 is session_2

    def test_sessions_are_not_reused_if_still_in_use(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session_1 = driver.session()
        session_2 = driver.session()
        session_2.close()
        session_1.close()
        assert session_1 is not session_2


class SecurityTestCase(ServerTestCase):

    def test_default_session_uses_tofu(self):
        driver = GraphDatabase.driver("bolt://localhost")
        assert driver.trust == TRUST_ON_FIRST_USE

    def test_insecure_session_uses_normal_socket(self):
        driver = GraphDatabase.driver("bolt://localhost",  auth=auth_token, encrypted=False)
        session = driver.session()
        connection = session.connection
        assert isinstance(connection.channel.socket, socket)
        assert connection.der_encoded_server_certificate is None
        session.close()

    def test_tofu_session_uses_secure_socket(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token, encrypted=True, trust=TRUST_ON_FIRST_USE)
        session = driver.session()
        connection = session.connection
        assert isinstance(connection.channel.socket, SSLSocket)
        assert connection.der_encoded_server_certificate is not None
        session.close()

    def test_tofu_session_trusts_certificate_after_first_use(self):
        driver = GraphDatabase.driver("bolt://localhost",  auth=auth_token, encrypted=True, trust=TRUST_ON_FIRST_USE)
        session = driver.session()
        connection = session.connection
        certificate = connection.der_encoded_server_certificate
        session.close()
        session = driver.session()
        connection = session.connection
        assert connection.der_encoded_server_certificate == certificate
        session.close()


class RunTestCase(ServerTestCase):

    def test_can_run_simple_statement(self):
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
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
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
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
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
        with self.assertRaises(CypherError):
            session.run("X").consume()

    def test_fails_on_missing_parameter(self):
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
        with self.assertRaises(CypherError):
            session.run("RETURN {x}").consume()

    def test_can_run_simple_statement_from_bytes_string(self):
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
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
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
        count = 0
        for record in session.run("unwind(range(1, 10)) AS z RETURN z"):
            assert 1 <= record[0] <= 10
            count += 1
        session.close()
        assert count == 10

    def test_can_use_with_to_auto_close_session(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            record_list = list(session.run("RETURN 1"))
            assert len(record_list) == 1
            for record in record_list:
                assert record[0] == 1

    def test_can_return_node(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            record_list = list(session.run("MERGE (a:Person {name:'Alice'}) RETURN a"))
            assert len(record_list) == 1
            for record in record_list:
                alice = record[0]
                assert isinstance(alice, Node)
                assert alice.labels == {"Person"}
                assert alice.properties == {"name": "Alice"}

    def test_can_return_relationship(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            reocrd_list = list(session.run("MERGE ()-[r:KNOWS {since:1999}]->() RETURN r"))
            assert len(reocrd_list) == 1
            for record in reocrd_list:
                rel = record[0]
                assert isinstance(rel, Relationship)
                assert rel.type == "KNOWS"
                assert rel.properties == {"since": 1999}

    def test_can_return_path(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            record_list = list(session.run("MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p"))
            assert len(record_list) == 1
            for record in record_list:
                path = record[0]
                assert isinstance(path, Path)
                assert path.start.properties == {"name": "Alice"}
                assert path.end.properties == {"name": "Bob"}
                assert path.relationships[0].type == "KNOWS"
                assert len(path.nodes) == 2
                assert len(path.relationships) == 1

    def test_can_handle_cypher_error(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            with self.assertRaises(CypherError):
                session.run("X").consume()

    def test_keys_are_available_before_and_after_stream(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("UNWIND range(1, 10) AS n RETURN n")
            assert list(result.keys()) == ["n"]
            list(result)
            assert list(result.keys()) == ["n"]

    def test_keys_with_an_error(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("X")
            with self.assertRaises(CypherError):
                list(result.keys())


class SummaryTestCase(ServerTestCase):

    def test_can_obtain_summary_after_consuming_result(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.consume()
            assert summary.statement == "CREATE (n) RETURN n"
            assert summary.parameters == {}
            assert summary.statement_type == "rw"
            assert summary.counters.nodes_created == 1

    def test_no_plan_info(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.consume()
            assert summary.plan is None
            assert summary.profile is None

    def test_can_obtain_plan_info(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("EXPLAIN CREATE (n) RETURN n")
            summary = result.consume()
            plan = summary.plan
            assert plan.operator_type == "ProduceResults"
            assert plan.identifiers == ["n"]
            assert plan.arguments == {"planner": "COST", "EstimatedRows": 1.0, "version": "CYPHER 3.0",
                                      "KeyNames": "n", "runtime-impl": "INTERPRETED", "planner-impl": "IDP",
                                      "runtime": "INTERPRETED"}
            assert len(plan.children) == 1

    def test_can_obtain_profile_info(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("PROFILE CREATE (n) RETURN n")
            summary = result.consume()
            profile = summary.profile
            assert profile.db_hits == 0
            assert profile.rows == 1
            assert profile.operator_type == "ProduceResults"
            assert profile.identifiers == ["n"]
            assert profile.arguments == {"planner": "COST", "EstimatedRows": 1.0, "version": "CYPHER 3.0",
                                         "KeyNames": "n", "runtime-impl": "INTERPRETED", "planner-impl": "IDP",
                                         "runtime": "INTERPRETED", "Rows": 1, "DbHits": 0}
            assert len(profile.children) == 1

    def test_no_notification_info(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("CREATE (n) RETURN n")
            summary = result.consume()
            notifications = summary.notifications
            assert notifications == []

    def test_can_obtain_notification_info(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
            summary = result.consume()
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
            assert position.offset == 0
            assert position.line == 1
            assert position.column == 1


class ResetTestCase(ServerTestCase):

    def test_automatic_reset_after_failure(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            try:
                session.run("X").consume()
            except CypherError:
                result = session.run("RETURN 1")
                record = next(iter(result))
                assert record[0] == 1
            else:
                assert False, "A Cypher error should have occurred"

    def test_defunct(self):
        from neo4j.v1.connection import ChunkChannel, ProtocolError
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            assert not session.connection.defunct
            with patch.object(ChunkChannel, "chunk_reader", side_effect=ProtocolError()):
                with self.assertRaises(ProtocolError):
                    session.run("RETURN 1").consume()
            assert session.connection.defunct
            assert session.connection.closed


class RecordTestCase(ServerTestCase):
    def test_record_equality(self):
        record1 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record2 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record3 = Record(["name", "empire"], ["Stefan", "Das Deutschland"])
        assert record1 == record2
        assert record1 != record3
        assert record2 != record3

    def test_record_hashing(self):
        record1 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record2 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record3 = Record(["name", "empire"], ["Stefan", "Das Deutschland"])
        assert hash(record1) == hash(record2)
        assert hash(record1) != hash(record3)
        assert hash(record2) != hash(record3)

    def test_record_keys(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.keys()) == ["name", "empire"]

    def test_record_values(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.values()) == ["Nigel", "The British Empire"]

    def test_record_items(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.items()) == [("name", "Nigel"), ("empire", "The British Empire")]

    def test_record_index(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert a_record.index("name") == 0
        assert a_record.index("empire") == 1
        with self.assertRaises(KeyError):
            a_record.index("crap")

    def test_record_contains(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert "name" in a_record
        assert "empire" in a_record
        assert "Germans" not in a_record

    def test_record_iter(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.__iter__()) == ["name", "empire"]

    def test_record_copy(self):
        original = Record(["name", "empire"], ["Nigel", "The British Empire"])
        duplicate = original.copy()
        assert dict(original) == dict(duplicate)
        assert original.keys() == duplicate.keys()
        assert original is not duplicate

    def test_record_as_dict(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert dict(a_record) == {"name": "Nigel", "empire": "The British Empire"}

    def test_record_as_list(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record) == ["name", "empire"]

    def test_record_len(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert len(a_record) == 2

    def test_record_repr(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert repr(a_record) == "<Record name='Nigel' empire='The British Empire'>"


class TransactionTestCase(ServerTestCase):

    def test_can_commit_transaction(self):
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
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
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
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
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
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
        with GraphDatabase.driver("bolt://localhost", auth=auth_token).session() as session:
            with session.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                record = next(iter(result))
                node_id = record[0]
                assert isinstance(node_id, int)

                # Update a property
                tx.run("MATCH (a) WHERE id(a) = {n} "
                       "SET a.foo = {foo}", {"n": node_id, "foo": "bar"})

            # Check the property value
            result = session.run("MATCH (a) WHERE id(a) = {n} "
                                 "RETURN a.foo", {"n": node_id})
            assert len(list(result)) == 0
