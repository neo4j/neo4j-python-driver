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


from time import sleep
from unittest import SkipTest
from uuid import uuid4

from neo4j.pipelines import Pipeline
from neo4j import \
    READ_ACCESS, WRITE_ACCESS, \
    CypherError, SessionError, TransactionError, unit_of_work, Statement
from neo4j.types.graph import Node, Relationship, Path
from neo4j.exceptions import CypherSyntaxError, TransientError

from test.integration.tools import DirectIntegrationTestCase


class PipelineTestCase(DirectIntegrationTestCase):

    def test_can_run_simple_statement(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        pipeline.push("RETURN 1 AS n")
        for record in pipeline.pull():
            assert record[0] == 1
            # TODO: why does pipeline not look like a regular result?
            #assert record["n"] == 1
            #with self.assertRaises(KeyError):
            #    _ = record["x"]
            #assert record["n"] == 1
            #with self.assertRaises(KeyError):
            #    _ = record["x"]
            with self.assertRaises(TypeError):
                _ = record[object()]
            assert repr(record)
            assert len(record) == 1
        pipeline.close()

    def test_can_run_simple_statement_with_params(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        count = 0
        pipeline.push("RETURN {x} AS n", {"x": {"abc": ["d", "e", "f"]}})
        for record in pipeline.pull():
            assert record[0] == {"abc": ["d", "e", "f"]}
            # TODO: why does pipeline not look like a regular result?
            #assert record["n"] == {"abc": ["d", "e", "f"]}
            assert repr(record)
            assert len(record) == 1
            count += 1
        pipeline.close()
        assert count == 1

    def test_fails_on_bad_syntax(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        with self.assertRaises(CypherError):
            pipeline.push("X")
            next(pipeline.pull())

    def test_doesnt_fail_on_bad_syntax_somewhere(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        pipeline.push("RETURN 1 AS n")
        pipeline.push("X")
        assert next(pipeline.pull())[0] == 1
        with self.assertRaises(CypherError):
            next(pipeline.pull())

    def test_fails_on_missing_parameter(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        with self.assertRaises(CypherError):
            pipeline.push("RETURN {x}")
            next(pipeline.pull())


    def test_can_run_simple_statement_from_bytes_string(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        count = 0
        pipeline.push(b"RETURN 1 AS n")
        for record in pipeline.pull():
            assert record[0] == 1
            assert record["n"] == 1
            assert repr(record)
            assert len(record) == 1
            count += 1
        pipeline.close()
        assert count == 1

    def test_can_run_statement_that_returns_multiple_records(self):
        pipeline: Pipeline = self.driver.pipeline(flush_every=0)
        count = 0
        pipeline.push("unwind(range(1, 10)) AS z RETURN z")
        for record in pipeline.pull():
            assert 1 <= record[0] <= 10
            count += 1
        pipeline.close()
        print(count)
        assert count == 10

    def test_can_return_node(self):
        with self.driver.pipeline(flush_every=0) as pipeline:
            pipeline.push("CREATE (a:Person {name:'Alice'}) RETURN a")
            record_list = list(pipeline.pull())
            assert len(record_list) == 1
            for record in record_list:
                alice = record[0]
                assert isinstance(alice, Node)
                assert alice.labels == {"Person"}
                assert dict(alice) == {"name": "Alice"}

    def test_can_return_relationship(self):
        with self.driver.pipeline(flush_every=0) as pipeline:
            pipeline.push("CREATE ()-[r:KNOWS {since:1999}]->() RETURN r")
            record_list = list(pipeline.pull())
            assert len(record_list) == 1
            for record in record_list:
                rel = record[0]
                assert isinstance(rel, Relationship)
                assert rel.type == "KNOWS"
                assert dict(rel) == {"since": 1999}

    def test_can_return_path(self):
        with self.driver.pipeline(flush_every=0) as pipeline:
            pipeline.push("MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p")
            record_list = list(pipeline.pull())
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
        with self.driver.pipeline(flush_every=0) as pipeline:
            pipeline.push("X")
            with self.assertRaises(CypherError):
                next(pipeline.pull())

    def test_should_not_allow_empty_statements(self):
        with self.driver.pipeline(flush_every=0) as pipeline:
            pipeline.push("")
            with self.assertRaises(CypherSyntaxError):
                next(pipeline.pull())


class ResetTestCase(DirectIntegrationTestCase):

    def test_automatic_reset_after_failure(self):
        with self.driver.pipeline(flush_every=0) as pipeline:
            try:
                pipeline.push("X")
                next(pipeline.pull())
            except CypherError:
                pipeline.push("RETURN 1")
                record = next(pipeline.pull())
                assert record[0] == 1
            else:
                assert False, "A Cypher error should have occurred"
