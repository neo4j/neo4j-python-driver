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

from sys import stderr
from threading import Thread
from timeit import default_timer as time

from neo4j.v1 import GraphDatabase, CypherError


class Runner(Thread):

    def __init__(self, size, batch_size):
        super(Runner, self).__init__()
        self.size = size
        self.batch_size = batch_size
        self.driver = GraphDatabase.driver("bolt://localhost:7687/", auth=("neo4j", "password"))

    def run(self):
        self.drop_index()
        self.delete_all()
        self.create_nodes()
        self.create_index()
        self.match_nodes()

    def drop_index(self):
        with self.driver.session() as session:
            try:
                session.run("DROP INDEX ON :Thing(integer)").consume()
            except CypherError:
                pass

    def delete_all(self):
        t0 = time()
        with self.driver.session() as session:
            total_nodes_deleted = 0
            deleting = True
            while deleting:
                summary = session.run("""\
                MATCH (a) WITH a LIMIT {size}
                DETACH DELETE a
                """, size=self.batch_size).summary()
                total_nodes_deleted += summary.counters.nodes_deleted
                stderr.write("Deleted %d nodes\r" % total_nodes_deleted)
                deleting = bool(summary.counters.nodes_deleted)
            t1 = time()
            stderr.write("Deleted %d nodes in %fs\n" % (total_nodes_deleted, t1 - t0))

    def create_nodes(self):
        t0 = time()
        with self.driver.session() as session:
            p = 0
            while p < self.size:
                q = min(p + self.batch_size, self.size)
                with session.begin_transaction() as tx:
                    for n in range(p + 1, q + 1):
                        tx.run("CREATE (a:Thing {x})", x={
                            "boolean": n % 2 == 0,
                            "integer": n,
                            "float": float(n),
                            "string": "number %d" % n,
                        })
                stderr.write("Created %d nodes\r" % q)
                p = q
            t1 = time()
            stderr.write("Created %d nodes in %fs\n" % (self.size, t1 - t0))

    def create_index(self):
        with self.driver.session() as session:
            session.run("CREATE INDEX ON :Thing(integer)").consume()

    def match_nodes(self):
        t0 = time()
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                n = 0
                result = tx.run("MATCH (a:Thing) RETURN a")
                for n, record in enumerate(result, 1):
                    _ = record["a"]
                    if n % self.batch_size == 0:
                        stderr.write("Matched %d nodes\r" % n)
                t1 = time()
                stderr.write("Matched %d nodes in %fs\n" % (n, t1 - t0))


if __name__ == "__main__":
    Runner(100000, 20000).run()
