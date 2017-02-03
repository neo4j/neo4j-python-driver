#!/usr/bin/env python
# -*- coding: utf-8 -*-


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
                session.run("DROP INDEX ON :Person(name)").consume()
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
            session.run("""\
            UNWIND range(1, {count}) AS id
            CREATE (a:Person {id: id, name: 'name ' + id})
            """, count=self.size)
            t1 = time()
            stderr.write("Created %d nodes in %fs\n" % (self.size, t1 - t0))

    def create_index(self):
        with self.driver.session() as session:
            session.run("CREATE INDEX ON :Person(name)").consume()

    def match_nodes(self):
        t0 = time()
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                n = 0
                result = tx.run("MATCH (a) RETURN a.name AS name")
                for n, record in enumerate(result, 1):
                    _ = record["name"]
                    if n % self.batch_size == 0:
                        stderr.write("Matched %d nodes\r" % n)
                t1 = time()
                stderr.write("Matched %d nodes in %fs\n" % (n, t1 - t0))


if __name__ == "__main__":
    Runner(1000000, 20000).run()
