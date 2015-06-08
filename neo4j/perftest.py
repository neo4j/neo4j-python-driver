#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

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


"""
python -m neo4j.perftest
"""

from __future__ import print_function

from multiprocessing import Array, Process
from os import getenv
from os.path import basename
from timeit import timeit, repeat
import sys
import subprocess
from math import log, ceil

import neo4j
try:
    from py2neo import Graph
except ImportError:
    print("Py2neo must be installed")
    sys.exit(1)


USAGE = """\
Usage: {script} [«options»] «statement» [ «statement» ... ]

Test performance of one or more Cypher statements against a
Neo4j server.

Options:
  -? --help              display this help text
  -x --times COUNT       number of times to execute each statement
                         (default 2500)
  -p --parallels VALUES  comma separated list of parallel values
                         (default: 1,2,4,8,16)

Environment:
  NEO4J_URI - base URI of Neo4j database, e.g. neo4j://localhost

Report bugs to nigel@neotechnology.com
"""


def processors():
    p = 0
    output = subprocess.check_output("cat /proc/cpuinfo", shell=True).strip()
    for line in output.split(b"\n"):
        if line.startswith(b"processor"):
            p += 1
    return p


class Remoting(Process):

    @classmethod
    def run_all(cls, process_count, run_count, record_count):
        times = [Array("d", run_count + 1) for _ in range(process_count)]
        processes = [cls(run_count, record_count, times[i]) for i in range(process_count)]
        for process in processes:
            process.start()
        for process in processes:
            process.join()
        return [[n - t[i - 1] for i, n in enumerate(t)][1:] for t in times]

    def __init__(self, run_count, statement, times):
        super(Remoting, self).__init__()
        self.run_count = run_count
        self.statement = statement
        self.times = times


class HTTPLegacy(Remoting):

    def __init__(self, run_count, statement, times):
        super(HTTPLegacy, self).__init__(run_count, statement, times)
        self.graph = Graph("http://neo4j:password@localhost:13676/db/data/")
        self.cypher = self.graph.cypher

    def run(self):
        post = self.cypher.post
        hydrate = self.graph.hydrate
        statement = self.statement
        run_count = self.run_count
        #times = self.times
        for i in range(run_count):
            #times[i] = perf_counter()
            response = post(statement)
            try:
                hydrate(response.content)
            finally:
                response.close()
        #times[run_count] = perf_counter()


class HTTPTransactional(Remoting):

    def __init__(self, run_count, statement, times):
        super(HTTPTransactional, self).__init__(run_count, statement, times)
        self.graph = Graph("http://neo4j:password@localhost:13676/db/data/")
        self.cypher = self.graph.cypher

    def run(self):
        cypher = self.cypher
        statement = self.statement
        run_count = self.run_count
        #times = self.times
        for i in range(run_count):
            #times[i] = perf_counter()
            tx = cypher.begin()
            tx.append(statement)
            tx.commit()
        #times[run_count] = perf_counter()


class NDP(Remoting):

    def __init__(self, run_count, statement, times):
        super(NDP, self).__init__(run_count, statement, times)
        self.session = neo4j.session(getenv("NEO4J_URI", "neo4j://localhost"))

    def run(self):
        run = self.session.run
        statement = self.statement
        run_count = self.run_count
        #times = self.times
        for i in range(run_count):
            #times[i] = perf_counter()
            run(statement)
        #times[run_count] = perf_counter()


def help_(**kwargs):
    print(USAGE.format(**kwargs))


def main():
    processor_count = processors()
    print("This machine has %d processors" % processor_count)
    script, args = sys.argv[0], sys.argv[1:]
    run_count = 2500
    statements = []
    parallels = list(2 ** n for n in range(ceil(log(16, 2)) + 1))
    legacy_remoting = False
    old_remoting = False
    while args:
        arg = args.pop(0)
        if arg.startswith("-"):
            if arg in ("-h", "--help"):
                help_(script=basename(script))
                sys.exit(0)
            elif arg in ("-x", "--times"):
                run_count = int(args.pop(0))
            elif arg in ("-p", "--parallels"):
                parallels = list(map(int, args.pop(0).split(",")))
            elif arg in ("-l", "--legacy-remoting"):
                legacy_remoting = True
            elif arg in ("-o", "--old-remoting"):
                old_remoting = True
            else:
                print("Unknown option %r" % arg)
                sys.exit(1)
        else:
            statements.append(arg)
    if not statements:
        print("No statements specified, using defaults")
        session = neo4j.session(getenv("NEO4J_URI", "neo4j://localhost"))
        session.run("CREATE CONSTRAINT ON (a:Thing) ASSERT a.foo IS UNIQUE")
        results = session.run("MERGE (a:Thing {foo:'bar'}) RETURN id(a)")
        node_id = results[0][0]
        session.close()
        statements = ["unwind(range(1, %d)) AS z RETURN z" % n for n in [0, 1, 10, 100]] + \
                     ["MATCH (a) WHERE id(a) = %d RETURN a" % node_id,
                      "MATCH (a:Thing) WHERE a.foo = 'bar' RETURN a"]
    for statement in statements:
        print("Running %r × %d (best of 3)" % (statement, run_count))
        for process_count in parallels:
            print("  × %2d client%s" % (
                process_count, " " if process_count == 1 else "s"), end="")

            if legacy_remoting:
                python_statement = 'HTTPLegacy.run_all(%d, %d, %r)' % (
                    process_count, run_count, statement)
                t = repeat(python_statement, 'from neo4j.perftest import HTTPLegacy', number=1)
                process_run_count = process_count * run_count
                tx_per_second = process_run_count / min(t)
                print(" -> {:13,.6f} tx/s (HTTPLegacy)".format(tx_per_second), end="")

            if old_remoting:
                python_statement = 'HTTPTransactional.run_all(%d, %d, %r)' % (
                    process_count, run_count, statement)
                t = repeat(python_statement, 'from neo4j.perftest import HTTPTransactional', number=1)
                process_run_count = process_count * run_count
                tx_per_second = process_run_count / min(t)
                print(" -> {:13,.6f} tx/s (HTTPTx)".format(tx_per_second), end="")

            python_statement = 'NDP.run_all(%d, %d, %r)' % (
                process_count, run_count, statement)
            t = repeat(python_statement, 'from neo4j.perftest import NDP', number=1)
            process_run_count = process_count * run_count
            tx_per_second = process_run_count / min(t)
            print(" -> {:13,.6f} tx/s (NDP)".format(tx_per_second), end="")

            print()

        print()


if __name__ == "__main__":
    main()
