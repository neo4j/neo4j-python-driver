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


from __future__ import unicode_literals

import logging
from argparse import ArgumentParser
from json import loads as json_loads
from sys import stdout, stderr

from .util import Watcher
from .v1.session import GraphDatabase, CypherError


def main():
    parser = ArgumentParser(description="Execute one or more Cypher statements using Bolt.")
    parser.add_argument("statement", nargs="+")
    parser.add_argument("-u", "--url", default="bolt://localhost", metavar="CONNECTION_URL")
    parser.add_argument("-p", "--parameter", action="append", metavar="NAME=VALUE")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-s", "--secure", action="store_true")
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-x", "--times", type=int, default=1)
    parser.add_argument("-z", "--summarize", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        level = logging.INFO if args.verbose == 1 else logging.DEBUG
        Watcher("neo4j").watch(level, stderr)

    parameters = {}
    for parameter in args.parameter or []:
        name, _, value = parameter.partition("=")
        if value == "" and name in parameters:
            del parameters[name]
        else:
            try:
                parameters[name] = json_loads(value)
            except ValueError:
                parameters[name] = value

    driver = GraphDatabase.driver(args.url, secure=args.secure)
    session = driver.session()
    for _ in range(args.times):
        for statement in args.statement:
            try:
                cursor = session.run(statement, parameters)
            except CypherError as error:
                stderr.write("%s: %s\r\n" % (error.code, error.message))
            else:
                if not args.quiet:
                    has_results = False
                    for i, record in enumerate(cursor.stream()):
                        has_results = True
                        if i == 0:
                            stdout.write("%s\r\n" % "\t".join(record.keys()))
                        stdout.write("%s\r\n" % "\t".join(map(repr, record)))
                    if has_results:
                        stdout.write("\r\n")
                    if args.summary:
                        summary = cursor.summary
                        stdout.write("Statement      : %r\r\n" % summary.statement)
                        stdout.write("Parameters     : %r\r\n" % summary.parameters)
                        stdout.write("Statement Type : %r\r\n" % summary.statement_type)
                        stdout.write("Statistics     : %r\r\n" % summary.statistics)
                        stdout.write("\r\n")
    session.close()


if __name__ == "__main__":
    main()
