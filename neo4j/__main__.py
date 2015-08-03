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


from __future__ import unicode_literals

from argparse import ArgumentParser
import logging
from sys import stdout, stderr

from .driver import GraphDatabase, CypherError


class ColourFormatter(logging.Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        if record.levelno == logging.CRITICAL:
            return "\x1b[31;1m%s\x1b[0m" % s  # bright red
        elif record.levelno == logging.ERROR:
            return "\x1b[33;1m%s\x1b[0m" % s  # bright yellow
        elif record.levelno == logging.WARNING:
            return "\x1b[33m%s\x1b[0m" % s    # yellow
        elif record.levelno == logging.INFO:
            return "\x1b[36m%s\x1b[0m" % s    # cyan
        elif record.levelno == logging.DEBUG:
            return "\x1b[34m%s\x1b[0m" % s    # blue
        else:
            return s


class Watcher(object):
    """ Log watcher for debug output.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def watch(self, level=logging.INFO, out=stdout):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass
        handler = logging.StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)


def main():
    parser = ArgumentParser(description="Execute one or more Cypher statements using GAP.")
    parser.add_argument("statement", nargs="+")
    parser.add_argument("-u", "--url", default="gap://localhost")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-s", "--secure", action="store_true")
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-x", "--times", type=int, default=1)
    args = parser.parse_args()

    if args.verbose:
        level = logging.INFO if args.verbose == 1 else logging.DEBUG
        Watcher("neo4j").watch(level, stderr)

    driver = GraphDatabase.driver(args.url, secure=args.secure)
    session = driver.session()
    if session:
        for _ in range(args.times):
            for statement in args.statement:
                try:
                    records = session.run(statement, {})
                except CypherError as error:
                    stderr.write("%s: %s\r\n" % (error.code, error.message))
                else:
                    if not args.quiet:
                        has_results = False
                        for i, record in enumerate(records):
                            has_results = True
                            if i == 0:
                                stdout.write("%s\r\n" % "\t".join(record.__fields__))
                            stdout.write("%s\r\n" % "\t".join(map(repr, record)))
                        if has_results:
                            stdout.write("\r\n")
        session.close()


if __name__ == "__main__":
    main()
