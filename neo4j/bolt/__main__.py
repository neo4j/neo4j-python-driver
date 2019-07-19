#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from argparse import ArgumentParser
from asyncio import get_event_loop
from getpass import getpass
from os.path import basename
from sys import argv

from neo4j.addressing import Address
from neo4j.bolt import Bolt
from neo4j.debug import watch


async def a_main(prog):
    parser = ArgumentParser(prog=prog)
    parser.add_argument("cypher",
                        help="Cypher query to execute")
    parser.add_argument("-a", "--auth", metavar="USER:PASSWORD", default="",
                        help="user name and password")
    parser.add_argument("-s", "--server-addr", metavar="HOST:PORT", default=":7687",
                        help="address of server")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    parsed = parser.parse_args()
    if parsed.verbose:
        watch("neo4j")
    addr = Address.parse(parsed.server_addr)
    user, _, password = parsed.auth.partition(":")
    if not password:
        password = getpass()
    auth = (user or "neo4j", password)
    bolt = await Bolt.open(addr, auth=auth)
    try:
        result = await bolt.run(parsed.cypher)
        print("\t".join(await result.fields()))
        async for record in result:
            print("\t".join(map(repr, record)))
    finally:
        await bolt.close()


def main(prog=None):
    get_event_loop().run_until_complete(a_main(prog or basename(argv[0])))


if __name__ == "__main__":
    main("python -m neo4j.bolt")
