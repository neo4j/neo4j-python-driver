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

# tag::config-custom-resolver-import[]
from neo4j import GraphDatabase, WRITE_ACCESS


# end::config-custom-resolver-import[]


class ConfigCustomResolverExample:
    def __init__(self, virtual_uri, user, password, addresses):
        self._driver = self.create_driver(virtual_uri, user, password, addresses)

    # tag::config-custom-resolver[]
    def create_driver(self, virtual_uri, user, password, addresses):
        def custom_resolver(socket_address):
            return addresses

        return GraphDatabase.driver(virtual_uri, auth=(user, password), resolver=custom_resolver)

    def add_person(self, name):
        user = "neo4j"
        password = "some password"

        with self.create_driver("bolt+routing://x.acme.com", user, password,
                                [("a.acme.com", 7676), ("b.acme.com", 8787), ("c.acme.com", 9898)]) as driver:
            with driver.session(access_mode=WRITE_ACCESS) as session:
                session.run("CREATE (a:Person {name: $name})", name=name)

    # end::config-custom-resolver[]

    def close(self):
        self._driver.close()

    def can_connect(self):
        result = self._driver.session().run("RETURN 1")
        return result.single()[0] == 1
