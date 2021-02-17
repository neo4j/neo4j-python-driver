#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


from test.examples.base_application import BaseApplication

# tag::autocommit-transaction-import[]
from neo4j import Statement
# end::autocommit-transaction-import[]


class AutocommitTransactionExample(BaseApplication):
    def __init__(self, uri, user, password):
        super(AutocommitTransactionExample, self).__init__(uri, user, password)

    # tag::autocommit-transaction[]
    def add_person(self, name):
        with self._driver.session() as session:
            session.run("CREATE (a:Person {name: $name})", name=name)

    # Alternative implementation, with timeout
    def add_person_within_half_a_second(self, name):
        with self._driver.session() as session:
            session.run(Statement("CREATE (a:Person {name: $name})", timeout=0.5), name=name)
    # end::autocommit-transaction[]
