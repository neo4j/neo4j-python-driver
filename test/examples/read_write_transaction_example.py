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

# tag::read-write-transaction-import[]
# end::read-write-transaction-import[]


class ReadWriteTransactionExample(BaseApplication):
    def __init__(self, uri, user, password):
        super(ReadWriteTransactionExample, self).__init__(uri, user, password)

    # tag::read-write-transaction[]
    def add_person(self, name):
        with self._driver.session() as session:
            session.write_transaction(self.create_person_node, name)
            return session.read_transaction(self.match_person_node, name)

    @staticmethod
    def create_person_node(tx, name):
        tx.run("CREATE (a:Person {name: $name})", name=name)
        return None

    @staticmethod
    def match_person_node(tx, name):
        result = tx.run("MATCH (a:Person {name: $name}) RETURN count(a)", name=name)
        return result.single()[0]
    # end::read-write-transaction[]
