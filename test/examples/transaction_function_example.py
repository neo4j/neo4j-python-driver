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


from test.examples.base_application import BaseApplication

# tag::transaction-function-import[]
from neo4j import unit_of_work
# end::transaction-function-import[]


# tag::transaction-function[]
def add_person(driver, name):
    with driver.session() as session:
        # Caller for transactional unit of work
        return session.write_transaction(create_person_node, name)


# Simple implementation of the unit of work
def create_person_node(tx, name):
    return tx.run("CREATE (a:Person {name: $name}) RETURN id(a)", name=name).single().value()


# Alternative implementation, with timeout
@unit_of_work(timeout=0.5)
def create_person_node_within_half_a_second(tx, name):
    return tx.run("CREATE (a:Person {name: $name}) RETURN id(a)", name=name).single().value()
# end::transaction-function[]


class TransactionFunctionExample(BaseApplication):
    def __init__(self, uri, user, password):
        super(TransactionFunctionExample, self).__init__(uri, user, password)

    def add_person(self, name):
        return add_person(self._driver, name)
