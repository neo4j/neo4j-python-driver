#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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

from test.examples.base_application import BaseApplication

# tag::cypher-error-import[]
from neo4j.v1 import ClientError
# end::cypher-error-import[]


class CypherErrorExample(BaseApplication):
    def __init__(self, uri, user, password):
        super(CypherErrorExample, self).__init__(uri, user, password)

    # tag::cypher-error[]
    def get_employee_number(self, name):
        with self._driver.session() as session:
            return session.read_transaction(self.select_employee, name)

    @staticmethod
    def select_employee(tx, name):
        try:
            result = tx.run("SELECT * FROM Employees WHERE name = $name", name=name)
            return result.single()["employee_number"]
        except ClientError as error:
            print(error.message)
            return -1
    # end::cypher-error[]
