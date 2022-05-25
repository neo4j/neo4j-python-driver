# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from contextlib import redirect_stdout
from io import StringIO


# isort: off
# tag::cypher-error-import[]
from neo4j.exceptions import ClientError
# end::cypher-error-import[]
# isort: on


class Neo4jErrorExample:

    def __init__(self, driver):
        self.driver = driver

    # tag::cypher-error[]
    def get_employee_number(self, name):
        with self.driver.session() as session:
            try:
                session.read_transaction(self.select_employee, name)
            except ClientError as error:
                print(error.message)
                return -1

    @staticmethod
    def select_employee(tx, name):
        result = tx.run("SELECT * FROM Employees WHERE name = $name", name=name)
        return result.single()["employee_number"]
    # end::cypher-error[]


def test_example(driver):
    s = StringIO()
    with redirect_stdout(s):
        example = Neo4jErrorExample(driver)
        example.get_employee_number('Alice')
        assert s.getvalue().startswith("Invalid input")
