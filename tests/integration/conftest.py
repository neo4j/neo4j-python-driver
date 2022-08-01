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


import pytest


class ForcedRollback(Exception):
    def __init__(self, return_value):
        super().__init__()
        self.return_value = return_value


@pytest.fixture
def cypher_eval(driver):
    def run_and_rollback(tx, cypher, **parameters):
        result = tx.run(cypher, **parameters)
        value = result.single().value()
        raise ForcedRollback(value)

    def f(cypher, **parameters):
        with driver.session() as session:
            try:
                session.write_transaction(run_and_rollback, cypher,
                                          **parameters)
                raise RuntimeError("Expected rollback")
            except ForcedRollback as e:
                return e.return_value

    return f
