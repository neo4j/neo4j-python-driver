# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from __future__ import annotations

import pytest

from neo4j import (
    _typing as t,
    AsyncGraphDatabase,
    GraphDatabase,
)

from ..conftest import (
    async_driver_factory,
    driver_factory,
)


if t.TYPE_CHECKING:
    from pytest_mock import MockFixture


class ForcedRollback(Exception):  # noqa: N818 not really an error
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
                session.execute_write(run_and_rollback, cypher, **parameters)
                raise RuntimeError("Expected rollback")
            except ForcedRollback as e:
                return e.return_value

    return f


@pytest.fixture
def patch_driver_factory(mocker: MockFixture) -> t.Callable[[object], None]:
    def factory(obj: object):
        if obj is AsyncGraphDatabase:
            mocker.patch.object(obj, "driver", async_driver_factory)
        elif obj is GraphDatabase:
            mocker.patch.object(obj, "driver", driver_factory)
        else:
            raise ValueError(f"Unsupported object type: {type(obj)}")

    return factory
