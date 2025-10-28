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

from typing import (
    Any,
    LiteralString,
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    import neo4j

    from ._bencher import Bencher


def deserialization_template(
    bencher: Bencher,
    workload_query: LiteralString,
    /,
    *,
    setup_query: LiteralString | None = None,
    setup_data: Any | None = None,
) -> None:
    with (
        bencher.ctx.new_driver() as driver,
        driver.session(database=bencher.ctx.db_name) as session,
        session.begin_transaction() as tx,
    ):
        if setup_query is not None:
            if setup_data is None:
                setup_parameters = None
            else:
                setup_parameters = {"data": setup_data}
            tx.run(setup_query, parameters=setup_parameters).consume()

        bencher.timed_loop(_work, tx, workload_query)

        tx.rollback()


def _work(tx: neo4j.Transaction, workload_query: LiteralString) -> None:
    result = tx.run(workload_query)
    _ = list(result)
