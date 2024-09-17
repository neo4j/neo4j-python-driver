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

from neo4j import (
    AsyncDriver,
    AsyncGraphDatabase,
)

from .env import env
from .workloads import Workloads


__all__ = [
    "BenchKitContext",
]


class BenchKitContext:
    _db: AsyncDriver | None
    workloads: Workloads

    def __init__(self) -> None:
        self._db = None
        self.workloads = Workloads()

    async def get_db(self) -> AsyncDriver:
        if self._db is None:
            url = f"{env.neo4j_scheme}://{env.neo4j_host}:{env.neo4j_port}"
            auth = (env.neo4j_user, env.neo4j_pass)
            self._db = AsyncGraphDatabase.driver(url, auth=auth)
            try:
                await self._db.verify_connectivity()
            except Exception:
                db = self._db
                self._db = None
                await db.close()
                raise
        return self._db

    async def shutdown(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None
