from __future__ import annotations

import typing as t

import neo4j
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
    _db: t.Optional[AsyncDriver]
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
