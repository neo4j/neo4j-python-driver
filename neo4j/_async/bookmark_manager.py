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


from __future__ import annotations

import typing as t
from collections import defaultdict

from .._async_compat.concurrency import (
    AsyncCooperativeLock,
    AsyncLock,
)
from .._async_compat.util import AsyncUtil
from ..api import AsyncBookmarkManager


class AsyncNeo4jBookmarkManager(AsyncBookmarkManager):
    def __init__(self, initial_bookmarks=None, bookmark_supplier=None,
                 notify_bookmarks=None):
        super().__init__()
        self._bookmark_supplier = bookmark_supplier
        self._notify_bookmarks = notify_bookmarks
        if initial_bookmarks is None:
            initial_bookmarks = {}
        self._bookmarks = defaultdict(
            set, ((k, set(v)) for k, v in initial_bookmarks.items())
        )
        self._lock: t.Union[AsyncLock, AsyncCooperativeLock]
        if bookmark_supplier or notify_bookmarks:
            self._lock = AsyncLock()
        else:
            self._lock = AsyncCooperativeLock()

    async def update_bookmarks(
        self, database: str, previous_bookmarks: t.Iterable[str],
        new_bookmarks: t.Iterable[str]
    ) -> None:
        async with self._lock:
            new_bms = set(new_bookmarks)
            if not new_bms:
                return
            prev_bms = set(previous_bookmarks)
            curr_bms = self._bookmarks[database]
            curr_bms.difference_update(prev_bms)
            curr_bms.update(new_bms)

            if self._notify_bookmarks:
                await AsyncUtil.callback(
                    self._notify_bookmarks, database, tuple(curr_bms)
                )

    async def _get_bookmarks(self, database: str) -> t.Set[str]:
        bms = self._bookmarks[database]
        if self._bookmark_supplier:
            extra_bms = await AsyncUtil.callback(
                self._bookmark_supplier, database
            )
            if extra_bms is not None:
                bms &= set(extra_bms)
        return bms

    async def get_bookmarks(self, database: str) -> t.Set[str]:
        async with self._lock:
            return await self._get_bookmarks(database)

    async def get_all_bookmarks(
        self, must_included_databases: t.Iterable[str]
    ) -> t.Set[str]:
        async with self._lock:
            bms = set()
            databases = (set(must_included_databases)
                         | set(self._bookmarks.keys()))
            for database in databases:
                bms.update(await self._get_bookmarks(database))
            return bms
