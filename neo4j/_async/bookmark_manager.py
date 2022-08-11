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

from .._async_compat.concurrency import AsyncCooperativeLock
from .._async_compat.util import AsyncUtil
from ..api import (
    AsyncBookmarkManager,
    Bookmarks,
)


T_BmSupplier = t.Callable[[t.Optional[str]],
                          t.Union[Bookmarks, t.Awaitable[Bookmarks]]]
T_BmConsumer = t.Callable[[str, Bookmarks], t.Union[None, t.Awaitable[None]]]


class AsyncNeo4jBookmarkManager(AsyncBookmarkManager):
    def __init__(
        self,
        initial_bookmarks: t.Mapping[str, t.Iterable[str]] = None,
        bookmark_supplier: T_BmSupplier = None,
        bookmarks_consumer: T_BmConsumer = None
    ) -> None:
        super().__init__()
        self._bookmark_supplier = bookmark_supplier
        self._bookmarks_consumer = bookmarks_consumer
        if initial_bookmarks is None:
            initial_bookmarks = {}
        self._bookmarks = defaultdict(
            set, ((k, set(map(str, v)))
                  for k, v in initial_bookmarks.items())
        )
        self._lock = AsyncCooperativeLock()

    async def update_bookmarks(
        self, database: str, previous_bookmarks: t.Iterable[str],
        new_bookmarks: t.Iterable[str]
    ) -> None:
        new_bms = set(new_bookmarks)
        prev_bms = set(previous_bookmarks)
        with self._lock:
            if not new_bms:
                return
            curr_bms = self._bookmarks[database]
            curr_bms.difference_update(prev_bms)
            curr_bms.update(new_bms)
            if self._bookmarks_consumer:
                curr_bms_snapshot = Bookmarks.from_raw_values(curr_bms)
        if self._bookmarks_consumer:
            await AsyncUtil.callback(
                self._bookmarks_consumer, database, curr_bms_snapshot
            )

    async def get_bookmarks(self, database: str) -> t.Set[str]:
        with self._lock:
            bms = set(self._bookmarks[database])
        if self._bookmark_supplier:
            extra_bms = await AsyncUtil.callback(
                self._bookmark_supplier, database
            )
            bms.update(extra_bms)
        return bms

    async def get_all_bookmarks(self) -> t.Set[str]:
        bms: t.Set[str] = set()
        with self._lock:
            for database in self._bookmarks.keys():
                bms.update(self._bookmarks[database])
        if self._bookmark_supplier:
            extra_bms = await AsyncUtil.callback(
                self._bookmark_supplier, None
            )
            bms.update(extra_bms)
        return bms

    async def forget(self, databases: t.Iterable[str]) -> None:
        for database in databases:
            with self._lock:
                del self._bookmarks[database]
