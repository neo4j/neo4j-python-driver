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

from .._async_compat.concurrency import CooperativeLock
from .._async_compat.util import Util
from ..api import (
    BookmarkManager,
    Bookmarks,
)


T_BmSupplier = t.Callable[[t.Optional[str]],
                          t.Union[Bookmarks, t.Union[Bookmarks]]]
T_BmConsumer = t.Callable[[str, Bookmarks], t.Union[None, t.Union[None]]]


def _bookmarks_to_set(
    bookmarks: t.Union[Bookmarks, t.Iterable[str]]
) -> t.Set[str]:
    if isinstance(bookmarks, Bookmarks):
        return set(bookmarks.raw_values)
    return set(map(str, bookmarks))


class Neo4jBookmarkManager(BookmarkManager):
    def __init__(
        self,
        initial_bookmarks: t.Mapping[str, t.Union[Bookmarks,
                                                  t.Iterable[str]]] = None,
        bookmarks_supplier: T_BmSupplier = None,
        bookmarks_consumer: T_BmConsumer = None
    ) -> None:
        super().__init__()
        self._bookmarks_supplier = bookmarks_supplier
        self._bookmarks_consumer = bookmarks_consumer
        if initial_bookmarks is None:
            initial_bookmarks = {}
        self._bookmarks = defaultdict(
            set, ((k, _bookmarks_to_set(v))
                  for k, v in initial_bookmarks.items())
        )
        self._lock = CooperativeLock()

    def update_bookmarks(
        self, database: str, previous_bookmarks: t.Collection[str],
        new_bookmarks: t.Collection[str]
    ) -> None:
        if not new_bookmarks:
            return
        with self._lock:
            curr_bms = self._bookmarks[database]
            curr_bms.difference_update(previous_bookmarks)
            curr_bms.update(new_bookmarks)
            if self._bookmarks_consumer:
                curr_bms_snapshot = Bookmarks.from_raw_values(curr_bms)
        if self._bookmarks_consumer:
            Util.callback(
                self._bookmarks_consumer, database, curr_bms_snapshot
            )

    def get_bookmarks(self, database: str) -> t.Set[str]:
        with self._lock:
            bms = set(self._bookmarks[database])
        if self._bookmarks_supplier:
            extra_bms = Util.callback(
                self._bookmarks_supplier, database
            )
            bms.update(extra_bms.raw_values)
        return bms

    def get_all_bookmarks(self) -> t.Set[str]:
        bms: t.Set[str] = set()
        with self._lock:
            for database in self._bookmarks.keys():
                bms.update(self._bookmarks[database])
        if self._bookmarks_supplier:
            extra_bms = Util.callback(
                self._bookmarks_supplier, None
            )
            bms.update(extra_bms.raw_values)
        return bms

    def forget(self, databases: t.Iterable[str]) -> None:
        with self._lock:
            for database in databases:
                self._bookmarks.pop(database, None)
