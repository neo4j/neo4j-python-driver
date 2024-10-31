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

import heapq
import math
import typing as t
from time import monotonic

from .._async_compat.concurrency import AsyncCooperativeLock


if t.TYPE_CHECKING:
    # TAuthKey = t.Tuple[t.Tuple[]]
    TKey = str | tuple[tuple[str, t.Hashable], ...] | tuple[None]
    TVal = tuple[float, str]


class AsyncHomeDbCache:
    _ttl: float
    _enabled: bool
    _max_size: int | None

    def __init__(
        self,
        enabled: bool = True,
        ttl: float = float("inf"),
        max_size: int | None = None,
    ) -> None:
        if math.isnan(ttl) or ttl <= 0:
            raise ValueError(f"home db cache ttl must be greater 0, got {ttl}")
        self._enabled = enabled
        self._ttl = ttl
        self._cache: dict[TKey, TVal] = {}
        self._lock = AsyncCooperativeLock()
        self._last_clean = monotonic()
        self._max_size = max_size

    def compute_key(
        self,
        imp_user: str | None,
        auth: dict | None,
    ) -> TKey:
        if not self._enabled:
            return (None,)
        if imp_user is not None:
            return imp_user
        if auth is not None:
            return _hashable_dict(auth)
        return (None,)

    def get(self, key: TKey) -> str | None:
        if not self._enabled:
            return None
        with self._lock:
            val = self._cache.get(key)
            if val is None:
                return None
            now = monotonic()
            if now - val[0] > self._ttl:
                del self._cache[key]
                return None
            # Saved some time with a cache hit,
            # so we can waste some with cleaning the cache ;)
            self._clean(now)
            return val[1]

    def set(self, key: TKey, value: str | None) -> None:
        if not self._enabled:
            return
        with self._lock:
            if value is None:
                self._cache.pop(key, None)
            else:
                self._cache[key] = (monotonic(), value)

    def clear(self) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._cache = {}
            self._last_clean = monotonic()

    def _clean(self, now: float) -> None:
        if self._max_size is not None and len(self._cache) > self._max_size:
            self._cache = dict(
                heapq.nlargest(
                    self._max_size,
                    self._cache.items(),
                    key=lambda item: item[1][0],
                )
            )
        if now - self._last_clean > self._ttl:
            self._cache = {
                k: v for k, v in self._cache.items() if now - v[0] < self._ttl
            }
            self._last_clean = now

    @property
    def enabled(self) -> bool:
        return self._enabled


def _hashable_dict(d: dict) -> tuple:
    return tuple(
        (k, _hashable_dict(v) if isinstance(v, dict) else v)
        for k, v in sorted(d.items())
    )
