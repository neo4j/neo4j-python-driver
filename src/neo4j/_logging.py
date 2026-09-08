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

from . import _typing as t


R = t.TypeVar("R")
P = t.ParamSpec("P")

__all__ = [
    "LazyStr",
]


class LazyStr(t.Generic[R, P]):
    _resolve: t.Callable[[], str]
    _str: str | None

    def __init__(
        self,
        value: t.Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._resolve = lambda: str(value(*args, **kwargs))
        self._str = None

    def __str__(self) -> str:
        if self._str is None:
            self._str = self._resolve()
        return self._str
