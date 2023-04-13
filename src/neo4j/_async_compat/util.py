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

import asyncio
import inspect
import typing as t
from functools import wraps


if t.TYPE_CHECKING:
    import typing_extensions as te

    _T = t.TypeVar("_T")
    _P = te.ParamSpec("_P")


__all__ = [
    "AsyncUtil",
    "Util",
]


class AsyncUtil:
    @staticmethod
    async def iter(it):
        async for x in it:
            yield x

    @staticmethod
    async def next(it):
        return await it.__anext__()

    @staticmethod
    async def list(it):
        return [x async for x in it]

    @staticmethod
    @t.overload
    async def callback(cb: None, *args: object, **kwargs: object) -> None:
        ...

    @staticmethod
    @t.overload
    async def callback(
        cb: t.Union[
            t.Callable[_P, t.Union[_T, t.Awaitable[_T]]],
            t.Callable[_P, t.Awaitable[_T]],
            t.Callable[_P, _T],
        ],
        *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        ...

    @staticmethod
    async def callback(cb, *args, **kwargs):
        if callable(cb):
            res = cb(*args, **kwargs)
            if inspect.isawaitable(res):
                return await res
            return res

    @staticmethod
    def shielded(coro_function):
        assert asyncio.iscoroutinefunction(coro_function)

        @wraps(coro_function)
        async def shielded_function(*args, **kwargs):
            return await asyncio.shield(coro_function(*args, **kwargs))

        return shielded_function

    is_async_code: t.ClassVar = True


class Util:
    iter: t.ClassVar = iter
    next: t.ClassVar = next
    list: t.ClassVar = list

    @staticmethod
    @t.overload
    def callback(cb: None, *args: object, **kwargs: object) -> None:
        ...

    @staticmethod
    @t.overload
    def callback(cb: t.Callable[_P, _T],
                 *args: _P.args, **kwargs: _P.kwargs) -> _T:
        ...

    @staticmethod
    def callback(cb, *args, **kwargs):
        if callable(cb):
            return cb(*args, **kwargs)

    @staticmethod
    def shielded(coro_function):
        return coro_function

    is_async_code: t.ClassVar = False
