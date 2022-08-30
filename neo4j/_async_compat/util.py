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


import asyncio
import inspect
from functools import wraps

from .._meta import experimental


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

    is_async_code = True


class Util:
    iter = iter
    next = next
    list = list

    @staticmethod
    def callback(cb, *args, **kwargs):
        if callable(cb):
            return cb(*args, **kwargs)

    @staticmethod
    def shielded(coro_function):
        return coro_function

    is_async_code = False
