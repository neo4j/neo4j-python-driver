# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import inspect

from ..meta import experimental


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

    experimental_async = experimental


class Util:
    iter = iter
    next = next
    list = list

    @staticmethod
    def callback(cb, *args, **kwargs):
        if callable(cb):
            return cb(*args, **kwargs)

    @staticmethod
    def experimental_async(message):
        def f_(f):
            return f
        return f_
