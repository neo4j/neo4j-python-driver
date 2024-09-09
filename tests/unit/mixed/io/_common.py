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


import asyncio
import time
from asyncio import (
    Condition as AsyncCondition,
    Lock as AsyncLock,
)
from threading import (
    Condition,
    Lock,
)

from neo4j._async_compat.shims import wait_for


class MultiEvent:
    # Adopted from threading.Event

    def __init__(self):
        super().__init__()
        self._cond = Condition(Lock())
        self._counter = 0

    def _reset_internal_locks(self):
        # private!  called by Thread._reset_internal_locks by _after_fork()
        self._cond.__init__(Lock())  # noqa: PLC2801 (called on object, not class)

    def counter(self):
        return self._counter

    def increment(self):
        with self._cond:
            self._counter += 1
            self._cond.notify_all()

    def decrement(self):
        with self._cond:
            self._counter -= 1
            self._cond.notify_all()

    def clear(self):
        with self._cond:
            self._counter = 0
            self._cond.notify_all()

    def wait(self, value=0, timeout=None):
        with self._cond:
            t_start = time.time()
            while True:
                if value == self._counter:
                    return True
                if timeout is None:
                    time_left = None
                else:
                    time_left = timeout - (time.time() - t_start)
                    if time_left <= 0:
                        return False
                if not self._cond.wait(time_left):
                    return False


class AsyncMultiEvent:
    # Adopted from threading.Event

    def __init__(self):
        super().__init__()
        self._cond = AsyncCondition()
        self._counter = 0

    def _reset_internal_locks(self):
        # private!  called by Thread._reset_internal_locks by _after_fork()
        self._cond.__init__(AsyncLock())  # noqa: PLC2801 (called on object, not class)

    def counter(self):
        return self._counter

    async def increment(self):
        async with self._cond:
            self._counter += 1
            self._cond.notify_all()

    async def decrement(self):
        async with self._cond:
            self._counter -= 1
            self._cond.notify_all()

    async def clear(self):
        async with self._cond:
            self._counter = 0
            self._cond.notify_all()

    async def wait(self, value=0, timeout=None):
        async with self._cond:
            t_start = time.time()
            while True:
                if value == self._counter:
                    return True
                if timeout is None:
                    time_left = None
                else:
                    time_left = timeout - (time.time() - t_start)
                    if time_left <= 0:
                        return False
                try:
                    await wait_for(self._cond.wait(), time_left)
                except asyncio.TimeoutError:
                    return False
