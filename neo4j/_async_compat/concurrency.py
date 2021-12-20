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


import asyncio
import collections
import re
import threading


__all__ = [
    "AsyncCondition",
    "AsyncRLock",
    "Condition",
    "RLock",
]


class AsyncRLock(asyncio.Lock):
    """Reentrant asyncio.lock

    Inspired by Python's RLock implementation
    """

    _WAITERS_RE = re.compile(r"(?:\W|^)waiters[:=](\d+)(?:\W|$)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._owner = None
        self._count = 0

    def __repr__(self):
        res = object.__repr__(self)
        lock_repr = super().__repr__()
        extra = "locked" if self._count > 0 else "unlocked"
        extra += f" count={self._count}"
        waiters_match = self._WAITERS_RE.search(lock_repr)
        if waiters_match:
            extra += f" waiters={waiters_match.group(1)}"
        if self._owner:
            extra += f" owner={self._owner}"
        return f'<{res[1:-1]} [{extra}]>'

    def is_owner(self, task=None):
        if task is None:
            task = asyncio.current_task()
        return self._owner == task

    async def _acquire(self, me):
        if self.is_owner(task=me):
            self._count += 1
            return
        await super().acquire()
        self._owner = me
        self._count = 1

    async def acquire(self, timeout=None):
        """Acquire the lock."""
        me = asyncio.current_task()
        if timeout is None:
            return await self._acquire(me)
        return await asyncio.wait_for(self._acquire(me), timeout)

    __aenter__ = acquire

    def _release(self, me):
        if not self.is_owner(task=me):
            if self._owner is None:
                raise RuntimeError("Cannot release un-acquired lock.")
            raise RuntimeError("Cannot release foreign lock.")
        self._count -= 1
        if not self._count:
            self._owner = None
            super().release()

    def release(self):
        """Release the lock"""
        me = asyncio.current_task()
        return self._release(me)

    async def __aexit__(self, t, v, tb):
        self.release()


# copied and modified from asyncio.locks (3.7)
# to add support for `.wait(timeout)`
class AsyncCondition:
    """Asynchronous equivalent to threading.Condition.

    This class implements condition variable objects. A condition variable
    allows one or more coroutines to wait until they are notified by another
    coroutine.

    A new Lock object is created and used as the underlying lock.
    """

    def __init__(self, lock=None, *, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        if lock is None:
            lock = asyncio.Lock(loop=self._loop)
        elif (hasattr(lock, "_loop")
              and lock._loop is not None
              and lock._loop is not self._loop):
            raise ValueError("loop argument must agree with lock")

        self._lock = lock
        # Export the lock's locked(), acquire() and release() methods.
        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

        self._waiters = collections.deque()

    async def __aenter__(self):
        await self.acquire()
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    def __repr__(self):
        res = super().__repr__()
        extra = 'locked' if self.locked() else 'unlocked'
        if self._waiters:
            extra = f'{extra}, waiters:{len(self._waiters)}'
        return f'<{res[1:-1]} [{extra}]>'

    async def _wait(self, me=None):
        """Wait until notified.

        If the calling coroutine has not acquired the lock when this
        method is called, a RuntimeError is raised.

        This method releases the underlying lock, and then blocks
        until it is awakened by a notify() or notify_all() call for
        the same condition variable in another coroutine.  Once
        awakened, it re-acquires the lock and returns True.
        """
        if not self.locked():
            raise RuntimeError('cannot wait on un-acquired lock')

        if isinstance(self._lock, AsyncRLock):
            self._lock._release(me)
        else:
            self._lock.release()
        try:
            fut = self._loop.create_future()
            self._waiters.append(fut)
            try:
                await fut
                return True
            finally:
                self._waiters.remove(fut)

        finally:
            # Must reacquire lock even if wait is cancelled
            cancelled = False
            while True:
                try:
                    if isinstance(self._lock, AsyncRLock):
                        await self._lock._acquire(me)
                    else:
                        await self._lock.acquire()
                    break
                except asyncio.CancelledError:
                    cancelled = True

            if cancelled:
                raise asyncio.CancelledError

    async def wait(self, timeout=None):
        if not timeout:
            return await self._wait()
        me = asyncio.current_task()
        return await asyncio.wait_for(self._wait(me), timeout)

    def notify(self, n=1):
        """By default, wake up one coroutine waiting on this condition, if any.
        If the calling coroutine has not acquired the lock when this method
        is called, a RuntimeError is raised.

        This method wakes up at most n of the coroutines waiting for the
        condition variable; it is a no-op if no coroutines are waiting.

        Note: an awakened coroutine does not actually return from its
        wait() call until it can reacquire the lock. Since notify() does
        not release the lock, its caller should.
        """
        if not self.locked():
            raise RuntimeError('cannot notify on un-acquired lock')

        idx = 0
        for fut in self._waiters:
            if idx >= n:
                break

            if not fut.done():
                idx += 1
                fut.set_result(False)

    def notify_all(self):
        """Wake up all threads waiting on this condition. This method acts
        like notify(), but wakes up all waiting threads instead of one. If the
        calling thread has not acquired the lock when this method is called,
        a RuntimeError is raised.
        """
        self.notify(len(self._waiters))


Condition = threading.Condition
RLock = threading.RLock
