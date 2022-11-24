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
import functools
import sys


# === patch asyncio.wait_for ===
# The shipped wait_for can swallow cancellation errors (starting with 3.8).
# See: https://github.com/python/cpython/pull/26097
# and https://github.com/python/cpython/pull/28149
# Since 3.8 and 3.9 already received their final maintenance release, there
# will be now fix for this. So this patch needs to stick around at least until
# we remove support for Python 3.9.


if sys.version_info >= (3, 8):
    # copied from asyncio 3.10 with applied patch
    def _release_waiter(waiter, *args):
        if not waiter.done():
            waiter.set_result(None)

    async def _cancel_and_wait(fut, loop):
        """Cancel the *fut* future or task and wait until it completes."""

        waiter = loop.create_future()
        cb = functools.partial(_release_waiter, waiter)
        fut.add_done_callback(cb)

        try:
            fut.cancel()
            # We cannot wait on *fut* directly to make
            # sure _cancel_and_wait itself is reliably cancellable.
            await waiter
        finally:
            fut.remove_done_callback(cb)

    async def wait_for(fut, timeout):
        """Wait for the single Future or coroutine to complete, with timeout.

        Coroutine will be wrapped in Task.

        Returns result of the Future or coroutine.  When a timeout occurs,
        it cancels the task and raises TimeoutError.  To avoid the task
        cancellation, wrap it in shield().

        If the wait is cancelled, the task is also cancelled.

        This function is a coroutine.
        """
        loop = asyncio.get_running_loop()

        if timeout is None:
            return await fut

        if timeout <= 0:
            fut = asyncio.ensure_future(fut, loop=loop)

            if fut.done():
                return fut.result()

            await _cancel_and_wait(fut, loop=loop)
            try:
                return fut.result()
            except asyncio.CancelledError as exc:
                raise asyncio.TimeoutError() from exc

        waiter = loop.create_future()
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
        cb = functools.partial(_release_waiter, waiter)

        fut = asyncio.ensure_future(fut, loop=loop)
        fut.add_done_callback(cb)

        try:
            # wait until the future completes or the timeout
            try:
                await waiter
            except asyncio.CancelledError:
                if fut.done():
                    # [PATCH]
                    # Applied patch to not swallow the outer cancellation.
                    # See: https://github.com/python/cpython/pull/26097
                    # and https://github.com/python/cpython/pull/28149

                    # Even though the future we're waiting for is already done,
                    # we should not swallow the cancellation.
                    raise
                    # [/PATCH]
                else:
                    fut.remove_done_callback(cb)
                    # We must ensure that the task is not running
                    # after wait_for() returns.
                    # See https://bugs.python.org/issue32751
                    await _cancel_and_wait(fut, loop=loop)
                    raise

            if fut.done():
                return fut.result()
            else:
                fut.remove_done_callback(cb)
                # We must ensure that the task is not running
                # after wait_for() returns.
                # See https://bugs.python.org/issue32751
                await _cancel_and_wait(fut, loop=loop)
                # In case task cancellation failed with some
                # exception, we should re-raise it
                # See https://bugs.python.org/issue40607
                try:
                    return fut.result()
                except asyncio.CancelledError as exc:
                    raise asyncio.TimeoutError() from exc
        finally:
            timeout_handle.cancel()
else:
    wait_for = asyncio.wait_for
