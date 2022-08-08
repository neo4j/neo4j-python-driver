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
import random
from functools import wraps

import pytest

import neo4j
from neo4j import exceptions as neo4j_exceptions

from ..._async_compat import mark_async_test
from ...conftest import get_async_driver_no_warning


def _get_work():
    work_cancelled = False

    async def work(tx, i=1):
        nonlocal work_cancelled
        assert not work_cancelled  # no retries after cancellation!
        try:
            result = await tx.run(f"RETURN {i}")
            try:
                for _ in range(3):
                    await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                e.during_sleep = True
                raise
            records = [record async for record in result]
            summary = await result.consume()
            assert isinstance(summary, neo4j.ResultSummary)
            assert len(records) == 1
            assert list(records[0]) == [i]
        except asyncio.CancelledError:
            work_cancelled = True
            raise

    return work


async def _do_the_read_tx_func(session_, i=1):
    await session_.read_transaction(_get_work(), i=i)


def _with_retry(outer):
    @wraps(outer)
    async def inner(*args, **kwargs):
        for _ in range(15):  # super simple retry-mechanism
            try:
                return await outer(*args, **kwargs)
            except (neo4j_exceptions.DriverError,
                    neo4j_exceptions.Neo4jError) as e:
                if not e.is_retryable():
                    raise
                await asyncio.sleep(1.5)
    return inner


@_with_retry
async def _do_the_read_tx_context(session_, i=1):
    async with await session_.begin_transaction() as tx:
        await _get_work()(tx, i=i)


@_with_retry
async def _do_the_read_explicit_tx(session_, i=1):
    tx = await session_.begin_transaction()
    try:
        await _get_work()(tx, i=i)
    except asyncio.CancelledError:
        tx.cancel()
        raise
    await tx.commit()


@_with_retry
async def _do_the_read(session_, i=1):
    try:
        return await _get_work()(session_, i=i)
    except asyncio.CancelledError:
        session_.cancel()
        raise


REPETITIONS = 1000


@mark_async_test
@pytest.mark.parametrize(("i", "read_func", "waits", "cancel_count"), (
    (
        f"{i + 1:0{len(str(REPETITIONS))}}/{REPETITIONS}",
        random.choice((
            _do_the_read, _do_the_read_tx_context, _do_the_read_explicit_tx,
            _do_the_read_tx_func
        )),
        random.randint(0, 1000),
        random.randint(1, 20),
    )
    for i in range(REPETITIONS)
))
async def test_async_cancellation(
    uri, auth, mocker, read_func, waits, cancel_count, i
):
    async with get_async_driver_no_warning(
        uri, auth=auth, connection_acquisition_timeout=10
    ) as driver:
        async with driver.session() as session:
            session._handle_cancellation = mocker.Mock(
                wraps=session._handle_cancellation
            )
            fut = asyncio.ensure_future(read_func(session))
            for _ in range(waits):
                await asyncio.sleep(0)
            # time for crazy abuse!
            was_done = fut.done() and not fut.cancelled()
            for _ in range(cancel_count):
                fut.cancel()
                await asyncio.sleep(0)
            cancelled_error = None
            if not was_done:
                with pytest.raises(asyncio.CancelledError) as exc:
                    await fut
                cancelled_error = exc.value

            else:
                await fut

            bookmarks = await session.last_bookmarks()
            if not waits:
                assert not bookmarks
                session._handle_cancellation.assert_not_called()
            elif cancelled_error is not None:
                assert not bookmarks
                if (
                    read_func is _do_the_read
                    and not getattr(cancelled_error, "during_sleep", False)
                ):
                    # manually handling the session can lead to calling
                    # `session.cancel` twice, but that's ok, it's a noop if
                    # already cancelled.
                    assert len(session._handle_cancellation.call_args) == 2
                else:
                    session._handle_cancellation.assert_called_once()
            else:
                assert bookmarks
                session._handle_cancellation.assert_not_called()
            for read_func in (
                _do_the_read, _do_the_read_tx_context,
                _do_the_read_explicit_tx, _do_the_read_tx_func
            ):
                await read_func(session, i=2)

        # test driver is still working
        async with driver.session() as session:
            await _do_the_read_tx_func(session, i=3)
            new_bookmarks = await session.last_bookmarks()
            assert new_bookmarks
            assert bookmarks != new_bookmarks


SESSION_REPETITIONS = 100
READS_PER_SESSION = 20


@mark_async_test
async def test_async_cancellation_does_not_leak(uri, auth):
    async with get_async_driver_no_warning(
        uri, auth=auth,
        connection_acquisition_timeout=10,
        # driver needs to cope with a single connection in the pool!
        max_connection_pool_size=1,
    ) as driver:
        for session_number in range(SESSION_REPETITIONS):
            async with driver.session() as session:
                for read_number in range(READS_PER_SESSION):
                    read_func = random.choice((
                        _do_the_read, _do_the_read_tx_context,
                        _do_the_read_explicit_tx, _do_the_read_tx_func
                    ))
                    waits = random.randint(0, 1000)
                    cancel_count = random.randint(1, 20)

                    fut = asyncio.ensure_future(read_func(session))
                    for _ in range(waits):
                        await asyncio.sleep(0)
                    # time for crazy abuse!
                    was_done = fut.done() and not fut.cancelled()
                    for _ in range(cancel_count):
                        fut.cancel()
                        await asyncio.sleep(0)
                    if not was_done:
                        with pytest.raises(asyncio.CancelledError):
                            await fut
                    else:
                        await fut
                    await _do_the_read_tx_func(session, i=2)

                    pool_connections = driver._pool.connections
                    for connections in pool_connections.values():
                        assert len(connections) <= 1
