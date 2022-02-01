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


from contextlib import contextmanager

import pytest

from neo4j import (
    AsyncSession,
    AsyncTransaction,
    Bookmarks,
    SessionConfig,
    unit_of_work,
)
from neo4j._async.io._pool import AsyncIOPool

from ...._async_compat import (
    AsyncMock,
    mark_async_test,
    mock,
)
from ._fake_connection import AsyncFakeConnection


@pytest.fixture()
def pool():
    pool = AsyncMock(spec=AsyncIOPool)
    pool.acquire.side_effect = iter(AsyncFakeConnection, 0)
    return pool


@mark_async_test
async def test_session_context_calls_close():
    s = AsyncSession(None, SessionConfig())
    with mock.patch.object(s, 'close', autospec=True) as mock_close:
        async with s:
            pass
        mock_close.assert_called_once_with()


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize(("repetitions", "consume"), (
    (1, False), (2, False), (2, True)
))
@mark_async_test
async def test_opens_connection_on_run(
    pool, test_run_args, repetitions, consume
):
    async with AsyncSession(pool, SessionConfig()) as session:
        assert session._connection is None
        result = await session.run(*test_run_args)
        assert session._connection is not None
        if consume:
            await result.consume()


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
@mark_async_test
async def test_closes_connection_after_consume(
    pool, test_run_args, repetitions
):
    async with AsyncSession(pool, SessionConfig()) as session:
        result = await session.run(*test_run_args)
        await result.consume()
        assert session._connection is None
    assert session._connection is None


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@mark_async_test
async def test_keeps_connection_until_last_result_consumed(
    pool, test_run_args
):
    async with AsyncSession(pool, SessionConfig()) as session:
        result1 = await session.run(*test_run_args)
        result2 = await session.run(*test_run_args)
        assert session._connection is not None
        await result1.consume()
        assert session._connection is not None
        await result2.consume()
        assert session._connection is None


@mark_async_test
async def test_opens_connection_on_tx_begin(pool):
    async with AsyncSession(pool, SessionConfig()) as session:
        assert session._connection is None
        async with await session.begin_transaction() as _:
            assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
@mark_async_test
async def test_keeps_connection_on_tx_run(pool, test_run_args, repetitions):
    async with AsyncSession(pool, SessionConfig()) as session:
        async with await session.begin_transaction() as tx:
            for _ in range(repetitions):
                await tx.run(*test_run_args)
                assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
@mark_async_test
async def test_keeps_connection_on_tx_consume(
    pool, test_run_args, repetitions
):
    async with AsyncSession(pool, SessionConfig()) as session:
        async with await session.begin_transaction() as tx:
            for _ in range(repetitions):
                result = await tx.run(*test_run_args)
                await result.consume()
                assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@mark_async_test
async def test_closes_connection_after_tx_close(pool, test_run_args):
    async with AsyncSession(pool, SessionConfig()) as session:
        async with await session.begin_transaction() as tx:
            for _ in range(2):
                result = await tx.run(*test_run_args)
                await result.consume()
            await tx.close()
            assert session._connection is None
        assert session._connection is None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@mark_async_test
async def test_closes_connection_after_tx_commit(pool, test_run_args):
    async with AsyncSession(pool, SessionConfig()) as session:
        async with await session.begin_transaction() as tx:
            for _ in range(2):
                result = await tx.run(*test_run_args)
                await result.consume()
            await tx.commit()
            assert session._connection is None
        assert session._connection is None


@pytest.mark.parametrize(
    "bookmark_values",
    (None, [], ["abc"], ["foo", "bar"], {"a", "b"}, ("1", "two"))
)
@mark_async_test
async def test_session_returns_bookmarks_directly(pool, bookmark_values):
    if bookmark_values is not None:
        bookmarks = Bookmarks.from_raw_values(bookmark_values)
    else:
        bookmarks = Bookmarks()
    async with AsyncSession(
        pool, SessionConfig(bookmarks=bookmarks)
    ) as session:
        ret_bookmarks = (await session.last_bookmarks())
        assert isinstance(ret_bookmarks, Bookmarks)
        ret_bookmarks = ret_bookmarks.raw_values
        if bookmark_values is None:
            assert ret_bookmarks == frozenset()
        else:
            assert ret_bookmarks == frozenset(bookmark_values)


@pytest.mark.parametrize(
    "bookmarks",
    (None, [], ["abc"], ["foo", "bar"], ("1", "two"))
)
@mark_async_test
async def test_session_last_bookmark_is_deprecated(pool, bookmarks):
    async with AsyncSession(pool, SessionConfig(
        bookmarks=bookmarks
    )) as session:
        with pytest.warns(DeprecationWarning):
            if bookmarks:
                assert (await session.last_bookmark()) == bookmarks[-1]
            else:
                assert (await session.last_bookmark()) is None


@pytest.mark.parametrize(
    "bookmarks",
    (("foo",), ("foo", "bar"), (), ["foo", "bar"], {"a", "b"})
)
@mark_async_test
async def test_session_bookmarks_as_iterable_is_deprecated(pool, bookmarks):
    with pytest.warns(DeprecationWarning):
        async with AsyncSession(pool, SessionConfig(
            bookmarks=bookmarks
        )) as session:
            ret_bookmarks = (await session.last_bookmarks()).raw_values
            assert ret_bookmarks == frozenset(bookmarks)


@pytest.mark.parametrize(("query", "error_type"), (
    (None, ValueError),
    (1234, TypeError),
    ({"how about": "no?"}, TypeError),
    (["I don't", "think so"], TypeError),
))
@mark_async_test
async def test_session_run_wrong_types(pool, query, error_type):
    async with AsyncSession(pool, SessionConfig()) as session:
        with pytest.raises(error_type):
            await session.run(query)


@pytest.mark.parametrize("tx_type", ("write_transaction", "read_transaction"))
@mark_async_test
async def test_tx_function_argument_type(pool, tx_type):
    async def work(tx):
        assert isinstance(tx, AsyncTransaction)

    async with AsyncSession(pool, SessionConfig()) as session:
        getattr(session, tx_type)(work)


@pytest.mark.parametrize("tx_type", ("write_transaction", "read_transaction"))
@pytest.mark.parametrize("decorator_kwargs", (
    {},
    {"timeout": 5},
    {"metadata": {"foo": "bar"}},
    {"timeout": 5, "metadata": {"foo": "bar"}},

))
@mark_async_test
async def test_decorated_tx_function_argument_type(pool, tx_type, decorator_kwargs):
    @unit_of_work(**decorator_kwargs)
    async def work(tx):
        assert isinstance(tx, AsyncTransaction)

    async with AsyncSession(pool, SessionConfig()) as session:
        getattr(session, tx_type)(work)


@mark_async_test
async def test_session_tx_type(pool):
    async with AsyncSession(pool, SessionConfig()) as session:
        tx = await session.begin_transaction()
        assert isinstance(tx, AsyncTransaction)


@pytest.mark.parametrize(("parameters", "error_type"), (
    ({"x": None}, None),
    ({"x": True}, None),
    ({"x": False}, None),
    ({"x": 123456789}, None),
    ({"x": 3.1415926}, None),
    ({"x": float("nan")}, None),
    ({"x": float("inf")}, None),
    ({"x": float("-inf")}, None),
    ({"x": "foo"}, None),
    ({"x": bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])}, None),
    ({"x": b"\x00\x33\x66\x99\xcc\xff"}, None),
    ({"x": [1, 2, 3]}, None),
    ({"x": ["a", "b", "c"]}, None),
    ({"x": ["a", 2, 1.234]}, None),
    ({"x": ["a", 2, ["c"]]}, None),
    ({"x": {"one": "eins", "two": "zwei", "three": "drei"}}, None),
    ({"x": {"one": ["eins", "uno", 1], "two": ["zwei", "dos", 2]}}, None),

    # maps must have string keys
    ({"x": {1: 'eins', 2: 'zwei', 3: 'drei'}}, TypeError),
    ({"x": {(1, 2): '1+2i', (2, 0): '2'}}, TypeError),
))
@pytest.mark.parametrize("run_type", ("auto", "unmanaged", "managed"))
@mark_async_test
async def test_session_run_with_parameters(
    pool, parameters, error_type, run_type
):
    @contextmanager
    def raises():
        if error_type is not None:
            with pytest.raises(error_type) as exc:
                yield exc
        else:
            yield None

    async with AsyncSession(pool, SessionConfig()) as session:
        if run_type == "auto":
            with raises():
                await session.run("RETURN $x", **parameters)
        elif run_type == "unmanaged":
            tx = await session.begin_transaction()
            with raises():
                await tx.run("RETURN $x", **parameters)
        elif run_type == "managed":
            async def work(tx):
                with raises() as exc:
                    await tx.run("RETURN $x", **parameters)
                if exc is not None:
                    raise exc
            with raises():
                await session.write_transaction(work)
        else:
            raise ValueError(run_type)
