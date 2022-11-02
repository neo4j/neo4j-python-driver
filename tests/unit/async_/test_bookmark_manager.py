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

import itertools
import typing as t

import pytest

import neo4j
from neo4j._async.bookmark_manager import AsyncNeo4jBookmarkManager
from neo4j._async_compat.util import AsyncUtil
from neo4j._meta import copy_signature
from neo4j.api import Bookmarks

from ..._async_compat import mark_async_test


supplier_async_options = (True, False) if AsyncUtil.is_async_code else (False,)
consumer_async_options = supplier_async_options


@copy_signature(neo4j.AsyncGraphDatabase.bookmark_manager)
def bookmark_manager(*args, **kwargs):
    return neo4j.AsyncGraphDatabase.bookmark_manager(*args, **kwargs)


@pytest.mark.parametrize("db", ("foobar", "system"))
@mark_async_test
async def test_return_empty_if_db_doesnt_exists(db) -> None:
    bmm = bookmark_manager()

    assert set(await bmm.get_bookmarks(db)) == set()


@pytest.mark.parametrize("db", ("db1", "db2", "db3"))
@mark_async_test
async def test_return_initial_bookmarks_for_the_given_db(db) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"]
    }
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)

    assert set(await bmm.get_bookmarks(db)) == set(initial_bookmarks[db])


@pytest.mark.parametrize("db", ("db1", "db2", "db3"))
@pytest.mark.parametrize("supplier_async", supplier_async_options)
@mark_async_test
async def test_return_get_bookmarks_from_bookmarks_supplier(
    db, mocker, supplier_async
) -> None:
    extra_bookmarks = ["foo:bm1", "bar:bm2", "foo:bm1"]
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"]
    }
    mock_cls = mocker.AsyncMock if supplier_async else mocker.Mock
    supplier = mock_cls(
        return_value=Bookmarks.from_raw_values(extra_bookmarks)
    )
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks,
                           bookmarks_supplier=supplier)

    assert set(await bmm.get_bookmarks(db)) == {
        *extra_bookmarks, *initial_bookmarks.get(db, [])
    }
    if supplier_async:
        supplier.assert_awaited_once_with(db)
    else:
        supplier.assert_called_once_with(db)


@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@mark_async_test
async def test_return_all_bookmarks(with_initial_bookmarks) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"],
        "db5": ["db3:bm1"]
    }
    bmm = bookmark_manager(
        initial_bookmarks=initial_bookmarks if with_initial_bookmarks else None
    )

    all_bookmarks = await bmm.get_all_bookmarks()

    if with_initial_bookmarks:
        assert all_bookmarks == set(
            itertools.chain.from_iterable(initial_bookmarks.values())
        )
    else:
        assert all_bookmarks == set()


@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@pytest.mark.parametrize("supplier_async", supplier_async_options)
@mark_async_test
async def test_return_enriched_bookmarks_list_with_supplied_bookmarks(
    with_initial_bookmarks, supplier_async, mocker
) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"],
    }
    extra_bookmarks = ["foo:bm1", "bar:bm2", "db3:bm1", "foo:bm1"]
    mock_cls = mocker.AsyncMock if supplier_async else mocker.Mock
    supplier = mock_cls(
        return_value=Bookmarks.from_raw_values(extra_bookmarks)
    )
    bmm = bookmark_manager(
        initial_bookmarks=(initial_bookmarks
                           if with_initial_bookmarks else None),
        bookmarks_supplier=supplier
    )

    all_bookmarks = await bmm.get_all_bookmarks()

    if with_initial_bookmarks:
        assert all_bookmarks == set(
            itertools.chain(*initial_bookmarks.values(), extra_bookmarks)
        )
    else:
        assert all_bookmarks == set(extra_bookmarks)
    if supplier_async:
        supplier.assert_awaited_once_with(None)
    else:
        supplier.assert_called_once_with(None)


@mark_async_test
async def test_chains_bookmarks_for_existing_db() -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"],
    }
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)
    await bmm.update_bookmarks("db3", ["db3:bm1"], ["db3:bm3"])
    new_bookmarks = await bmm.get_bookmarks("db3")
    all_bookmarks = await bmm.get_all_bookmarks()

    assert new_bookmarks == {"db3:bm2", "db3:bm3"}
    assert all_bookmarks == set(
        itertools.chain.from_iterable(initial_bookmarks.values())
    ) - {"db3:bm1"} | {"db3:bm2", "db3:bm3"}


@mark_async_test
async def test_add_bookmarks_for_a_non_existing_database() -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1"],
        "db2": [],
        "db3": ["db3:bm1", "db3:bm2"],
        "db4": ["db4:bm4"],
    }
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)
    await bmm.update_bookmarks(
        "db5", ["db3:bm1", "db5:bm1"], ["db3:bm3", "db3:bm5"]
    )
    new_bookmarks = await bmm.get_bookmarks("db5")
    all_bookmarks = await bmm.get_all_bookmarks()

    assert new_bookmarks == {"db3:bm3", "db3:bm5"}
    assert all_bookmarks == set(
        itertools.chain.from_iterable(initial_bookmarks.values())
    ) | {"db3:bm3", "db3:bm5"}


@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@pytest.mark.parametrize("consumer_async", consumer_async_options)
@pytest.mark.parametrize("db", ("db1", "db2", "db3"))
@mark_async_test
async def test_notify_on_new_bookmarks(
    with_initial_bookmarks, consumer_async, db, mocker
) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1", "db1:bm2"],
        "db2": ["db2:bm1"],
    }
    mock_cls = mocker.AsyncMock if consumer_async else mocker.Mock
    consumer = mock_cls()
    bmm = bookmark_manager(
        initial_bookmarks=(initial_bookmarks
                           if with_initial_bookmarks else None),
        bookmarks_consumer=consumer
    )
    bookmarks_old = {"db1:bm1", "db3:bm1"}
    bookmarks_new = {"db1:bm4"}
    await bmm.update_bookmarks(db, bookmarks_old, bookmarks_new)

    if consumer_async:
        consumer.assert_awaited_once()
        args = consumer.await_args.args
    else:
        consumer.assert_called_once()
        args = consumer.call_args.args
    assert args[0] == db
    assert isinstance(args[1], Bookmarks)
    if with_initial_bookmarks:
        expected_bms = (
            set(initial_bookmarks.get(db, [])) - bookmarks_old | bookmarks_new
        )
    else:
        expected_bms = bookmarks_new
    assert args[1].raw_values == expected_bms


@pytest.mark.parametrize("consumer_async", consumer_async_options)
@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@pytest.mark.parametrize("db", ("db1", "db2"))
@mark_async_test
async def test_does_not_notify_on_empty_new_bookmark_set(
    with_initial_bookmarks, consumer_async, db, mocker
) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm2"]
    }
    mock_cls = mocker.AsyncMock if consumer_async else mocker.Mock
    consumer = mock_cls()
    bmm = bookmark_manager(
        initial_bookmarks=(initial_bookmarks
                           if with_initial_bookmarks else None),
        bookmarks_consumer=consumer
    )
    await bmm.update_bookmarks(db, ["db1:bm1"], [])

    consumer.assert_not_called()


@pytest.mark.parametrize("dbs", (
    ["db1"], ["db2"], ["db1", "db2"], ["db1", "db3"], ["db1", "db2", "db3"]
))
@mark_async_test
async def test_forget_database(dbs) -> None:
    initial_bookmarks: t.Dict[str, t.List[str]] = {
        "db1": ["db1:bm1", "db1:bm1", "db1:bm2"],
        "db2": ["db2:bm1"],
    }
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)

    for db in dbs:
        assert (await bmm.get_bookmarks(db)
                == set(initial_bookmarks.get(db, [])))

    await bmm.forget(dbs)

    # assert the key has been removed (memory optimization)
    assert isinstance(bmm, AsyncNeo4jBookmarkManager)
    assert (set(bmm._bookmarks.keys())
            == set(initial_bookmarks.keys()) - set(dbs))

    for db in dbs:
        assert await bmm.get_bookmarks(db) == set()
    assert await bmm.get_all_bookmarks() == set(
        bm for k, v in initial_bookmarks.items() if k not in dbs for bm in v
    )
