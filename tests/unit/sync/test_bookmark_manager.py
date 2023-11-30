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

import typing as t

import pytest

import neo4j
from neo4j._async_compat.util import Util
from neo4j._meta import copy_signature
from neo4j.api import Bookmarks

from ..._async_compat import mark_sync_test


supplier_async_options = (True, False) if Util.is_async_code else (False,)
consumer_async_options = supplier_async_options


@copy_signature(neo4j.GraphDatabase.bookmark_manager)
def bookmark_manager(*args, **kwargs):
    return neo4j.GraphDatabase.bookmark_manager(*args, **kwargs)


@mark_sync_test
def test_return_empty_if_empty() -> None:
    bmm = bookmark_manager()

    assert not bmm.get_bookmarks()


@pytest.mark.parametrize("initial_bookmarks", (
    None, (), ("bm1", "bm2"), Bookmarks.from_raw_values(("bm1", "bm2")),
    ("bm1", "bm2", "bm1")
))
@mark_sync_test
def test_return_initial_bookmarks(
    initial_bookmarks: t.Union[None, t.Iterable[str], Bookmarks]
) -> None:
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)

    expected_bookmarks: t.Set[str]
    if initial_bookmarks is None:
        expected_bookmarks = set()
    elif isinstance(initial_bookmarks, Bookmarks):
        expected_bookmarks = set(initial_bookmarks.raw_values)
    else:
        expected_bookmarks = set(initial_bookmarks)

    assert (sorted(list(bmm.get_bookmarks()))
            == sorted(list(expected_bookmarks)))


@pytest.mark.parametrize("supplier_async", supplier_async_options)
@pytest.mark.parametrize("initial_bookmarks", (
    ["db1:bm1", "db1:bm1", "db3:bm1", "db3:bm2", "db4:bm4"],
    None,
))
@mark_sync_test
def test_get_bookmarks_return_from_bookmarks_supplier(
    mocker, initial_bookmarks: t.Optional[t.List[str]], supplier_async: bool
) -> None:
    extra_bookmarks = ["foo:bm1", "bar:bm2", "foo:bm1"]
    mock_cls = mocker.MagicMock if supplier_async else mocker.Mock
    supplier = mock_cls(
        return_value=Bookmarks.from_raw_values(extra_bookmarks)
    )
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks,
                           bookmarks_supplier=supplier)

    received_bookmarks = bmm.get_bookmarks()

    expected_bookmarks = {*extra_bookmarks, *(initial_bookmarks or [])}
    assert sorted(list(received_bookmarks)) == sorted(list(expected_bookmarks))
    if supplier_async:
        supplier.assert_called_once_with()
    else:
        supplier.assert_called_once_with()


@mark_sync_test
@pytest.mark.parametrize(
    ("update_old", "update_new"),
    (
        (["db3:bm1"], ["db3:bm3"]),
        (["db3:bm1", "db5:bm1"], ["db3:bm3", "db3:bm5"]),
    )
)
def test_chains_bookmarks(update_old, update_new) -> None:
    initial_bookmarks = [
        "db1:bm1", "db1:bm1", "db3:bm1", "db3:bm2", "db4:bm4"
    ]
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)
    bmm.update_bookmarks(update_old, update_new)
    received_bookmarks = bmm.get_bookmarks()

    expected_bookmarks = (set(initial_bookmarks)
                          - set(update_old) | set(update_new))
    assert sorted(list(received_bookmarks)) == sorted(list(expected_bookmarks))


@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@pytest.mark.parametrize("consumer_async", consumer_async_options)
@mark_sync_test
def test_notify_on_new_bookmarks(
    with_initial_bookmarks, consumer_async, mocker
) -> None:
    if with_initial_bookmarks:
        initial_bookmarks = ["db1:bm1", "db1:bm1", "db1:bm2", "db2:bm1"]
    else:
        initial_bookmarks = None
    mock_cls = mocker.MagicMock if consumer_async else mocker.Mock
    consumer = mock_cls()
    bmm = bookmark_manager(
        initial_bookmarks=initial_bookmarks,
        bookmarks_consumer=consumer
    )
    bookmarks_old = {"db1:bm1", "db3:bm1"}
    bookmarks_new = {"db1:bm4"}

    bmm.update_bookmarks(bookmarks_old, bookmarks_new)

    if consumer_async:
        consumer.assert_called_once()
        args = consumer.await_args.args
    else:
        consumer.assert_called_once()
        args = consumer.call_args.args
    assert len(args) == 1
    assert isinstance(args[0], Bookmarks)
    expected_bookmarks = (
        set(initial_bookmarks or []) - bookmarks_old | bookmarks_new
    )
    assert args[0].raw_values == expected_bookmarks


@pytest.mark.parametrize("consumer_async", consumer_async_options)
@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@mark_sync_test
def test_does_not_notify_on_empty_new_bookmark_set(
    with_initial_bookmarks, consumer_async, mocker
) -> None:
    if with_initial_bookmarks:
        initial_bookmarks = ["db1:bm1", "db1:bm2"]
    else:
        initial_bookmarks = None
    mock_cls = mocker.MagicMock if consumer_async else mocker.Mock
    consumer = mock_cls()
    bmm = bookmark_manager(
        initial_bookmarks=initial_bookmarks,
        bookmarks_consumer=consumer
    )
    bmm.update_bookmarks(["db1:bm1"], [])

    consumer.assert_not_called()


@pytest.mark.parametrize("with_initial_bookmarks", (True, False))
@mark_sync_test
def test_does_not_update_on_empty_new_bookmark_set(
    with_initial_bookmarks
) -> None:
    if with_initial_bookmarks:
        initial_bookmarks = ["db1:bm1", "db1:bm2"]
    else:
        initial_bookmarks = None
    bmm = bookmark_manager(initial_bookmarks=initial_bookmarks)

    bmm.update_bookmarks(["db1:bm1"], [])
    received_bookmarks = bmm.get_bookmarks()

    expected_bookmarks = set(initial_bookmarks or [])
    assert sorted(list(received_bookmarks)) == sorted(list(expected_bookmarks))
