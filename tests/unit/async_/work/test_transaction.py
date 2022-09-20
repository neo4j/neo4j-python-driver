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


from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from neo4j import (
    AsyncTransaction,
    Query,
)

from ...._async_compat import mark_async_test


@pytest.mark.parametrize(("explicit_commit", "close"), (
    (False, False),
    (True, False),
    (True, True),
))
@mark_async_test
async def test_transaction_context_when_committing(
    mocker, async_fake_connection, explicit_commit, close
):
    on_closed = mocker.AsyncMock()
    on_error = mocker.AsyncMock()
    on_cancel = mocker.Mock()
    tx = AsyncTransaction(async_fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    async with tx as tx_:
        assert mock_commit.call_count == 0
        assert mock_rollback.call_count == 0
        assert tx is tx_
        if explicit_commit:
            await tx_.commit()
            mock_commit.assert_awaited_once_with()
            assert tx_.closed()
        if close:
            await tx_.close()
            assert tx_.closed()
    mock_commit.assert_awaited_once_with()
    assert mock_rollback.call_count == 0
    assert tx_.closed()


@pytest.mark.parametrize(("rollback", "close"), (
    (True, False),
    (False, True),
    (True, True),
))
@mark_async_test
async def test_transaction_context_with_explicit_rollback(
    mocker, async_fake_connection, rollback, close
):
    on_closed = mocker.AsyncMock()
    on_error = mocker.AsyncMock()
    on_cancel = mocker.Mock()
    tx = AsyncTransaction(async_fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    async with tx as tx_:
        assert mock_commit.call_count == 0
        assert mock_rollback.call_count == 0
        assert tx is tx_
        if rollback:
            await tx_.rollback()
            mock_rollback.assert_awaited_once_with()
            assert tx_.closed()
        if close:
            await tx_.close()
            mock_rollback.assert_awaited_once_with()
            assert tx_.closed()
    assert mock_commit.call_count == 0
    mock_rollback.assert_awaited_once_with()
    assert tx_.closed()


@mark_async_test
async def test_transaction_context_calls_rollback_on_error(
    mocker, async_fake_connection
):
    class OopsError(RuntimeError):
        pass

    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = AsyncTransaction(async_fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    with pytest.raises(OopsError):
        async with tx as tx_:
            assert mock_commit.call_count == 0
            assert mock_rollback.call_count == 0
            assert tx is tx_
            raise OopsError
    assert mock_commit.call_count == 0
    mock_rollback.assert_awaited_once_with()
    assert tx_.closed()


@mark_async_test
async def test_transaction_run_takes_no_query_object(async_fake_connection):
    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = AsyncTransaction(async_fake_connection, 2, on_closed, on_error,
                          on_cancel)
    with pytest.raises(ValueError):
        await tx.run(Query("RETURN 1"))


@mark_async_test
async def test_transaction_rollbacks_on_open_connections(
    async_fake_connection
):
    tx = AsyncTransaction(
        async_fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    async with tx as tx_:
        async_fake_connection.is_reset_mock.return_value = False
        async_fake_connection.is_reset_mock.reset_mock()
        await tx_.rollback()
        async_fake_connection.is_reset_mock.assert_called_once()
        async_fake_connection.reset.assert_not_called()
        async_fake_connection.rollback.assert_called_once()


@mark_async_test
async def test_transaction_no_rollback_on_reset_connections(
    async_fake_connection
):
    tx = AsyncTransaction(
        async_fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    async with tx as tx_:
        async_fake_connection.is_reset_mock.return_value = True
        async_fake_connection.is_reset_mock.reset_mock()
        await tx_.rollback()
        async_fake_connection.is_reset_mock.assert_called_once()
        async_fake_connection.reset.assert_not_called()
        async_fake_connection.rollback.assert_not_called()


@mark_async_test
async def test_transaction_no_rollback_on_closed_connections(
    async_fake_connection
):
    tx = AsyncTransaction(
        async_fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    async with tx as tx_:
        async_fake_connection.closed.return_value = True
        async_fake_connection.closed.reset_mock()
        async_fake_connection.is_reset_mock.reset_mock()
        await tx_.rollback()
        async_fake_connection.closed.assert_called_once()
        async_fake_connection.is_reset_mock.assert_not_called()
        async_fake_connection.reset.assert_not_called()
        async_fake_connection.rollback.assert_not_called()


@mark_async_test
async def test_transaction_no_rollback_on_defunct_connections(
    async_fake_connection
):
    tx = AsyncTransaction(
        async_fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    async with tx as tx_:
        async_fake_connection.defunct.return_value = True
        async_fake_connection.defunct.reset_mock()
        async_fake_connection.is_reset_mock.reset_mock()
        await tx_.rollback()
        async_fake_connection.defunct.assert_called_once()
        async_fake_connection.is_reset_mock.assert_not_called()
        async_fake_connection.reset.assert_not_called()
        async_fake_connection.rollback.assert_not_called()
