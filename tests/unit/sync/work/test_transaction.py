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
    Query,
    Transaction,
)

from ...._async_compat import mark_sync_test


@pytest.mark.parametrize(("explicit_commit", "close"), (
    (False, False),
    (True, False),
    (True, True),
))
@mark_sync_test
def test_transaction_context_when_committing(
    mocker, fake_connection, explicit_commit, close
):
    on_closed = mocker.Mock()
    on_error = mocker.Mock()
    on_cancel = mocker.Mock()
    tx = Transaction(fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    with tx as tx_:
        assert mock_commit.call_count == 0
        assert mock_rollback.call_count == 0
        assert tx is tx_
        if explicit_commit:
            tx_.commit()
            mock_commit.assert_called_once_with()
            assert tx_.closed()
        if close:
            tx_.close()
            assert tx_.closed()
    mock_commit.assert_called_once_with()
    assert mock_rollback.call_count == 0
    assert tx_.closed()


@pytest.mark.parametrize(("rollback", "close"), (
    (True, False),
    (False, True),
    (True, True),
))
@mark_sync_test
def test_transaction_context_with_explicit_rollback(
    mocker, fake_connection, rollback, close
):
    on_closed = mocker.Mock()
    on_error = mocker.Mock()
    on_cancel = mocker.Mock()
    tx = Transaction(fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    with tx as tx_:
        assert mock_commit.call_count == 0
        assert mock_rollback.call_count == 0
        assert tx is tx_
        if rollback:
            tx_.rollback()
            mock_rollback.assert_called_once_with()
            assert tx_.closed()
        if close:
            tx_.close()
            mock_rollback.assert_called_once_with()
            assert tx_.closed()
    assert mock_commit.call_count == 0
    mock_rollback.assert_called_once_with()
    assert tx_.closed()


@mark_sync_test
def test_transaction_context_calls_rollback_on_error(
    mocker, fake_connection
):
    class OopsError(RuntimeError):
        pass

    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = Transaction(fake_connection, 2, on_closed, on_error,
                          on_cancel)
    mock_commit = mocker.patch.object(tx, "_commit", wraps=tx._commit)
    mock_rollback = mocker.patch.object(tx, "_rollback", wraps=tx._rollback)
    with pytest.raises(OopsError):
        with tx as tx_:
            assert mock_commit.call_count == 0
            assert mock_rollback.call_count == 0
            assert tx is tx_
            raise OopsError
    assert mock_commit.call_count == 0
    mock_rollback.assert_called_once_with()
    assert tx_.closed()


@mark_sync_test
def test_transaction_run_takes_no_query_object(fake_connection):
    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = Transaction(fake_connection, 2, on_closed, on_error,
                          on_cancel)
    with pytest.raises(ValueError):
        tx.run(Query("RETURN 1"))


@mark_sync_test
def test_transaction_rollbacks_on_open_connections(
    fake_connection
):
    tx = Transaction(
        fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    with tx as tx_:
        fake_connection.is_reset_mock.return_value = False
        fake_connection.is_reset_mock.reset_mock()
        tx_.rollback()
        fake_connection.is_reset_mock.assert_called_once()
        fake_connection.reset.assert_not_called()
        fake_connection.rollback.assert_called_once()


@mark_sync_test
def test_transaction_no_rollback_on_reset_connections(
    fake_connection
):
    tx = Transaction(
        fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    with tx as tx_:
        fake_connection.is_reset_mock.return_value = True
        fake_connection.is_reset_mock.reset_mock()
        tx_.rollback()
        fake_connection.is_reset_mock.assert_called_once()
        fake_connection.reset.assert_not_called()
        fake_connection.rollback.assert_not_called()


@mark_sync_test
def test_transaction_no_rollback_on_closed_connections(
    fake_connection
):
    tx = Transaction(
        fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    with tx as tx_:
        fake_connection.closed.return_value = True
        fake_connection.closed.reset_mock()
        fake_connection.is_reset_mock.reset_mock()
        tx_.rollback()
        fake_connection.closed.assert_called_once()
        fake_connection.is_reset_mock.assert_not_called()
        fake_connection.reset.assert_not_called()
        fake_connection.rollback.assert_not_called()


@mark_sync_test
def test_transaction_no_rollback_on_defunct_connections(
    fake_connection
):
    tx = Transaction(
        fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    with tx as tx_:
        fake_connection.defunct.return_value = True
        fake_connection.defunct.reset_mock()
        fake_connection.is_reset_mock.reset_mock()
        tx_.rollback()
        fake_connection.defunct.assert_called_once()
        fake_connection.is_reset_mock.assert_not_called()
        fake_connection.reset.assert_not_called()
        fake_connection.rollback.assert_not_called()
