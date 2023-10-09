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

import pytest

from neo4j import (
    AsyncTransaction,
    NotificationMinimumSeverity,
    Query,
)
from neo4j.exceptions import (
    ClientError,
    ResultFailedError,
    ServiceUnavailable,
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
@pytest.mark.parametrize("params", (
    {"x": 1},
    {"x": "1"},
    {"x": "1", "y": 2},
    {"parameters": {"nested": "parameters"}},
))
@pytest.mark.parametrize("as_kwargs", (True, False))
async def test_transaction_run_parameters(
    async_fake_connection, params, as_kwargs
):
    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = AsyncTransaction(async_fake_connection, 2, on_closed, on_error,
                          on_cancel)
    if not as_kwargs:
        params = {"parameters": params}
    await tx.run("RETURN $x", **params)
    calls = [call for call in async_fake_connection.method_calls
             if call[0] in ("run", "send_all", "fetch_message")]
    assert [call[0] for call in calls] == ["run", "send_all", "fetch_message"]
    run = calls[0]
    assert run[1][0] == "RETURN $x"
    if "parameters" in params:
        params = params["parameters"]
    assert run[2]["parameters"] == params


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


@pytest.mark.parametrize("pipeline", (True, False))
@mark_async_test
async def test_transaction_begin_pipelining(
    async_fake_connection, pipeline
) -> None:
    tx = AsyncTransaction(
        async_fake_connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    database = "db"
    imp_user = None
    bookmarks = ["bookmark1", "bookmark2"]
    access_mode = "r"
    metadata = {"key": "value"}
    timeout = 42
    notifications_min_severity = NotificationMinimumSeverity.INFORMATION
    notifications_disabled_categories = ["cat1", "cat2"]

    await tx._begin(
        database, imp_user, bookmarks, access_mode, metadata, timeout,
        notifications_min_severity, notifications_disabled_categories,
        pipelined=pipeline
    )
    expected_calls: list = [
        (
            "begin",
            {
                "db": database,
                "imp_user": imp_user,
                "bookmarks": bookmarks,
                "mode": access_mode,
                "metadata": metadata,
                "timeout": timeout,
                "notifications_min_severity": notifications_min_severity,
                "notifications_disabled_categories":
                    notifications_disabled_categories,
            }
        ),
    ]
    if not pipeline:
        expected_calls.append(("send_all",))
        expected_calls.append(("fetch_all",))
    assert async_fake_connection.method_calls == expected_calls


@pytest.mark.parametrize("error", ("server", "connection"))
@mark_async_test
async def test_server_error_propagates(async_scripted_connection, error):
    connection = async_scripted_connection
    script = [
        # res 1
        ("run", {"on_success": ({"fields": ["n"]},), "on_summary": None}),
        ("pull", {"on_records": ([[1], [2]],),
                  "on_success": ({"has_more": True},)}),
        # res 2
        ("run", {"on_success": ({"fields": ["n"]},), "on_summary": None}),
        ("pull", {"on_records": ([[1], [2]],),
                  "on_success": ({"has_more": True},)}),
    ]
    if error == "server":
        script.append(
            ("pull", {"on_failure": ({"code": "Neo.ClientError.Made.Up"},),
                      "on_summary": None})
        )
        expected_error = ClientError
    elif error == "connection":
        script.append(("pull", ServiceUnavailable()))
        expected_error = ServiceUnavailable
    else:
        raise ValueError(f"Unknown error type {error}")
    connection.set_script(script)

    tx = AsyncTransaction(
        connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    res1 = await tx.run("UNWIND range(1, 1000) AS n RETURN n")
    assert await res1.__anext__() == {"n": 1}

    res2 = await tx.run("RETURN 'causes error later'")
    assert await res2.fetch(2) == [{"n": 1}, {"n": 2}]
    with pytest.raises(expected_error) as exc1:
        await res2.__anext__()

    # can finish the buffer
    assert await res1.fetch(1) == [{"n": 2}]
    # then fails because the connection was broken by res2
    with pytest.raises(ResultFailedError) as exc2:
        await res1.__anext__()

    assert exc1.value is exc2.value.__cause__
