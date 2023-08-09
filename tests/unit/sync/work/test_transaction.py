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


from unittest.mock import MagicMock

import pytest

from neo4j import (
    NotificationMinimumSeverity,
    Query,
    Transaction,
)
from neo4j.exceptions import (
    ClientError,
    ResultFailedError,
    ServiceUnavailable,
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
    on_closed = mocker.MagicMock()
    on_error = mocker.MagicMock()
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
    on_closed = mocker.MagicMock()
    on_error = mocker.MagicMock()
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
@pytest.mark.parametrize("params", (
    {"x": 1},
    {"x": "1"},
    {"x": "1", "y": 2},
    {"parameters": {"nested": "parameters"}},
))
@pytest.mark.parametrize("as_kwargs", (True, False))
def test_transaction_run_parameters(
    fake_connection, params, as_kwargs
):
    on_closed = MagicMock()
    on_error = MagicMock()
    on_cancel = MagicMock()
    tx = Transaction(fake_connection, 2, on_closed, on_error,
                          on_cancel)
    if not as_kwargs:
        params = {"parameters": params}
    tx.run("RETURN $x", **params)
    calls = [call for call in fake_connection.method_calls
             if call[0] in ("run", "send_all", "fetch_message")]
    assert [call[0] for call in calls] == ["run", "send_all", "fetch_message"]
    run = calls[0]
    assert run[1][0] == "RETURN $x"
    if "parameters" in params:
        params = params["parameters"]
    assert run[2]["parameters"] == params


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


@pytest.mark.parametrize("pipeline", (True, False))
@mark_sync_test
def test_transaction_begin_pipelining(
    fake_connection, pipeline
) -> None:
    noop_cb = lambda *args, **kwargs: None
    tx = Transaction(
        fake_connection, 2, noop_cb, noop_cb, noop_cb, noop_cb
    )
    database = "db"
    imp_user = None
    bookmarks = ["bookmark1", "bookmark2"]
    access_mode = "r"
    metadata = {"key": "value"}
    timeout = 42
    notifications_min_severity = NotificationMinimumSeverity.INFORMATION
    notifications_disabled_categories = ["cat1", "cat2"]

    tx._begin(
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
    assert fake_connection.method_calls == expected_calls


@pytest.mark.parametrize("error", ("server", "connection"))
@mark_sync_test
def test_server_error_propagates(scripted_connection, error):
    connection = scripted_connection
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

    tx = Transaction(
        connection, 2, lambda *args, **kwargs: None,
        lambda *args, **kwargs: None, lambda *args, **kwargs: None
    )
    res1 = tx.run("UNWIND range(1, 1000) AS n RETURN n")
    assert res1.__next__() == {"n": 1}

    res2 = tx.run("RETURN 'causes error later'")
    assert res2.fetch(2) == [{"n": 1}, {"n": 2}]
    with pytest.raises(expected_error) as exc1:
        res2.__next__()

    # can finish the buffer
    assert res1.fetch(1) == [{"n": 2}]
    # then fails because the connection was broken by res2
    with pytest.raises(ResultFailedError) as exc2:
        res1.__next__()

    assert exc1.value is exc2.value.__cause__
