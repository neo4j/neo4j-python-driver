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


import logging

import pytest

from neo4j._codec.packstream.v1 import PackableBuffer
from neo4j._sync.io._common import (
    Outbox,
    ResetResponse,
)

from ...._async_compat import mark_sync_test


@pytest.mark.parametrize(
    ("chunk_size", "data", "result"),
    (
        (
            2,
            bytes(range(10, 15)),
            bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14)),
        ),
        (
            2,
            bytes(range(10, 14)),
            bytes((0, 2, 10, 11, 0, 2, 12, 13)),
        ),
        (
            2,
            bytes((5,)),
            bytes((0, 1, 5)),
        ),
    ),
)
@mark_sync_test
def test_async_outbox_chunking(chunk_size, data, result, mocker):
    buffer = PackableBuffer()
    socket_mock = mocker.MagicMock()
    packer_mock = mocker.Mock()
    packer_mock.return_value = packer_mock
    packer_mock.new_packable_buffer.return_value = buffer
    packer_mock.pack_struct.side_effect = lambda *args, **kwargs: buffer.write(
        data
    )
    outbox = Outbox(socket_mock, pytest.fail, packer_mock, chunk_size)
    outbox.append_message(None, None, None)
    socket_mock.sendall.assert_not_called()
    assert outbox.flush()
    socket_mock.sendall.assert_called_once_with(result + b"\x00\x00")

    assert not outbox.flush()
    socket_mock.sendall.assert_called_once()


def get_handler_arg(response):
    if response == "RECORD":
        return []
    elif response in {"IGNORED", "FAILURE", "SUCCESS"}:
        return {}
    else:
        raise ValueError(f"Unexpected response: {response}")


def call_handler(handler, response, arg=None):
    if arg is None:
        arg = get_handler_arg(response)

    if response == "RECORD":
        return handler.on_records(arg)
    elif response == "IGNORED":
        return handler.on_ignored(arg)
    elif response == "FAILURE":
        return handler.on_failure(arg)
    elif response == "SUCCESS":
        return handler.on_success(arg)
    else:
        raise ValueError(f"Unexpected response: {response}")


@pytest.mark.parametrize(
    ("response", "unexpected"),
    (
        ("RECORD", True),
        ("IGNORED", True),
        ("FAILURE", True),
        ("SUCCESS", False),
    ),
)
@mark_sync_test
def test_reset_response_closes_connection_on_unexpected_responses(
    response, unexpected, fake_connection
):
    handler = ResetResponse(fake_connection, "reset", {})
    fake_connection.close.assert_not_called()

    call_handler(handler, response)

    if unexpected:
        fake_connection.close.assert_called_once()
    else:
        fake_connection.close.assert_not_called()


@pytest.mark.parametrize(
    ("response", "unexpected"),
    (
        ("RECORD", True),
        ("IGNORED", True),
        ("FAILURE", True),
        ("SUCCESS", False),
    ),
)
@mark_sync_test
def test_reset_response_logs_warning_on_unexpected_responses(
    response, unexpected, fake_connection, caplog
):
    handler = ResetResponse(fake_connection, "reset", {})

    with caplog.at_level(logging.WARNING):
        call_handler(handler, response)

    log_message_found = any(
        "RESET" in msg and "unexpected response" in msg
        for msg in caplog.messages
    )
    if unexpected:
        assert log_message_found
    else:
        assert not log_message_found


@pytest.mark.parametrize(
    "response", ("RECORD", "IGNORED", "FAILURE", "SUCCESS")
)
@mark_sync_test
def test_reset_response_never_calls_handlers(
    response, fake_connection, mocker
):
    handlers = {
        key: mocker.MagicMock(name=key)
        for key in (
            "on_records",
            "on_ignored",
            "on_failure",
            "on_success",
            "on_summary",
        )
    }

    handler = ResetResponse(fake_connection, "reset", {}, **handlers)

    arg = get_handler_arg(response)
    call_handler(handler, response, arg)

    for handler in handlers.values():
        handler.assert_not_called()
