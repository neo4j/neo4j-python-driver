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


import logging

import pytest

from neo4j.io._common import (
    Outbox,
    ResetResponse,
)

from ..work import fake_connection


@pytest.mark.parametrize(("chunk_size", "data", "result"), (
    (
        2,
        (bytes(range(10, 15)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14))
    ),
    (
        2,
        (bytes(range(10, 14)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13))
    ),
    (
        2,
        (bytes((5, 6, 7)), bytes((8, 9))),
        bytes((0, 2, 5, 6, 0, 2, 7, 8, 0, 1, 9))
    ),
))
def test_outbox_chunking(chunk_size, data, result):
    outbox = Outbox(max_chunk_size=chunk_size)
    assert bytes(outbox.view()) == b""
    for d in data:
        outbox.write(d)
    assert bytes(outbox.view()) == result
    # make sure this works multiple times
    assert bytes(outbox.view()) == result
    outbox.clear()
    assert bytes(outbox.view()) == b""


def get_handler_arg(response):
    if response == "RECORD":
        return []
    elif response == "IGNORED":
        return {}
    elif response == "FAILURE":
        return {}
    elif response == "SUCCESS":
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
    )
)
def test_reset_response_closes_connection_on_unexpected_responses(
    response, unexpected, fake_connection
):
    handler = ResetResponse(fake_connection, "reset")
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
    )
)
def test_reset_response_logs_warning_on_unexpected_responses(
    response, unexpected, fake_connection, caplog
):
    handler = ResetResponse(fake_connection, "reset")

    with caplog.at_level(logging.WARNING):
        call_handler(handler, response)

    log_message_found = any("RESET" in msg and "unexpected response" in msg
                            for msg in caplog.messages)
    if unexpected:
        assert log_message_found
    else:
        assert not log_message_found


@pytest.mark.parametrize("response",
                         ("RECORD", "IGNORED", "FAILURE", "SUCCESS"))
def test_reset_response_never_calls_handlers(
    response, fake_connection, mocker
):
    handlers = {
        key: mocker.MagicMock(name=key)
        for key in
        ("on_records", "on_ignored", "on_failure", "on_success", "on_summary")
    }

    handler = ResetResponse(fake_connection, "reset", **handlers)

    arg = get_handler_arg(response)
    call_handler(handler, response, arg)

    for handler in handlers.values():
        handler.assert_not_called()
