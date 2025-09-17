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

from neo4j._async.io._common import ResetResponse

from ....._async_compat import mark_async_test


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
@mark_async_test
async def test_reset_response_closes_connection_on_unexpected_responses(
    response, unexpected, async_fake_connection
):
    handler = ResetResponse(async_fake_connection, "reset", {})
    async_fake_connection.close.assert_not_called()

    await call_handler(handler, response)

    if unexpected:
        async_fake_connection.close.assert_awaited_once()
    else:
        async_fake_connection.close.assert_not_called()


@pytest.mark.parametrize(
    ("response", "unexpected"),
    (
        ("RECORD", True),
        ("IGNORED", True),
        ("FAILURE", True),
        ("SUCCESS", False),
    ),
)
@mark_async_test
async def test_reset_response_logs_warning_on_unexpected_responses(
    response, unexpected, async_fake_connection, caplog
):
    handler = ResetResponse(async_fake_connection, "reset", {})

    with caplog.at_level(logging.WARNING):
        await call_handler(handler, response)

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
@mark_async_test
async def test_reset_response_never_calls_handlers(
    response, async_fake_connection, mocker
):
    handlers = {
        key: mocker.AsyncMock(name=key)
        for key in (
            "on_records",
            "on_ignored",
            "on_failure",
            "on_success",
            "on_summary",
        )
    }

    handler = ResetResponse(async_fake_connection, "reset", {}, **handlers)

    arg = get_handler_arg(response)
    await call_handler(handler, response, arg)

    for handler in handlers.values():
        handler.assert_not_called()
