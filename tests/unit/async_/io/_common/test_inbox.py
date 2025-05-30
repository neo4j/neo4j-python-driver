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


import asyncio

import pytest

from neo4j._async.io._common import AsyncInbox
from neo4j._codec.packstream.v1 import Unpacker
from neo4j._exceptions import SocketDeadlineExceededError

from ....._async_compat import mark_async_test


class InboxMockHolder:
    def __init__(self, mocker):
        self.socket_mock = mocker.Mock()
        self.socket_mock.getsockname.return_value = ("host", 1234)
        self.on_error = mocker.AsyncMock()
        self.inbox = AsyncInbox(self.socket_mock, self.on_error, Unpacker)
        self.unpacker = mocker.Mock(wraps=self.inbox._unpacker)
        self.inbox._unpacker = self.unpacker
        # plenty of nonsense messages to read
        self.mock_set_data(b"\x00\x01\xff\x00\x00" * 1000)

    def mock_set_data(self, data):
        async def side_effect(buffer, n):
            nonlocal data

            if not data:
                pytest.fail("Read more data than mocked")

            n = min(len(data), len(buffer), n)
            buffer[:n] = data[:n]
            data = data[n:]
            return n

        self.socket_mock.recv_into.side_effect = side_effect

    def assert_no_error(self):
        self.on_error.assert_not_called()
        assert not self.inbox._broken

    def mock_receive_failure(self, exception):
        self.socket_mock.recv_into.side_effect = exception

    def mock_unpack_failure(self, exception):
        self.unpacker.unpack_structure_header.side_effect = exception


@pytest.mark.parametrize(
    ("data", "result"),
    (
        (
            bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14, 0, 0)),
            bytes(range(10, 15)),
        ),
        (
            bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 0)),
            bytes(range(10, 14)),
        ),
        (
            bytes((0, 1, 5, 0, 0)),
            bytes((5,)),
        ),
    ),
)
@mark_async_test
async def test_inbox_dechunking(data, result, mocker):
    # Given
    mocks = InboxMockHolder(mocker)
    mocks.mock_set_data(data)
    inbox = mocks.inbox
    buffer = inbox._buffer

    # When
    await inbox._buffer_one_chunk()

    # Then
    mocks.assert_no_error()
    assert buffer.used == len(result)
    assert buffer.data[: len(result)] == result


@pytest.mark.parametrize(
    "error",
    (
        asyncio.CancelledError("test"),
        SocketDeadlineExceededError("test"),
        OSError("test"),
    ),
)
@mark_async_test
async def test_inbox_receive_failure_error_handler(mocker, error):
    mocks = InboxMockHolder(mocker)
    mocks.mock_receive_failure(error)
    inbox = mocks.inbox

    with pytest.raises(type(error)) as exc:
        await inbox.pop({})

    assert exc.value is error
    mocks.on_error.assert_awaited_once_with(error)
    assert inbox._broken


@pytest.mark.parametrize(
    "error",
    (
        SocketDeadlineExceededError("test"),
        OSError("test"),
        RecursionError("2deep4u"),
        RuntimeError("something funny happened"),
    ),
)
@mark_async_test
async def test_inbox_unpack_failure(mocker, error):
    mocks = InboxMockHolder(mocker)
    mocks.mock_unpack_failure(error)
    inbox = mocks.inbox

    with pytest.raises(type(error)) as exc:
        await inbox.pop({})

    assert exc.value is error
    mocks.on_error.assert_awaited_once_with(error)
    assert inbox._broken
