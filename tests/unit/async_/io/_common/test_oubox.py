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

from neo4j._async.io._common import AsyncOutbox
from neo4j._codec.packstream.v1 import PackableBuffer
from neo4j._exceptions import SocketDeadlineExceededError

from ....._async_compat import mark_async_test


class OutboxMockHolder:
    def __init__(self, mocker, *, max_chunk_size=16384):
        self.buffer = PackableBuffer()
        self.socket_mock = mocker.AsyncMock()
        self.packer_mock = mocker.Mock()
        self.packer_mock.return_value = self.packer_mock
        self.packer_mock.new_packable_buffer.return_value = self.buffer
        self.on_error = mocker.AsyncMock()
        self.outbox = AsyncOutbox(
            self.socket_mock,
            self.on_error,
            self.packer_mock,
            max_chunk_size=max_chunk_size,
        )

    def mock_write_message(self, data):
        def side_effect(*_args, **_kwargs):
            self.buffer.write(data)

        self.packer_mock.pack_struct.side_effect = side_effect

    def assert_no_error(self):
        self.on_error.assert_not_called()

    def mock_pack_failure(self, exception):
        def side_effect(*_args, **_kwargs):
            self.buffer.write(b"some data")
            raise exception

        self.packer_mock.pack_struct.side_effect = side_effect

    def mock_send_failure(self, exception):
        self.socket_mock.sendall.side_effect = exception


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
@mark_async_test
async def test_async_outbox_chunking(chunk_size, data, result, mocker):
    # Given
    mocks = OutboxMockHolder(mocker, max_chunk_size=chunk_size)
    mocks.mock_write_message(data)
    outbox = mocks.outbox
    socket_mock = mocks.socket_mock

    # When
    outbox.append_message(None, None, None)

    # Then
    mocks.assert_no_error()
    socket_mock.sendall.assert_not_called()
    assert await outbox.flush()
    socket_mock.sendall.assert_awaited_once_with(result + b"\x00\x00")

    assert not await outbox.flush()
    socket_mock.sendall.assert_awaited_once()


@pytest.mark.parametrize(
    "error",
    (
        asyncio.CancelledError("test"),
        SocketDeadlineExceededError("test"),
        OSError("test"),
    ),
)
@mark_async_test
async def test_outbox_send_failure_error_handler(mocker, error):
    mocks = OutboxMockHolder(mocker, max_chunk_size=12345)
    mocks.mock_send_failure(error)
    outbox = mocks.outbox

    outbox.append_message(None, None, None)
    assert not await outbox.flush()

    mocks.on_error.assert_awaited_once_with(error)


@pytest.mark.parametrize(
    "error",
    (
        asyncio.CancelledError("test"),
        SocketDeadlineExceededError("test"),
        OSError("test"),
        RecursionError("2deep4u"),
        RuntimeError("something funny happened"),
    ),
)
@mark_async_test
async def test_outbox_pack_failure(mocker, error):
    mocks = OutboxMockHolder(mocker, max_chunk_size=12345)
    mocks.mock_pack_failure(error)
    outbox = mocks.outbox
    socket_mock = mocks.socket_mock

    with pytest.raises(type(error)) as exc:
        outbox.append_message(None, None, None)
    assert not await outbox.flush()

    assert exc.value is error
    mocks.on_error.assert_not_called()
    socket_mock.sendall.assert_not_called()
