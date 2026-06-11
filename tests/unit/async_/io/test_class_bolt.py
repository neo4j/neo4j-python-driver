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
import random

import pytest

import neo4j.auth_management
from neo4j._async.config import AsyncPoolConfig
from neo4j._async.io import AsyncBolt
from neo4j._async.io._bolt5 import AsyncBolt5x8
from neo4j._async.io._bolt_socket import AsyncBoltSocket
from neo4j._async.io._common import (
    CommitResponse,
    ResetResponse,
    Response,
)
from neo4j._exceptions import SocketDeadlineExceededError
from neo4j.exceptions import (
    IncompleteCommit,
    ServiceUnavailable,
    SessionExpired,
    UnsupportedServerProduct,
)

from ...._async_compat import (
    AsyncTestDecorators,
    mark_async_test,
)


# [bolt-version-bump] search tag when changing bolt version support
def test_class_method_protocol_handlers():
    # fmt: off
    expected_handlers = {
        (3, 0),
        (4, 2), (4, 3), (4, 4),
        (5, 0), (5, 1), (5, 2), (5, 3), (5, 4), (5, 5), (5, 6), (5, 7), (5, 8),
        (6, 0),
    }
    # fmt: on

    protocol_handlers = AsyncBolt.protocol_handlers

    assert len(protocol_handlers) == len(expected_handlers)
    assert protocol_handlers.keys() == expected_handlers


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ((0, 0), 0),
        ((1, 0), 0),
        ((2, 0), 0),
        ((3, 0), 1),
        ((4, 0), 0),
        ((4, 1), 0),
        ((4, 2), 1),
        ((4, 3), 1),
        ((4, 4), 1),
        ((5, 0), 1),
        ((5, 1), 1),
        ((5, 2), 1),
        ((5, 3), 1),
        ((5, 4), 1),
        ((5, 5), 1),
        ((5, 6), 1),
        ((5, 7), 1),
        ((5, 8), 1),
        ((5, 9), 0),
        ((6, 0), 1),
        ((6, 1), 0),
    ],
)
def test_class_method_protocol_handlers_with_protocol_version(
    test_input, expected
):
    protocol_handlers = AsyncBolt.protocol_handlers
    assert (test_input in protocol_handlers) == expected


def test_class_method_get_handshake():
    handshake = AsyncBolt.get_handshake()
    assert (
        handshake
        == b"\x00\x00\x01\xff\x00\x08\x08\x05\x00\x02\x04\x04\x00\x00\x00\x03"
    )


def test_magic_preamble():
    preamble = 0x6060B017
    preamble_bytes = preamble.to_bytes(4, byteorder="big")
    assert preamble_bytes == AsyncBolt.MAGIC_PREAMBLE


@AsyncTestDecorators.mark_async_only_test
async def test_cancel_hello_in_open(mocker, none_auth):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, (5, 0))
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch(
        "neo4j._async.io._bolt5.AsyncBolt5x0", autospec=True
    )
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.hello.side_effect = asyncio.CancelledError()
    bolt_mock.local_port = 1234
    mocker.patch.dict(
        AsyncBolt.protocol_handlers,
        {(5, 0): bolt_cls_mock},
    )

    with pytest.raises(asyncio.CancelledError):
        await AsyncBolt.open(address, auth_manager=none_auth)

    bolt_mock.kill.assert_called_once_with()


@AsyncTestDecorators.mark_async_only_test
async def test_set_write_timeout(mocker, none_auth):
    write_timeout_value = object()
    pool_config = AsyncPoolConfig()
    pool_config.connection_write_timeout = write_timeout_value

    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, (6, 0))
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch(
        "neo4j._async.io._bolt6.AsyncBolt6x0", autospec=True
    )
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.local_port = 1234
    mocker.patch.dict(
        AsyncBolt.protocol_handlers,
        {(6, 0): bolt_cls_mock},
    )

    _ = await AsyncBolt.open(
        address, auth_manager=none_auth, pool_config=pool_config
    )

    socket_mock.set_write_timeout.assert_called_once_with(write_timeout_value)


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize(
    ("bolt_version", "bolt_cls_path"),
    (
        ((3, 0), "neo4j._async.io._bolt3.AsyncBolt3"),
        ((4, 0), "neo4j._async.io._bolt4.AsyncBolt4x0"),
        ((4, 1), "neo4j._async.io._bolt4.AsyncBolt4x1"),
        ((4, 2), "neo4j._async.io._bolt4.AsyncBolt4x2"),
        ((4, 3), "neo4j._async.io._bolt4.AsyncBolt4x3"),
        ((4, 4), "neo4j._async.io._bolt4.AsyncBolt4x4"),
        ((5, 0), "neo4j._async.io._bolt5.AsyncBolt5x0"),
        ((5, 1), "neo4j._async.io._bolt5.AsyncBolt5x1"),
        ((5, 2), "neo4j._async.io._bolt5.AsyncBolt5x2"),
        ((5, 3), "neo4j._async.io._bolt5.AsyncBolt5x3"),
        ((5, 4), "neo4j._async.io._bolt5.AsyncBolt5x4"),
        ((5, 5), "neo4j._async.io._bolt5.AsyncBolt5x5"),
        ((5, 6), "neo4j._async.io._bolt5.AsyncBolt5x6"),
        ((5, 7), "neo4j._async.io._bolt5.AsyncBolt5x7"),
        ((5, 8), "neo4j._async.io._bolt5.AsyncBolt5x8"),
        ((6, 0), "neo4j._async.io._bolt6.AsyncBolt6x0"),
    ),
)
@mark_async_test
async def test_version_negotiation(
    mocker, bolt_version, bolt_cls_path, none_auth
):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, bolt_version)
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch(bolt_cls_path, autospec=True)
    bolt_cls_mock.return_value.local_port = 1234
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    mocker.patch.dict(
        AsyncBolt.protocol_handlers,
        {bolt_version: bolt_cls_mock},
    )

    connection = await AsyncBolt.open(address, auth_manager=none_auth)

    bolt_cls_mock.assert_called_once()
    assert connection is bolt_mock


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize(
    "bolt_version",
    (
        (0, 0),
        (1, 0),
        (2, 0),
        (3, 1),
        (4, 0),
        (4, 1),
        (5, 9),
        (6, 1),
    ),
)
@mark_async_test
async def test_failing_version_negotiation(mocker, bolt_version, none_auth):
    supported_protocols = (
        "('3.0', '4.2', '4.3', '4.4', "
        "'5.0', '5.1', '5.2', '5.3', '5.4', '5.5', '5.6', '5.7', '5.8', "
        "'6.0')"
    )

    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, bolt_version)
    socket_mock.getpeername.return_value = address

    with pytest.raises(UnsupportedServerProduct) as exc:
        await AsyncBolt.open(address, auth_manager=none_auth)

    assert exc.match(supported_protocols)


@AsyncTestDecorators.mark_async_only_test
async def test_cancel_auth_manager_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, (5, 0))
    socket_mock.getpeername.return_value = address

    auth_manager = mocker.AsyncMock(
        spec=neo4j.auth_management.AsyncAuthManager
    )
    auth_manager.get_auth.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await AsyncBolt.open(address, auth_manager=auth_manager)

    socket_mock.close.assert_called_once_with()


@AsyncTestDecorators.mark_async_only_test
async def test_fail_auth_manager_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncBoltSocket", autospec=True
    )
    socket_cls_mock.connect.return_value = (socket_mock, (5, 0))
    socket_mock.getpeername.return_value = address

    auth_manager = mocker.AsyncMock(
        spec=neo4j.auth_management.AsyncAuthManager
    )
    auth_manager.get_auth.side_effect = RuntimeError("token fetching failed")

    with pytest.raises(RuntimeError) as exc:
        await AsyncBolt.open(address, auth_manager=auth_manager)
    assert exc.value is auth_manager.get_auth.side_effect

    socket_mock.close.assert_called_once_with()


@pytest.mark.parametrize("mode", ("r", "w"))
@pytest.mark.parametrize(
    "error",
    (
        RuntimeError("test error"),
        RecursionError("How deep is your ~~love~~ recursion?"),
        asyncio.CancelledError("STOP! Cancel time!"),
    ),
)
@pytest.mark.parametrize("queued_commit", (None, 0, 1, 10))
@mark_async_test
async def test_error_handler_bubbling(
    mocker, fake_socket, mode, error, queued_commit
):
    mocks = ErrorHandlerTestMockHolder(mocker)
    if queued_commit is not None:
        mocks.queue_commit_message_at(queued_commit)

    connection = mocks.connection
    handler = mocks.get_error_handler(mode)

    with pytest.raises(type(error)) as exc:
        await handler(error)
    assert exc.value is error

    connection.socket.close.assert_called_once()

    assert connection.closed()
    assert connection.defunct()


@pytest.mark.parametrize("mode", ("r", "w"))
@pytest.mark.parametrize(
    "error",
    (
        OSError("computer says no! *cough*"),
        SocketDeadlineExceededError("too late, too little"),
        ServiceUnavailable("borked connection"),
        SessionExpired("nobody at home"),
    ),
)
@pytest.mark.parametrize("routing", (True, False))
@pytest.mark.parametrize("queued_commit", (None, 0, 1, 10))
@mark_async_test
async def test_error_handler_rewritten(
    mocker, fake_socket, mode, error, routing, queued_commit
):
    mocks = ErrorHandlerTestMockHolder(mocker)
    mocks.mock_driver_routing(routing)
    if queued_commit is not None:
        mocks.queue_commit_message_at(queued_commit)

    connection = mocks.connection
    handler = mocks.get_error_handler(mode)

    if queued_commit is not None:
        expected_error = IncompleteCommit
    elif routing:
        expected_error = SessionExpired
    else:
        expected_error = ServiceUnavailable

    with pytest.raises(expected_error) as exc:
        await handler(error)
    assert exc.value.__cause__ is error
    connection.socket.close.assert_called_once()

    assert connection.closed()
    assert connection.defunct()


class ErrorHandlerTestMockHolder:
    def __init__(self, mocker):
        self.address = neo4j.Address(("127.0.0.1", 7687))
        self.socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)
        self.socket_mock.getpeername.return_value = self.address
        self.connection = AsyncBolt5x8(self.address, self.socket_mock, 108000)
        self.pool = mocker.AsyncMock()
        self.connection.pool = self.pool

    def mock_driver_routing(self, routing):
        self.pool.is_direct_pool = not routing

    def queue_random_non_commit_response(self):
        resp_cls = random.choice((ResetResponse, Response))
        resp = resp_cls(self.connection, "MESSAGE", {})
        self.connection.responses.append(resp)

    def queue_commit_message(self):
        resp = CommitResponse(self.connection, "MESSAGE", {})
        self.connection.responses.append(resp)

    def queue_commit_message_at(self, position):
        self.connection.responses.clear()
        for _ in range(position - 1):
            self.queue_random_non_commit_response()
        self.queue_commit_message()
        self.queue_random_non_commit_response()

    def get_error_handler(self, mode):
        if mode == "r":
            return self.connection._set_defunct_read
        elif mode == "w":
            return self.connection._set_defunct_write
        else:
            raise ValueError(f"Invalid handler mode {mode!r}")


def test_configures_inbox_error_handler(mocker):
    inbox_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncInbox", autospec=True
    )
    mocks = ErrorHandlerTestMockHolder(mocker)
    inbox_cls_mock.assert_called_once()
    call_args = inbox_cls_mock.call_args
    assert call_args.kwargs["on_error"] == mocks.connection._set_defunct_read


def test_configures_outbox_error_handler(mocker):
    inbox_cls_mock = mocker.patch(
        "neo4j._async.io._bolt.AsyncOutbox", autospec=True
    )
    mocks = ErrorHandlerTestMockHolder(mocker)
    inbox_cls_mock.assert_called_once()
    call_args = inbox_cls_mock.call_args
    assert call_args.kwargs["on_error"] == mocks.connection._set_defunct_write
