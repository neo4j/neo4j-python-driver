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


from __future__ import annotations

import asyncio
import contextlib
import socket
from dataclasses import dataclass
from ssl import (
    SSLContext,
    SSLSocket,
)

import freezegun
import pytest
from mock import mock  # noqa UP026 - to use same mock classes as `mocker`

import neo4j._async_compat.network._bolt_socket
from neo4j import _typing as t
from neo4j._async.io._bolt_socket import AsyncBoltSocket
from neo4j._deadline import Deadline
from neo4j._exceptions import (
    BoltError,
    BoltSecurityError,
    SocketDeadlineExceededError,
)
from neo4j._sync.io._bolt_socket import BoltSocket
from neo4j.addressing import Address
from neo4j.exceptions import ServiceUnavailable

from ...._async_compat.mark_decorator import (
    async_fixture,
    mark_async_test,
)


if t.TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from freezegun.api import (
        FrozenDateTimeFactory,
        StepTickTimeFactory,
        TickingDateTimeFactory,
    )
    from pytest_mock import MockerFixture

    TFreezeTime: t.TypeAlias = (
        StepTickTimeFactory | TickingDateTimeFactory | FrozenDateTimeFactory
    )


@pytest.fixture
def reader_factory(mocker):
    def factory():
        return mocker.create_autospec(asyncio.StreamReader)

    return factory


@pytest.fixture
def writer_factory(mocker):
    def factory():
        return mocker.create_autospec(asyncio.StreamWriter)

    return factory


@pytest.fixture
def socket_factory(reader_factory, writer_factory):
    def factory():
        protocol = None
        return AsyncBoltSocket(reader_factory(), protocol, writer_factory())

    return factory


def reader(s: AsyncBoltSocket):
    return s._reader


def writer(s: AsyncBoltSocket):
    return s._writer


class AwaitableMock(mock.NonCallableMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__async_mock = mock.AsyncMock()

    def __await__(self):
        return self.__async_mock().__await__()


@dataclass
class AsyncIoMock:
    wait_for_mock: mock.AsyncMock
    socket_socket_mock: mock.Mock
    event_loop_mock: mock.Mock
    transport_mock: mock.Mock
    protocol_mock: mock.Mock
    sock_connect_awaitable_mock: AwaitableMock
    get_event_loop_mock: mock.Mock


@async_fixture
async def mocked_asyncio(mocker: MockerFixture) -> AsyncIoMock:
    wait_for_mock = mocker.patch(
        "neo4j._async_compat.network._bolt_socket.wait_for",
        side_effect=neo4j._async_compat.network._bolt_socket.wait_for,
    )
    socket_socket_mock = mocker.patch(
        "neo4j._async_compat.network._bolt_socket.socket", autospec=True
    )
    event_loop_mock = mocker.Mock(spec=asyncio.AbstractEventLoop)
    transport_mock = mocker.Mock(spec=asyncio.Transport)
    protocol_mock = mocker.Mock(spec=asyncio.Protocol)
    event_loop_mock.create_connection.return_value = (
        transport_mock,
        protocol_mock,
    )
    sock_connect_awaitable_mock = AwaitableMock()
    event_loop_mock.sock_connect = mocker.Mock(
        return_value=sock_connect_awaitable_mock
    )
    get_event_loop_mock = mocker.patch(
        "asyncio.get_event_loop", return_value=event_loop_mock
    )

    assert isinstance(wait_for_mock, mocker.AsyncMock)
    assert isinstance(socket_socket_mock, mock.Mock)
    assert isinstance(event_loop_mock, mock.Mock)
    assert isinstance(transport_mock, mock.Mock)
    assert isinstance(protocol_mock, mock.Mock)
    assert isinstance(sock_connect_awaitable_mock, AwaitableMock)
    assert isinstance(get_event_loop_mock, mock.Mock)

    return AsyncIoMock(
        wait_for_mock,
        socket_socket_mock,
        event_loop_mock,
        transport_mock,
        protocol_mock,
        sock_connect_awaitable_mock,
        get_event_loop_mock,
    )


@dataclass
class IoMock:
    socket_socket_mock: mock.Mock


@pytest.fixture
def mocked_io(mocker: MockerFixture) -> IoMock:
    socket_socket_mock = mocker.patch(
        "neo4j._async_compat.network._bolt_socket.socket", autospec=True
    )

    assert isinstance(socket_socket_mock, mock.Mock)

    return IoMock(socket_socket_mock)


@pytest.mark.parametrize(
    ("timeout", "deadline", "pre_tick", "tick", "exception"),
    (
        (None, None, 60 * 60 * 10, 60 * 60 * 10, None),
        # test timeout
        (5, None, 0, 4, None),
        # timeout is not affected by time passed before the call
        (5, None, 7, 4, None),
        (5, None, 0, 6, socket.timeout),
        # test deadline
        (None, 5, 0, 4, None),
        (None, 5, 2, 2, None),
        # deadline is affected by time passed before the call
        (None, 5, 2, 4, SocketDeadlineExceededError),
        (None, 5, 6, 0, SocketDeadlineExceededError),
        (None, 5, 0, 6, SocketDeadlineExceededError),
        # test combination
        (5, 5, 0, 4, None),
        (5, 5, 2, 2, None),
        # deadline triggered by time passed before
        (5, 5, 2, 4, SocketDeadlineExceededError),
        # the shorter one determines the error
        (4, 5, 0, 6, socket.timeout),
        (5, 4, 0, 6, SocketDeadlineExceededError),
    ),
)
@pytest.mark.parametrize(
    ("method", "op"),
    (
        ("recv", "read"),
        ("recv_into", "read"),
        ("sendall", "write"),
    ),
)
@mark_async_test
async def test_async_bolt_socket_timeout(
    socket_factory, timeout, deadline, pre_tick, tick, exception, method, op
):
    def make_read_side_effect(freeze_time: TFreezeTime):
        async def read_side_effect(n):
            assert n == 1
            freeze_time.tick(tick)
            for _ in range(10):
                await asyncio.sleep(0)
            return b"y"

        return read_side_effect

    def make_drain_side_effect(freeze_time: TFreezeTime):
        async def drain_side_effect():
            freeze_time.tick(tick)
            for _ in range(10):
                await asyncio.sleep(0)

        return drain_side_effect

    async def call_method(s: AsyncBoltSocket):
        if method == "recv":
            res = await s.recv(1)
            assert res == b"y"
        elif method == "recv_into":
            b = bytearray(1)
            await s.recv_into(b, 1)
            assert b == b"y"
        elif method == "sendall":
            await s.sendall(b"y")
        else:
            raise NotImplementedError(f"method: {method}")

    with freezegun.freeze_time("1970-01-01T00:00:00") as frozen_time:
        socket = socket_factory()
        if timeout is not None:
            getattr(socket, f"set_{op}_timeout")(timeout)
        if deadline is not None:
            getattr(socket, f"set_{op}_deadline")(deadline)
        if pre_tick:
            frozen_time.tick(pre_tick)

        if method in {"recv", "recv_into"}:
            reader(socket).read.side_effect = make_read_side_effect(
                frozen_time
            )
        elif method == "sendall":
            writer(socket).drain.side_effect = make_drain_side_effect(
                frozen_time
            )
        else:
            raise NotImplementedError(f"method: {method}")

        if exception:
            with pytest.raises(exception):
                await call_method(socket)
        else:
            await call_method(socket)


@pytest.mark.parametrize("timeout", [None, 0, 0.1, -0.1])
@pytest.mark.parametrize("raw_deadline", [None, 1000, 1e-4, -50, 0])
@pytest.mark.parametrize("ssl", [True, False])
@mark_async_test
async def test_async_bolt_connection_timeout(
    timeout: float | None,
    raw_deadline: float | None,
    ssl: bool,
    mocker: MockerFixture,
    mocked_asyncio: AsyncIoMock,
) -> None:
    ssl_context_mock = mocker.Mock(spec=SSLContext) if ssl else None

    exc_expectation: AbstractContextManager = contextlib.nullcontext()
    connect_fails = False
    expected_timeout = timeout
    if timeout == 0:  # sync io: 0 timeout means non-blocking socket operations
        expected_timeout = None

    if timeout is not None and timeout < 0:
        exc_expectation = pytest.raises(
            ValueError,
            match=r"^Timeout value out of range$",
        )
        connect_fails = True
    elif ssl and raw_deadline is not None and raw_deadline <= 0:
        exc_expectation = pytest.raises(
            (ServiceUnavailable, BoltError, OSError)
        )

    with freezegun.freeze_time():
        deadline = Deadline(raw_deadline)

        with exc_expectation:
            await AsyncBoltSocket._connect_secure(
                Address(("localhost", 7687)),
                timeout,
                deadline,
                False,
                ssl_context_mock,
            )

    if not connect_fails:
        mocked_asyncio.wait_for_mock.assert_awaited_once_with(
            mocked_asyncio.sock_connect_awaitable_mock, expected_timeout
        )


@pytest.mark.parametrize(
    ("raw_deadline", "time_step"),
    (
        (None, None),
        (None, 1_000_000),
        (float("inf"), None),
        (float("inf"), 1_000_000),
        (-1, None),
        (-1, 0.01),
        (0, None),
        (0, 0.0001),
        (0.5, None),
        (0.5, 0.4999),
        (0.5, 0.5),
        (0.5, 1_000_000),
    ),
)
@pytest.mark.parametrize("timeout", [None, 0, 0.1])
@mark_async_test
async def test_async_bolt_ssl_timeout(
    raw_deadline: float | None,
    time_step: float | None,
    timeout: float | None,
    mocker: MockerFixture,
    mocked_asyncio: AsyncIoMock,
) -> None:
    ssl_context_mock = mocker.Mock(spec=SSLContext)

    fails = raw_deadline is not None and (
        (time_step is not None and time_step >= raw_deadline)
        or raw_deadline <= 0
    )

    exc_expectation: AbstractContextManager
    if fails:
        exc_expectation = pytest.raises(BoltSecurityError)
    else:
        exc_expectation = contextlib.nullcontext()

    with freezegun.freeze_time() as frozen_time:
        deadline = Deadline(raw_deadline)
        if time_step is not None:
            frozen_time.tick(time_step)
        expected_timeout = deadline.to_timeout()

        with exc_expectation as exc_capture:
            await AsyncBoltSocket._connect_secure(
                Address(("localhost", 7687)),
                timeout,
                deadline,
                False,
                ssl_context_mock,
            )

    con_mock = mocked_asyncio.event_loop_mock.create_connection
    if fails:
        con_mock.assert_not_called()
        assert isinstance(exc_capture, pytest.ExceptionInfo)
        assert isinstance(
            exc_capture.value.__cause__, SocketDeadlineExceededError
        )
    else:
        con_mock.assert_awaited_once()

        # handshake timeout must be > 0 or None (causes `ValueError` otherwise)
        con_kwargs = con_mock.call_args_list[0].kwargs
        assert con_kwargs.get("ssl_handshake_timeout") == expected_timeout


@pytest.mark.parametrize(
    ("timeout", "deadline", "expected_timeout"),
    (
        (None, None, None),
        (5, None, 5),
        (1.23, None, 1.23),
        (None, 5, 5),
        (None, 1.23, 1.23),
        (1, 2, 1),
        (2, 1, 1),
        (1.2, 2, 1.2),
        (2, 1.2, 1.2),
        (1, 2.3, 1),
        (2.3, 1, 1),
    ),
)
@pytest.mark.parametrize(
    ("method", "op"),
    (
        ("recv", "read"),
        ("recv_into", "read"),
        ("sendall", "write"),
    ),
)
def test_bolt_socket_timeout_forwarding(
    timeout, deadline, expected_timeout, method, op, mocker
):
    def call_method(s: BoltSocket):
        if method == "recv":
            s.recv(1)
        elif method == "recv_into":
            b = bytearray(1)
            s.recv_into(b, 1)
        elif method == "sendall":
            s.sendall(b"y")
        else:
            raise NotImplementedError(f"method: {method}")

    socket_mock = mocker.Mock(spec=socket.socket)
    bolt_socket = BoltSocket(socket_mock)

    with freezegun.freeze_time("1970-01-01T00:00:00"):
        if timeout is not None:
            getattr(bolt_socket, f"set_{op}_timeout")(timeout)
        if deadline is not None:
            getattr(bolt_socket, f"set_{op}_deadline")(deadline)

        socket_mock.settimeout.assert_not_called()

        call_method(bolt_socket)

        socket_mock.settimeout.assert_called_once_with(expected_timeout)


@pytest.mark.parametrize(
    ("raw_deadline", "time_step"),
    (
        (None, None),
        (None, 1_000_000),
        (float("inf"), None),
        (float("inf"), 1_000_000),
        (-1, None),
        (-1, 0.01),
        (0.5, None),
        (0.5, 0.49999),
        (0.5, 0.5),
        (0.5, 1_000_000),
    ),
)
@pytest.mark.parametrize("timeout", [None, 0, 0.1])
@mark_async_test
async def test_bolt_ssl_timeout(
    raw_deadline: float | None,
    time_step: float | None,
    timeout: float | None,
    mocker: MockerFixture,
    mocked_io: IoMock,
) -> None:
    last_ssl_timeout = None
    ssl_socket_mock = mocker.Mock(spec=SSLSocket)

    def ssl_wrap_side_effect(socket, *args, **kwargs):
        nonlocal last_ssl_timeout
        if socket.settimeout.call_args_list:
            last_timeout = socket.settimeout.call_args_list[-1].args[0]
            if not isinstance(last_timeout, mock.Mock):
                last_ssl_timeout = last_timeout
        return ssl_socket_mock

    ssl_context_mock = mocker.Mock(spec=SSLContext)
    ssl_context_mock.wrap_socket.side_effect = ssl_wrap_side_effect

    fails = raw_deadline is not None and (
        (time_step is not None and time_step >= raw_deadline)
        or raw_deadline <= 0
    )

    exc_expectation: AbstractContextManager
    if fails:
        exc_expectation = pytest.raises(BoltSecurityError)
    else:
        exc_expectation = contextlib.nullcontext()

    with freezegun.freeze_time() as frozen_time:
        deadline = Deadline(raw_deadline)
        if time_step is not None:
            frozen_time.tick(time_step)
        expected_timeout = deadline.to_timeout()

        with exc_expectation as exc_capture:
            BoltSocket._connect_secure(
                Address(("localhost", 7687)),
                timeout,
                deadline,
                False,
                ssl_context_mock,
            )

    if fails:
        ssl_context_mock.wrap_socket.assert_not_called()
        assert isinstance(exc_capture, pytest.ExceptionInfo)
        assert isinstance(
            exc_capture.value.__cause__, SocketDeadlineExceededError
        )
    else:
        ssl_context_mock.wrap_socket.assert_called_once()

        assert last_ssl_timeout == expected_timeout
