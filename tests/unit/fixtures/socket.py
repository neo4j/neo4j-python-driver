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
import logging
import typing as t
from collections import deque
from socket import socket

import pytest

from neo4j._async.io._bolt_socket import AsyncBoltSocket
from neo4j._sync.io._bolt_socket import BoltSocket


if t.TYPE_CHECKING:
    _T_co = t.TypeVar("_T_co", covariant=True)

    class _SocketFactory(t.Protocol, t.Generic[_T_co]):
        def __call__(
            self, bytes_to_read: deque, bytes_written: deque | None = None
        ) -> _T_co: ...


log = logging.getLogger(__name__)


def _pop_read_n(d: deque, n: int, fn_name: str) -> bytes:
    res: str | bytes = f"Not enough data: {d}"
    try:
        res = bytes(d.popleft() for _ in range(n))
        return res  # noqa: RET504 - false positive, res is used in finally
    finally:
        log.debug("%s(%s): %s", fn_name, n, res)


@pytest.fixture
def async_bolt_socket_factory(mocker) -> _SocketFactory[AsyncBoltSocket]:
    def factory(bytes_to_read, bytes_written=None) -> AsyncBoltSocket:
        assert isinstance(bytes_to_read, deque)
        bytes(bytes_to_read)  # test that bytes_to_read contains only bytes
        write_buffer = []

        async def read(n):
            nonlocal bytes_to_read
            return _pop_read_n(bytes_to_read, n, "read")

        def write(b):
            nonlocal write_buffer
            log.debug("write(%s)", b)
            write_buffer.extend(b)

        async def drain():
            log.debug("drain()")
            if bytes_written is not None:
                bytes_written.extend(write_buffer)
            write_buffer.clear()

        def transport_get_extra(key):
            if key == "sockname":
                return "localhost", 0x1234
            if key == "peername":
                return "peer_name"
            raise KeyError(f"not mocked: {key}")

        reader = mocker.Mock(spec=asyncio.StreamReader)
        writer = mocker.Mock(spec=asyncio.StreamWriter)
        protocol = mocker.Mock(spec=asyncio.StreamReaderProtocol)

        reader.read.side_effect = read
        writer.write.side_effect = write
        writer.drain.side_effect = drain
        writer.transport.get_extra_info.side_effect = transport_get_extra

        return AsyncBoltSocket(reader, protocol, writer)

    return factory


@pytest.fixture
def bolt_socket_factory(mocker) -> _SocketFactory[BoltSocket]:
    def factory(bytes_to_read, bytes_written=None) -> BoltSocket:
        assert isinstance(bytes_to_read, deque)
        bytes(bytes_to_read)  # test that bytes_to_read contains only bytes

        def recv(n):
            nonlocal bytes_to_read
            return _pop_read_n(bytes_to_read, n, "recv")

        def recv_into(buff, n=None):
            if n is None:
                raise NotImplementedError("n=None not mocked")
            nonlocal bytes_to_read
            res = _pop_read_n(bytes_to_read, n, "recv_into")
            buff[:n] = res
            return n

        def send_all(b):
            nonlocal bytes_written
            log.debug("send_all(%s)", b)
            if bytes_written is not None:
                bytes_written.extend(b)

        socket_mock = mocker.Mock(spec=socket)
        socket_mock.recv.side_effect = recv
        socket_mock.recv_into.side_effect = recv_into
        socket_mock.sendall.side_effect = send_all
        socket_mock.getsockname.return_value = ("localhost", 0x1234)
        socket_mock.getpeername.return_value = "peer_name"

        return BoltSocket(socket_mock)

    return factory
