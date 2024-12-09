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
import socket
import typing as t

import freezegun
import pytest

from neo4j._async_compat.network import AsyncBoltSocket
from neo4j._exceptions import SocketDeadlineExceededError

from ...._async_compat.mark_decorator import mark_async_test


if t.TYPE_CHECKING:
    import typing_extensions as te
    from freezegun.api import (
        FrozenDateTimeFactory,
        StepTickTimeFactory,
        TickingDateTimeFactory,
    )

    TFreezeTime: te.TypeAlias = (
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
@pytest.mark.parametrize("method", ("recv", "recv_into", "sendall"))
@mark_async_test
async def test_async_bolt_socket_read_timeout(
    socket_factory, timeout, deadline, pre_tick, tick, exception, method
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
            socket.settimeout(timeout)
        if deadline is not None:
            socket.set_deadline(deadline)
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
