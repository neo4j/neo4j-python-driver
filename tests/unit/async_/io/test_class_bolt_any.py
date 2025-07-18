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


import pytest

import neo4j
from neo4j._async.io._bolt3 import AsyncBolt3
from neo4j._async.io._bolt4 import (
    AsyncBolt4x0,
    AsyncBolt4x1,
    AsyncBolt4x2,
    AsyncBolt4x3,
)
from neo4j._async.io._bolt5 import (
    AsyncBolt5x0,
    AsyncBolt5x1,
    AsyncBolt5x2,
    AsyncBolt5x3,
    AsyncBolt5x4,
    AsyncBolt5x5,
    AsyncBolt5x6,
    AsyncBolt5x7,
    AsyncBolt5x8,
)
from neo4j.exceptions import ServiceUnavailable

from ...._async_compat import mark_async_test


@pytest.fixture(
    params=[
        AsyncBolt3,
        AsyncBolt4x0,
        AsyncBolt4x1,
        AsyncBolt4x2,
        AsyncBolt4x3,
        AsyncBolt5x0,
        AsyncBolt5x1,
        AsyncBolt5x2,
        AsyncBolt5x3,
        AsyncBolt5x4,
        AsyncBolt5x5,
        AsyncBolt5x6,
        AsyncBolt5x7,
        AsyncBolt5x8,
    ]
)
def bolt_cls(request):
    return request.param


@mark_async_test
async def test_liveness_check_calls_reset(bolt_cls, fake_socket_pair):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=AsyncBolt5x8.PACKER_CLS,
        unpacker_cls=AsyncBolt5x8.UNPACKER_CLS,
    )
    connection = bolt_cls(address, sockets.client, 0)

    await sockets.server.send_message(b"\x70", {})
    await connection.liveness_check()
    tag, fields = await sockets.server.pop_message()
    assert tag == b"\x0f"
    assert len(fields) == 0
    sockets.server.assert_no_more_messages()


@mark_async_test
async def test_failed_liveness_check_does_not_call_pool(
    bolt_cls, fake_socket_pair, mocker
):
    def broken_recv_into(*args, **kwargs):
        raise OSError("nope")

    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=bolt_cls.PACKER_CLS,
        unpacker_cls=bolt_cls.UNPACKER_CLS,
    )
    connection = bolt_cls(address, sockets.client, 0)
    pool_mock = mocker.AsyncMock()
    connection.pool = pool_mock
    sockets.client.recv_into = broken_recv_into

    with pytest.raises(ServiceUnavailable):
        await connection.liveness_check()

    assert not pool_mock.method_calls
