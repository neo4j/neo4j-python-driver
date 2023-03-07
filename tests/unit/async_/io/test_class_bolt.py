# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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

import neo4j.auth_management
from neo4j._async.io import AsyncBolt
from neo4j._async_compat.network import AsyncBoltSocket

from ...._async_compat import AsyncTestDecorators


# python -m pytest tests/unit/io/test_class_bolt.py -s -v


def test_class_method_protocol_handlers():
    protocol_handlers = AsyncBolt.protocol_handlers()
    expected_versions = {
        (3, 0),
        (4, 1), (4, 2), (4, 3), (4, 4),
        (5, 0), (5, 1),
    }
    assert len(protocol_handlers) == len(expected_versions)
    assert protocol_handlers.keys() == expected_versions


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ((0, 0), 0),
        ((1, 0), 0),
        ((2, 0), 0),
        ((3, 0), 1),
        ((4, 0), 0),
        ((4, 1), 1),
        ((4, 2), 1),
        ((4, 3), 1),
        ((4, 4), 1),
        ((5, 0), 1),
        ((5, 1), 1),
        ((5, 2), 0),
        ((6, 0), 0),
    ]
)
def test_class_method_protocol_handlers_with_protocol_version(test_input,
                                                              expected):
    protocol_handlers = AsyncBolt.protocol_handlers(
        protocol_version=test_input
    )
    assert len(protocol_handlers) == expected


def test_class_method_protocol_handlers_with_invalid_protocol_version():
    with pytest.raises(TypeError):
        AsyncBolt.protocol_handlers(protocol_version=2)


def test_class_method_get_handshake():
    handshake = AsyncBolt.get_handshake()
    assert (b"\x00\x01\x01\x05\x00\x02\x04\x04\x00\x00\x01\x04\x00\x00\x00\x03"
            == handshake)


def test_magic_preamble():
    preamble = 0x6060B017
    preamble_bytes = preamble.to_bytes(4, byteorder="big")
    assert AsyncBolt.MAGIC_PREAMBLE == preamble_bytes


@AsyncTestDecorators.mark_async_only_test
async def test_cancel_hello_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch("neo4j._async.io._bolt.AsyncBoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, (5, 0), None, None
    )
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch("neo4j._async.io._bolt5.AsyncBolt5x0",
                                 autospec=True)
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.hello.side_effect = asyncio.CancelledError()
    bolt_mock.local_port = 1234

    with pytest.raises(asyncio.CancelledError):
        await AsyncBolt.open(
            address,
            auth_manager=neo4j.auth_management.AsyncAuthManagers.static(None)
        )

    bolt_mock.kill.assert_called_once_with()


@AsyncTestDecorators.mark_async_only_test
async def test_cancel_manager_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch("neo4j._async.io._bolt.AsyncBoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, (5, 0), None, None
    )
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch("neo4j._async.io._bolt5.AsyncBolt5x0",
                                 autospec=True)
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.local_port = 1234

    auth_manager = mocker.AsyncMock(
        spec=neo4j.auth_management.AsyncAuthManager
    )
    auth_manager.get_auth.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await AsyncBolt.open(address, auth_manager=auth_manager)

    socket_mock.kill.assert_called_once_with()


@AsyncTestDecorators.mark_async_only_test
async def test_fail_manager_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.AsyncMock(spec=AsyncBoltSocket)

    socket_cls_mock = mocker.patch("neo4j._async.io._bolt.AsyncBoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, (5, 0), None, None
    )
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch("neo4j._async.io._bolt5.AsyncBolt5x0",
                                 autospec=True)
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.local_port = 1234

    auth_manager = mocker.AsyncMock(
        spec=neo4j.auth_management.AsyncAuthManager
    )
    auth_manager.get_auth.side_effect = RuntimeError("token fetching failed")

    with pytest.raises(RuntimeError) as exc:
        await AsyncBolt.open(address, auth_manager=auth_manager)
    assert exc.value is auth_manager.get_auth.side_effect

    socket_mock.close.assert_called_once_with()
