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

from neo4j._async_compat.network import BoltSocket
from neo4j._exceptions import BoltHandshakeError
from neo4j._sync.io import Bolt

from ...._async_compat import (
    mark_sync_test,
    TestDecorators,
)


# python -m pytest tests/unit/io/test_class_bolt.py -s -v


# [bolt-version-bump] search tag when changing bolt version support
def test_class_method_protocol_handlers():
    expected_handlers = {
        (3, 0),
        (4, 1), (4, 2), (4, 3), (4, 4),
        (5, 0), (5, 1), (5, 2), (5, 3),
    }

    protocol_handlers = Bolt.protocol_handlers()

    assert len(protocol_handlers) == len(expected_handlers)
    assert protocol_handlers.keys() == expected_handlers


# [bolt-version-bump] search tag when changing bolt version support
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
        ((5, 2), 1),
        ((5, 3), 1),
        ((5, 4), 0),
        ((6, 0), 0),
    ]
)
def test_class_method_protocol_handlers_with_protocol_version(test_input,
                                                              expected):
    protocol_handlers = Bolt.protocol_handlers(
        protocol_version=test_input
    )
    assert len(protocol_handlers) == expected


def test_class_method_protocol_handlers_with_invalid_protocol_version():
    with pytest.raises(TypeError):
        Bolt.protocol_handlers(protocol_version=2)


# [bolt-version-bump] search tag when changing bolt version support
def test_class_method_get_handshake():
    handshake = Bolt.get_handshake()
    assert (b"\x00\x03\x03\x05\x00\x02\x04\x04\x00\x00\x01\x04\x00\x00\x00\x03"
            == handshake)


def test_magic_preamble():
    preamble = 0x6060B017
    preamble_bytes = preamble.to_bytes(4, byteorder="big")
    assert Bolt.MAGIC_PREAMBLE == preamble_bytes


@TestDecorators.mark_async_only_test
def test_cancel_hello_in_open(mocker):
    address = ("localhost", 7687)
    socket_mock = mocker.MagicMock(spec=BoltSocket)

    socket_cls_mock = mocker.patch("neo4j._sync.io._bolt.BoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, (5, 0), None, None
    )
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch("neo4j._sync.io._bolt5.Bolt5x0",
                                 autospec=True)
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock
    bolt_mock.hello.side_effect = asyncio.CancelledError()
    bolt_mock.local_port = 1234

    with pytest.raises(asyncio.CancelledError):
        Bolt.open(address)

    bolt_mock.kill.assert_called_once_with()


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize(
    ("bolt_version", "bolt_cls_path"),
    (
        ((3, 0), "neo4j._sync.io._bolt3.Bolt3"),
        ((4, 1), "neo4j._sync.io._bolt4.Bolt4x1"),
        ((4, 2), "neo4j._sync.io._bolt4.Bolt4x2"),
        ((4, 3), "neo4j._sync.io._bolt4.Bolt4x3"),
        ((4, 4), "neo4j._sync.io._bolt4.Bolt4x4"),
        ((5, 0), "neo4j._sync.io._bolt5.Bolt5x0"),
        ((5, 1), "neo4j._sync.io._bolt5.Bolt5x1"),
        ((5, 2), "neo4j._sync.io._bolt5.Bolt5x2"),
        ((5, 3), "neo4j._sync.io._bolt5.Bolt5x3"),
    ),
)
@mark_sync_test
def test_version_negotiation(mocker, bolt_version, bolt_cls_path):
    address = ("localhost", 7687)
    socket_mock = mocker.MagicMock(spec=BoltSocket)

    socket_cls_mock = mocker.patch("neo4j._sync.io._bolt.BoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, bolt_version, None, None
    )
    socket_mock.getpeername.return_value = address
    bolt_cls_mock = mocker.patch(bolt_cls_path, autospec=True)
    bolt_cls_mock.return_value.local_port = 1234
    bolt_mock = bolt_cls_mock.return_value
    bolt_mock.socket = socket_mock

    connection = Bolt.open(address)

    bolt_cls_mock.assert_called_once()
    assert connection is bolt_mock


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize("bolt_version", (
    (0, 0),
    (2, 0),
    (4, 0),
    (3, 1),
    (5, 4),
    (6, 0),
))
@mark_sync_test
def test_failing_version_negotiation(mocker, bolt_version):
    supported_protocols = \
        "('3.0', '4.1', '4.2', '4.3', '4.4', '5.0', '5.1', '5.2', '5.3')"

    address = ("localhost", 7687)
    socket_mock = mocker.MagicMock(spec=BoltSocket)

    socket_cls_mock = mocker.patch("neo4j._sync.io._bolt.BoltSocket",
                                   autospec=True)
    socket_cls_mock.connect.return_value = (
        socket_mock, bolt_version, None, None
    )
    socket_mock.getpeername.return_value = address

    with pytest.raises(BoltHandshakeError) as exc:
        Bolt.open(address)

    assert exc.match(supported_protocols)
