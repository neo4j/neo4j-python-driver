# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


import pytest

from neo4j._async.io import AsyncBolt


# python -m pytest tests/unit/io/test_class_bolt.py -s -v


def test_class_method_protocol_handlers():
    protocol_handlers = AsyncBolt.protocol_handlers()
    assert len(protocol_handlers) == 6
    assert protocol_handlers.keys() == {
        (3, 0),
        (4, 1), (4, 2), (4, 3), (4, 4),
        (5, 0),
    }


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
        ((5, 1), 0),
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
    assert (b"\x00\x00\x00\x05\x00\x02\x04\x04\x00\x00\x01\x04\x00\x00\x00\x03"
            == handshake)


def test_magic_preamble():
    preamble = 0x6060B017
    preamble_bytes = preamble.to_bytes(4, byteorder="big")
    assert AsyncBolt.MAGIC_PREAMBLE == preamble_bytes
