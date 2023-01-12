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


import pytest

from neo4j._codec.packstream.v1 import PackableBuffer
from neo4j._sync.io._common import Outbox

from ...._async_compat import mark_sync_test


@pytest.mark.parametrize(("chunk_size", "data", "result"), (
    (
        2,
        bytes(range(10, 15)),
        bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14))
    ),
    (
        2,
        bytes(range(10, 14)),
        bytes((0, 2, 10, 11, 0, 2, 12, 13))
    ),
    (
        2,
        bytes((5,)),
        bytes((0, 1, 5))
    ),
))
@mark_sync_test
def test_async_outbox_chunking(chunk_size, data, result, mocker):
    buffer = PackableBuffer()
    socket_mock = mocker.Mock()
    packer_mock = mocker.Mock()
    packer_mock.return_value = packer_mock
    packer_mock.new_packable_buffer.return_value = buffer
    packer_mock.pack_struct.side_effect = \
        lambda *args, **kwargs: buffer.write(data)
    outbox = Outbox(socket_mock, pytest.fail, packer_mock, chunk_size)
    outbox.append_message(None, None, None)
    socket_mock.sendall.assert_not_called()
    assert outbox.flush()
    socket_mock.sendall.assert_called_once_with(result + b"\x00\x00")

    assert not outbox.flush()
    socket_mock.sendall.assert_called_once()
