# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pytest

from neo4j._async.io._common import Outbox


@pytest.mark.parametrize(("chunk_size", "data", "result"), (
    (
        2,
        (bytes(range(10, 15)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14))
    ),
    (
        2,
        (bytes(range(10, 14)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13))
    ),
    (
        2,
        (bytes((5, 6, 7)), bytes((8, 9))),
        bytes((0, 2, 5, 6, 0, 2, 7, 8, 0, 1, 9))
    ),
))
def test_async_outbox_chunking(chunk_size, data, result):
    outbox = Outbox(max_chunk_size=chunk_size)
    assert bytes(outbox.chunked_data()) == b""
    for d in data:
        outbox.write(d)
    assert bytes(outbox.chunked_data()) == result
    # make sure this works multiple times
    assert bytes(outbox.chunked_data()) == result
    outbox.clear()
    assert bytes(outbox.chunked_data()) == b""
