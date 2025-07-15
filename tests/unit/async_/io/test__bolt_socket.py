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

import logging
import typing
from collections import deque

import pytest

from neo4j._async.io._bolt import AsyncBolt
from neo4j._deadline import Deadline
from neo4j.addressing import Address

from ...._async_compat import mark_async_test


if typing.TYPE_CHECKING:
    _T = typing.TypeVar("_T")


def _deque_popleft_n(d: deque[_T], n: int) -> list[_T]:
    return [d.popleft() for _ in range(n)]


PREAMBLE = AsyncBolt.MAGIC_PREAMBLE
BOLT_HANDSHAKE = AsyncBolt.get_handshake()
DEADLINE = Deadline(float("inf"))


# [bolt-version-bump] search tag when changing bolt version support
@mark_async_test
@pytest.mark.parametrize("log_level", (1, logging.DEBUG, logging.CRITICAL))
async def test_handshake(async_bolt_socket_factory, caplog, log_level):
    chosen_version = (5, 8)

    caplog.set_level(log_level)
    response = deque((0, 0, *reversed(chosen_version)))
    out = deque()
    socket = async_bolt_socket_factory(response, out)

    await socket._handshake(Address(("localhost", 7687)), DEADLINE)

    assert _deque_popleft_n(out, len(PREAMBLE)) == list(PREAMBLE)
    assert _deque_popleft_n(out, len(BOLT_HANDSHAKE)) == list(BOLT_HANDSHAKE)
    assert len(response) == 0, "not all bytes were read"


# [bolt-version-bump] search tag when changing bolt version support
@mark_async_test
@pytest.mark.parametrize("log_level", (1, logging.DEBUG, logging.CRITICAL))
async def test_handshake_manifest_v1(
    async_bolt_socket_factory,
    caplog,
    log_level,
):
    chosen_version = (6, 0)
    expected_feature_bits = b"\x00"  # varint(0)

    caplog.set_level(log_level)
    chosen_version_bytes = (0, 0, *reversed(chosen_version))
    response = deque(
        (
            *b"\x00\x00\x01\xff",  # manifest v1
            *b"\x01",  # varint(1) number of versions offered
            *chosen_version_bytes,
            *expected_feature_bits,
        )
    )
    out = deque()
    socket = async_bolt_socket_factory(response, out)

    await socket._handshake(Address(("localhost", 7687)), DEADLINE)

    assert _deque_popleft_n(out, len(PREAMBLE)) == list(PREAMBLE)
    assert _deque_popleft_n(out, len(BOLT_HANDSHAKE)) == list(BOLT_HANDSHAKE)
    assert _deque_popleft_n(out, len(chosen_version_bytes)) == list(
        chosen_version_bytes
    )
    assert _deque_popleft_n(out, len(expected_feature_bits)) == list(
        expected_feature_bits
    )
    assert len(response) == 0, "not all bytes were read"
