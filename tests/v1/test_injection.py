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


import sys
import traceback

import pytest

from neo4j._codec.hydration import DehydrationHooks
from neo4j._codec.packstream import Structure
from neo4j._codec.packstream.v1 import (
    PackableBuffer,
    Packer,
    UnpackableBuffer,
    Unpacker,
)


@pytest.fixture
def packer_with_buffer():
    packable_buffer = Packer.new_packable_buffer()
    return Packer(packable_buffer), packable_buffer


@pytest.fixture
def unpacker_with_buffer():
    unpackable_buffer = Unpacker.new_unpackable_buffer()
    return Unpacker(unpackable_buffer), unpackable_buffer


def test_pack_injection_works(packer_with_buffer):
    class TestClass:
        pass

    class TestException(Exception):
        pass

    def raise_test_exception(*args, **kwargs):
        raise TestException()

    dehydration_hooks = DehydrationHooks(
        exact_types={TestClass: raise_test_exception},
        subtypes={},
    )
    test_object = TestClass()
    packer, _ = packer_with_buffer

    with pytest.raises(TestException) as exc:
        packer.pack(test_object, dehydration_hooks=dehydration_hooks)

    # printing the traceback to stdout to make it easier to debug
    traceback.print_exception(exc.type, exc.value, exc.tb, file=sys.stdout)

    assert any("_rust_pack" in str(entry.statement) for entry in exc.traceback)
    assert not any("_py_pack" in str(entry.statement)
                   for entry in exc.traceback)


def test_unpack_injection_works(unpacker_with_buffer):
    class TestException(Exception):
        pass

    def raise_test_exception(*args, **kwargs):
        raise TestException()

    hydration_hooks = {Structure: raise_test_exception}
    unpacker, buffer = unpacker_with_buffer

    buffer.reset()
    buffer.data = bytearray(b"\xB0\xFF")

    with pytest.raises(TestException) as exc:
        unpacker.unpack(hydration_hooks)

    # printing the traceback to stdout to make it easier to debug
    traceback.print_exception(exc.type, exc.value, exc.tb, file=sys.stdout)

    assert any("_rust_unpack" in str(entry.statement)
               for entry in exc.traceback)
    assert not any("_py_unpack" in str(entry.statement)
                   for entry in exc.traceback)
