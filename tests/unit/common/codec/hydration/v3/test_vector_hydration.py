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

from neo4j._codec.hydration.v3 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.vector import Vector

from .._base import HydrationHandlerTestBase


class TestVectorHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.mark.parametrize(
        ("dtype", "marker", "data"),
        (
            *(
                (dtype, marker, data)
                for (dtype, marker) in (
                    ("i8", b"\xc8"),
                    ("i16", b"\xc9"),
                    ("i32", b"\xca"),
                    ("i64", b"\xcb"),
                    ("f32", b"\xc6"),
                    ("f64", b"\xc1"),
                )
                for data in (b"", bytes(range(128)))
            ),
            ("i8", b"\xc8", bytes(range(1))),
            ("i16", b"\xc9", bytes(range(2))),
            ("i32", b"\xca", bytes(range(4))),
            ("i64", b"\xcb", bytes(range(8))),
            ("f32", b"\xc6", bytes(range(4))),
            ("f64", b"\xc1", bytes(range(8))),
        ),
    )
    def test_vector(self, hydration_scope, dtype, marker, data):
        struct = Structure(b"V", marker, data)
        vector = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(vector, Vector)
        assert vector.dtype == dtype
        assert vector.raw() == data
