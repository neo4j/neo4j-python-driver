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
from neo4j.types import UnsupportedType

from .._base import HydrationHandlerTestBase


class TestUnsupportedTypeHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.mark.parametrize("with_message", (True, False))
    def test_vector(self, hydration_scope, with_message):
        name = "2Cool4UType"
        min_bolt_major = 42
        min_bolt_minor = 128
        extra = {}
        if with_message:
            expected_message = "If only your driver were cooler..."
            extra["message"] = expected_message
        else:
            expected_message = None
        expected_min_version = (min_bolt_major, min_bolt_minor)

        struct = Structure(b"?", name, min_bolt_major, min_bolt_minor, extra)
        unsupported = hydration_scope.hydration_hooks[Structure](struct)

        assert isinstance(unsupported, UnsupportedType)
        assert unsupported.name == name
        assert unsupported.minimum_protocol_version == expected_min_version
        assert unsupported.message == expected_message
