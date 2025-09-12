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
from neo4j.types import UnsupportedType

from .._base import HydrationHandlerTestBase


class TestUnsupportedTypeDehydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_has_no_transformer(self, hydration_scope):
        value = UnsupportedType._new("UUID", (255, 255), None)

        transformer = hydration_scope.dehydration_hooks.get_transformer(value)

        assert transformer is None
