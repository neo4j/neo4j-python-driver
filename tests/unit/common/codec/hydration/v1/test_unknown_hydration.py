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

from neo4j._codec.hydration import BrokenHydrationObject
from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j._codec.packstream import Structure

from ._base import HydrationHandlerTestBase


class TestUnknownHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_unknown_structure_tag(self, hydration_scope):
        struct = Structure(b"a", "lol wut?")
        res = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(res, BrokenHydrationObject)
        error = res.error
        assert isinstance(error, ValueError)
        assert repr(b"a") in str(error)

    def test_broken_object_propagates_through_lists(self, hydration_scope):
        broken_obj = BrokenHydrationObject(Exception("test"), "b")
        data = [1, broken_obj, 3]
        res = hydration_scope.hydration_hooks[list](data)
        assert isinstance(res, BrokenHydrationObject)
        assert res.raw_data == data
        assert res.error is broken_obj.error

    def test_broken_object_propagates_through_dicts(self, hydration_scope):
        broken_obj = BrokenHydrationObject(Exception("test"), "b")
        data = {"a": 1, "b": broken_obj, "c": 3}
        res = hydration_scope.hydration_hooks[dict](data)
        assert isinstance(res, BrokenHydrationObject)
        assert res.raw_data == data
        assert res.error is broken_obj.error
