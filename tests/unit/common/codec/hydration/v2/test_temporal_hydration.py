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
import pytz

from neo4j._codec.hydration import BrokenHydrationObject
from neo4j._codec.hydration.v2 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import DateTime

from ..v1.test_temporal_hydration import (
    TestTemporalHydration as _TestTemporalHydrationV1,
)


class TestTemporalHydration(_TestTemporalHydrationV1):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_hydrate_date_time_structure_v1(self, hydration_scope):
        struct = Structure(b"F", 1539344261, 474716862, 3600)
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, BrokenHydrationObject)
        assert repr(b"F") in str(dt.error)

    def test_hydrate_date_time_structure_v2(self, hydration_scope):
        struct = Structure(b"I", 1539344261, 474716862, 3600)
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 12
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo == pytz.FixedOffset(60)

    def test_hydrate_date_time_zone_id_structure_v1(self, hydration_scope):
        struct = Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, BrokenHydrationObject)
        assert repr(b"f") in str(dt.error)

    def test_hydrate_date_time_zone_id_structure_v2(self, hydration_scope):
        struct = Structure(b"i", 1539344261, 474716862, "Europe/Stockholm")
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 13
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        tz = (
            pytz.timezone("Europe/Stockholm")
            .localize(dt.replace(tzinfo=None))
            .tzinfo
        )
        assert dt.tzinfo == tz

    def test_hydrate_date_time_unknown_zone_id_structure(
        self, hydration_scope
    ):
        struct = Structure(b"i", 1539344261, 474716862, "Europe/Neo4j")
        res = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(res, BrokenHydrationObject)
        exc = None
        try:
            pytz.timezone("Europe/Neo4j")
        except Exception as e:
            exc = e
        assert exc.__class__ == res.error.__class__
        assert str(exc) == str(res.error)
