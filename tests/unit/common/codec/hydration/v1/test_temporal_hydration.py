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
import pytz

from neo4j._codec.hydration import BrokenHydrationObject
from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

from ._base import HydrationHandlerTestBase


class TestTemporalHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_hydrate_date_structure(self, hydration_scope):
        struct = Structure(b"D", 7905)
        d = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(d, Date)
        assert d.year == 1991
        assert d.month == 8
        assert d.day == 24

    def test_hydrate_time_structure(self, hydration_scope):
        struct = Structure(b"T", 3723000000004, 3600)
        t = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(t, Time)
        assert t.hour == 1
        assert t.minute == 2
        assert t.second == 3
        assert t.nanosecond == 4
        assert t.tzinfo == pytz.FixedOffset(60)

    def test_hydrate_local_time_structure(self, hydration_scope):
        struct = Structure(b"t", 3723000000004)
        t = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(t, Time)
        assert t.hour == 1
        assert t.minute == 2
        assert t.second == 3
        assert t.nanosecond == 4
        assert t.tzinfo is None

    def test_hydrate_date_time_structure_v1(self, hydration_scope):
        struct = Structure(b"F", 1539344261, 474716862, 3600)
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo == pytz.FixedOffset(60)

    def test_hydrate_date_time_structure_v2(self, hydration_scope):
        struct = Structure(b"I", 1539344261, 474716862, 3600)
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, BrokenHydrationObject)
        assert repr(b"I") in str(dt.error)

    def test_hydrate_date_time_zone_id_structure_v1(self, hydration_scope):
        struct = Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        tz = pytz.timezone("Europe/Stockholm") \
            .localize(dt.replace(tzinfo=None)).tzinfo
        assert dt.tzinfo == tz

    def test_hydrate_date_time_unknown_zone_id_structure(self,
                                                         hydration_scope):
        struct = Structure(b"f", 1539344261, 474716862, "Europe/Neo4j")
        res = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(res, BrokenHydrationObject)
        exc = None
        try:
            pytz.timezone("Europe/Neo4j")
        except Exception as e:
            exc = e
        assert exc.__class__ == res.error.__class__
        assert str(exc) == str(res.error)

    def test_hydrate_date_time_zone_id_structure_v2(self, hydration_scope):
        struct = Structure(b"i", 1539344261, 474716862, "Europe/Stockholm")
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, BrokenHydrationObject)
        assert repr(b"i") in str(dt.error)

    def test_hydrate_local_date_time_structure(self, hydration_scope):
        struct = Structure(b"d", 1539344261, 474716862)
        dt = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo is None

    def test_hydrate_duration_structure(self, hydration_scope):
        struct = Structure(b"E", 1, 2, 3, 4)
        d = hydration_scope.hydration_hooks[Structure](struct)
        assert isinstance(d, Duration)
        assert d.months == 1
        assert d.days == 2
        assert d.seconds == 3
        assert d.nanoseconds == 4


class TestUTCPatchedTemporalHydration(TestTemporalHydration):
    @pytest.fixture
    def hydration_handler(self):
        handler = HydrationHandler()
        handler.patch_utc()
        return handler

    def test_hydrate_date_time_structure_v1(self, hydration_scope):
        from ..v2.test_temporal_hydration import (
            TestTemporalHydration as TestTimeHydrationV2,
        )
        TestTimeHydrationV2().test_hydrate_date_time_structure_v1(
            hydration_scope
        )

    def test_hydrate_date_time_structure_v2(self, hydration_scope):
        from ..v2.test_temporal_hydration import (
            TestTemporalHydration as TestTimeHydrationV2,
        )
        TestTimeHydrationV2().test_hydrate_date_time_structure_v2(
            hydration_scope
        )

    def test_hydrate_date_time_zone_id_structure_v1(self, hydration_scope):
        from ..v2.test_temporal_hydration import (
            TestTemporalHydration as TestTimeHydrationV2,
        )
        TestTimeHydrationV2().test_hydrate_date_time_zone_id_structure_v1(
            hydration_scope
        )

    def test_hydrate_date_time_zone_id_structure_v2(self, hydration_scope):
        from ..v2.test_temporal_hydration import (
            TestTemporalHydration as TestTimeHydrationV2,
        )
        TestTimeHydrationV2().test_hydrate_date_time_zone_id_structure_v2(
            hydration_scope
        )

    def test_hydrate_date_time_unknown_zone_id_structure(self,
                                                         hydration_scope):

        from ..v2.test_temporal_hydration import (
            TestTemporalHydration as TestTimeHydrationV2,
        )
        TestTimeHydrationV2().test_hydrate_date_time_unknown_zone_id_structure(
            hydration_scope
        )
