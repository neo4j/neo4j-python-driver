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


import datetime

import pytest
import pytz

from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

from ._base import HydrationHandlerTestBase


class TestTimeDehydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_date(self, hydration_scope):
        date = Date(1991, 8, 24)
        struct = hydration_scope.dehydration_hooks[type(date)](date)
        assert struct == Structure(b"D", 7905)

    def test_native_date(self, hydration_scope):
        date = datetime.date(1991, 8, 24)
        struct = hydration_scope.dehydration_hooks[type(date)](date)
        assert struct == Structure(b"D", 7905)

    def test_time(self, hydration_scope):
        time = Time(1, 2, 3, 4, pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(time)](time)
        assert struct == Structure(b"T", 3723000000004, 3600)

    def test_native_time(self, hydration_scope):
        time = datetime.time(1, 2, 3, 4, pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(time)](time)
        assert struct == Structure(b"T", 3723000004000, 3600)

    def test_local_time(self, hydration_scope):
        time = Time(1, 2, 3, 4)
        struct = hydration_scope.dehydration_hooks[type(time)](time)
        assert struct == Structure(b"t", 3723000000004)

    def test_local_native_time(self, hydration_scope):
        time = datetime.time(1, 2, 3, 4)
        struct = hydration_scope.dehydration_hooks[type(time)](time)
        assert struct == Structure(b"t", 3723000004000)

    def test_date_time(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"F", 1539344261, 474716862, 3600)

    def test_native_date_time(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"F", 1539344261, 474716000, 3600)

    def test_date_time_negative_offset(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(-60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"F", 1539344261, 474716862, -3600)

    def test_native_date_time_negative_offset(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(-60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"F", 1539344261, 474716000, -3600)

    def test_date_time_zone_id(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.timezone("Europe/Stockholm"))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"f", 1539344261, 474716862,
                                   "Europe/Stockholm")

    def test_native_date_time_zone_id(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.timezone("Europe/Stockholm"))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"f", 1539344261, 474716000,
                                   "Europe/Stockholm")

    def test_local_date_time(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"d", 1539344261, 474716862)

    def test_native_local_date_time(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716)
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"d", 1539344261, 474716000)

    def test_duration(self, hydration_scope):
        duration = Duration(months=1, days=2, seconds=3, nanoseconds=4)
        struct = hydration_scope.dehydration_hooks[type(duration)](duration)
        assert struct == Structure(b"E", 1, 2, 3, 4)

    def test_native_duration(self, hydration_scope):
        duration = datetime.timedelta(days=1, seconds=2, microseconds=3)
        struct = hydration_scope.dehydration_hooks[type(duration)](duration)
        assert struct == Structure(b"E", 0, 1, 2, 3000)

    def test_duration_mixed_sign(self, hydration_scope):
        duration = Duration(months=1, days=-2, seconds=3, nanoseconds=4)
        struct = hydration_scope.dehydration_hooks[type(duration)](duration)
        assert struct == Structure(b"E", 1, -2, 3, 4)

    def test_native_duration_mixed_sign(self, hydration_scope):
        duration = datetime.timedelta(days=-1, seconds=2, microseconds=3)
        struct = hydration_scope.dehydration_hooks[type(duration)](duration)
        assert struct == Structure(b"E", 0, -1, 2, 3000)


class TestUTCPatchedTimeDehydration(TestTimeDehydration):
    @pytest.fixture
    def hydration_handler(self):
        handler = HydrationHandler()
        handler.patch_utc()
        return handler

    def test_date_time(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_date_time(
            hydration_scope
        )

    def test_native_date_time(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_native_date_time(
            hydration_scope
        )

    def test_date_time_negative_offset(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_date_time_negative_offset(
            hydration_scope
        )

    def test_native_date_time_negative_offset(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_native_date_time_negative_offset(
            hydration_scope
        )

    def test_date_time_zone_id(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_date_time_zone_id(
            hydration_scope
        )

    def test_native_date_time_zone_id(self, hydration_scope):
        from ..v2.test_time_dehydration import (
            TestTimeDehydration as TestTimeDehydrationV2,
        )
        TestTimeDehydrationV2().test_native_date_time_zone_id(
            hydration_scope
        )
