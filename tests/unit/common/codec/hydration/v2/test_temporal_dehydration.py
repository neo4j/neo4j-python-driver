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

from neo4j._codec.hydration.v2 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import DateTime

from ..v1.test_temporal_dehydration import (
    TestTimeDehydration as _TestTemporalDehydrationV1,
)


class TestTimeDehydration(_TestTemporalDehydrationV1):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_date_time(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"I", 1539340661, 474716862, 3600)

    def test_native_date_time(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"I", 1539340661, 474716000, 3600)

    def test_date_time_negative_offset(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(-60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"I", 1539347861, 474716862, -3600)

    def test_native_date_time_negative_offset(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(-60))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"I", 1539347861, 474716000, -3600)

    def test_date_time_zone_id(self, hydration_scope):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.timezone("Europe/Stockholm"))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"i", 1539339941, 474716862,
                                   "Europe/Stockholm")

    def test_native_date_time_zone_id(self, hydration_scope):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.timezone("Europe/Stockholm"))
        struct = hydration_scope.dehydration_hooks[type(dt)](dt)
        assert struct == Structure(b"i", 1539339941, 474716000,
                                   "Europe/Stockholm")
