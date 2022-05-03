# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


from unittest import TestCase

import pytz

from neo4j.data import DataHydrator
from neo4j.packstream import Structure
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)


class TestTemporalHydration(TestCase):

    def setUp(self):
        self.hydrant = DataHydrator()

    def test_hydrate_date_structure(self):
        struct = Structure(b"D", 7905)
        d, = self.hydrant.hydrate([struct])
        assert isinstance(d, Date)
        assert d.year == 1991
        assert d.month == 8
        assert d.day == 24

    def test_hydrate_time_structure(self):
        struct = Structure(b"T", 3723000000004, 3600)
        t, = self.hydrant.hydrate([struct])
        assert isinstance(t, Time)
        assert t.hour == 1
        assert t.minute == 2
        assert t.second == 3
        assert t.nanosecond == 4
        assert t.tzinfo == pytz.FixedOffset(60)

    def test_hydrate_local_time_structure(self):
        struct = Structure(b"t", 3723000000004)
        t, = self.hydrant.hydrate([struct])
        assert isinstance(t, Time)
        assert t.hour == 1
        assert t.minute == 2
        assert t.second == 3
        assert t.nanosecond == 4
        assert t.tzinfo is None

    def test_hydrate_date_time_structure(self):
        struct = Structure(b"F", 1539344261, 474716862, 3600)
        dt, = self.hydrant.hydrate([struct])
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo == pytz.FixedOffset(60)

    def test_hydrate_date_time_zone_id_structure(self):
        struct = Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        dt, = self.hydrant.hydrate([struct])
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

    def test_hydrate_local_date_time_structure(self):
        struct = Structure(b"d", 1539344261, 474716862)
        dt, = self.hydrant.hydrate([struct])
        assert isinstance(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo is None

    def test_hydrate_duration_structure(self):
        struct = Structure(b"E", 1, 2, 3, 4)
        d, = self.hydrant.hydrate([struct])
        assert isinstance(d, Duration)
        assert d.months == 1
        assert d.days == 2
        assert d.seconds == 3
        assert d.nanoseconds == 4
