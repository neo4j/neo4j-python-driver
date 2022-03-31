# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import datetime
from unittest import TestCase

import pytz

from neo4j.data import DataDehydrator
from neo4j.packstream import Structure
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)


class TestTemporalDehydration(TestCase):

    def setUp(self):
        self.dehydrator = DataDehydrator()

    def test_date(self):
        date = Date(1991, 8, 24)
        struct, = self.dehydrator.dehydrate((date,))
        assert struct == Structure(b"D", 7905)

    def test_native_date(self):
        date = datetime.date(1991, 8, 24)
        struct, = self.dehydrator.dehydrate((date,))
        assert struct == Structure(b"D", 7905)

    def test_time(self):
        time = Time(1, 2, 3, 4, pytz.FixedOffset(60))
        struct, = self.dehydrator.dehydrate((time,))
        assert struct == Structure(b"T", 3723000000004, 3600)

    def test_native_time(self):
        time = datetime.time(1, 2, 3, 4, pytz.FixedOffset(60))
        struct, = self.dehydrator.dehydrate((time,))
        assert struct == Structure(b"T", 3723000004000, 3600)

    def test_local_time(self):
        time = Time(1, 2, 3, 4)
        struct, = self.dehydrator.dehydrate((time,))
        assert struct == Structure(b"t", 3723000000004)

    def test_local_native_time(self):
        time = datetime.time(1, 2, 3, 4)
        struct, = self.dehydrator.dehydrate((time,))
        assert struct == Structure(b"t", 3723000004000)

    def test_date_time(self):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(60))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"F", 1539344261, 474716862, 3600)

    def test_native_date_time(self):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(60))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"F", 1539344261, 474716000, 3600)

    def test_date_time_negative_offset(self):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(-60))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"F", 1539344261, 474716862, -3600)

    def test_native_date_time_negative_offset(self):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(-60))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"F", 1539344261, 474716000, -3600)

    def test_date_time_zone_id(self):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.timezone("Europe/Stockholm"))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"f", 1539344261, 474716862,
                                   "Europe/Stockholm")

    def test_native_date_time_zone_id(self):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.timezone("Europe/Stockholm"))
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"f", 1539344261, 474716000,
                                   "Europe/Stockholm")

    def test_local_date_time(self):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"d", 1539344261, 474716862)

    def test_native_local_date_time(self):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716)
        struct, = self.dehydrator.dehydrate((dt,))
        assert struct == Structure(b"d", 1539344261, 474716000)

    def test_duration(self):
        duration = Duration(months=1, days=2, seconds=3, nanoseconds=4)
        struct, = self.dehydrator.dehydrate((duration,))
        assert struct == Structure(b"E", 1, 2, 3, 4)

    def test_native_duration(self):
        duration = datetime.timedelta(days=1, seconds=2, microseconds=3)
        struct, = self.dehydrator.dehydrate((duration,))
        assert struct == Structure(b"E", 0, 1, 2, 3000)

    def test_duration_mixed_sign(self):
        duration = Duration(months=1, days=-2, seconds=3, nanoseconds=4)
        struct, = self.dehydrator.dehydrate((duration,))
        assert struct == Structure(b"E", 1, -2, 3, 4)

    def test_native_duration_mixed_sign(self):
        duration = datetime.timedelta(days=-1, seconds=2, microseconds=3)
        struct, = self.dehydrator.dehydrate((duration,))
        assert struct == Structure(b"E", 0, -1, 2, 3000)
