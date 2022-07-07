#!/usr/bin/env python
# coding: utf-8

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


from decimal import Decimal
from unittest import TestCase

import pytz

from neo4j.data import DataHydrator
from neo4j.packstream import Structure
from neo4j.time import DateTime


class TestTemporalHydration(TestCase):

    def setUp(self):
        self.hydrant = DataHydrator()

    def test_local_date_time(self):
        struct = Structure(b'd', 1539344261, 474716862)
        dt, = self.hydrant.hydrate([struct])
        self.assertEqual(dt.year, 2018)
        self.assertEqual(dt.month, 10)
        self.assertEqual(dt.day, 12)
        self.assertEqual(dt.hour, 11)
        self.assertEqual(dt.minute, 37)
        self.assertEqual(dt.second, Decimal("41.474716862"))

    def test_date_time(self):
        struct = Structure(b"F", 1539344261, 474716862, 3600)
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.FixedOffset(60).localize(expected_dt)
        assert dt == expected_dt

    def test_date_time_negative_offset(self):
        struct = Structure(b"F", 1539344261, 474716862, -3600)
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.FixedOffset(-60).localize(expected_dt)
        assert dt == expected_dt

    def test_date_time_zone_id(self):
        struct = Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.timezone("Europe/Stockholm").localize(expected_dt)
        assert dt == expected_dt

    def test_does_not_handle_patched_date_time(self):
        struct = Structure(b"I", 123, 456, 3600)
        dt, = self.hydrant.hydrate([struct])
        assert struct == dt  # no hydration defined

    def test_does_not_handle_patched_date_time_zone_id(self):
        struct = Structure(b"i", 123, 456, "Europe/Stockholm")
        dt, = self.hydrant.hydrate([struct])
        assert struct == dt  # no hydration defined


class TestPatchedTemporalHydration(TestCase):

    def setUp(self):
        self.hydrant = DataHydrator(patch_utc=True)

    test_local_date_time = TestTemporalHydration.test_local_date_time

    def test_date_time(self):
        struct = Structure(b"I", 1539340661, 474716862, 3600)
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.FixedOffset(60).localize(expected_dt)
        assert dt == expected_dt

    def test_date_time_negative_offset(self):
        struct = Structure(b"I", 1539347861, 474716862, -3600)
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.FixedOffset(-60).localize(expected_dt)
        assert dt == expected_dt

    def test_date_time_zone_id(self):
        struct = Structure(b"i", 1539337061, 474716862, "Europe/Stockholm")
        dt, = self.hydrant.hydrate([struct])
        expected_dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        expected_dt = pytz.timezone("Europe/Stockholm").localize(expected_dt)
        assert dt == expected_dt

    def test_does_not_handle_unpatched_date_time(self):
        struct = Structure(b"F", 123, 456, 3600)
        dt, = self.hydrant.hydrate([struct])
        assert struct == dt  # no hydration defined

    def test_does_not_handle_unpatched_date_time_zone_id(self):
        struct = Structure(b"f", 123, 456, "Europe/Stockholm")
        dt, = self.hydrant.hydrate([struct])
        assert struct == dt  # no hydration defined
