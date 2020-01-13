#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2020 "Neo4j,"
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


from unittest import TestCase

from neo4j.data import DataHydrator
from neo4j.packstream import Structure


class TemporalHydrationTestCase(TestCase):

    def setUp(self):
        self.hydrant = DataHydrator()

    def test_can_hydrate_date_time_structure(self):
        struct = Structure(b'd', 1539344261, 474716862)
        dt, = self.hydrant.hydrate([struct])
        self.assertEqual(dt.year, 2018)
        self.assertEqual(dt.month, 10)
        self.assertEqual(dt.day, 12)
        self.assertEqual(dt.hour, 11)
        self.assertEqual(dt.minute, 37)
        self.assertEqual(dt.second, 41.474716862)
