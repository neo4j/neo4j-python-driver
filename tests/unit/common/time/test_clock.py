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


from unittest import TestCase

from neo4j.time import (
    Clock,
    ClockTime,
)


class TestClock(TestCase):

    def test_no_clock_implementations(self):
        try:
            Clock._Clock__implementations = []
            with self.assertRaises(RuntimeError):
                _ = Clock()
        finally:
            Clock._Clock__implementations = None

    def test_base_clock_precision(self):
        clock = object.__new__(Clock)
        with self.assertRaises(NotImplementedError):
            _ = clock.precision()

    def test_base_clock_available(self):
        clock = object.__new__(Clock)
        with self.assertRaises(NotImplementedError):
            _ = clock.available()

    def test_base_clock_utc_time(self):
        clock = object.__new__(Clock)
        with self.assertRaises(NotImplementedError):
            _ = clock.utc_time()

    def test_local_offset(self):
        clock = object.__new__(Clock)
        offset = clock.local_offset()
        self.assertIsInstance(offset, ClockTime)

    def test_local_time(self):
        _ = Clock()
        for impl in Clock._Clock__implementations:
            self.assert_(issubclass(impl, Clock))
            clock = object.__new__(impl)
            time = clock.local_time()
            self.assertIsInstance(time, ClockTime)

    def test_utc_time(self):
        _ = Clock()
        for impl in Clock._Clock__implementations:
            self.assert_(issubclass(impl, Clock))
            clock = object.__new__(impl)
            time = clock.utc_time()
            self.assertIsInstance(time, ClockTime)
