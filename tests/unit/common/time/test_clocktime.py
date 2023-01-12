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

from neo4j.time import (
    ClockTime,
    Duration,
)


class TestClockTime:

    def test_zero_(self):
        ct = ClockTime()
        assert ct.seconds == 0
        assert ct.nanoseconds == 0

    def test_only_seconds(self):
        ct = ClockTime(123456)
        assert ct.seconds == 123456
        assert ct.nanoseconds == 0

    def test_float(self):
        ct = ClockTime(123456.789)
        assert ct.seconds == 123456
        assert ct.nanoseconds == 789000000

    def test_only_nanoseconds(self):
        ct = ClockTime(0, 123456789)
        assert ct.seconds == 0
        assert ct.nanoseconds == 123456789

    def test_nanoseconds_overflow(self):
        ct = ClockTime(0, 2123456789)
        assert ct.seconds == 2
        assert ct.nanoseconds == 123456789

    def test_positive_nanoseconds(self):
        ct = ClockTime(1, 1)
        assert ct.seconds == 1
        assert ct.nanoseconds == 1

    def test_negative_nanoseconds(self):
        ct = ClockTime(1, -1)
        assert ct.seconds == 0
        assert ct.nanoseconds == 999999999

    def test_add_float(self):
        ct = ClockTime(123456.789) + 0.1
        assert ct.seconds == 123456
        assert ct.nanoseconds == 889000000

    def test_add_duration(self):
        ct = ClockTime(123456.789) + Duration(seconds=1)
        assert ct.seconds == 123457
        assert ct.nanoseconds == 789000000

    def test_add_duration_with_months(self):
        with pytest.raises(ValueError):
            _ = ClockTime(123456.789) + Duration(months=1)

    def test_add_object(self):
        with pytest.raises(TypeError):
            _ = ClockTime(123456.789) + object()

    def test_sub_float(self):
        ct = ClockTime(123456.789) - 0.1
        assert ct.seconds == 123456
        assert ct.nanoseconds == 689000000

    def test_sub_duration(self):
        ct = ClockTime(123456.789) - Duration(seconds=1)
        assert ct.seconds == 123455
        assert ct.nanoseconds == 789000000

    def test_sub_duration_with_months(self):
        with pytest.raises(ValueError):
            _ = ClockTime(123456.789) - Duration(months=1)

    def test_sub_object(self):
        with pytest.raises(TypeError):
            _ = ClockTime(123456.789) - object()

    def test_repr(self):
        ct = ClockTime(123456.789)
        assert repr(ct).startswith("ClockTime")
