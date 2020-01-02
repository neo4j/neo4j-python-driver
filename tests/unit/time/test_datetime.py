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


from datetime import datetime, timedelta
from unittest import TestCase

from pytz import timezone, FixedOffset

from neo4j.time import DateTime, MIN_YEAR, MAX_YEAR, Duration
from neo4j.time.arithmetic import nano_add, nano_div
from neo4j.time.clock_implementations import Clock, ClockTime


eastern = timezone("US/Eastern")


class FixedClock(Clock):

    @classmethod
    def available(cls):
        return True

    @classmethod
    def precision(cls):
        return 12

    @classmethod
    def local_offset(cls):
        return ClockTime()

    def utc_time(self):
        return ClockTime(45296, 789000000)


class DateTimeTestCase(TestCase):

    def test_zero(self):
        t = DateTime(0, 0, 0, 0, 0, 0)
        self.assertEqual(t.year, 0)
        self.assertEqual(t.month, 0)
        self.assertEqual(t.day, 0)
        self.assertEqual(t.hour, 0)
        self.assertEqual(t.minute, 0)
        self.assertEqual(t.second, 0)

    def test_non_zero_naive(self):
        t = DateTime(2018, 4, 26, 23, 0, 17.914390409)
        self.assertEqual(t.year, 2018)
        self.assertEqual(t.month, 4)
        self.assertEqual(t.day, 26)
        self.assertEqual(t.hour, 23)
        self.assertEqual(t.minute, 0)
        self.assertEqual(t.second, 17.914390409)

    def test_year_lower_bound(self):
        with self.assertRaises(ValueError):
            _ = DateTime(MIN_YEAR - 1, 1, 1, 0, 0, 0)

    def test_year_upper_bound(self):
        with self.assertRaises(ValueError):
            _ = DateTime(MAX_YEAR + 1, 1, 1, 0, 0, 0)

    def test_month_lower_bound(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 0, 1, 0, 0, 0)

    def test_month_upper_bound(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 13, 1, 0, 0, 0)

    def test_day_zero(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 1, 0, 0, 0, 0)

    def test_day_30_of_29_day_month(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 2, 30, 0, 0, 0)

    def test_day_32_of_31_day_month(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 3, 32, 0, 0, 0)

    def test_day_31_of_30_day_month(self):
        with self.assertRaises(ValueError):
            _ = DateTime(2000, 4, 31, 0, 0, 0)

    def test_day_29_of_28_day_month(self):
        with self.assertRaises(ValueError):
            _ = DateTime(1999, 2, 29, 0, 0, 0)

    def test_last_day_of_month(self):
        t = DateTime(2000, 1, -1, 0, 0, 0)
        self.assertEqual(t.year, 2000)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 31)

    def test_today(self):
        t = DateTime.today()
        self.assertEqual(t.year, 1970)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 1)
        self.assertEqual(t.hour, 12)
        self.assertEqual(t.minute, 34)
        self.assertEqual(t.second, 56.789)

    def test_now_without_tz(self):
        t = DateTime.now()
        self.assertEqual(t.year, 1970)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 1)
        self.assertEqual(t.hour, 12)
        self.assertEqual(t.minute, 34)
        self.assertEqual(t.second, 56.789)
        self.assertIsNone(t.tzinfo)

    def test_now_with_tz(self):
        t = DateTime.now(eastern)
        self.assertEqual(t.year, 1970)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 1)
        self.assertEqual(t.hour, 7)
        self.assertEqual(t.minute, 34)
        self.assertEqual(t.second, 56.789)
        self.assertEqual(t.utcoffset(), timedelta(seconds=-18000))
        self.assertEqual(t.dst(), timedelta())
        self.assertEqual(t.tzname(), "EST")

    def test_utc_now(self):
        t = DateTime.utc_now()
        self.assertEqual(t.year, 1970)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 1)
        self.assertEqual(t.hour, 12)
        self.assertEqual(t.minute, 34)
        self.assertEqual(t.second, 56.789)
        self.assertIsNone(t.tzinfo)

    def test_from_timestamp(self):
        t = DateTime.from_timestamp(0)
        self.assertEqual(t.year, 1970)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 1)
        self.assertEqual(t.hour, 0)
        self.assertEqual(t.minute, 0)
        self.assertEqual(t.second, 0.0)
        self.assertIsNone(t.tzinfo)

    def test_from_overflowing_timestamp(self):
        with self.assertRaises(ValueError):
            _ = DateTime.from_timestamp(999999999999999999)

    def test_from_timestamp_with_tz(self):
        t = DateTime.from_timestamp(0, eastern)
        self.assertEqual(t.year, 1969)
        self.assertEqual(t.month, 12)
        self.assertEqual(t.day, 31)
        self.assertEqual(t.hour, 19)
        self.assertEqual(t.minute, 0)
        self.assertEqual(t.second, 0.0)
        self.assertEqual(t.utcoffset(), timedelta(seconds=-18000))
        self.assertEqual(t.dst(), timedelta())
        self.assertEqual(t.tzname(), "EST")

    def test_conversion_to_t(self):
        dt = DateTime(2018, 4, 26, 23, 0, 17.914390409)
        t = dt.to_clock_time()
        self.assertEqual(t, ClockTime(63660380417, 914390409))

    def test_add_timedelta(self):
        dt1 = DateTime(2018, 4, 26, 23, 0, 17.914390409)
        delta = timedelta(days=1)
        dt2 = dt1 + delta
        self.assertEqual(dt2, DateTime(2018, 4, 27, 23, 0, 17.914390409))

    def test_subtract_datetime_1(self):
        dt1 = DateTime(2018, 4, 26, 23, 0, 17.914390409)
        dt2 = DateTime(2018, 1, 1, 0, 0, 0.0)
        t = dt1 - dt2
        self.assertEqual(t, Duration(months=3, days=25, hours=23, seconds=17.914390409))

    def test_subtract_datetime_2(self):
        dt1 = DateTime(2018, 4, 1, 23, 0, 17.914390409)
        dt2 = DateTime(2018, 1, 26, 0, 0, 0.0)
        t = dt1 - dt2
        self.assertEqual(t, Duration(months=3, days=-25, hours=23, seconds=17.914390409))

    def test_subtract_native_datetime_1(self):
        dt1 = DateTime(2018, 4, 26, 23, 0, 17.914390409)
        dt2 = datetime(2018, 1, 1, 0, 0, 0)
        t = dt1 - dt2
        self.assertEqual(t, timedelta(days=115, hours=23, seconds=17.914390409))

    def test_subtract_native_datetime_2(self):
        dt1 = DateTime(2018, 4, 1, 23, 0, 17.914390409)
        dt2 = datetime(2018, 1, 26, 0, 0, 0)
        t = dt1 - dt2
        self.assertEqual(t, timedelta(days=65, hours=23, seconds=17.914390409))

    def test_normalization(self):
        ndt1 = eastern.normalize(DateTime(2018, 4, 27, 23, 0, 17, tzinfo=eastern))
        ndt2 = eastern.normalize(datetime(2018, 4, 27, 23, 0, 17, tzinfo=eastern))
        self.assertEqual(ndt1, ndt2)

    def test_localization(self):
        ldt1 = eastern.localize(datetime(2018, 4, 27, 23, 0, 17))
        ldt2 = eastern.localize(DateTime(2018, 4, 27, 23, 0, 17))
        self.assertEqual(ldt1, ldt2)

    def test_from_native(self):
        native = datetime(2018, 10, 1, 12, 34, 56, 789123)
        dt = DateTime.from_native(native)
        self.assertEqual(dt.year, native.year)
        self.assertEqual(dt.month, native.month)
        self.assertEqual(dt.day, native.day)
        self.assertEqual(dt.hour, native.hour)
        self.assertEqual(dt.minute, native.minute)
        self.assertEqual(dt.second, nano_add(native.second, nano_div(native.microsecond, 1000000)))

    def test_to_native(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789123456)
        native = dt.to_native()
        self.assertEqual(dt.year, native.year)
        self.assertEqual(dt.month, native.month)
        self.assertEqual(dt.day, native.day)
        self.assertEqual(dt.hour, native.hour)
        self.assertEqual(dt.minute, native.minute)
        self.assertEqual(56.789123, nano_add(native.second, nano_div(native.microsecond, 1000000)))

    def test_iso_format(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789123456)
        self.assertEqual("2018-10-01T12:34:56.789123456", dt.iso_format())

    def test_iso_format_with_trailing_zeroes(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789)
        self.assertEqual("2018-10-01T12:34:56.789000000", dt.iso_format())

    def test_iso_format_with_tz(self):
        dt = eastern.localize(DateTime(2018, 10, 1, 12, 34, 56.789123456))
        self.assertEqual("2018-10-01T12:34:56.789123456-04:00", dt.iso_format())

    def test_iso_format_with_tz_and_trailing_zeroes(self):
        dt = eastern.localize(DateTime(2018, 10, 1, 12, 34, 56.789))
        self.assertEqual("2018-10-01T12:34:56.789000000-04:00", dt.iso_format())

    def test_from_iso_format_hour_only(self):
        expected = DateTime(2018, 10, 1, 12, 0, 0)
        actual = DateTime.from_iso_format("2018-10-01T12")
        self.assertEqual(expected, actual)

    def test_from_iso_format_hour_and_minute(self):
        expected = DateTime(2018, 10, 1, 12, 34, 0)
        actual = DateTime.from_iso_format("2018-10-01T12:34")
        self.assertEqual(expected, actual)

    def test_from_iso_format_hour_minute_second(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56")
        self.assertEqual(expected, actual)

    def test_from_iso_format_hour_minute_second_milliseconds(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123")
        self.assertEqual(expected, actual)

    def test_from_iso_format_hour_minute_second_microseconds(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456")
        self.assertEqual(expected, actual)

    def test_from_iso_format_hour_minute_second_nanoseconds(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456789)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789")
        self.assertEqual(expected, actual)

    def test_from_iso_format_with_positive_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456789, tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34")
        self.assertEqual(expected, actual)

    def test_from_iso_format_with_negative_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456789, tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34")
        self.assertEqual(expected, actual)

    def test_from_iso_format_with_positive_long_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456789, tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34:56.123456")
        self.assertEqual(expected, actual)

    def test_from_iso_format_with_negative_long_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56.123456789, tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34:56.123456")
        self.assertEqual(expected, actual)
