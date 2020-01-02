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


from datetime import date
from time import struct_time
from unittest import TestCase

import pytz

from neo4j.time import Duration, Date, UnixEpoch, ZeroDate


eastern = pytz.timezone("US/Eastern")


class DateTestCase(TestCase):

    def test_bad_attribute(self):
        d = Date(2000, 1, 1)
        with self.assertRaises(AttributeError):
            _ = d.x

    def test_zero_date(self):
        d = Date(0, 0, 0)
        self.assertEqual(d.year_month_day, (0, 0, 0))
        self.assertEqual(d.year, 0)
        self.assertEqual(d.month, 0)
        self.assertEqual(d.day, 0)
        self.assertIs(d, ZeroDate)

    def test_zero_ordinal(self):
        d = Date.from_ordinal(0)
        self.assertEqual(d.year_month_day, (0, 0, 0))
        self.assertEqual(d.year, 0)
        self.assertEqual(d.month, 0)
        self.assertEqual(d.day, 0)
        self.assertIs(d, ZeroDate)

    def test_ordinal_at_start_of_1970(self):
        d = Date.from_ordinal(719163)
        self.assertEqual(d.year_month_day, (1970, 1, 1))
        self.assertEqual(d.year, 1970)
        self.assertEqual(d.month, 1)
        self.assertEqual(d.day, 1)

    def test_ordinal_at_end_of_1969(self):
        d = Date.from_ordinal(719162)
        self.assertEqual(d.year_month_day, (1969, 12, 31))
        self.assertEqual(d.year, 1969)
        self.assertEqual(d.month, 12)
        self.assertEqual(d.day, 31)

    def test_ordinal_at_start_of_2018(self):
        d = Date.from_ordinal(736695)
        self.assertEqual(d.year_month_day, (2018, 1, 1))
        self.assertEqual(d.year, 2018)
        self.assertEqual(d.month, 1)
        self.assertEqual(d.day, 1)

    def test_ordinal_at_end_of_2017(self):
        d = Date.from_ordinal(736694)
        self.assertEqual(d.year_month_day, (2017, 12, 31))
        self.assertEqual(d.year, 2017)
        self.assertEqual(d.month, 12)
        self.assertEqual(d.day, 31)

    def test_all_positive_days_of_month_for_31_day_month(self):
        for day in range(1, 32):
            t = Date(1976, 1, day)
            self.assertEqual(t.year_month_day, (1976, 1, day))
            self.assertEqual(t.year, 1976)
            self.assertEqual(t.month, 1)
            self.assertEqual(t.day, day)
        with self.assertRaises(ValueError):
            _ = Date(1976, 1, 32)

    def test_all_positive_days_of_month_for_30_day_month(self):
        for day in range(1, 31):
            t = Date(1976, 6, day)
            self.assertEqual(t.year_month_day, (1976, 6, day))
            self.assertEqual(t.year, 1976)
            self.assertEqual(t.month, 6)
            self.assertEqual(t.day, day)
        with self.assertRaises(ValueError):
            _ = Date(1976, 6, 31)

    def test_all_positive_days_of_month_for_29_day_month(self):
        for day in range(1, 30):
            t = Date(1976, 2, day)
            self.assertEqual(t.year_month_day, (1976, 2, day))
            self.assertEqual(t.year, 1976)
            self.assertEqual(t.month, 2)
            self.assertEqual(t.day, day)
        with self.assertRaises(ValueError):
            _ = Date(1976, 2, 30)

    def test_all_positive_days_of_month_for_28_day_month(self):
        for day in range(1, 29):
            t = Date(1977, 2, day)
            self.assertEqual(t.year_month_day, (1977, 2, day))
            self.assertEqual(t.year, 1977)
            self.assertEqual(t.month, 2)
            self.assertEqual(t.day, day)
        with self.assertRaises(ValueError):
            _ = Date(1977, 2, 29)

    def test_last_but_2_day_for_31_day_month(self):
        t = Date(1976, 1, -3)
        self.assertEqual(t.year_month_day, (1976, 1, 29))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 29)

    def test_last_but_1_day_for_31_day_month(self):
        t = Date(1976, 1, -2)
        self.assertEqual(t.year_month_day, (1976, 1, 30))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 30)

    def test_last_day_for_31_day_month(self):
        t = Date(1976, 1, -1)
        self.assertEqual(t.year_month_day, (1976, 1, 31))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 1)
        self.assertEqual(t.day, 31)

    def test_last_but_1_day_for_30_day_month(self):
        t = Date(1976, 6, -2)
        self.assertEqual(t.year_month_day, (1976, 6, 29))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 6)
        self.assertEqual(t.day, 29)

    def test_last_day_for_30_day_month(self):
        t = Date(1976, 6, -1)
        self.assertEqual(t.year_month_day, (1976, 6, 30))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 6)
        self.assertEqual(t.day, 30)

    def test_day_28_for_29_day_month(self):
        t = Date(1976, 2, 28)
        self.assertEqual(t.year_month_day, (1976, 2, 28))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 2)
        self.assertEqual(t.day, 28)

    def test_last_day_for_29_day_month(self):
        t = Date(1976, 2, -1)
        self.assertEqual(t.year_month_day, (1976, 2, 29))
        self.assertEqual(t.year, 1976)
        self.assertEqual(t.month, 2)
        self.assertEqual(t.day, 29)

    def test_last_day_for_28_day_month(self):
        t = Date(1977, 2, -1)
        self.assertEqual(t.year_month_day, (1977, 2, 28))
        self.assertEqual(t.year, 1977)
        self.assertEqual(t.month, 2)
        self.assertEqual(t.day, 28)

    def test_cannot_use_year_lower_than_one(self):
        with self.assertRaises(ValueError):
            _ = Date(0, 2, 1)

    def test_cannot_use_year_higher_than_9999(self):
        with self.assertRaises(ValueError):
            _ = Date(10000, 2, 1)

    def test_today(self):
        d = Date.today()
        self.assertIsInstance(d, Date)

    def test_today_with_tz(self):
        d = Date.today(tz=eastern)
        self.assertIsInstance(d, Date)

    def test_utc_today(self):
        d = Date.utc_today()
        self.assertIsInstance(d, Date)

    def test_from_timestamp_without_tz(self):
        d = Date.from_timestamp(0)
        self.assertEqual(d, Date(1970, 1, 1))

    def test_from_timestamp_with_tz(self):
        d = Date.from_timestamp(0, tz=eastern)
        self.assertEqual(d, Date(1969, 12, 31))

    def test_utc_from_timestamp(self):
        d = Date.utc_from_timestamp(0)
        self.assertEqual(d, Date(1970, 1, 1))

    def test_from_ordinal(self):
        d = Date.from_ordinal(1)
        self.assertEqual(d, Date(1, 1, 1))

    def test_parse(self):
        d = Date.parse("2018-04-30")
        self.assertEqual(d, Date(2018, 4, 30))

    def test_bad_parse_1(self):
        with self.assertRaises(ValueError):
            _ = Date.parse("30 April 2018")

    def test_bad_parse_2(self):
        with self.assertRaises(ValueError):
            _ = Date.parse("2018-04")

    def test_bad_parse_3(self):
        with self.assertRaises(ValueError):
            _ = Date.parse(object())

    def test_replace(self):
        d1 = Date(2018, 4, 30)
        d2 = d1.replace(year=2017)
        self.assertEqual(d2, Date(2017, 4, 30))

    def test_from_clock_time(self):
        d = Date.from_clock_time((0, 0), epoch=UnixEpoch)
        self.assertEqual(d, Date(1970, 1, 1))

    def test_bad_from_clock_time(self):
        with self.assertRaises(ValueError):
            _ = Date.from_clock_time(object(), None)

    def test_is_leap_year(self):
        self.assertTrue(Date.is_leap_year(2000))
        self.assertFalse(Date.is_leap_year(2001))

    def test_days_in_year(self):
        self.assertEqual(Date.days_in_year(2000), 366)
        self.assertEqual(Date.days_in_year(2001), 365)

    def test_days_in_month(self):
        self.assertEqual(Date.days_in_month(2000, 1), 31)
        self.assertEqual(Date.days_in_month(2000, 2), 29)
        self.assertEqual(Date.days_in_month(2001, 2), 28)

    def test_instance_attributes(self):
        d = Date(2018, 4, 30)
        self.assertEqual(d.year, 2018)
        self.assertEqual(d.month, 4)
        self.assertEqual(d.day, 30)
        self.assertEqual(d.year_month_day, (2018, 4, 30))
        self.assertEqual(d.year_week_day, (2018, 18, 1))
        self.assertEqual(d.year_day, (2018, 120))

    def test_can_add_years(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=2)
        self.assertEqual(d2, Date(1978, 6, 13))

    def test_can_add_negative_years(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=-2)
        self.assertEqual(d2, Date(1974, 6, 13))

    def test_can_add_years_and_months(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=2, months=3)
        self.assertEqual(d2, Date(1978, 9, 13))

    def test_can_add_negative_years_and_months(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=-2, months=-3)
        self.assertEqual(d2, Date(1974, 3, 13))

    def test_can_retain_offset_from_end_of_month(self):
        d = Date(1976, 1, -1)
        self.assertEqual(d, Date(1976, 1, 31))
        d += Duration(months=1)
        self.assertEqual(d, Date(1976, 2, 29))
        d += Duration(months=1)
        self.assertEqual(d, Date(1976, 3, 31))
        d += Duration(months=1)
        self.assertEqual(d, Date(1976, 4, 30))
        d += Duration(months=1)
        self.assertEqual(d, Date(1976, 5, 31))
        d += Duration(months=1)
        self.assertEqual(d, Date(1976, 6, 30))

    def test_can_roll_over_end_of_year(self):
        d = Date(1976, 12, 1)
        self.assertEqual(d, Date(1976, 12, 1))
        d += Duration(months=1)
        self.assertEqual(d, Date(1977, 1, 1))

    def test_can_add_months_and_days(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=1, days=1)
        self.assertEqual(d2, Date(1976, 7, 14))

    def test_can_add_months_then_days(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        self.assertEqual(d2, Date(1976, 7, 14))

    def test_cannot_add_seconds(self):
        d1 = Date(1976, 6, 13)
        with self.assertRaises(ValueError):
            _ = d1 + Duration(seconds=1)

    def test_adding_empty_duration_returns_self(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration()
        self.assertIs(d1, d2)

    def test_adding_object(self):
        d1 = Date(1976, 6, 13)
        with self.assertRaises(TypeError):
            _ = d1 + object()

    def test_can_add_days_then_months(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        self.assertEqual(d2, Date(1976, 7, 14))

    def test_can_add_months_and_days_for_last_day_of_short_month(self):
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(months=1, days=1)
        self.assertEqual(d2, Date(1976, 8, 1))

    def test_can_add_months_then_days_for_last_day_of_short_month(self):
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        self.assertEqual(d2, Date(1976, 8, 1))

    def test_can_add_days_then_months_for_last_day_of_short_month(self):
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        self.assertEqual(d2, Date(1976, 8, 1))

    def test_can_add_months_and_days_for_last_day_of_long_month(self):
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=1, days=1)
        self.assertEqual(d2, Date(1976, 3, 1))

    def test_can_add_months_then_days_for_last_day_of_long_month(self):
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        self.assertEqual(d2, Date(1976, 3, 1))

    def test_can_add_days_then_months_for_last_day_of_long_month(self):
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        self.assertEqual(d2, Date(1976, 3, 1))

    def test_can_add_negative_months_and_days(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=-1, days=-1)
        self.assertEqual(d2, Date(1976, 5, 12))

    def test_can_add_negative_months_then_days(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=-1) + Duration(days=-1)
        self.assertEqual(d2, Date(1976, 5, 12))

    def test_can_add_negative_days_then_months(self):
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(days=-1) + Duration(months=-1)
        self.assertEqual(d2, Date(1976, 5, 12))

    def test_can_add_negative_months_and_days_for_first_day_of_month(self):
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(months=-1, days=-1)
        self.assertEqual(d2, Date(1976, 4, 30))

    def test_can_add_negative_months_then_days_for_first_day_of_month(self):
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(months=-1) + Duration(days=-1)
        self.assertEqual(d2, Date(1976, 4, 30))

    def test_can_add_negative_days_then_months_for_last_day_of_month(self):
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(days=-1) + Duration(months=-1)
        self.assertEqual(d2, Date(1976, 4, 30))

    def test_can_add_negative_month_for_last_day_of_long_month(self):
        d1 = Date(1976, 5, 31)
        d2 = d1 + Duration(months=-1)
        self.assertEqual(d2, Date(1976, 4, 30))

    def test_can_add_negative_month_for_january(self):
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=-1)
        self.assertEqual(d2, Date(1975, 12, 31))

    def test_subtract_date(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertEqual(new_year - christmas, Duration(days=7))

    def test_subtract_duration(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertEqual(new_year - Duration(days=7), christmas)

    def test_subtract_object(self):
        new_year = Date(2000, 1, 1)
        with self.assertRaises(TypeError):
            _ = new_year - object()

    def test_date_less_than(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertLess(christmas, new_year)

    def test_date_less_than_object(self):
        d = Date(2000, 1, 1)
        with self.assertRaises(TypeError):
            _ = d < object()

    def test_date_less_than_or_equal_to(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertLessEqual(christmas, new_year)

    def test_date_less_than_or_equal_to_object(self):
        d = Date(2000, 1, 1)
        with self.assertRaises(TypeError):
            _ = d <= object()

    def test_date_greater_than_or_equal_to(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertGreaterEqual(new_year, christmas)

    def test_date_greater_than_or_equal_to_object(self):
        d = Date(2000, 1, 1)
        with self.assertRaises(TypeError):
            _ = d >= object()

    def test_date_greater_than(self):
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        self.assertGreater(new_year, christmas)

    def test_date_greater_than_object(self):
        d = Date(2000, 1, 1)
        with self.assertRaises(TypeError):
            _ = d > object()

    def test_date_equal(self):
        d1 = Date(2000, 1, 1)
        d2 = Date(2000, 1, 1)
        self.assertEqual(d1, d2)

    def test_date_not_equal(self):
        d1 = Date(2000, 1, 1)
        d2 = Date(2000, 1, 2)
        self.assertNotEqual(d1, d2)

    def test_date_not_equal_to_object(self):
        d1 = Date(2000, 1, 1)
        self.assertNotEqual(d1, object())

    def test_year_week_day(self):
        for ordinal in range(Date(2001, 1, 1).to_ordinal(), Date(2008, 1, 1).to_ordinal()):
            self.assertEqual(Date.from_ordinal(ordinal).iso_calendar(), date.fromordinal(ordinal).isocalendar())

    def test_time_tuple(self):
        d = Date(2018, 4, 30)
        self.assertEqual(d.time_tuple(), struct_time((2018, 4, 30, 0, 0, 0, 0, 120, -1)))

    def test_to_clock_time(self):
        d = Date(2018, 4, 30)
        self.assertEqual(d.to_clock_time(UnixEpoch), (1525046400, 0))
        self.assertEqual(d.to_clock_time(d), (0, 0))
        with self.assertRaises(TypeError):
            _ = d.to_clock_time(object())

    def test_weekday(self):
        d = Date(2018, 4, 30)
        self.assertEqual(d.weekday(), 0)

    def test_iso_weekday(self):
        d = Date(2018, 4, 30)
        self.assertEqual(d.iso_weekday(), 1)

    def test_str(self):
        self.assertEqual(str(Date(2018, 4, 30)), "2018-04-30")
        self.assertEqual(str(Date(0, 0, 0)), "0000-00-00")

    def test_repr(self):
        self.assertEqual(repr(Date(2018, 4, 30)), "neo4j.time.Date(2018, 4, 30)")
        self.assertEqual(repr(Date(0, 0, 0)), "neo4j.time.ZeroDate")

    def test_format(self):
        d = Date(2018, 4, 30)
        with self.assertRaises(NotImplementedError):
            _ = d.__format__("")

    def test_from_native(self):
        native = date(2018, 10, 1)
        d = Date.from_native(native)
        self.assertEqual(d.year, native.year)
        self.assertEqual(d.month, native.month)
        self.assertEqual(d.day, native.day)

    def test_to_native(self):
        d = Date(2018, 10, 1)
        native = d.to_native()
        self.assertEqual(d.year, native.year)
        self.assertEqual(d.month, native.month)
        self.assertEqual(d.day, native.day)

    def test_iso_format(self):
        d = Date(2018, 10, 1)
        self.assertEqual("2018-10-01", d.iso_format())

    def test_from_iso_format(self):
        expected = Date(2018, 10, 1)
        actual = Date.from_iso_format("2018-10-01")
        self.assertEqual(expected, actual)
