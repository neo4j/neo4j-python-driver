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


from datetime import timedelta
from unittest import TestCase

from neo4j.time import Duration


class DurationTestCase(TestCase):

    def test_zero(self):
        d = Duration()
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.0))
        self.assertFalse(bool(d))

    def test_years_only(self):
        d = Duration(years=2)
        self.assertEqual(d.months, 24)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (2, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.0))

    def test_months_only(self):
        d = Duration(months=20)
        self.assertEqual(d.months, 20)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (1, 8, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.0))

    def test_months_out_of_range(self):
        with self.assertRaises(ValueError):
            _ = Duration(months=(2**64))

    def test_weeks_only(self):
        d = Duration(weeks=4)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 28)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (0, 0, 28))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.0))

    def test_days_only(self):
        d = Duration(days=40)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 40)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (0, 0, 40))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.0))

    def test_days_out_of_range(self):
        with self.assertRaises(ValueError):
            _ = Duration(days=(2**64))

    def test_hours_only(self):
        d = Duration(hours=10)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 36000)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (10, 0, 0.0))

    def test_minutes_only(self):
        d = Duration(minutes=90.5)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 5430)
        self.assertEqual(d.subseconds, 0.0)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (1, 30, 30.0))

    def test_seconds_only(self):
        d = Duration(seconds=123.456)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 123)
        self.assertEqual(d.subseconds, 0.456)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 2, 3.456))

    def test_seconds_out_of_range(self):
        with self.assertRaises(ValueError):
            _ = Duration(seconds=(2**64))

    def test_subseconds_only(self):
        d = Duration(subseconds=123.456)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 123)
        self.assertEqual(d.subseconds, 0.456)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 2, 3.456))

    def test_milliseconds_only(self):
        d = Duration(milliseconds=1234.567)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 1)
        self.assertEqual(d.subseconds, 0.234567)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 1.234567))

    def test_microseconds_only(self):
        d = Duration(microseconds=1234.567)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.001234567)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.001234567))

    def test_nanoseconds_only(self):
        d = Duration(nanoseconds=1234.567)
        self.assertEqual(d.months, 0)
        self.assertEqual(d.days, 0)
        self.assertEqual(d.seconds, 0)
        self.assertEqual(d.subseconds, 0.000001234)
        self.assertEqual(d.years_months_days, (0, 0, 0))
        self.assertEqual(d.hours_minutes_seconds, (0, 0, 0.000001234))

    def test_can_combine_years_months(self):
        t = Duration(years=5, months=3)
        self.assertEqual(t.months, 63)

    def test_can_combine_weeks_and_days(self):
        t = Duration(weeks=5, days=3)
        self.assertEqual(t.days, 38)

    def test_can_combine_hours_minutes_seconds(self):
        t = Duration(hours=5, minutes=4, seconds=3)
        self.assertEqual(t.seconds, 18243)

    def test_can_combine_seconds_and_subseconds(self):
        t = Duration(seconds=123.456, subseconds=0.321)
        self.assertEqual(t.seconds, 123)
        self.assertEqual(t.subseconds, 0.777)

    def test_full_positive(self):
        d = Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.789)
        self.assertEqual(d.months, 14)
        self.assertEqual(d.days, 3)
        self.assertEqual(d.seconds, 14706)
        self.assertEqual(d.subseconds, 0.789)
        self.assertEqual(d.years_months_days, (1, 2, 3))
        self.assertEqual(d.hours_minutes_seconds, (4, 5, 6.789))
        self.assertTrue(bool(d))

    def test_full_negative(self):
        d = Duration(years=-1, months=-2, days=-3, hours=-4, minutes=-5, seconds=-6.789)
        self.assertEqual(d.months, -14)
        self.assertEqual(d.days, -3)
        self.assertEqual(d.seconds, -14706)
        self.assertEqual(d.subseconds, -0.789)
        self.assertEqual(d.years_months_days, (-1, -2, -3))
        self.assertEqual(d.hours_minutes_seconds, (-4, -5, -6.789))
        self.assertTrue(bool(d))

    def test_negative_positive_negative(self):
        d = Duration(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6.789)
        self.assertEqual(d.months, -14)
        self.assertEqual(d.days, 3)
        self.assertEqual(d.seconds, -14706)
        self.assertEqual(d.subseconds, -0.789)
        self.assertEqual(d.years_months_days, (-1, -2, 3))
        self.assertEqual(d.hours_minutes_seconds, (-4, -5, -6.789))

    def test_positive_negative_positive(self):
        d = Duration(years=1, months=2, days=-3, hours=4, minutes=5, seconds=6.789)
        self.assertEqual(d.months, 14)
        self.assertEqual(d.days, -3)
        self.assertEqual(d.seconds, 14706)
        self.assertEqual(d.subseconds, 0.789)
        self.assertEqual(d.years_months_days, (1, 2, -3))
        self.assertEqual(d.hours_minutes_seconds, (4, 5, 6.789))

    def test_add_duration(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        d2 = Duration(months=7, days=5, seconds=3.2)
        self.assertEqual(d1 + d2, Duration(months=9, days=8, seconds=8.9))

    def test_add_timedelta(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        td = timedelta(days=5, seconds=3.2)
        self.assertEqual(d1 + td, Duration(months=2, days=8, seconds=8.9))

    def test_add_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) + object()

    def test_subtract_duration(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        d2 = Duration(months=7, days=5, seconds=3.2)
        self.assertEqual(d1 - d2, Duration(months=-5, days=-2, seconds=2.5))

    def test_subtract_timedelta(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        td = timedelta(days=5, seconds=3.2)
        self.assertEqual(d1 - td, Duration(months=2, days=-2, seconds=2.5))

    def test_subtract_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) - object()

    def test_multiplication_by_int(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        i = 11
        self.assertEqual(d1 * i, Duration(months=22, days=33, seconds=62.7))

    def test_multiplication_by_float(self):
        d1 = Duration(months=2, days=3, seconds=5.7)
        f = 5.5
        self.assertEqual(d1 * f, Duration(months=11, days=16, seconds=31.35))

    def test_multiplication_by_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) * object()

    def test_floor_division_by_int(self):
        d1 = Duration(months=11, days=33, seconds=55.77)
        i = 2
        self.assertEqual(d1 // i, Duration(months=5, days=16, seconds=27.0))

    def test_floor_division_by_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) // object()

    def test_modulus_by_int(self):
        d1 = Duration(months=11, days=33, seconds=55.77)
        i = 2
        self.assertEqual(d1 % i, Duration(months=1, days=1, seconds=1.77))

    def test_modulus_by_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) % object()

    def test_floor_division_and_modulus_by_int(self):
        d1 = Duration(months=11, days=33, seconds=55.77)
        i = 2
        self.assertEqual(divmod(d1, i), (Duration(months=5, days=16, seconds=27.0),
                                         Duration(months=1, days=1, seconds=1.77)))

    def test_floor_division_and_modulus_by_object(self):
        with self.assertRaises(TypeError):
            _ = divmod(Duration(months=2, days=3, seconds=5.7), object())

    def test_true_division_by_int(self):
        d1 = Duration(months=11, days=33, seconds=55.77)
        i = 2
        self.assertEqual(d1 / i, Duration(months=6, days=16, seconds=27.885))

    def test_true_division_by_float(self):
        d1 = Duration(months=11, days=33, seconds=55.77)
        f = 2.5
        self.assertEqual(d1 / f, Duration(months=4, days=13, seconds=22.308))

    def test_true_division_by_object(self):
        with self.assertRaises(TypeError):
            _ = Duration(months=2, days=3, seconds=5.7) / object()

    def test_unary_plus(self):
        d = Duration(months=11, days=33, seconds=55.77)
        self.assertEqual(+d, Duration(months=11, days=33, seconds=55.77))

    def test_unary_minus(self):
        d = Duration(months=11, days=33, seconds=55.77)
        self.assertEqual(-d, Duration(months=-11, days=-33, seconds=-55.77))

    def test_absolute(self):
        d = Duration(months=-11, days=-33, seconds=-55.77)
        self.assertEqual(abs(d), Duration(months=11, days=33, seconds=55.77))

    def test_str(self):
        self.assertEqual(str(Duration()), "PT0S")
        self.assertEqual(str(Duration(years=1, months=2)), "P1Y2M")
        self.assertEqual(str(Duration(years=-1, months=2)), "P-10M")
        self.assertEqual(str(Duration(months=-13)), "P-1Y-1M")
        self.assertEqual(str(Duration(months=2, days=3, seconds=5.7)), "P2M3DT5.7S")
        self.assertEqual(str(Duration(hours=12, minutes=34)), "PT12H34M")
        self.assertEqual(str(Duration(seconds=59)), "PT59S")
        self.assertEqual(str(Duration(seconds=0.123456789)), "PT0.123456789S")
        self.assertEqual(str(Duration(seconds=-0.123456789)), "PT-0.123456789S")

    def test_repr(self):
        d = Duration(months=2, days=3, seconds=5.7)
        self.assertEqual(repr(d), "Duration(months=2, days=3, seconds=5, subseconds=0.7)")

    def test_iso_format(self):
        self.assertEqual(Duration().iso_format(), "PT0S")
        self.assertEqual(Duration(years=1, months=2).iso_format(), "P1Y2M")
        self.assertEqual(Duration(years=-1, months=2).iso_format(), "P-10M")
        self.assertEqual(Duration(months=-13).iso_format(), "P-1Y-1M")
        self.assertEqual(Duration(months=2, days=3, seconds=5.7).iso_format(), "P2M3DT5.7S")
        self.assertEqual(Duration(hours=12, minutes=34).iso_format(), "PT12H34M")
        self.assertEqual(Duration(seconds=59).iso_format(), "PT59S")
        self.assertEqual(Duration(seconds=0.123456789).iso_format(), "PT0.123456789S")
        self.assertEqual(Duration(seconds=-0.123456789).iso_format(), "PT-0.123456789S")

    def test_from_iso_format(self):
        self.assertEqual(Duration(), Duration.from_iso_format("PT0S"))
        self.assertEqual(Duration(hours=12, minutes=34, seconds=56.789),
                         Duration.from_iso_format("PT12H34M56.789S"))
        self.assertEqual(Duration(years=1, months=2, days=3),
                         Duration.from_iso_format("P1Y2M3D"))
        self.assertEqual(Duration(years=1, months=2, days=3, hours=12, minutes=34, seconds=56.789),
                         Duration.from_iso_format("P1Y2M3DT12H34M56.789S"))
