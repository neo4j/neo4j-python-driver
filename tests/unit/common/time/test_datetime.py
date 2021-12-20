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


import copy
from datetime import (
    datetime,
    timedelta,
)
from decimal import Decimal

import pytest
from pytz import (
    FixedOffset,
    timezone,
)

from neo4j.time import (
    DateTime,
    Duration,
    MAX_YEAR,
    MIN_YEAR,
)
from neo4j.time.arithmetic import (
    nano_add,
    nano_div,
)
from neo4j.time.clock_implementations import (
    Clock,
    ClockTime,
)


timezone_us_eastern = timezone("US/Eastern")
timezone_utc = timezone("UTC")


def seconds_options(seconds, nanoseconds):
    yield seconds, nanoseconds
    yield seconds + nanoseconds / 1000000000,


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


class TestDateTime:

    def test_zero(self):
        t = DateTime(0, 0, 0, 0, 0, 0)
        assert t.year == 0
        assert t.month == 0
        assert t.day == 0
        assert t.hour == 0
        assert t.minute == 0
        assert t.second == 0

    @pytest.mark.parametrize("seconds_args", [*seconds_options(17, 914390409)])
    def test_non_zero_naive(self, seconds_args):
        t = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        assert t.year == 2018
        assert t.month == 4
        assert t.day == 26
        assert t.hour == 23
        assert t.minute == 0
        assert t.second == Decimal("17.914390409")
        assert t.nanosecond == 914390409

    def test_year_lower_bound(self):
        with pytest.raises(ValueError):
            _ = DateTime(MIN_YEAR - 1, 1, 1, 0, 0, 0)

    def test_year_upper_bound(self):
        with pytest.raises(ValueError):
            _ = DateTime(MAX_YEAR + 1, 1, 1, 0, 0, 0)

    def test_month_lower_bound(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 0, 1, 0, 0, 0)

    def test_month_upper_bound(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 13, 1, 0, 0, 0)

    def test_day_zero(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 1, 0, 0, 0, 0)

    def test_day_30_of_29_day_month(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 2, 30, 0, 0, 0)

    def test_day_32_of_31_day_month(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 3, 32, 0, 0, 0)

    def test_day_31_of_30_day_month(self):
        with pytest.raises(ValueError):
            _ = DateTime(2000, 4, 31, 0, 0, 0)

    def test_day_29_of_28_day_month(self):
        with pytest.raises(ValueError):
            _ = DateTime(1999, 2, 29, 0, 0, 0)

    def test_last_day_of_month(self):
        t = DateTime(2000, 1, -1, 0, 0, 0)
        assert t.year == 2000
        assert t.month == 1
        assert t.day == 31

    def test_today(self):
        t = DateTime.today()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000000")
        assert t.nanosecond == 789000000

    def test_now_without_tz(self):
        t = DateTime.now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000000")
        assert t.nanosecond == 789000000
        assert t.tzinfo is None

    def test_now_with_tz(self):
        t = DateTime.now(timezone_us_eastern)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 7
        assert t.minute == 34
        assert t.second == Decimal("56.789000000")
        assert t.nanosecond == 789000000
        assert t.utcoffset() == timedelta(seconds=-18000)
        assert t.dst() == timedelta()
        assert t.tzname() == "EST"

    def test_utc_now(self):
        t = DateTime.utc_now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000000")
        assert t.nanosecond == 789000000
        assert t.tzinfo is None

    def test_from_timestamp(self):
        t = DateTime.from_timestamp(0)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 0
        assert t.minute == 0
        assert t.second == Decimal("0.0")
        assert t.nanosecond == 0
        assert t.tzinfo is None

    def test_from_overflowing_timestamp(self):
        with pytest.raises(ValueError):
            _ = DateTime.from_timestamp(999999999999999999)

    def test_from_timestamp_with_tz(self):
        t = DateTime.from_timestamp(0, timezone_us_eastern)
        assert t.year == 1969
        assert t.month == 12
        assert t.day == 31
        assert t.hour == 19
        assert t.minute == 0
        assert t.second == Decimal("0.0")
        assert t.nanosecond == 0
        assert t.utcoffset() == timedelta(seconds=-18000)
        assert t.dst() == timedelta()
        assert t.tzname() == "EST"

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_conversion_to_t(self, seconds_args):
        dt = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        t = dt.to_clock_time()
        assert t, ClockTime(63660380417 == 914390409)

    @pytest.mark.parametrize("seconds_args1", seconds_options(17, 914390409))
    @pytest.mark.parametrize("seconds_args2", seconds_options(17, 914390409))
    def test_add_timedelta(self, seconds_args1, seconds_args2):
        dt1 = DateTime(2018, 4, 26, 23, 0, *seconds_args1)
        delta = timedelta(days=1)
        dt2 = dt1 + delta
        assert dt2, DateTime(2018, 4, 27, 23, 0 == seconds_args2)

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_subtract_datetime_1(self, seconds_args):
        dt1 = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        dt2 = DateTime(2018, 1, 1, 0, 0, 0)
        t = dt1 - dt2

        assert t == Duration(months=3, days=25, hours=23, seconds=17.914390409)
        assert t == Duration(months=3, days=25, hours=23, seconds=17,
                             nanoseconds=914390409)

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_subtract_datetime_2(self, seconds_args):
        dt1 = DateTime(2018, 4, 1, 23, 0, *seconds_args)
        dt2 = DateTime(2018, 1, 26, 0, 0, 0.0)
        t = dt1 - dt2
        assert t == Duration(months=3, days=-25, hours=23, seconds=17.914390409)
        assert t == Duration(months=3, days=-25, hours=23, seconds=17,
                             nanoseconds=914390409)

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_subtract_native_datetime_1(self, seconds_args):
        dt1 = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        dt2 = datetime(2018, 1, 1, 0, 0, 0)
        t = dt1 - dt2
        assert t == timedelta(days=115, hours=23, seconds=17.914390409)

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_subtract_native_datetime_2(self, seconds_args):
        dt1 = DateTime(2018, 4, 1, 23, 0, *seconds_args)
        dt2 = datetime(2018, 1, 26, 0, 0, 0)
        t = dt1 - dt2
        assert t == timedelta(days=65, hours=23, seconds=17.914390409)

    def test_normalization(self):
        ndt1 = timezone_us_eastern.normalize(DateTime(2018, 4, 27, 23, 0, 17, tzinfo=timezone_us_eastern))
        ndt2 = timezone_us_eastern.normalize(datetime(2018, 4, 27, 23, 0, 17, tzinfo=timezone_us_eastern))
        assert ndt1 == ndt2

    def test_localization(self):
        ldt1 = timezone_us_eastern.localize(datetime(2018, 4, 27, 23, 0, 17))
        ldt2 = timezone_us_eastern.localize(DateTime(2018, 4, 27, 23, 0, 17))
        assert ldt1 == ldt2

    def test_from_native(self):
        native = datetime(2018, 10, 1, 12, 34, 56, 789123)
        dt = DateTime.from_native(native)
        assert dt.year == native.year
        assert dt.month == native.month
        assert dt.day == native.day
        assert dt.hour == native.hour
        assert dt.minute == native.minute
        assert dt.second == (native.second
                             + Decimal(native.microsecond) / 1000000)
        assert int(dt.second) == native.second
        assert dt.nanosecond == native.microsecond * 1000

    def test_to_native(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789123456)
        native = dt.to_native()
        assert dt.year == native.year
        assert dt.month == native.month
        assert dt.day == native.day
        assert dt.hour == native.hour
        assert dt.minute == native.minute
        assert 56.789123, nano_add(native.second, nano_div(native.microsecond == 1000000))

    def test_iso_format(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789123456)
        assert "2018-10-01T12:34:56.789123456" == dt.iso_format()

    def test_iso_format_with_trailing_zeroes(self):
        dt = DateTime(2018, 10, 1, 12, 34, 56.789)
        assert "2018-10-01T12:34:56.789000000" == dt.iso_format()

    def test_iso_format_with_tz(self):
        dt = timezone_us_eastern.localize(DateTime(2018, 10, 1, 12, 34, 56.789123456))
        assert "2018-10-01T12:34:56.789123456-04:00" == dt.iso_format()

    def test_iso_format_with_tz_and_trailing_zeroes(self):
        dt = timezone_us_eastern.localize(DateTime(2018, 10, 1, 12, 34, 56.789))
        assert "2018-10-01T12:34:56.789000000-04:00" == dt.iso_format()

    def test_from_iso_format_hour_only(self):
        expected = DateTime(2018, 10, 1, 12, 0, 0)
        actual = DateTime.from_iso_format("2018-10-01T12")
        assert expected == actual

    def test_from_iso_format_hour_and_minute(self):
        expected = DateTime(2018, 10, 1, 12, 34, 0)
        actual = DateTime.from_iso_format("2018-10-01T12:34")
        assert expected == actual

    def test_from_iso_format_hour_minute_second(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_milliseconds(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123000000)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_microseconds(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456000)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_nanosecond(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789")
        assert expected == actual

    def test_from_iso_format_with_positive_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34")
        assert expected == actual

    def test_from_iso_format_with_negative_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34")
        assert expected == actual

    def test_from_iso_format_with_positive_long_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34:56.123456")
        assert expected == actual

    def test_from_iso_format_with_negative_long_tz(self):
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34:56.123456")
        assert expected == actual

    def test_datetime_copy(self):
        d = DateTime(2010, 10, 1, 10, 0, 10)
        d2 = copy.copy(d)
        assert d is not d2
        assert d == d2

    def test_datetime_deep_copy(self):
        d = DateTime(2010, 10, 1, 10, 0, 12)
        d2 = copy.deepcopy(d)
        assert d is not d2
        assert d == d2


def test_iso_format_with_time_zone_case_1():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_iso_format_with_time_zone_case_1
    expected = DateTime(2019, 10, 30, 7, 54, 2.129790999, tzinfo=timezone_utc)
    assert expected.iso_format() == "2019-10-30T07:54:02.129790999+00:00"
    assert expected.tzinfo == FixedOffset(0)
    actual = DateTime.from_iso_format("2019-10-30T07:54:02.129790999+00:00")
    assert expected == actual


def test_iso_format_with_time_zone_case_2():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_iso_format_with_time_zone_case_2
    expected = DateTime.from_iso_format("2019-10-30T07:54:02.129790999+01:00")
    assert expected.tzinfo == FixedOffset(60)
    assert expected.iso_format() == "2019-10-30T07:54:02.129790999+01:00"


def test_to_native_case_1():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_1
    dt = DateTime.from_iso_format("2019-10-30T12:34:56.789123456")
    native = dt.to_native()
    assert native.hour == dt.hour
    assert native.minute == dt.minute
    assert nano_add(native.second, nano_div(native.microsecond, 1000000)) == 56.789123
    assert native.tzinfo is None
    assert native.isoformat() == "2019-10-30T12:34:56.789123"


def test_to_native_case_2():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_2
    dt = DateTime.from_iso_format("2019-10-30T12:34:56.789123456+00:00")
    native = dt.to_native()
    assert native.hour == dt.hour
    assert native.minute == dt.minute
    assert nano_add(native.second, nano_div(native.microsecond, 1000000)) == 56.789123
    assert native.tzinfo == FixedOffset(0)
    assert native.isoformat() == "2019-10-30T12:34:56.789123+00:00"


def test_to_native_case_3():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_3
    timestamp = "2021-04-06T00:00:00.500006+00:00"
    neo4j_datetime = DateTime.from_iso_format(timestamp)
    native_from_neo4j = neo4j_datetime.to_native()
    native_from_datetime = datetime(2021, 4, 6, 0, 0, 0, 500006,
                                    tzinfo=timezone_utc)

    assert neo4j_datetime == native_from_datetime
    assert native_from_neo4j == native_from_datetime


def test_from_native_case_1():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_from_native_case_1
    native = datetime(2018, 10, 1, 12, 34, 56, 789123)
    dt = DateTime.from_native(native)
    assert dt.year == native.year
    assert dt.month == native.month
    assert dt.day == native.day
    assert dt.hour == native.hour
    assert dt.minute == native.minute
    assert dt.second == (native.second
                         + Decimal(native.microsecond) / 1000000)
    assert int(dt.second) == native.second
    assert dt.nanosecond == native.microsecond * 1000
    assert dt.tzinfo is None


def test_from_native_case_2():
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_from_native_case_2
    native = datetime(2018, 10, 1, 12, 34, 56, 789123, FixedOffset(0))
    dt = DateTime.from_native(native)
    assert dt.year == native.year
    assert dt.month == native.month
    assert dt.day == native.day
    assert dt.hour == native.hour
    assert dt.minute == native.minute
    assert dt.second == (native.second
                         + Decimal(native.microsecond) / 1000000)
    assert int(dt.second) == native.second
    assert dt.nanosecond == native.microsecond * 1000
    assert dt.tzinfo == FixedOffset(0)
