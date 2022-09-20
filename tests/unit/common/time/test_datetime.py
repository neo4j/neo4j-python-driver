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


from __future__ import annotations

import copy
import itertools
import operator
from datetime import (
    datetime,
    timedelta,
    timezone as datetime_timezone,
)

import pytest
from pytz import (
    FixedOffset,
    timezone,
    utc,
)

from neo4j.time import (
    DateTime,
    Duration,
    MAX_YEAR,
    MIN_YEAR,
)
from neo4j.time._arithmetic import (
    nano_add,
    nano_div,
)
from neo4j.time._clock_implementations import ClockTime


timezone_us_eastern = timezone("US/Eastern")
timezone_london = timezone("Europe/London")
timezone_berlin = timezone("Europe/Berlin")
timezone_utc = timezone("UTC")


def seconds_options(seconds, nanoseconds):
    yield seconds, nanoseconds
    yield seconds + nanoseconds / 1000000000,


class TestDateTime:

    @pytest.mark.parametrize("args", (
        (0, 0, 0), (0, 0, 0, 0, 0, 0, 0)
    ))
    def test_zero(self, args) -> None:
        t = DateTime(*args)
        assert t.year == 0
        assert t.month == 0
        assert t.day == 0
        assert t.hour == 0
        assert t.minute == 0
        assert t.second == 0
        assert t.nanosecond == 0

    def test_non_zero_naive(self) -> None:
        t = DateTime(2018, 4, 26, 23, 0, 17, 914390409)
        assert t.year == 2018
        assert t.month == 4
        assert t.day == 26
        assert t.hour == 23
        assert t.minute == 0
        assert t.second == 17
        assert t.nanosecond == 914390409

    def test_year_lower_bound(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(MIN_YEAR - 1, 1, 1, 0, 0, 0)

    def test_year_upper_bound(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(MAX_YEAR + 1, 1, 1, 0, 0, 0)

    def test_month_lower_bound(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 0, 1, 0, 0, 0)

    def test_month_upper_bound(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 13, 1, 0, 0, 0)

    def test_day_zero(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 1, 0, 0, 0, 0)

    def test_day_30_of_29_day_month(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 2, 30, 0, 0, 0)

    def test_day_32_of_31_day_month(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 3, 32, 0, 0, 0)

    def test_day_31_of_30_day_month(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(2000, 4, 31, 0, 0, 0)

    def test_day_29_of_28_day_month(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime(1999, 2, 29, 0, 0, 0)

    def test_last_day_of_month(self) -> None:
        t = DateTime(2000, 1, -1, 0, 0, 0)
        assert t.year == 2000
        assert t.month == 1
        assert t.day == 31

    def test_today(self) -> None:
        t = DateTime.today()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == 56
        assert t.nanosecond == 789000001

    def test_now_without_tz(self) -> None:
        t = DateTime.now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == 56
        assert t.nanosecond == 789000001
        assert t.tzinfo is None

    def test_now_with_tz(self) -> None:
        t = DateTime.now(timezone_us_eastern)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 7
        assert t.minute == 34
        assert t.second == 56
        assert t.nanosecond == 789000001
        assert t.utcoffset() == timedelta(seconds=-18000)
        assert t.dst() == timedelta()
        assert t.tzname() == "EST"

    def test_now_with_utc_tz(self) -> None:
        t = DateTime.now(timezone_utc)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == 56
        assert t.nanosecond == 789000001
        assert t.utcoffset() == timedelta(seconds=0)
        assert t.dst() == timedelta()
        assert t.tzname() == "UTC"

    def test_utc_now(self) -> None:
        t = DateTime.utc_now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == 56
        assert t.nanosecond == 789000001
        assert t.tzinfo is None

    @pytest.mark.parametrize(("tz", "expected"), (
        (None, (1970, 1, 1, 0, 0, 0, 0)),
        (timezone_utc, (1970, 1, 1, 0, 0, 0, 0)),
        (datetime_timezone.utc, (1970, 1, 1, 0, 0, 0, 0)),
        (FixedOffset(60), (1970, 1, 1, 1, 0, 0, 0)),
        (datetime_timezone(timedelta(hours=1)), (1970, 1, 1, 1, 0, 0, 0)),
        (timezone_us_eastern, (1969, 12, 31, 19, 0, 0, 0)),
    ))
    def test_from_timestamp(self, tz, expected) -> None:
        t = DateTime.from_timestamp(0, tz=tz)
        assert t.year_month_day == expected[:3]
        assert t.hour_minute_second_nanosecond == expected[3:]
        assert str(t.tzinfo) == str(tz)

    def test_from_overflowing_timestamp(self) -> None:
        with pytest.raises(ValueError):
            _ = DateTime.from_timestamp(999999999999999999)

    def test_from_timestamp_with_tz(self) -> None:
        t = DateTime.from_timestamp(0, timezone_us_eastern)
        assert t.year == 1969
        assert t.month == 12
        assert t.day == 31
        assert t.hour == 19
        assert t.minute == 0
        assert t.second == 0
        assert t.nanosecond == 0
        assert t.utc_offset() == timedelta(seconds=-18000)
        assert t.dst() == timedelta()
        assert t.tzname() == "EST"

    @pytest.mark.parametrize("seconds_args", seconds_options(17, 914390409))
    def test_conversion_to_t(self, seconds_args) -> None:
        dt = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        t = dt.to_clock_time()
        assert t, ClockTime(63660380417 == 914390409)

    @pytest.mark.parametrize("seconds_args1", seconds_options(17, 914390409))
    @pytest.mark.parametrize("seconds_args2", seconds_options(17, 914390409))
    def test_add_timedelta(self, seconds_args1, seconds_args2) -> None:
        dt1 = DateTime(2018, 4, 26, 23, 0, *seconds_args1)
        delta = timedelta(days=1)
        dt2 = dt1 + delta
        assert dt2, DateTime(2018, 4, 27, 23, 0 == seconds_args2)

    def test_subtract_datetime_1(self) -> None:
        dt1 = DateTime(2018, 4, 26, 23, 0, 17, 914390409)
        dt2 = DateTime(2018, 1, 1, 0, 0, 0)
        t = dt1 - dt2

        assert t == Duration(months=3, days=25, hours=23, seconds=17.914390409)
        assert t == Duration(months=3, days=25, hours=23, seconds=17,
                             nanoseconds=914390409)

    def test_subtract_datetime_2(self) -> None:
        dt1 = DateTime(2018, 4, 1, 23, 0, 17, 914390409)
        dt2 = DateTime(2018, 1, 26, 0, 0, 0)
        t = dt1 - dt2
        assert t == Duration(months=3, days=-25, hours=23, seconds=17.914390409)
        assert t == Duration(months=3, days=-25, hours=23, seconds=17,
                             nanoseconds=914390409)

    def test_subtract_native_datetime_1(self) -> None:
        dt1 = DateTime(2018, 4, 26, 23, 0, 17, 914390409)
        dt2 = datetime(2018, 1, 1, 0, 0, 0)
        t = dt1 - dt2
        assert t == timedelta(days=115, hours=23, seconds=17.914390409)

    def test_subtract_native_datetime_2(self) -> None:
        dt1 = DateTime(2018, 4, 1, 23, 0, 17, 914390409)
        dt2 = datetime(2018, 1, 26, 0, 0, 0)
        t = dt1 - dt2
        assert t == timedelta(days=65, hours=23, seconds=17.914390409)

    def test_normalization(self) -> None:
        ndt1 = timezone_us_eastern.normalize(
            DateTime(2018, 4, 27, 23, 0, 17, tzinfo=timezone_us_eastern)
        )
        ndt2 = timezone_us_eastern.normalize(
            datetime(2018, 4, 27, 23, 0, 17, tzinfo=timezone_us_eastern)
        )
        assert ndt1 == ndt2

    def test_localization(self) -> None:
        ldt1 = timezone_us_eastern.localize(datetime(2018, 4, 27, 23, 0, 17))
        ldt2 = timezone_us_eastern.localize(DateTime(2018, 4, 27, 23, 0, 17))
        assert ldt1 == ldt2

    def test_from_native(self) -> None:
        native = datetime(2018, 10, 1, 12, 34, 56, 789123)
        dt = DateTime.from_native(native)
        assert dt.year == native.year
        assert dt.month == native.month
        assert dt.day == native.day
        assert dt.hour == native.hour
        assert dt.minute == native.minute
        assert dt.second == native.second
        assert dt.nanosecond == native.microsecond * 1000

    def test_to_native(self) -> None:
        dt = DateTime(2018, 10, 1, 12, 34, 56, 789123456)
        native = dt.to_native()
        assert dt.year == native.year
        assert dt.month == native.month
        assert dt.day == native.day
        assert dt.hour == native.hour
        assert dt.minute == native.minute
        assert dt.second == native.second
        assert dt.nanosecond // 1000 == native.microsecond

    @pytest.mark.parametrize(("dt", "expected"), (
        (
            DateTime(2018, 10, 1, 12, 34, 56, 789123456),
            "2018-10-01T12:34:56.789123456"
        ),
        (
            datetime(2018, 10, 1, 12, 34, 56, 789123),
            "2018-10-01T12:34:56.789123"
        ),
        (
            DateTime(2018, 10, 1, 12, 34, 56, 789000000),
            "2018-10-01T12:34:56.789000000"
        ),
        (
            datetime(2018, 10, 1, 12, 34, 56, 789000),
            "2018-10-01T12:34:56.789000"
        ),
        (
            timezone_us_eastern.localize(
                DateTime(2018, 10, 1, 12, 34, 56, 789123456)
            ),
            "2018-10-01T12:34:56.789123456-04:00"
        ),
        (
            timezone_us_eastern.localize(
                datetime(2018, 10, 1, 12, 34, 56, 789123)
            ),
            "2018-10-01T12:34:56.789123-04:00"
        ),
        (
            timezone_us_eastern.localize(
                DateTime(2018, 10, 1, 12, 34, 56, 789000000)
            ),
            "2018-10-01T12:34:56.789000000-04:00"
        ),
        (
            timezone_us_eastern.localize(
                datetime(2018, 10, 1, 12, 34, 56, 789000)
            ),
            "2018-10-01T12:34:56.789000-04:00"
        ),
        (
            utc.localize(DateTime(2018, 10, 1, 12, 34, 56, 789123456)),
            "2018-10-01T12:34:56.789123456+00:00"
        ),
        (
            utc.localize(datetime(2018, 10, 1, 12, 34, 56, 789123)),
            "2018-10-01T12:34:56.789123+00:00"
        ),
    ))
    def test_iso_format(self, dt, expected) -> None:
        assert dt.isoformat() == expected

    def test_from_iso_format_hour_only(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 0, 0)
        actual = DateTime.from_iso_format("2018-10-01T12")
        assert expected == actual

    def test_from_iso_format_hour_and_minute(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 0)
        actual = DateTime.from_iso_format("2018-10-01T12:34")
        assert expected == actual

    def test_from_iso_format_hour_minute_second(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_milliseconds(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123000000)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_microseconds(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456000)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456")
        assert expected == actual

    def test_from_iso_format_hour_minute_second_nanosecond(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789)
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789")
        assert expected == actual

    def test_from_iso_format_with_positive_tz(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34")
        assert expected == actual

    def test_from_iso_format_with_negative_tz(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34")
        assert expected == actual

    def test_from_iso_format_with_positive_long_tz(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789+12:34:56.123456")
        assert expected == actual

    def test_from_iso_format_with_negative_long_tz(self) -> None:
        expected = DateTime(2018, 10, 1, 12, 34, 56, 123456789,
                            tzinfo=FixedOffset(-754))
        actual = DateTime.from_iso_format("2018-10-01T12:34:56.123456789-12:34:56.123456")
        assert expected == actual

    def test_datetime_copy(self) -> None:
        d = DateTime(2010, 10, 1, 10, 0, 10)
        d2 = copy.copy(d)
        assert d is not d2
        assert d == d2

    def test_datetime_deep_copy(self) -> None:
        d = DateTime(2010, 10, 1, 10, 0, 12)
        d2 = copy.deepcopy(d)
        assert d is not d2
        assert d == d2


def test_iso_format_with_time_zone_case_1() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_iso_format_with_time_zone_case_1
    expected = DateTime(2019, 10, 30, 7, 54, 2, 129790999, tzinfo=timezone_utc)
    assert expected.iso_format() == "2019-10-30T07:54:02.129790999+00:00"
    assert expected.tzinfo == FixedOffset(0)
    actual = DateTime.from_iso_format("2019-10-30T07:54:02.129790999+00:00")
    assert expected == actual


def test_iso_format_with_time_zone_case_2() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_iso_format_with_time_zone_case_2
    expected = DateTime.from_iso_format("2019-10-30T07:54:02.129790999+01:00")
    assert expected.tzinfo == FixedOffset(60)
    assert expected.iso_format() == "2019-10-30T07:54:02.129790999+01:00"


def test_to_native_case_1() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_1
    dt = DateTime.from_iso_format("2019-10-30T12:34:56.789123456")
    native = dt.to_native()
    assert native.hour == dt.hour
    assert native.minute == dt.minute
    assert nano_add(native.second, nano_div(native.microsecond, 1000000)) == 56.789123
    assert native.tzinfo is None
    assert native.isoformat() == "2019-10-30T12:34:56.789123"


def test_to_native_case_2() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_2
    dt = DateTime.from_iso_format("2019-10-30T12:34:56.789123456+00:00")
    native = dt.to_native()
    assert native.hour == dt.hour
    assert native.minute == dt.minute
    assert nano_add(native.second, nano_div(native.microsecond, 1000000)) == 56.789123
    assert native.tzinfo == FixedOffset(0)
    assert native.isoformat() == "2019-10-30T12:34:56.789123+00:00"


def test_to_native_case_3() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_to_native_case_3
    timestamp = "2021-04-06T00:00:00.500006+00:00"
    neo4j_datetime = DateTime.from_iso_format(timestamp)
    native_from_neo4j = neo4j_datetime.to_native()
    native_from_datetime = datetime(2021, 4, 6, 0, 0, 0, 500006,
                                    tzinfo=timezone_utc)

    assert neo4j_datetime == native_from_datetime
    assert native_from_neo4j == native_from_datetime


def test_from_native_case_1() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_from_native_case_1
    native = datetime(2018, 10, 1, 12, 34, 56, 789123)
    dt = DateTime.from_native(native)
    assert dt.year == native.year
    assert dt.month == native.month
    assert dt.day == native.day
    assert dt.hour == native.hour
    assert dt.minute == native.minute
    assert dt.second == native.second
    assert dt.nanosecond == native.microsecond * 1000
    assert dt.tzinfo is None


def test_from_native_case_2() -> None:
    # python -m pytest tests/unit/time/test_datetime.py -s -v -k test_from_native_case_2
    native = datetime(2018, 10, 1, 12, 34, 56, 789123, FixedOffset(0))
    dt = DateTime.from_native(native)
    assert dt.year == native.year
    assert dt.month == native.month
    assert dt.day == native.day
    assert dt.hour == native.hour
    assert dt.minute == native.minute
    assert dt.second == native.second
    assert dt.nanosecond == native.microsecond * 1000
    assert dt.tzinfo == FixedOffset(0)


@pytest.mark.parametrize("datetime_cls", (DateTime, datetime))
def test_transition_to_summertime(datetime_cls) -> None:
    dt = datetime_cls(2022, 3, 27, 1, 30)
    dt = timezone_berlin.localize(dt)
    assert dt.utcoffset() == timedelta(hours=1)
    assert isinstance(dt, datetime_cls)
    time = dt.time()
    assert (time.hour, time.minute) == (1, 30)

    dt += timedelta(hours=1)

    # The native datetime object just bluntly carries over the timezone. You'd
    # have to manually convert to UTC, do the calculation, and then convert
    # back. Not pretty, but we should make sure our implementation does
    assert dt.utcoffset() == timedelta(hours=1)
    assert isinstance(dt, datetime_cls)
    time = dt.time()
    assert (time.hour, time.minute) == (2, 30)


@pytest.mark.parametrize("datetime_cls", (DateTime, datetime))
@pytest.mark.parametrize("utc_impl", (
    utc,
    datetime_timezone(timedelta(0)),
))
@pytest.mark.parametrize("tz", (
    timezone_berlin, datetime_timezone(timedelta(hours=-1))
))
def test_transition_to_summertime_in_utc_space(datetime_cls, utc_impl, tz) -> None:
    if datetime_cls == DateTime:
        dt = datetime_cls(2022, 3, 27, 1, 30, 1, 123456789)
    else:
        dt = datetime_cls(2022, 3, 27, 1, 30, 1, 123456)
    dt = timezone_berlin.localize(dt)
    assert isinstance(dt, datetime_cls)
    assert dt.utcoffset() == timedelta(hours=1)
    time = dt.time()
    assert (time.hour, time.minute, time.second) == (1, 30, 1)
    if datetime_cls == DateTime:
        assert time.nanosecond == 123456789
    else:
        assert time.microsecond == 123456

    dt = dt.astimezone(utc_impl)
    assert isinstance(dt, datetime_cls)
    assert dt.utcoffset() == timedelta(0)
    time = dt.time()
    assert (time.hour, time.minute) == (0, 30)

    dt += timedelta(hours=1)
    assert isinstance(dt, datetime_cls)
    assert dt.utcoffset() == timedelta(0)
    time = dt.time()
    assert (time.hour, time.minute) == (1, 30)

    dt = dt.astimezone(timezone_berlin)
    assert isinstance(dt, datetime_cls)
    assert dt.utcoffset() == timedelta(hours=2)
    time = dt.time()
    assert (time.hour, time.minute) == (3, 30)
    if datetime_cls == DateTime:
        assert time.nanosecond == 123456789
    else:
        assert time.microsecond == 123456


@pytest.mark.parametrize(("dt1", "dt2"), (
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000)
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456)
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(1))
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(-1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(-1))
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(1))
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(-1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(-1))
    ),
    (
        DateTime(2022, 11, 25, 12, 35, 56, 789123456, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(0))
    ),
    (
        # Not testing our library directly, but asserting that Python's
        # datetime implementation is aligned with ours.
        datetime(2022, 11, 25, 12, 35, 56, 789123, FixedOffset(1)),
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(0))
    ),
    (
        datetime(2022, 11, 25, 12, 35, 56, 789123, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(0))
    ),
    (
        DateTime(2022, 11, 25, 12, 35, 56, 789123123, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123123, FixedOffset(0))
    ),
    (
        timezone_london.localize(datetime(2022, 11, 25, 12, 34, 56, 789123)),
        timezone_berlin.localize(datetime(2022, 11, 25, 13, 34, 56, 789123))
    ),
    (
        timezone_london.localize(datetime(2022, 11, 25, 12, 34, 56, 789123)),
        timezone_berlin.localize(DateTime(2022, 11, 25, 13, 34, 56, 789123000))
    ),
    (
        timezone_london.localize(DateTime(2022, 1, 25, 12, 34, 56, 789123123)),
        timezone_berlin.localize(DateTime(2022, 1, 25, 13, 34, 56, 789123123))
    ),

))
def test_equality(dt1, dt2) -> None:
    assert dt1 == dt2
    assert dt2 == dt1
    assert dt1 <= dt2
    assert dt2 <= dt1
    assert dt1 >= dt2
    assert dt2 >= dt1


@pytest.mark.parametrize(("dt1", "dt2"), (
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123001)
     ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789124000)
     ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 57, 789123000)
     ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 35, 56, 789123000)
     ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 13, 34, 56, 789123000)
     ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 12, 34, 56, 789123450)
     ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 12, 34, 57, 789123456)
     ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 12, 35, 56, 789123456)
     ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 13, 34, 56, 789123456)
     ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(2)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(1))
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(-2)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(-1))
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(0))
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(2)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(1))
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(-2)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(-1))
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123456),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(0))
    ),
    (
        DateTime(2022, 11, 25, 13, 34, 56, 789123456, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(0))
    ),
    (
        DateTime(2022, 11, 25, 11, 34, 56, 789123456, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(0))
    ),
))
def test_inequality(dt1, dt2) -> None:
    assert dt1 != dt2
    assert dt2 != dt1


@pytest.mark.parametrize(
    ("dt1", "dt2"),
    itertools.product(
        (
            datetime(2022, 11, 25, 12, 34, 56, 789123),
            DateTime(2022, 11, 25, 12, 34, 56, 789123000),
            datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(0)),
            DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(0)),
            datetime(2022, 11, 25, 12, 35, 56, 789123, FixedOffset(1)),
            DateTime(2022, 11, 25, 12, 35, 56, 789123456, FixedOffset(1)),
            datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(-1)),
            DateTime(2022, 11, 25, 12, 34, 56, 789123456, FixedOffset(-1)),
            datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(60 * -16)),
            DateTime(2022, 11, 25, 12, 34, 56, 789123000,
                     FixedOffset(60 * -16)),
            datetime(2022, 11, 25, 11, 34, 56, 789123, FixedOffset(60 * -17)),
            DateTime(2022, 11, 25, 11, 34, 56, 789123000,
                     FixedOffset(60 * -17)),
            DateTime(2022, 11, 25, 12, 34, 56, 789123456,
                     FixedOffset(60 * -16)),
            DateTime(2022, 11, 25, 11, 34, 56, 789123456,
                     FixedOffset(60 * -17)),
        ),
        repeat=2
    )
)
def test_hashed_equality(dt1, dt2) -> None:
    if dt1 == dt2:
        s = {dt1}
        assert dt1 in s
        assert dt2 in s
        s = {dt2}
        assert dt1 in s
        assert dt2 in s
    else:
        s = {dt1}
        assert dt1 in s
        assert dt2 not in s
        s = {dt2}
        assert dt1 not in s
        assert dt2 in s


@pytest.mark.parametrize(("dt1", "dt2"), (
    itertools.product(
        (
            datetime(2022, 11, 25, 12, 34, 56, 789123),
            DateTime(2022, 11, 25, 12, 34, 56, 789123000),
            DateTime(2022, 11, 25, 12, 34, 56, 789123001),
        ),
        repeat=2
    )
))
@pytest.mark.parametrize("tz", (
    FixedOffset(0), FixedOffset(1), FixedOffset(-1), utc,
))
@pytest.mark.parametrize("op", (
    operator.lt, operator.le, operator.gt, operator.ge,
))
def test_comparison_with_only_one_naive_fails(dt1, dt2, tz, op) -> None:
    dt1 = dt1.replace(tzinfo=tz)
    with pytest.raises(TypeError, match="naive"):
        op(dt1, dt2)


@pytest.mark.parametrize(
    ("dt1", "dt2"),
    itertools.product(
        (
            datetime(2022, 11, 25, 12, 34, 56, 789123),
            DateTime(2022, 11, 25, 12, 34, 56, 789123000),
            DateTime(2022, 11, 25, 12, 34, 56, 789123001),
        ),
        repeat=2
    )
)
@pytest.mark.parametrize("tz", (
    timezone("Europe/Paris"), timezone("Europe/Berlin"),
))
@pytest.mark.parametrize("op", (
    operator.lt, operator.le, operator.gt, operator.ge,
))
def test_comparison_with_one_naive_and_not_fixed_tz(dt1, dt2, tz, op) -> None:
    dt1tz = tz.localize(dt1)
    with pytest.raises(TypeError, match="naive"):
        op(dt1tz, dt2)


@pytest.mark.parametrize(("dt1", "dt2"), (
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        datetime(2022, 11, 25, 12, 34, 56, 789124)
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123000),
        datetime(2022, 11, 25, 12, 34, 56, 789124)
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789124000)
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123000),
        DateTime(2022, 11, 25, 12, 34, 56, 789124000)
    ),
    (
        datetime(2022, 11, 24, 12, 34, 56, 789123),
        datetime(2022, 11, 25, 12, 34, 56, 789123)
    ),
    (
        datetime(2022, 11, 24, 12, 34, 56, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000)
    ),
    (
        DateTime(2022, 11, 24, 12, 34, 56, 789123123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123123)
    ),
    (
        datetime(2022, 11, 24, 12, 34, 57, 789123),
        datetime(2022, 11, 25, 12, 34, 56, 789123)
    ),
    (
        datetime(2022, 11, 24, 12, 34, 57, 789123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123000)
    ),
    (
        DateTime(2022, 11, 24, 12, 34, 57, 789123123),
        DateTime(2022, 11, 25, 12, 34, 56, 789123123)
    ),
    (
        datetime(2022, 11, 25, 12, 34, 56, 789123, FixedOffset(1)),
        datetime(2022, 11, 25, 12, 34, 56, 789124, FixedOffset(1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(1)),
        datetime(2022, 11, 25, 12, 34, 56, 789124, FixedOffset(1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789124000, FixedOffset(1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 34, 56, 789123000, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123001, FixedOffset(1)),
    ),

    (
        datetime(2022, 11, 25, 12, 36, 56, 789123, FixedOffset(1)),
        datetime(2022, 11, 25, 12, 34, 56, 789124, FixedOffset(-1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 36, 56, 789123000, FixedOffset(1)),
        datetime(2022, 11, 25, 12, 34, 56, 789124, FixedOffset(-1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 36, 56, 789123000, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789124000, FixedOffset(-1)),
    ),
    (
        DateTime(2022, 11, 25, 12, 36, 56, 789123000, FixedOffset(1)),
        DateTime(2022, 11, 25, 12, 34, 56, 789123001, FixedOffset(-1)),
    ),
))
def test_comparison(dt1, dt2) -> None:
    assert dt1 < dt2
    assert not dt2 < dt1
    assert dt1 <= dt2
    assert not dt2 <= dt1
    assert dt2 > dt1
    assert not dt1 > dt2
    assert dt2 >= dt1
    assert not dt1 >= dt2
