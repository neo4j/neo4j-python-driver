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

import copy
from decimal import Decimal
from datetime import (
    datetime,
    timedelta,
    timezone as datetime_timezone,
)
import itertools
import operator

import pytest
from pytz import (
    timezone,
    FixedOffset,
    utc,
)

from neo4j.time import (
    DateTime as _DateTime,
    MIN_YEAR,
    MAX_YEAR,
    Duration,
)
from neo4j.time.arithmetic import (
    nano_add,
    nano_div,
)
from neo4j.time.clock_implementations import ClockTime

timezone_us_eastern = timezone("US/Eastern")
timezone_london = timezone("Europe/London")
timezone_berlin = timezone("Europe/Berlin")
timezone_utc = timezone("UTC")
timezone_utc_p2 = FixedOffset(120)


class DateTime(_DateTime):
    def __new__(cls, *args, **kwargs):
        second = kwargs.get("seconds", args[5] if len(args) > 5 else None)
        if isinstance(second, float) and not second.is_integer():
            with pytest.warns(
                DeprecationWarning,
                match="Float support for `second` will be removed in 5.0. "
                      "Use `nanosecond` instead."
            ):
                return super().__new__(cls, *args, **kwargs)
        return super().__new__(cls, *args, **kwargs)


def seconds_options(seconds, nanoseconds):
    yield seconds, nanoseconds
    yield seconds + nanoseconds / 1000000000,


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
        if isinstance(seconds_args[0], float):
            t = DateTime(2018, 4, 26, 23, 0, *seconds_args)
        else:
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
        assert t.second == Decimal("56.789000001")
        assert t.nanosecond == 789000001

    def test_now_without_tz(self):
        t = DateTime.now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000001")
        assert t.nanosecond == 789000001
        assert t.tzinfo is None

    def test_now_with_tz(self):
        t = DateTime.now(timezone_us_eastern)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 7
        assert t.minute == 34
        assert t.second == Decimal("56.789000001")
        assert t.nanosecond == 789000001
        assert t.utcoffset() == timedelta(seconds=-18000)
        assert t.dst() == timedelta()
        assert t.tzname() == "EST"

    def test_now_with_utc_tz(self):
        t = DateTime.now(timezone_utc)
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000001")
        assert t.nanosecond == 789000001
        assert t.utcoffset() == timedelta(seconds=0)
        assert t.dst() == timedelta()
        assert t.tzname() == "UTC"

    def test_utc_now(self):
        t = DateTime.utc_now()
        assert t.year == 1970
        assert t.month == 1
        assert t.day == 1
        assert t.hour == 12
        assert t.minute == 34
        assert t.second == Decimal("56.789000001")
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
    def test_from_timestamp(self, tz, expected):
        t = DateTime.from_timestamp(0, tz=tz)
        assert t.year_month_day == expected[:3]
        assert t.hour_minute_second_nanosecond == expected[3:]
        assert str(t.tzinfo) == str(tz)

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
        assert t.utc_offset() == timedelta(seconds=-18000)
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

    @pytest.mark.parametrize(
        ("dt_early", "delta", "dt_late"),
        (
            (
                DateTime(2024, 3, 31, 0, 30, 0),
                Duration(nanoseconds=1),
                DateTime(2024, 3, 31, 0, 30, 0, 1),
            ),
            (
                DateTime(2024, 3, 31, 0, 30, 0),
                Duration(hours=24),
                DateTime(2024, 4, 1, 0, 30, 0),
            ),
            (
                DateTime(2024, 3, 31, 0, 30, 0),
                timedelta(microseconds=1),
                DateTime(2024, 3, 31, 0, 30, 0, 1000),
            ),
            (
                DateTime(2024, 3, 31, 0, 30, 0),
                timedelta(hours=24),
                DateTime(2024, 4, 1, 0, 30, 0),
            ),
        ),
    )
    @pytest.mark.parametrize(
        "tz",
        (None, timezone_utc, timezone_utc_p2, timezone_berlin),
    )
    def test_add_duration(self, dt_early, delta, dt_late, tz):
        if tz is not None:
            dt_early = timezone_utc.localize(dt_early).astimezone(tz)
            dt_late = timezone_utc.localize(dt_late).astimezone(tz)
        assert dt_early + delta == dt_late

    @pytest.mark.parametrize(
        ("datetime_cls", "delta_cls"),
        (
            (datetime, timedelta),  # baseline (what Python's datetime does)
            (DateTime, Duration),
            (DateTime, timedelta),
        ),
    )
    def test_transition_to_summertime(self, datetime_cls, delta_cls):
        dt = datetime_cls(2022, 3, 27, 1, 30)
        dt = timezone_berlin.localize(dt)
        assert dt.utcoffset() == timedelta(hours=1)
        assert isinstance(dt, datetime_cls)
        time = dt.time()
        assert (time.hour, time.minute) == (1, 30)

        dt += delta_cls(hours=1)

        # The native datetime object treats timedelta addition as wall time
        # addition. This is imo silly, but what Python decided to do. So want
        # our implementation to match that. See also:
        # https://stackoverflow.com/questions/76583100/is-pytz-deprecated-now-or-in-the-future-in-python
        assert dt.utcoffset() == timedelta(hours=1)
        assert isinstance(dt, datetime_cls)
        time = dt.time()
        assert (time.hour, time.minute) == (2, 30)

    @pytest.mark.parametrize(
        ("datetime_cls", "delta_cls"),
        (
            (datetime, timedelta),  # baseline (what Python's datetime does)
            (DateTime, Duration),
            (DateTime, timedelta),
        ),
    )
    def test_transition_from_summertime(self, datetime_cls, delta_cls):
        dt = datetime_cls(2022, 10, 30, 2, 30)
        dt = timezone_berlin.localize(dt, is_dst=True)
        assert dt.utcoffset() == timedelta(hours=2)
        assert isinstance(dt, datetime_cls)
        time = dt.time()
        assert (time.hour, time.minute) == (2, 30)

        dt += delta_cls(hours=1)

        # The native datetime object treats timedelta addition as wall time
        # addition. This is imo silly, but what Python decided to do. So want
        # our implementation to match that. See also:
        # https://stackoverflow.com/questions/76583100/is-pytz-deprecated-now-or-in-the-future-in-python
        assert dt.utcoffset() == timedelta(hours=2)
        assert isinstance(dt, datetime_cls)
        time = dt.time()
        assert (time.hour, time.minute) == (3, 30)

    @pytest.mark.parametrize(
        ("dt1", "dt2"),
        (
            (
                DateTime(2018, 4, 27, 23, 0, 17, 914390409),
                DateTime(2018, 4, 27, 23, 0, 17, 914390409),
            ),
            (
                utc.localize(DateTime(2018, 4, 27, 23, 0, 17, 914390409)),
                utc.localize(DateTime(2018, 4, 27, 23, 0, 17, 914390409)),
            ),
            (
                utc.localize(DateTime(2018, 4, 27, 23, 0, 17, 914390409)),
                utc.localize(
                    DateTime(2018, 4, 27, 23, 0, 17, 914390409)
                ).astimezone(timezone_berlin),
            ),
        ),
    )
    @pytest.mark.parametrize("native", (True, False))
    def test_eq( self, dt1, dt2, native):
        assert isinstance(dt1, DateTime)
        assert isinstance(dt2, DateTime)
        if native:
            dt1 = dt1.replace(nanosecond=dt1.nanosecond // 1000 * 1000)
            dt2 = dt2.to_native()
        assert dt1 == dt2
        assert dt2 == dt1
        # explicitly test that `not !=` is `==` (different code paths)
        assert not dt1 != dt2
        assert not dt2 != dt1

    @pytest.mark.parametrize(
        ("dt1", "dt2", "native"),
        (
            # nanosecond difference
            (
                DateTime(2018, 4, 27, 23, 0, 17, 914390408),
                DateTime(2018, 4, 27, 23, 0, 17, 914390409),
                False,
            ),
            *(
                (
                    dt1,
                    DateTime(2018, 4, 27, 23, 0, 17, 914390409),
                    native,
                )
                for dt1 in (
                    DateTime(2018, 4, 27, 23, 0, 17, 914391409),
                    DateTime(2018, 4, 27, 23, 0, 18, 914390409),
                    DateTime(2018, 4, 27, 23, 1, 17, 914390409),
                    DateTime(2018, 4, 27, 22, 0, 17, 914390409),
                    DateTime(2018, 4, 26, 23, 0, 17, 914390409),
                    DateTime(2018, 5, 27, 23, 0, 17, 914390409),
                    DateTime(2019, 4, 27, 23, 0, 17, 914390409),
                )
                for native in (True, False)
            ),
            *(
                (
                    # type ignore:
                    # https://github.com/python/typeshed/issues/12715
                    tz1.localize(dt1, is_dst=None),  # type: ignore[arg-type]
                    tz2.localize(
                        DateTime(2018, 4, 27, 23, 0, 17, 914390409),
                        is_dst=None,  # type: ignore[arg-type]
                    ),
                    native,
                )
                for dt1 in (
                    DateTime(2018, 4, 27, 23, 0, 17, 914391409),
                    DateTime(2018, 4, 27, 23, 0, 18, 914390409),
                    DateTime(2018, 4, 27, 23, 1, 17, 914390409),
                    DateTime(2018, 4, 27, 22, 0, 17, 914390409),
                    DateTime(2018, 4, 26, 23, 0, 17, 914390409),
                    DateTime(2018, 5, 27, 23, 0, 17, 914390409),
                    DateTime(2019, 4, 27, 23, 0, 17, 914390409),
                )
                for native in (True, False)
                for tz1, tz2 in itertools.combinations_with_replacement(
                    (timezone_utc, timezone_utc_p2, timezone_berlin), 2
                )
            ),
        ),
    )
    def test_ne(self, dt1, dt2, native):
        assert isinstance(dt1, DateTime)
        assert isinstance(dt2, DateTime)
        if native:
            dt2 = dt2.to_native()
        assert dt1 != dt2
        assert dt2 != dt1
        # explicitly test that `not ==` is `!=` (different code paths)
        assert not dt1 == dt2
        assert not dt2 == dt1

    @pytest.mark.parametrize(
        "other",
        (
            object(),
            1,
            DateTime(2018, 4, 27, 23, 0, 17, 914391409).to_clock_time(),
            (
                DateTime(2018, 4, 27, 23, 0, 17, 914391409)
                - DateTime(1970, 1, 1)
            ),
        ),
    )
    def test_ne_object(self, other):
        dt = DateTime(2018, 4, 27, 23, 0, 17, 914391409)
        assert dt != other
        assert other != dt
        # explicitly test that `not ==` is `!=` (different code paths)
        assert not dt == other
        assert not other == dt

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

    @pytest.mark.parametrize(("dt", "expected"), (
        (
            DateTime(2018, 10, 1, 12, 34, 56.789123456),
            "2018-10-01T12:34:56.789123456"
        ),
        (
            DateTime(2018, 10, 1, 12, 34, 56, 789123456),
            "2018-10-01T12:34:56.789123456"
        ),
        (
            datetime(2018, 10, 1, 12, 34, 56, 789123),
            "2018-10-01T12:34:56.789123"
        ),
        (
            DateTime(2018, 10, 1, 12, 34, 56.789),
            "2018-10-01T12:34:56.789000000"
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
                DateTime(2018, 10, 1, 12, 34, 56.789123456)
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
                DateTime(2018, 10, 1, 12, 34, 56.789)
            ),
            "2018-10-01T12:34:56.789000000-04:00"
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
    def test_iso_format(self, dt, expected):
        assert dt.isoformat() == expected

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
    expected = DateTime(2019, 10, 30, 7, 54, 2.129790999,
                                         tzinfo=timezone_utc)
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


@pytest.mark.parametrize("datetime_cls", (DateTime, datetime))
def test_transition_to_summertime(datetime_cls):
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
def test_transition_to_summertime_in_utc_space(datetime_cls, utc_impl, tz):
    if datetime_cls == DateTime:
        dt = datetime_cls(2022, 3, 27, 1, 30, 1, 123456789)
    else:
        dt = datetime_cls(2022, 3, 27, 1, 30, 1, 123456)
    dt = timezone_berlin.localize(dt)
    assert isinstance(dt, datetime_cls)
    assert dt.utcoffset() == timedelta(hours=1)
    time = dt.time()
    assert (time.hour, time.minute, int(time.second)) == (1, 30, 1)
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
def test_equality(dt1, dt2):
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
def test_inequality(dt1, dt2):
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
def test_hashed_equality(dt1, dt2):
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
def test_comparison_with_only_one_naive_fails(dt1, dt2, tz, op):
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
def test_comparison_with_one_naive_and_not_fixed_tz(dt1, dt2, tz, op):
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
def test_comparison(dt1, dt2):
    assert dt1 < dt2
    assert not dt2 < dt1
    assert dt1 <= dt2
    assert not dt2 <= dt1
    assert dt2 > dt1
    assert not dt1 > dt2
    assert dt2 >= dt1
    assert not dt1 >= dt2
