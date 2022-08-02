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
import datetime
from datetime import date
from time import struct_time

import pytest
import pytz

from neo4j.time import (
    Date,
    Duration,
    UnixEpoch,
    ZeroDate,
)


timezone_eastern = pytz.timezone("US/Eastern")
timezone_utc = pytz.utc


class TestDate:

    def test_bad_attribute(self) -> None:
        d = Date(2000, 1, 1)
        with pytest.raises(AttributeError):
            _ = d.x

    def test_zero_date(self) -> None:
        d = Date(0, 0, 0)
        assert d.year_month_day == (0, 0, 0)
        assert d.year == 0
        assert d.month == 0
        assert d.day == 0
        assert d is ZeroDate

    def test_zero_ordinal(self) -> None:
        d = Date.from_ordinal(0)
        assert d.year_month_day == (0, 0, 0)
        assert d.year == 0
        assert d.month == 0
        assert d.day == 0
        assert d is ZeroDate

    def test_ordinal_at_start_of_1970(self) -> None:
        d = Date.from_ordinal(719163)
        assert d.year_month_day == (1970, 1, 1)
        assert d.year == 1970
        assert d.month == 1
        assert d.day == 1

    def test_ordinal_at_end_of_1969(self) -> None:
        d = Date.from_ordinal(719162)
        assert d.year_month_day == (1969, 12, 31)
        assert d.year == 1969
        assert d.month == 12
        assert d.day == 31

    def test_ordinal_at_start_of_2018(self) -> None:
        d = Date.from_ordinal(736695)
        assert d.year_month_day == (2018, 1, 1)
        assert d.year == 2018
        assert d.month == 1
        assert d.day == 1

    def test_ordinal_at_end_of_2017(self) -> None:
        d = Date.from_ordinal(736694)
        assert d.year_month_day == (2017, 12, 31)
        assert d.year == 2017
        assert d.month == 12
        assert d.day == 31

    def test_all_positive_days_of_month_for_31_day_month(self) -> None:
        for day in range(1, 32):
            t = Date(1976, 1, day)
            assert t.year_month_day == (1976, 1, day)
            assert t.year == 1976
            assert t.month == 1
            assert t.day == day
        with pytest.raises(ValueError):
            _ = Date(1976, 1, 32)

    def test_all_positive_days_of_month_for_30_day_month(self) -> None:
        for day in range(1, 31):
            t = Date(1976, 6, day)
            assert t.year_month_day == (1976, 6, day)
            assert t.year == 1976
            assert t.month == 6
            assert t.day == day
        with pytest.raises(ValueError):
            _ = Date(1976, 6, 31)

    def test_all_positive_days_of_month_for_29_day_month(self) -> None:
        for day in range(1, 30):
            t = Date(1976, 2, day)
            assert t.year_month_day == (1976, 2, day)
            assert t.year == 1976
            assert t.month == 2
            assert t.day == day
        with pytest.raises(ValueError):
            _ = Date(1976, 2, 30)

    def test_all_positive_days_of_month_for_28_day_month(self) -> None:
        for day in range(1, 29):
            t = Date(1977, 2, day)
            assert t.year_month_day == (1977, 2, day)
            assert t.year == 1977
            assert t.month == 2
            assert t.day == day
        with pytest.raises(ValueError):
            _ = Date(1977, 2, 29)

    def test_last_but_2_day_for_31_day_month(self) -> None:
        t = Date(1976, 1, -3)
        assert t.year_month_day == (1976, 1, 29)
        assert t.year == 1976
        assert t.month == 1
        assert t.day == 29

    def test_last_but_1_day_for_31_day_month(self) -> None:
        t = Date(1976, 1, -2)
        assert t.year_month_day == (1976, 1, 30)
        assert t.year == 1976
        assert t.month == 1
        assert t.day == 30

    def test_last_day_for_31_day_month(self) -> None:
        t = Date(1976, 1, -1)
        assert t.year_month_day == (1976, 1, 31)
        assert t.year == 1976
        assert t.month == 1
        assert t.day == 31

    def test_last_but_1_day_for_30_day_month(self) -> None:
        t = Date(1976, 6, -2)
        assert t.year_month_day == (1976, 6, 29)
        assert t.year == 1976
        assert t.month == 6
        assert t.day == 29

    def test_last_day_for_30_day_month(self) -> None:
        t = Date(1976, 6, -1)
        assert t.year_month_day == (1976, 6, 30)
        assert t.year == 1976
        assert t.month == 6
        assert t.day == 30

    def test_day_28_for_29_day_month(self) -> None:
        t = Date(1976, 2, 28)
        assert t.year_month_day == (1976, 2, 28)
        assert t.year == 1976
        assert t.month == 2
        assert t.day == 28

    def test_last_day_for_29_day_month(self) -> None:
        t = Date(1976, 2, -1)
        assert t.year_month_day == (1976, 2, 29)
        assert t.year == 1976
        assert t.month == 2
        assert t.day == 29

    def test_last_day_for_28_day_month(self) -> None:
        t = Date(1977, 2, -1)
        assert t.year_month_day == (1977, 2, 28)
        assert t.year == 1977
        assert t.month == 2
        assert t.day == 28

    def test_cannot_use_year_lower_than_one(self) -> None:
        with pytest.raises(ValueError):
            _ = Date(0, 2, 1)

    def test_cannot_use_year_higher_than_9999(self) -> None:
        with pytest.raises(ValueError):
            _ = Date(10000, 2, 1)

    def test_from_timestamp_without_tz(self) -> None:
        d = Date.from_timestamp(0)
        assert d == Date(1970, 1, 1)

    def test_from_timestamp_with_tz(self) -> None:
        d = Date.from_timestamp(0, tz=timezone_eastern)
        assert d == Date(1969, 12, 31)

    def test_utc_from_timestamp(self) -> None:
        d = Date.utc_from_timestamp(0)
        assert d == Date(1970, 1, 1)

    def test_from_ordinal(self) -> None:
        d = Date.from_ordinal(1)
        assert d == Date(1, 1, 1)

    def test_parse(self) -> None:
        d = Date.parse("2018-04-30")
        assert d == Date(2018, 4, 30)

    def test_bad_parse_1(self) -> None:
        with pytest.raises(ValueError):
            _ = Date.parse("30 April 2018")

    def test_bad_parse_2(self) -> None:
        with pytest.raises(ValueError):
            _ = Date.parse("2018-04")

    def test_bad_parse_3(self) -> None:
        with pytest.raises(ValueError):
            _ = Date.parse(object())  # type: ignore[arg-type]

    def test_replace(self) -> None:
        d1 = Date(2018, 4, 30)
        d2 = d1.replace(year=2017)
        assert d2 == Date(2017, 4, 30)

    def test_from_clock_time(self) -> None:
        d = Date.from_clock_time((0, 0), epoch=UnixEpoch)
        assert d == Date(1970, 1, 1)

    def test_bad_from_clock_time(self) -> None:
        with pytest.raises(ValueError):
            _ = Date.from_clock_time(object(), None)  # type: ignore[arg-type]
    def test_is_leap_year(self) -> None:
        assert Date.is_leap_year(2000)
        assert not Date.is_leap_year(2001)

    def test_days_in_year(self) -> None:
        assert Date.days_in_year(2000) == 366
        assert Date.days_in_year(2001) == 365

    def test_days_in_month(self) -> None:
        assert Date.days_in_month(2000, 1) == 31
        assert Date.days_in_month(2000, 2) == 29
        assert Date.days_in_month(2001, 2) == 28

    def test_instance_attributes(self) -> None:
        d = Date(2018, 4, 30)
        assert d.year == 2018
        assert d.month == 4
        assert d.day == 30
        assert d.year_month_day == (2018, 4, 30)
        assert d.year_week_day == (2018, 18, 1)
        assert d.year_day == (2018, 120)

    def test_can_add_years(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=2)
        assert d2 == Date(1978, 6, 13)

    def test_can_add_negative_years(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=-2)
        assert d2 == Date(1974, 6, 13)

    def test_can_add_years_and_months(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=2, months=3)
        assert d2 == Date(1978, 9, 13)

    def test_can_add_negative_years_and_months(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(years=-2, months=-3)
        assert d2 == Date(1974, 3, 13)

    def test_can_retain_offset_from_end_of_month(self) -> None:
        d = Date(1976, 1, -1)
        assert d == Date(1976, 1, 31)
        d += Duration(months=1)
        assert d == Date(1976, 2, 29)
        d += Duration(months=1)
        assert d == Date(1976, 3, 31)
        d += Duration(months=1)
        assert d == Date(1976, 4, 30)
        d += Duration(months=1)
        assert d == Date(1976, 5, 31)
        d += Duration(months=1)
        assert d == Date(1976, 6, 30)

    def test_can_roll_over_end_of_year(self) -> None:
        d = Date(1976, 12, 1)
        assert d == Date(1976, 12, 1)
        d += Duration(months=1)
        assert d == Date(1977, 1, 1)

    def test_can_add_months_and_days(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=1, days=1)
        assert d2 == Date(1976, 7, 14)

    def test_can_add_months_then_days(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        assert d2 == Date(1976, 7, 14)

    def test_cannot_add_seconds(self) -> None:
        d1 = Date(1976, 6, 13)
        with pytest.raises(ValueError):
            _ = d1 + Duration(seconds=1)

    def test_adding_empty_duration_returns_self(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration()
        assert d1 is d2

    def test_adding_object(self) -> None:
        d1 = Date(1976, 6, 13)
        with pytest.raises(TypeError):
            _ = d1 + object()  # type: ignore[operator]

    def test_can_add_days_then_months(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        assert d2 == Date(1976, 7, 14)

    def test_can_add_months_and_days_for_last_day_of_short_month(self) -> None:
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(months=1, days=1)
        assert d2 == Date(1976, 8, 1)

    def test_can_add_months_then_days_for_last_day_of_short_month(
        self
    ) -> None:
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        assert d2 == Date(1976, 8, 1)

    def test_can_add_days_then_months_for_last_day_of_short_month(
        self
    ) -> None:
        d1 = Date(1976, 6, 30)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        assert d2 == Date(1976, 8, 1)

    def test_can_add_months_and_days_for_last_day_of_long_month(self) -> None:
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=1, days=1)
        assert d2 == Date(1976, 3, 1)

    def test_can_add_months_then_days_for_last_day_of_long_month(self) -> None:
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=1) + Duration(days=1)
        assert d2 == Date(1976, 3, 1)

    def test_can_add_days_then_months_for_last_day_of_long_month(self) -> None:
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(days=1) + Duration(months=1)
        assert d2 == Date(1976, 3, 1)

    def test_can_add_negative_months_and_days(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=-1, days=-1)
        assert d2 == Date(1976, 5, 12)

    def test_can_add_negative_months_then_days(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(months=-1) + Duration(days=-1)
        assert d2 == Date(1976, 5, 12)

    def test_can_add_negative_days_then_months(self) -> None:
        d1 = Date(1976, 6, 13)
        d2 = d1 + Duration(days=-1) + Duration(months=-1)
        assert d2 == Date(1976, 5, 12)

    def test_can_add_negative_months_and_days_for_first_day_of_month(
        self
    ) -> None:
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(months=-1, days=-1)
        assert d2 == Date(1976, 4, 30)

    def test_can_add_negative_months_then_days_for_first_day_of_month(
        self
    ) -> None:
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(months=-1) + Duration(days=-1)
        assert d2 == Date(1976, 4, 30)

    def test_can_add_negative_days_then_months_for_last_day_of_month(
        self
    ) -> None:
        d1 = Date(1976, 6, 1)
        d2 = d1 + Duration(days=-1) + Duration(months=-1)
        assert d2 == Date(1976, 4, 30)

    def test_can_add_negative_month_for_last_day_of_long_month(self) -> None:
        d1 = Date(1976, 5, 31)
        d2 = d1 + Duration(months=-1)
        assert d2 == Date(1976, 4, 30)

    def test_can_add_negative_month_for_january(self) -> None:
        d1 = Date(1976, 1, 31)
        d2 = d1 + Duration(months=-1)
        assert d2 == Date(1975, 12, 31)

    def test_subtract_date(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert new_year - christmas == Duration(days=7)

    def test_subtract_duration(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert new_year - Duration(days=7) == christmas

    def test_subtract_object(self) -> None:
        new_year = Date(2000, 1, 1)
        with pytest.raises(TypeError):
            _ = new_year - object()  # type: ignore[operator]

    def test_date_less_than(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert christmas < new_year

    def test_date_less_than_object(self) -> None:
        d = Date(2000, 1, 1)
        with pytest.raises(TypeError):
            _ = d < object()  # type: ignore[operator]

    def test_date_less_than_or_equal_to(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert christmas <= new_year

    def test_date_less_than_or_equal_to_object(self) -> None:
        d = Date(2000, 1, 1)
        with pytest.raises(TypeError):
            _ = d <= object()  # type: ignore[operator]

    def test_date_greater_than_or_equal_to(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert new_year >= christmas

    def test_date_greater_than_or_equal_to_object(self) -> None:
        d = Date(2000, 1, 1)
        with pytest.raises(TypeError):
            _ = d >= object()  # type: ignore[operator]

    def test_date_greater_than(self) -> None:
        new_year = Date(2000, 1, 1)
        christmas = Date(1999, 12, 25)
        assert new_year > christmas

    def test_date_greater_than_object(self) -> None:
        d = Date(2000, 1, 1)
        with pytest.raises(TypeError):
            _ = d > object()  # type: ignore[operator]

    def test_date_equal(self) -> None:
        d1 = Date(2000, 1, 1)
        d2 = Date(2000, 1, 1)
        assert d1 == d2

    def test_date_not_equal(self) -> None:
        d1 = Date(2000, 1, 1)
        d2 = Date(2000, 1, 2)
        assert d1 != d2

    def test_date_not_equal_to_object(self) -> None:
        d1 = Date(2000, 1, 1)
        assert d1 != object()

    @pytest.mark.parametrize("ordinal", (
        Date(2001, 1, 1).to_ordinal(),
        Date(2008, 1, 1).to_ordinal(),
    ))
    def test_year_week_day(self, ordinal) -> None:
        assert Date.from_ordinal(ordinal).iso_calendar() \
               == date.fromordinal(ordinal).isocalendar()

    def test_time_tuple(self) -> None:
        d = Date(2018, 4, 30)
        expected = struct_time((2018, 4, 30, 0, 0, 0, 0, 120, -1))
        assert d.time_tuple() == expected

    def test_to_clock_time(self) -> None:
        d = Date(2018, 4, 30)
        assert d.to_clock_time(UnixEpoch) == (1525046400, 0)
        assert d.to_clock_time(d) == (0, 0)
        with pytest.raises(TypeError):
            _ = d.to_clock_time(object())  # type: ignore[arg-type]

    def test_weekday(self) -> None:
        d = Date(2018, 4, 30)
        assert d.weekday() == 0

    def test_iso_weekday(self) -> None:
        d = Date(2018, 4, 30)
        assert d.iso_weekday() == 1

    def test_str(self) -> None:
        assert str(Date(2018, 4, 30)) == "2018-04-30"
        assert str(Date(0, 0, 0)) == "0000-00-00"

    def test_repr(self) -> None:
        assert repr(Date(2018, 4, 30)) == "neo4j.time.Date(2018, 4, 30)"
        assert repr(Date(0, 0, 0)) == "neo4j.time.ZeroDate"

    def test_format(self) -> None:
        d = Date(2018, 4, 30)
        with pytest.raises(NotImplementedError):
            _ = d.__format__("")

    def test_from_native(self) -> None:
        native = date(2018, 10, 1)
        d = Date.from_native(native)
        assert d.year == native.year
        assert d.month == native.month
        assert d.day == native.day

    def test_to_native(self) -> None:
        d = Date(2018, 10, 1)
        native = d.to_native()
        assert d.year == native.year
        assert d.month == native.month
        assert d.day == native.day

    def test_iso_format(self) -> None:
        d = Date(2018, 10, 1)
        assert "2018-10-01" == d.iso_format()

    def test_from_iso_format(self) -> None:
        expected = Date(2018, 10, 1)
        actual = Date.from_iso_format("2018-10-01")
        assert expected == actual

    def test_date_copy(self) -> None:
        d = Date(2010, 10, 1)
        d2 = copy.copy(d)
        assert d is not d2
        assert d == d2

    def test_date_deep_copy(self) -> None:
        d = Date(2010, 10, 1)
        d2 = copy.deepcopy(d)
        assert d is not d2
        assert d == d2


@pytest.mark.parametrize(("tz", "expected"), (
    (None, (1970, 1, 1)),
    (timezone_eastern, (1970, 1, 1)),
    (timezone_utc, (1970, 1, 1)),
    (pytz.FixedOffset(-12 * 60), (1970, 1, 1)),
    (datetime.timezone(datetime.timedelta(hours=-12)), (1970, 1, 1)),
    (pytz.FixedOffset(-13 * 60), (1969, 12, 31)),
    (datetime.timezone(datetime.timedelta(hours=-13)), (1969, 12, 31)),
    (pytz.FixedOffset(11 * 60), (1970, 1, 1)),
    (datetime.timezone(datetime.timedelta(hours=11)), (1970, 1, 1)),
    (pytz.FixedOffset(12 * 60), (1970, 1, 2)),
    (datetime.timezone(datetime.timedelta(hours=12)), (1970, 1, 2)),

))
def test_today(tz, expected) -> None:
    d = Date.today(tz=tz)
    assert isinstance(d, Date)
    assert d.year_month_day == expected
