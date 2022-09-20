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
from datetime import timedelta

import pytest

from neo4j import time
from neo4j.time import Duration


class TestDuration:

    def test_zero(self) -> None:
        d = Duration()
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 0
        assert d.nanoseconds == 0
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 0)
        assert not bool(d)

    def test_years_only(self) -> None:
        d = Duration(years=2)
        assert d.months == 24
        assert d.days == 0
        assert d.seconds == 0
        assert d.nanoseconds == 0
        assert d.years_months_days == (2, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 0)
        assert bool(d)

    def test_months_only(self) -> None:
        d = Duration(months=20)
        assert d.months == 20
        assert d.days == 0
        assert d.seconds == 0
        assert d.nanoseconds == 0
        assert d.years_months_days == (1, 8, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 0)
        assert bool(d)

    def test_months_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            _ = Duration(months=(2**64))

    def test_weeks_only(self) -> None:
        d = Duration(weeks=4)
        assert d.months == 0
        assert d.days == 28
        assert d.seconds == 0
        assert d.nanoseconds == 0
        assert d.years_months_days == (0, 0, 28)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 0)
        assert bool(d)

    def test_days_only(self) -> None:
        d = Duration(days=40)
        assert d.months == 0
        assert d.days == 40
        assert d.seconds == 0
        assert d.nanoseconds == 0
        assert d.years_months_days == (0, 0, 40)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 0)
        assert bool(d)

    def test_days_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            _ = Duration(days=(2**64))

    def test_hours_only(self) -> None:
        d = Duration(hours=10)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 36000
        assert d.nanoseconds == 0
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (10, 0, 0, 0)
        assert bool(d)

    def test_minutes_only(self) -> None:
        d = Duration(minutes=90.5)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 5430
        assert d.nanoseconds == 0
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (1, 30, 30, 0)
        assert bool(d)

    def test_seconds_only(self) -> None:
        d = Duration(seconds=123, nanoseconds=456000000)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 123
        assert d.nanoseconds == 456000000
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 2, 3, 456000000)
        assert bool(d)

    def test_seconds_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            _ = Duration(seconds=(2**64))

    def test_milliseconds_only(self) -> None:
        d = Duration(milliseconds=1234.567)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 1
        assert d.nanoseconds == 234567000
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 1, 234567000)
        assert bool(d)

    def test_microseconds_only(self) -> None:
        d = Duration(microseconds=1234.567)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 0
        assert d.nanoseconds == 1234567
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 1234567)
        assert bool(d)

    def test_nanoseconds_only(self) -> None:
        d = Duration(nanoseconds=1234.567)
        assert d.months == 0
        assert d.days == 0
        assert d.seconds == 0
        assert d.nanoseconds == 1234
        assert d.years_months_days == (0, 0, 0)
        assert d.hours_minutes_seconds_nanoseconds == (0, 0, 0, 1234)
        assert bool(d)

    def test_can_combine_years_months(self) -> None:
        t = Duration(years=5, months=3)
        assert t.months == 63

    def test_can_combine_weeks_and_days(self) -> None:
        t = Duration(weeks=5, days=3)
        assert t.days == 38

    def test_can_combine_hours_minutes_seconds(self) -> None:
        t = Duration(hours=5, minutes=4, seconds=3)
        assert t.seconds == 18243

    def test_can_combine_seconds_and_nanoseconds(self) -> None:
        t = Duration(seconds=123.456, nanoseconds=321000000)
        assert t.seconds == 123
        assert t.nanoseconds == 777000000
        assert t == Duration(seconds=123, nanoseconds=777000000)
        assert t == Duration(seconds=123.777)

    def test_full_positive(self) -> None:
        d = Duration(years=1, months=2, days=3, hours=4, minutes=5,
                     seconds=6.789)
        assert d.months == 14
        assert d.days == 3
        assert d.seconds == 14706
        assert d.nanoseconds == 789000000
        assert d.years_months_days == (1, 2, 3)
        assert d.hours_minutes_seconds_nanoseconds == (4, 5, 6, 789000000)
        assert bool(d)

    def test_full_negative(self) -> None:
        d = Duration(years=-1, months=-2, days=-3, hours=-4, minutes=-5,
                     seconds=-6.789)
        assert d.months == -14
        assert d.days == -3
        assert d.seconds == -14706
        assert d.nanoseconds == -789000000
        assert d.years_months_days == (-1, -2, -3)
        assert d.hours_minutes_seconds_nanoseconds == (-4, -5, -6, -789000000)
        assert bool(d)

    def test_negative_positive(self) -> None:
        d = Duration(years=-1, months=-2, days=3, hours=-4, minutes=-5,
                     seconds=-6.789)
        assert d.months == -14
        assert d.days == 3
        assert d.seconds == -14706
        assert d.nanoseconds == -789000000
        assert d.years_months_days == (-1, -2, 3)
        assert d.hours_minutes_seconds_nanoseconds == (-4, -5, -6, -789000000)

    def test_positive_negative(self) -> None:
        d = Duration(years=1, months=2, days=-3, hours=4, minutes=5,
                     seconds=6.789)
        assert d.months == 14
        assert d.days == -3
        assert d.seconds == 14706
        assert d.nanoseconds == 789000000
        assert d.years_months_days == (1, 2, -3)
        assert d.hours_minutes_seconds_nanoseconds == (4, 5, 6, 789000000)

    def test_add_duration(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5, nanoseconds=700000000)
        d2 = Duration(months=7, days=5, seconds=3, nanoseconds=200000000)
        d3 = Duration(months=9, days=8, seconds=8, nanoseconds=900000000)
        assert d1 + d2 == d3

    def test_add_timedelta(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5, nanoseconds=700000000)
        td = timedelta(days=5, seconds=3.2)
        d3 = Duration(months=2, days=8, seconds=8, nanoseconds=900000000)
        assert d1 + td == d3

    def test_add_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 + object())  # type: ignore[operator]

    def test_subtract_duration(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5, nanoseconds=700000000)
        d2 = Duration(months=7, days=5, seconds=3, nanoseconds=200000000)
        d3 = Duration(months=-5, days=-2, seconds=2, nanoseconds=500000000)
        assert d1 - d2 == d3

    def test_subtract_timedelta(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5, nanoseconds=700000000)
        td = timedelta(days=5, seconds=3.2)
        d3 = Duration(months=2, days=-2, seconds=2, nanoseconds=500000000)
        assert d1 - td == d3

    def test_subtract_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 - object())  # type: ignore[operator]

    def test_multiplication_by_int(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5.7)
        i = 11
        assert d1 * i == Duration(months=22, days=33, seconds=62.7)

    def test_multiplication_by_float(self) -> None:
        d1 = Duration(months=2, days=3, seconds=5.7)
        f = 5.5
        assert d1 * f == Duration(months=11, days=16, seconds=31.35)

    def test_multiplication_by_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 * object())  # type: ignore[operator]

    @pytest.mark.parametrize("ns", (0, 1))
    def test_floor_division_by_int(self, ns) -> None:
        d1 = Duration(months=11, days=33, seconds=55.77, nanoseconds=ns)
        i = 2
        assert d1 // i == Duration(months=5, days=16, seconds=27,
                                   nanoseconds=885000000)

    def test_floor_division_by_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 // object())  # type: ignore[operator]

    @pytest.mark.parametrize("ns", (0, 1))
    def test_modulus_by_int(self, ns) -> None:
        d1 = Duration(months=11, days=33, seconds=55.77, nanoseconds=ns)
        i = 2
        assert d1 % i == Duration(months=1, days=1, nanoseconds=ns)

    def test_modulus_by_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 % object())  # type: ignore[operator]

    @pytest.mark.parametrize("ns", (0, 1))
    def test_floor_division_and_modulus_by_int(self, ns) -> None:
        d1 = Duration(months=11, days=33, seconds=55.77, nanoseconds=ns)
        i = 2
        assert divmod(d1, i) == (Duration(months=5, days=16, seconds=27,
                                          nanoseconds=885000000),
                                 Duration(months=1, days=1, nanoseconds=ns))

    def test_floor_division_and_modulus_by_object(self) -> None:
        with pytest.raises(TypeError):
            _ = divmod(Duration(months=2, days=3, seconds=5.7),
                       object())  # type: ignore[operator]

    @pytest.mark.parametrize(
        ("year", "month", "day"),
        (
            *((i, 0, 0) for i in range(4)),
            *((0, i, 0) for i in range(4)),
            *((0, 0, i) for i in range(4)),
        )
    )
    @pytest.mark.parametrize("second", range(4))
    @pytest.mark.parametrize("ns", range(4))
    @pytest.mark.parametrize("divisor", (*range(1, 3), 1_000_000_000))
    def test_div_mod_is_well_defined(self, year, month, day, second, ns,
                                     divisor) -> None:
        d1 = Duration(years=year, months=month, days=day, seconds=second,
                      nanoseconds=ns)
        fraction, rest = divmod(d1, divisor)
        assert d1 == fraction * divisor + rest

    def test_true_division_by_int(self) -> None:
        d1 = Duration(months=11, days=33, seconds=55.77)
        i = 2
        assert d1 / i == Duration(months=6, days=16, seconds=27.885)

    def test_true_division_by_float(self) -> None:
        d1 = Duration(months=11, days=33, seconds=55.77)
        f = 2.5
        assert d1 / f == Duration(months=4, days=13, seconds=22.308)

    def test_true_division_by_object(self) -> None:
        with pytest.raises(TypeError):
            _ = (Duration(months=2, days=3, seconds=5.7)
                 / object())  # type: ignore[operator]

    def test_unary_plus(self) -> None:
        d = Duration(months=11, days=33, seconds=55.77)
        assert +d == Duration(months=11, days=33, seconds=55.77)

    def test_unary_minus(self) -> None:
        d = Duration(months=11, days=33, seconds=55.77)
        assert -d == Duration(months=-11, days=-33, seconds=-55.77)

    def test_absolute(self) -> None:
        d = Duration(months=-11, days=-33, seconds=-55.77)
        assert abs(d) == Duration(months=11, days=33, seconds=55.77)

    def test_str(self) -> None:
        assert str(Duration()) == "PT0S"
        assert str(Duration(years=1, months=2)) == "P1Y2M"
        assert str(Duration(years=-1, months=2)) == "P-10M"
        assert str(Duration(months=-13)) == "P-1Y-1M"
        assert str(Duration(months=2, days=3, seconds=5.7)) == "P2M3DT5.7S"
        assert str(Duration(hours=12, minutes=34)) == "PT12H34M"
        assert str(Duration(seconds=59)) == "PT59S"
        assert str(Duration(seconds=0.123456789)) == "PT0.123456789S"
        assert str(Duration(seconds=-0.123456789)) == "PT-0.123456789S"
        assert str(Duration(seconds=-2, nanoseconds=1)) == "PT-1.999999999S"

    def test_repr(self) -> None:
        d = Duration(months=2, days=3, seconds=5.7)
        assert repr(d) == (
            "Duration(months=2, days=3, seconds=5, nanoseconds=700000000)"
        )

    def test_iso_format(self) -> None:
        assert Duration().iso_format() == "PT0S"
        assert Duration(years=1, months=2).iso_format() == "P1Y2M"
        assert Duration(years=-1, months=2).iso_format() == "P-10M"
        assert Duration(months=-13).iso_format() == "P-1Y-1M"
        assert (Duration(months=2, days=3, seconds=5.7).iso_format()
                == "P2M3DT5.7S")
        assert Duration(hours=12, minutes=34).iso_format() == "PT12H34M"
        assert Duration(seconds=59).iso_format() == "PT59S"
        assert Duration(seconds=0.123456789).iso_format() == "PT0.123456789S"
        assert Duration(seconds=-0.123456789).iso_format() == "PT-0.123456789S"

    def test_copy(self) -> None:
        d = Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6,
                     milliseconds=7, microseconds=8, nanoseconds=9)
        d2 = copy.copy(d)
        assert d is not d2
        assert d == d2

    def test_deep_copy(self) -> None:
        d = Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6,
                     milliseconds=7, microseconds=8, nanoseconds=9)
        d2 = copy.deepcopy(d)
        assert d is not d2
        assert d == d2

    def test_from_iso_format(self) -> None:
        assert Duration() == Duration.from_iso_format("PT0S")
        assert Duration(
            hours=12, minutes=34, seconds=56.789
        ) == Duration.from_iso_format("PT12H34M56.789S")
        assert Duration(
            years=1, months=2, days=3
        ) == Duration.from_iso_format("P1Y2M3D")
        assert Duration(
            years=1, months=2, days=3, hours=12, minutes=34, seconds=56.789
        ) == Duration.from_iso_format("P1Y2M3DT12H34M56.789S")
        # test for float precision issues
        for i in range(500006000, 500010000, 1000):
            assert Duration(
                years=1, months=2, days=3, hours=12, minutes=34, nanoseconds=i
            ) == Duration.from_iso_format("P1Y2M3DT12H34M00.%sS" % str(i))
            assert Duration(
                years=1, months=2, days=3, hours=12, minutes=34, nanoseconds=i
            ) == Duration.from_iso_format("P1Y2M3DT12H34M00.%sS" % str(i)[:-3])

    @pytest.mark.parametrize("with_day", (True, False))
    @pytest.mark.parametrize("with_month", (True, False))
    @pytest.mark.parametrize("only_ns", (True, False))
    def test_minimal_value(self, with_day, with_month, only_ns) -> None:
        seconds = (time.MIN_INT64
                   + with_month * time.AVERAGE_SECONDS_IN_MONTH
                   + with_day * time.AVERAGE_SECONDS_IN_DAY)
        Duration(
            months=-with_month,
            days=-with_day,
            seconds=0 if only_ns else seconds,
            nanoseconds=(seconds * time.NANO_SECONDS) if only_ns else 0
        )

    @pytest.mark.parametrize("with_day", (True, False))
    @pytest.mark.parametrize("with_month", (True, False))
    @pytest.mark.parametrize("only_ns", (True, False))
    @pytest.mark.parametrize("overflow", (
        (0, 0, 0, -1),
        (0, 0, -1, 0),
        (0, -1, 0, 0),
        (-1, 0, 0, 0),
    ))
    def test_negative_overflow_value(self, with_day, with_month, only_ns,
                                     overflow) -> None:
        seconds = (time.MIN_INT64
                   + with_month * time.AVERAGE_SECONDS_IN_MONTH
                   + with_day * time.AVERAGE_SECONDS_IN_DAY)
        kwargs = {
            "months": overflow[0],
            "days": overflow[1],
            "seconds": overflow[2],
            "nanoseconds": overflow[3]
        }
        kwargs["months"] -= with_month
        kwargs["days"] -= with_day
        if only_ns:
            kwargs["nanoseconds"] += seconds * time.NANO_SECONDS
        else:
            kwargs["seconds"] += seconds

        with pytest.raises(ValueError):
            Duration(**kwargs)

    @pytest.mark.parametrize(("field", "module"), (
        ("days", time.AVERAGE_SECONDS_IN_DAY),
        ("months", time.AVERAGE_SECONDS_IN_MONTH),
    ))
    def test_minimal_value_only_secondary_field(self, field, module) -> None:
        kwargs = {
            field: (time.MIN_INT64 // module
                    - (time.MIN_INT64 % module == 0)
                    + 1)
        }
        Duration(**kwargs)

    @pytest.mark.parametrize(("field", "module"), (
        ("days", time.AVERAGE_SECONDS_IN_DAY),
        ("months", time.AVERAGE_SECONDS_IN_MONTH),
    ))
    def test_negative_overflow_value_only_secondary_field(
        self, field, module
    ) -> None:
        kwargs = {
            field: (time.MIN_INT64 // module
                    - (time.MIN_INT64 % module == 0))
        }
        with pytest.raises(ValueError):
            Duration(**kwargs)

    def test_negative_overflow_duration_addition(self) -> None:
        min_ = Duration.min
        ns = Duration(nanoseconds=1)
        with pytest.raises(ValueError):
            min_ - ns
        min_ + ns

    @pytest.mark.parametrize("with_day", (True, False))
    @pytest.mark.parametrize("with_month", (True, False))
    @pytest.mark.parametrize("only_ns", (True, False))
    def test_maximal_value(self, with_day, with_month, only_ns) -> None:
        seconds = (time.MAX_INT64
                   - with_month * time.AVERAGE_SECONDS_IN_MONTH
                   - with_day * time.AVERAGE_SECONDS_IN_DAY)
        Duration(
            months=with_month,
            days=with_day,
            seconds=0 if only_ns else seconds,
            nanoseconds=(seconds * time.NANO_SECONDS) if only_ns else 0
        )

    @pytest.mark.parametrize("with_day", (True, False))
    @pytest.mark.parametrize("with_month", (True, False))
    @pytest.mark.parametrize("only_ns", (True, False))
    @pytest.mark.parametrize("overflow", (
        (0, 0, 0, 1),
        (0, 0, 1, 0),
        (0, 1, 0, 0),
        (1, 0, 0, 0),
    ))
    def test_positive_overflow_value(self, with_day, with_month, only_ns,
                                     overflow) -> None:
        seconds = (time.MAX_INT64
                   - with_month * time.AVERAGE_SECONDS_IN_MONTH
                   - with_day * time.AVERAGE_SECONDS_IN_DAY)
        kwargs = {
            "months": overflow[0],
            "days": overflow[1],
            "seconds": overflow[2],
            "nanoseconds": time.NANO_SECONDS - 1 + overflow[3]
        }
        kwargs["months"] += with_month
        kwargs["days"] += with_day
        if only_ns:
            kwargs["nanoseconds"] += seconds * time.NANO_SECONDS
        else:
            kwargs["seconds"] += seconds

        with pytest.raises(ValueError):
            Duration(**kwargs)

    @pytest.mark.parametrize(("field", "module"), (
        ("days", time.AVERAGE_SECONDS_IN_DAY),
        ("months", time.AVERAGE_SECONDS_IN_MONTH),
    ))
    def test_maximal_value_only_secondary_field(self, field, module) -> None:
        kwargs = {
            field: time.MAX_INT64 // module
        }
        Duration(**kwargs)

    @pytest.mark.parametrize(("field", "module"), (
        ("days", time.AVERAGE_SECONDS_IN_DAY),
        ("months", time.AVERAGE_SECONDS_IN_MONTH),
    ))
    def test_positive_overflow_value_only_secondary_field(
        self, field, module
    ) -> None:
        kwargs = {
            field: time.MAX_INT64 // module + 1
        }
        with pytest.raises(ValueError):
            Duration(**kwargs)

    def test_positive_overflow_duration_addition(self) -> None:
        max_ = Duration.max
        ns = Duration(nanoseconds=1)
        with pytest.raises(ValueError):
            max_ + ns
        max_ - ns
