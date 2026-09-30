# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import typing as t

import pytest
import pytz

from neo4j._codec.hydration import BrokenHydrationObject
from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j._exceptions import QueryApiHttpError
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from neo4j._codec.hydration.http._common import HydrationScopeHttp


class TestTemporalHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    def test_hydrate_date(self, hydration_scope: HydrationScopeHttp) -> None:
        encoded = {
            "$type": "Date",
            "_value": "1991-08-24",
        }
        d = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(d, Date)
        assert d.year == 1991
        assert d.month == 8
        assert d.day == 24

    @pytest.mark.parametrize(
        ("encoded_value", "expected"),
        (
            (
                "01:02:03.000000004+01:00",
                Time(1, 2, 3, 4, pytz.FixedOffset(60)),
            ),
            (
                "01:02:03.000000004-00:01",
                Time(1, 2, 3, 4, pytz.FixedOffset(-1)),
            ),
            (
                "01:02:03.000000004+00:00",
                Time(1, 2, 3, 4, pytz.FixedOffset(0)),
            ),
            (
                "01:02:03.000000004Z",
                Time(1, 2, 3, 4, pytz.FixedOffset(0)),
            ),
        ),
    )
    def test_hydrate_time(
        self,
        encoded_value: str,
        expected: Time,
        hydration_scope: HydrationScopeHttp,
    ) -> None:
        encoded = {"$type": "Time", "_value": encoded_value}
        t = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(t, Time)
        assert t.hour == expected.hour
        assert t.minute == expected.minute
        assert t.second == expected.second
        assert t.nanosecond == expected.nanosecond
        assert t.tzinfo == expected.tzinfo

    @pytest.mark.parametrize(
        ("value", "offset_h"),
        (
            ("00:00:00.000000000+01:00", 1),
            ("00:00:00.000000000+01:00:00", 1),
            ("00:00:00.000000000+0100", 1),
            ("00:00:00.000000000+01", 1),
            ("00:00:00.000000000-01:00", -1),
            ("00:00:00.000000000-01:00:00", -1),
            ("00:00:00.000000000-0100", -1),
            ("00:00:00.000000000-01", -1),
            ("00:00:00.000000000Z", 0),
            ("00:00:00.000000000+00:00:00", 0),
            ("00:00:00.000000000+00:00", 0),
            ("00:00:00.000000000-00:00:00", 0),
            ("00:00:00.000000000-00:00", 0),
            ("00:00:00.0Z", 0),
            ("00:00:00Z", 0),
            ("00:00Z", 0),
        ),
    )
    def test_hydrate_formats(
        self, hydration_scope: HydrationScopeHttp, value: str, offset_h: int
    ) -> None:
        encoded = {
            "$type": "Time",
            "_value": value,
        }
        t = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(t, Time)
        assert t.hour == 0
        assert t.minute == 0
        assert t.second == 0
        assert t.nanosecond == 0
        assert t.tzinfo == pytz.FixedOffset(offset_h * 60)

    def test_hydrate_local_time(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "LocalTime",
            "_value": "01:02:03.000000004",
        }
        t = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(t, Time)
        assert t.hour == 1
        assert t.minute == 2
        assert t.second == 3
        assert t.nanosecond == 4
        assert t.tzinfo is None

    @pytest.mark.parametrize("type_", ("OffsetDateTime", "ZonedDateTime"))
    def test_hydrate_offset_date_time(
        self, type_: str, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": type_,
            "_value": "2018-10-12T11:37:41.474716862+01:00",
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo == pytz.FixedOffset(60)

    @pytest.mark.parametrize("type_", ("OffsetDateTime", "ZonedDateTime"))
    def test_hydrate_offset_date_time_zulu(
        self, type_: str, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": type_,
            "_value": "2018-10-12T11:37:41.474716862Z",
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo == pytz.FixedOffset(0)

    @pytest.mark.parametrize(
        "value",
        (
            "2018-10-12T13:37:41.474716862+02:00[Europe/Stockholm]",
            "2018-10-12T12:37:41.474716862+01:00[Europe/Stockholm]",
            "2018-10-12T11:37:41.474716862Z[Europe/Stockholm]",
            "2018-10-12T11:37:41.474716862+00:00[Europe/Stockholm]",
            "2018-10-11T23:37:41.474716862-12:00[Europe/Stockholm]",
            "2018-10-13T00:37:41.474716862+13:00[Europe/Stockholm]",
        ),
    )
    def test_hydrate_date_time_zone_id(
        self, hydration_scope: HydrationScopeHttp, value: str
    ) -> None:
        encoded = {
            "$type": "ZonedDateTime",
            "_value": value,
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 13
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        tz = (
            pytz.timezone("Europe/Stockholm")
            .localize(dt.replace(tzinfo=None))
            .tzinfo
        )
        assert dt.tzinfo == tz

    @pytest.mark.parametrize(
        "value",
        (
            "0001-01-01T00:00:00.000000000+00:00[Etc/UTC]",
            "0001-01-01T00:00:00.000000000+00:00:00[Etc/UTC]",
            "0001-01-01T00:00:00.000000000+0000[Etc/UTC]",
            "0001-01-01T00:00:00.000000000+00[Etc/UTC]",
            "0001-01-01T00:00:00.000000000Z[Etc/UTC]",
            "0001-01-01T00:00:00.000000000-00:00[Etc/UTC]",
            "0001-01-01T00:00:00.000000000-00:00:00[Etc/UTC]",
            "0001-01-01T00:00:00.000000000-0000[Etc/UTC]",
            "0001-01-01T00:00:00.000000000-00[Etc/UTC]",
            "0001-01-01T00:00:00.0000Z[Etc/UTC]",
            "0001-01-01T00:00:00Z[Etc/UTC]",
            "0001-01-01T00:00Z[Etc/UTC]",
        ),
    )
    def test_hydrate_date_time_zone_id_formats(
        self, hydration_scope: HydrationScopeHttp, value: str
    ) -> None:
        encoded = {
            "$type": "ZonedDateTime",
            "_value": value,
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 1
        assert dt.month == 1
        assert dt.day == 1
        assert dt.hour == 0
        assert dt.minute == 0
        assert dt.second == 0
        assert dt.nanosecond == 0
        tz = pytz.timezone("Etc/UTC").localize(dt.replace(tzinfo=None)).tzinfo
        assert dt.tzinfo == tz

    def test_hydrate_date_time_unknown_zone_id(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "ZonedDateTime",
            "_value": "2018-10-12T11:37:41.474716862Z[Europe/Neo4j]",
        }
        res = hydration_scope.hydration_hooks[type(encoded)](encoded)
        assert isinstance(res, BrokenHydrationObject)
        exc = None
        try:
            pytz.timezone("Europe/Neo4j")
        except Exception as e:
            exc = e
        assert res.error.__class__ == exc.__class__
        assert str(res.error) == str(exc)

    def test_hydrate_invalid_zoned_date_time(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        value = "2018-10-12T11:37:41.474716862Z[Europe/Berlin]"
        encoded = {
            "$type": "OffsetDateTime",
            "_value": value,
        }
        res = hydration_scope.hydration_hooks[type(encoded)](encoded)
        assert isinstance(res, BrokenHydrationObject)
        assert res.error.__class__ is QueryApiHttpError
        assert "offset datetime string" in str(res.error)
        assert repr(value) in str(res.error)

    def test_hydrate_local_date_time(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "LocalDateTime",
            "_value": "2018-10-12T11:37:41.474716862",
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 2018
        assert dt.month == 10
        assert dt.day == 12
        assert dt.hour == 11
        assert dt.minute == 37
        assert dt.second == 41
        assert dt.nanosecond == 474716862
        assert dt.tzinfo is None

    @pytest.mark.parametrize(
        "value",
        (
            "0400-01-01T00:00:00.000000000",
            "0400-01-01T00:00:00.0000",
            "0400-01-01T00:00:00",
            "0400-01-01T00:00",
        ),
    )
    def test_hydrate_local_date_time_formats(
        self, hydration_scope: HydrationScopeHttp, value: str
    ) -> None:
        encoded = {
            "$type": "LocalDateTime",
            "_value": value,
        }
        dt = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(dt, DateTime)
        assert dt.year == 400
        assert dt.month == 1
        assert dt.day == 1
        assert dt.hour == 0
        assert dt.minute == 0
        assert dt.second == 0
        assert dt.nanosecond == 0
        assert dt.tzinfo is None

    @pytest.mark.parametrize(
        ("value", "months", "days", "seconds", "nanoseconds"),
        (
            ("PT0S", 0, 0, 0, 0),
            ("P0Y", 0, 0, 0, 0),
            ("P0M", 0, 0, 0, 0),
            ("P0W", 0, 0, 0, 0),
            ("P0D", 0, 0, 0, 0),
            ("PT0H", 0, 0, 0, 0),
            ("PT0M", 0, 0, 0, 0),
            ("P0DT0.0S", 0, 0, 0, 0),
            ("P1M2DT3.000000004S", 1, 2, 3, 4),
            ("P-1M-2DT-3.000000004S", -1, -2, -3, -4),
            ("PT-0.999999999S", 0, 0, 0, -999999999),
            ("P1Y", 12, 0, 0, 0),
            ("P-1Y", -12, 0, 0, 0),
            ("P1M", 1, 0, 0, 0),
            ("P-1M", -1, 0, 0, 0),
            ("P1W", 0, 7, 0, 0),
            ("P-1W", 0, -7, 0, 0),
            ("P1D", 0, 1, 0, 0),
            ("P-1D", 0, -1, 0, 0),
            ("PT1H", 0, 0, 3600, 0),
            ("PT-1H", 0, 0, -3600, 0),
            ("PT1M", 0, 0, 60, 0),
            ("PT-1M", 0, 0, -60, 0),
            ("PT1S", 0, 0, 1, 0),
            ("PT-1S", 0, 0, -1, 0),
            ("P3507324295523M", 3507324295523, 0, 0, 0),
            ("P292277024626Y11M", 3507324295523, 0, 0, 0),
            ("P-3507324295523M", -3507324295523, 0, 0, 0),
            ("P-292277024627Y1M", -3507324295523, 0, 0, 0),
            ("P15250284452471W", 0, 106751991167297, 0, 0),
            ("P-15250284452471W", 0, -106751991167297, 0, 0),
            ("P-106751991167297D", 0, -106751991167297, 0, 0),
            ("P106751991167300D", 0, 106751991167300, 0, 0),
            ("P-106751991167300D", 0, -106751991167300, 0, 0),
            ("PT2562047788015215H", 0, 0, 9223372036854774000, 0),
            ("PT-2562047788015215H", 0, 0, -9223372036854774000, 0),
            ("PT153722867280912930M", 0, 0, 9223372036854775800, 0),
            ("PT2562047788015215H30M", 0, 0, 9223372036854775800, 0),
            ("PT-153722867280912930M", 0, 0, -9223372036854775800, 0),
            ("PT-2562047788015216H30M", 0, 0, -9223372036854775800, 0),
            ("PT-2562047788015214H-90M", 0, 0, -9223372036854775800, 0),
            ("PT9223372036854775807S", 0, 0, 9223372036854775807, 0),
            ("PT2562047788015215H30M7S", 0, 0, 9223372036854775807, 0),
            ("PT-9223372036854775808S", 0, 0, -9223372036854775808, 0),
            ("PT-2562047788015216H29M52S", 0, 0, -9223372036854775808, 0),
        ),
    )
    def test_hydrate_duration(
        self,
        hydration_scope: HydrationScopeHttp,
        value: str,
        months: int,
        days: int,
        seconds: int,
        nanoseconds: int,
    ) -> None:
        encoded = {
            "$type": "Duration",
            "_value": value,
        }
        d = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(d, Duration)
        assert d.months == months
        assert d.days == days
        assert d.seconds == seconds
        assert d.nanoseconds == nanoseconds
