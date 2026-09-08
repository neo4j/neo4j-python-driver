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

import datetime
import typing as t

import pytest
import pytz

from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.time import (
    _NANO_SECONDS,
    Date,
    DateTime,
    Duration,
    MAX_INT64,
    MIN_INT64,
    Time,
)

from ......._optional_deps import (
    mark_skip_without_optional_dependency,
    np,
    pd,
)
from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from .._base import T_Transformer


class TestTimeDehydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    def test_date(self, transformer: T_Transformer) -> None:
        date = Date(1991, 8, 24)
        encoded = transformer(date)
        assert encoded == {
            "$type": "Date",
            "_value": "1991-08-24",
        }

    def test_native_date(self, transformer: T_Transformer) -> None:
        date = datetime.date(1991, 8, 24)
        encoded = transformer(date)
        assert encoded == {
            "$type": "Date",
            "_value": "1991-08-24",
        }

    @pytest.mark.parametrize(
        ("time", "expected"),
        (
            (
                Time(1, 2, 3, 4, pytz.FixedOffset(60)),
                "01:02:03.000000004+01:00",
            ),
            (
                Time(1, 2, 3, 4, pytz.FixedOffset(-1)),
                "01:02:03.000000004-00:01",
            ),
            (
                Time(1, 2, 3, 4, pytz.FixedOffset(0)),
                "01:02:03.000000004+00:00",
            ),
        ),
    )
    def test_time(
        self,
        time: Time,
        expected: str,
        transformer: T_Transformer,
    ) -> None:
        encoded = transformer(time)
        assert encoded == {"$type": "Time", "_value": expected}

    @pytest.mark.parametrize(
        ("time", "expected"),
        (
            (
                datetime.time(1, 2, 3, 4, pytz.FixedOffset(60)),
                "01:02:03.000004+01:00",
            ),
            (
                datetime.time(1, 2, 3, 4, pytz.FixedOffset(-1)),
                "01:02:03.000004-00:01",
            ),
            (
                datetime.time(1, 2, 3, 4, pytz.FixedOffset(0)),
                "01:02:03.000004+00:00",
            ),
        ),
    )
    def test_native_time(
        self,
        time: datetime.time,
        expected: str,
        transformer: T_Transformer,
    ) -> None:
        encoded = transformer(time)
        assert encoded == {"$type": "Time", "_value": expected}

    def test_local_time(self, transformer: T_Transformer) -> None:
        time = Time(1, 2, 3, 4)
        encoded = transformer(time)
        assert encoded == {
            "$type": "LocalTime",
            "_value": "01:02:03.000000004",
        }

    def test_local_native_time(self, transformer: T_Transformer) -> None:
        time = datetime.time(1, 2, 3, 4)
        encoded = transformer(time)
        assert encoded == {
            "$type": "LocalTime",
            "_value": "01:02:03.000004",
        }

    def test_local_date_time(self, transformer: T_Transformer) -> None:
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "LocalDateTime",
            "_value": "2018-10-12T11:37:41.474716862",
        }

    def test_native_local_date_time(self, transformer: T_Transformer) -> None:
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "LocalDateTime",
            "_value": "2018-10-12T11:37:41.474716",
        }

    @mark_skip_without_optional_dependency(np)
    def test_numpy_local_date_time(self, transformer: T_Transformer) -> None:
        dt = np.datetime64("2018-10-12T11:37:41.474716862")
        encoded = transformer(dt)
        assert encoded == {
            "$type": "LocalDateTime",
            "_value": "2018-10-12T11:37:41.474716862",
        }

    @mark_skip_without_optional_dependency(np)
    def test_numpy_nat_local_date_time(
        self, transformer: T_Transformer
    ) -> None:
        dt = np.datetime64("NaT", "ns")
        encoded = transformer(dt)
        assert encoded == {"$type": "Null", "_value": None}

    @pytest.mark.parametrize(
        ("value", "error"),
        (
            (np.datetime64(10000 - 1970, "Y"), ValueError),
            (np.datetime64("+10000-01-01"), ValueError),
            (np.datetime64(-1970, "Y"), ValueError),
            (np.datetime64("0000-12-31"), ValueError),
        ),
    )
    @mark_skip_without_optional_dependency(np)
    def test_numpy_invalid_local_date_time(self, value, error, transformer):
        with pytest.raises(error):
            transformer(value)

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_local_date_time(self, transformer: T_Transformer) -> None:
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862")
        encoded = transformer(dt)
        assert encoded == {
            "$type": "LocalDateTime",
            "_value": "2018-10-12T11:37:41.474716862",
        }

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_nat_local_date_time(
        self, transformer: T_Transformer
    ) -> None:
        dt = pd.NaT
        encoded = transformer(dt)
        assert encoded == {"$type": "Null", "_value": None}

    def test_date_time_fixed_offset(self, transformer: T_Transformer) -> None:
        dt = DateTime(
            2018, 10, 12, 11, 37, 41, 474716862, pytz.FixedOffset(60)
        )
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862+01:00",
        }

    def test_native_date_time_fixed_offset(
        self, transformer: T_Transformer
    ) -> None:
        dt = datetime.datetime(
            2018, 10, 12, 11, 37, 41, 474716, pytz.FixedOffset(60)
        )
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716+01:00",
        }

    def test_date_time_fixed_native_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = datetime.timezone(datetime.timedelta(minutes=60))
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862+01:00",
        }

    def test_native_date_time_fixed_native_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = datetime.timezone(datetime.timedelta(minutes=60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716+01:00",
        }

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_fixed_offset(
        self, transformer: T_Transformer
    ) -> None:
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862+0100")
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862+01:00",
        }

    def test_date_time_fixed_negative_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = pytz.FixedOffset(-60)
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862-01:00",
        }

    def test_native_date_time_fixed_negative_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = pytz.FixedOffset(-60)
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716-01:00",
        }

    def test_date_time_fixed_negative_native_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = datetime.timezone(datetime.timedelta(minutes=-60))
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862-01:00",
        }

    def test_native_date_time_fixed_negative_native_offset(
        self, transformer: T_Transformer
    ) -> None:
        tz = datetime.timezone(datetime.timedelta(minutes=-60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716-01:00",
        }

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_fixed_negative_offset(
        self, transformer: T_Transformer
    ) -> None:
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862-0100")
        encoded = transformer(dt)
        assert encoded == {
            "$type": "OffsetDateTime",
            "_value": "2018-10-12T11:37:41.474716862-01:00",
        }

    def test_date_time_zone_id(self, transformer: T_Transformer) -> None:
        tz = pytz.timezone("Europe/Stockholm")
        dt = tz.localize(DateTime(2018, 10, 12, 11, 37, 41, 474716862))
        # dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        encoded = transformer(dt)
        assert encoded == {
            "$type": "ZonedDateTime",
            "_value": "2018-10-12T11:37:41.474716862+02:00[Europe/Stockholm]",
        }

    def test_native_date_time_zone_id(
        self, transformer: T_Transformer
    ) -> None:
        tz = pytz.timezone("Europe/Stockholm")
        dt = tz.localize(datetime.datetime(2018, 10, 12, 11, 37, 41, 474716))
        encoded = transformer(dt)
        assert encoded == {
            "$type": "ZonedDateTime",
            "_value": "2018-10-12T11:37:41.474716+02:00[Europe/Stockholm]",
        }

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_zone_id(
        self, transformer: T_Transformer
    ) -> None:
        dt = pd.Timestamp(
            "2018-10-12T11:37:41.474716862+0200", tz="Europe/Stockholm"
        )
        encoded = transformer(dt)
        assert encoded == {
            "$type": "ZonedDateTime",
            "_value": "2018-10-12T11:37:41.474716862+02:00[Europe/Stockholm]",
        }

    @pytest.mark.parametrize(
        ("mdsn", "value"),
        (
            ((0, 0, 0, 0), "PT0S"),
            ((0, 0, 1, 0), "PT1S"),
            ((0, 0, 60, 0), "PT1M"),
            ((0, 0, 3600, 0), "PT1H"),
            ((0, 0, 3723, 0), "PT1H2M3S"),
            ((0, 0, 0, -999999999), "PT-0.999999999S"),
            ((1, 0, 0, 0), "P1M"),
            ((12, 0, 0, 0), "P1Y"),
            ((14, 0, 0, 0), "P1Y2M"),
            ((0, 1, 0, 0), "P1D"),
            ((1, 2, 3, 4), "P1M2DT3.000000004S"),
            ((1, 2, 3, 4000), "P1M2DT3.000004S"),
            ((12, 2, 3, 0), "P1Y2DT3S"),
            ((13, 123456, 3, 0), "P1Y1M123456DT3S"),
            ((0, 0, -1, 0), "PT-1S"),
            ((0, 0, -60, 0), "PT-1M"),
            ((0, 0, -3600, 0), "PT-1H"),
            ((0, 0, -3723, 0), "PT-1H-2M-3S"),
            ((-1, 0, 0, 0), "P-1M"),
            ((-12, 0, 0, 0), "P-1Y"),
            ((-14, 0, 0, 0), "P-1Y-2M"),
            ((0, -1, 0, 0), "P-1D"),
            ((-1, -2, 3, 4), "P-1M-2DT3.000000004S"),
            ((-1, -2, 3, 4000), "P-1M-2DT3.000004S"),
            ((-12, -2, 3, 0), "P-1Y-2DT3S"),
            ((-14, -123456, 3, 0), "P-1Y-2M-123456DT3S"),
            ((1, -2, -3, -4), "P1M-2DT-3.000000004S"),
            ((1, -2, -3, -4000), "P1M-2DT-3.000004S"),
            ((12, -2, -3, 0), "P1Y-2DT-3S"),
            ((13, -123456, -3, 0), "P1Y1M-123456DT-3S"),
        ),
    )
    def test_duration(
        self,
        mdsn: tuple[int, int, int, int],
        value: str,
        transformer: T_Transformer,
    ) -> None:
        months, days, seconds, nanoseconds = mdsn
        duration = Duration(
            months=months,
            days=days,
            seconds=seconds,
            nanoseconds=nanoseconds,
        )
        encoded = transformer(duration)
        assert encoded == {
            "$type": "Duration",
            "_value": value,
        }

    @pytest.mark.parametrize(
        ("dsm", "value"),
        (
            ((0, 0, 0), "PT0S"),
            ((1, 2, 3), "P1DT2.000003S"),
            ((0, -2, -3), "P-1DT86397.999997S"),
            ((-1, 2, 300_000), "P-1DT2.3S"),
            ((123456, -2, -345_000), "P123455DT86397.655S"),
            ((-2, -3, -4000), "P-3DT86396.996S"),
            ((-2, 3, 4000), "P-2DT3.004S"),
            ((2, -3, -4000), "P1DT86396.996S"),
            ((-123456, -3, 0), "P-123457DT86397S"),
        ),
    )
    def test_native_duration(
        self, dsm: tuple[int, int, int], value: str, transformer: T_Transformer
    ) -> None:
        days, seconds, microseconds = dsm
        duration = datetime.timedelta(
            days=days,
            seconds=seconds,
            microseconds=microseconds,
        )
        encoded = transformer(duration)
        assert encoded == {
            "$type": "Duration",
            "_value": value,
        }

    @pytest.mark.parametrize(
        ("value", "encoded_value"),
        (
            (np.timedelta64(1, "Y"), "P1Y"),
            (np.timedelta64(1, "M"), "P1M"),
            (np.timedelta64(1, "D"), "P1D"),
            (np.timedelta64(1, "h"), "PT1H"),
            (np.timedelta64(1, "m"), "PT1M"),
            (np.timedelta64(1, "s"), "PT1S"),
            (np.timedelta64(60, "s"), "PT1M"),
            (np.timedelta64(3600, "s"), "PT1H"),
            (np.timedelta64(3723, "s"), "PT1H2M3S"),
            (
                np.timedelta64(MAX_INT64, "s"),
                (
                    f"PT{MAX_INT64 // 3600}H"
                    f"{(MAX_INT64 % 3600) // 60}M"
                    f"{MAX_INT64 % 60}S"
                ),
            ),
            (np.timedelta64(1, "ms"), "PT0.001S"),
            (np.timedelta64(1, "us"), "PT0.000001S"),
            (np.timedelta64(1, "ns"), "PT0.000000001S"),
            (np.timedelta64(_NANO_SECONDS, "ns"), "PT1S"),
            (np.timedelta64(_NANO_SECONDS + 1, "ns"), "PT1.000000001S"),
            (np.timedelta64(1000, "ps"), "PT0.000000001S"),
            (np.timedelta64(1, "ps"), "PT0S"),
            (np.timedelta64(1000000, "fs"), "PT0.000000001S"),
            (np.timedelta64(1, "fs"), "PT0S"),
            (np.timedelta64(1000000000, "as"), "PT0.000000001S"),
            (np.timedelta64(1, "as"), "PT0S"),
            (np.timedelta64(-1, "Y"), "P-1Y"),
            (np.timedelta64(-1, "M"), "P-1M"),
            (np.timedelta64(-1, "D"), "P-1D"),
            (np.timedelta64(-1, "h"), "PT-1H"),
            (np.timedelta64(-1, "m"), "PT-1M"),
            (np.timedelta64(-1, "s"), "PT-1S"),
            # numpy uses MIN_INT64 to encode NaT
            (
                np.timedelta64(MIN_INT64 + 1, "s"),
                (
                    f"PT-{abs(MIN_INT64 + 1) // 3600}H"
                    f"-{(abs(MIN_INT64 + 1) % 3600) // 60}M"
                    f"-{abs(MIN_INT64 + 1) % 60}S"
                ),
            ),
            (np.timedelta64(-1, "ms"), "PT-0.001S"),
            (np.timedelta64(-1, "us"), "PT-0.000001S"),
            (np.timedelta64(-1, "ns"), "PT-0.000000001S"),
            (np.timedelta64(-_NANO_SECONDS, "ns"), "PT-1S"),
            (np.timedelta64(-_NANO_SECONDS - 1, "ns"), "PT-1.000000001S"),
            (np.timedelta64(-1000, "ps"), "PT-0.000000001S"),
            (np.timedelta64(-1, "ps"), "PT-0.000000001S"),
            (np.timedelta64(-1000000, "fs"), "PT-0.000000001S"),
            (np.timedelta64(-1, "fs"), "PT-0.000000001S"),
            (np.timedelta64(-1000000000, "as"), "PT-0.000000001S"),
            (np.timedelta64(-1, "as"), "PT-0.000000001S"),
        ),
    )
    @mark_skip_without_optional_dependency(np)
    def test_numpy_duration(
        self,
        value: np.timedelta64,
        encoded_value: str,
        transformer: T_Transformer,
    ) -> None:
        encoded = transformer(value)
        assert encoded == {
            "$type": "Duration",
            "_value": encoded_value,
        }

    @mark_skip_without_optional_dependency(np)
    def test_numpy_nat_duration(self, transformer: T_Transformer) -> None:
        duration = np.timedelta64("NaT", "ns")
        encoded = transformer(duration)
        assert encoded == {"$type": "Null", "_value": None}

    @pytest.mark.parametrize(
        ("value", "error"),
        (
            (np.timedelta64((MAX_INT64 // 60) + 1, "m"), ValueError),
            (np.timedelta64((MIN_INT64 // 60), "m"), ValueError),
        ),
    )
    @mark_skip_without_optional_dependency(np)
    def test_numpy_invalid_durations(self, value, error, transformer):
        with pytest.raises(error):
            transformer(value)

    @pytest.mark.parametrize(
        ("dsmn", "encoded_value"),
        (
            ((1, 2, 3, 4), "PT24H2.000003004S"),
            ((-1, 2, 3, 4), "PT-23H-59M-57.999996996S"),
        ),
    )
    @mark_skip_without_optional_dependency(pd)
    def test_pandas_duration(
        self,
        dsmn: tuple[int, int, int, int],
        encoded_value: str,
        transformer: T_Transformer,
    ) -> None:
        days, seconds, microseconds, nanoseconds = dsmn
        value = pd.Timedelta(
            days=days,
            seconds=seconds,
            microseconds=microseconds,
            nanoseconds=nanoseconds,  # type: ignore[call-arg]
        )
        encoded = transformer(value)
        assert encoded == {
            "$type": "Duration",
            "_value": encoded_value,
        }
