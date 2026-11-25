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


import datetime
import zoneinfo

import pytest
import pytz

from neo4j._codec.hydration.bolt.v2 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import DateTime

from ......._optional_deps import (
    mark_skip_without_optional_dependency,
    pd,
)
from ......._time_warnings import unsupported_tzinfo_warning
from ..v1.test_temporal_dehydration import (
    TestTimeDehydration as _TestTemporalDehydration,
)


class TestTimeDehydration(_TestTemporalDehydration):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_date_time_fixed_offset(self, assert_transforms):
        tz = pytz.FixedOffset(60)
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        assert_transforms(dt, Structure(b"I", 1539340661, 474716862, 3600))

    def test_native_date_time_fixed_offset(self, assert_transforms):
        tz = pytz.FixedOffset(60)
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539340661, 474716000, 3600))

    def test_date_time_fixed_native_offset(self, assert_transforms):
        tz = datetime.timezone(datetime.timedelta(minutes=60))
        with unsupported_tzinfo_warning(tz):
            dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
            assert_transforms(dt, Structure(b"I", 1539340661, 474716862, 3600))

    def test_native_date_time_fixed_native_offset(self, assert_transforms):
        tz = datetime.timezone(datetime.timedelta(minutes=60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539340661, 474716000, 3600))

    def test_native_date_time_fixed_native_offset_s(self, assert_transforms):
        tz = datetime.timezone(datetime.timedelta(minutes=60, seconds=30))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539340631, 474716000, 3630))

    def test_native_date_time_fixed_native_offset_ms(self, transformer):
        tz = datetime.timezone(datetime.timedelta(minutes=60, microseconds=30))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        with pytest.raises(ValueError, match=r"\bsub-second\b.*\boffsets?\b"):
            transformer(dt)

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_fixed_offset(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862+0100")
        assert_transforms(dt, Structure(b"I", 1539340661, 474716862, 3600))

    def test_date_time_fixed_negative_offset(self, assert_transforms):
        dt = DateTime(
            2018, 10, 12, 11, 37, 41, 474716862, pytz.FixedOffset(-60)
        )
        assert_transforms(dt, Structure(b"I", 1539347861, 474716862, -3600))

    def test_native_date_time_fixed_negative_offset(self, assert_transforms):
        dt = datetime.datetime(
            2018, 10, 12, 11, 37, 41, 474716, pytz.FixedOffset(-60)
        )
        assert_transforms(dt, Structure(b"I", 1539347861, 474716000, -3600))

    def test_date_time_fixed_negative_native_offset(self, assert_transforms):
        tz = datetime.timezone(datetime.timedelta(minutes=-60))
        with unsupported_tzinfo_warning(tz):
            dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
            assert_transforms(
                dt,
                Structure(b"I", 1539347861, 474716862, -3600),
            )

    def test_native_date_time_fixed_negative_native_offset(
        self, assert_transforms
    ):
        tz = datetime.timezone(datetime.timedelta(minutes=-60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539347861, 474716000, -3600))

    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_fixed_negative_offset(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862-0100")
        assert_transforms(dt, Structure(b"I", 1539347861, 474716862, -3600))

    def test_date_time_zone_id(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        dt = pytz.timezone("Europe/Stockholm").localize(dt)
        # offset should be UTC+2 (7200 seconds)
        assert_transforms(
            dt, Structure(b"i", 1539337061, 474716862, "Europe/Stockholm")
        )

    def test_native_date_time_zone_id(self, assert_transforms):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716)
        dt = pytz.timezone("Europe/Stockholm").localize(dt)
        # offset should be UTC+2 (7200 seconds)
        assert_transforms(
            dt, Structure(b"i", 1539337061, 474716000, "Europe/Stockholm")
        )

    @pytest.mark.parametrize("fold", (0, 1))
    def test_native_zone_info_zone_id(self, assert_transforms, fold):
        tz = zoneinfo.ZoneInfo("Europe/Berlin")
        dt = datetime.datetime(
            2026, 10, 25, 2, 30, 5, 474716, fold=fold, tzinfo=tz
        )
        seconds = 1792895405  # 2026-10-25 02:30:05 as UNIX timestamp
        if fold:
            seconds -= 3600  # adjust for fold=1 (past DST end)
        else:
            seconds -= 7200  # adjust for fold=0 (pre DST end)
        assert_transforms(dt, Structure(b"i", seconds, 474716000, tz.key))

    @pytest.mark.parametrize(
        ("dt", "fields"),
        (
            (
                pd.Timestamp(
                    "2018-10-12T11:37:41.474716862+0200", tz="Europe/Stockholm"
                ),
                (1539337061, 474716862, "Europe/Stockholm"),
            ),
            (
                pd.Timestamp(
                    "2018-10-12T10:37:41.474716+0100", tz="Europe/Stockholm"
                ),
                (1539337061, 474716000, "Europe/Stockholm"),
            ),
            (
                # 1972-10-29 02:00:01.001000001+0100 pre DST change
                pd.Timestamp(
                    (1032 * 24 + 2) * 3600 * 1000000000 + 1001000001,
                    tz="Europe/London",
                ),
                ((1032 * 24 + 2) * 3600 + 1, 1000001, "Europe/London"),
            ),
            (
                # 1972-10-29 02:00:01.001000001+0000 post DST change
                pd.Timestamp(
                    (1032 * 24 + 1) * 3600 * 1000000000 + 1001000001,
                    tz="Europe/London",
                ),
                ((1032 * 24 + 1) * 3600 + 1, 1000001, "Europe/London"),
            ),
            (
                pd.Timestamp(
                    "9999-12-31T23:59:59.999999+0100", tz="Etc/GMT-1"
                ),
                (253402297199, 999999000, "Etc/GMT-1"),
            ),
            (
                pd.Timestamp("0001-01-01T00:00:00-0100", tz="Etc/GMT+1"),
                (-62135593200, 0, "Etc/GMT+1"),
            ),
        ),
    )
    @mark_skip_without_optional_dependency(pd)
    def test_pandas_date_time_zone_id(self, dt, fields, assert_transforms):
        assert_transforms(dt, Structure(b"i", *fields))
