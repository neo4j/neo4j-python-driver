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

import pandas as pd
import pytest
import pytz

from neo4j._codec.hydration.v2 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import DateTime

from ..v1.test_temporal_dehydration import (
    TestTimeDehydration as _TestTemporalDehydrationV1,
)


class TestTimeDehydration(_TestTemporalDehydrationV1):
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
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        assert_transforms(dt, Structure(b"I", 1539340661, 474716862, 3600))

    def test_native_date_time_fixed_native_offset(self, assert_transforms):
        tz = datetime.timezone(datetime.timedelta(minutes=60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539340661, 474716000, 3600))

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
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862, tz)
        assert_transforms(dt, Structure(b"I", 1539347861, 474716862, -3600))

    def test_native_date_time_fixed_negative_native_offset(
        self, assert_transforms
    ):
        tz = datetime.timezone(datetime.timedelta(minutes=-60))
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716, tz)
        assert_transforms(dt, Structure(b"I", 1539347861, 474716000, -3600))

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
        ),
    )
    def test_pandas_date_time_zone_id(self, dt, fields, assert_transforms):
        assert_transforms(dt, Structure(b"i", *fields))
