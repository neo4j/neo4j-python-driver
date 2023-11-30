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

import numpy as np
import pandas as pd
import pytest
import pytz

from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.time import (
    AVERAGE_SECONDS_IN_DAY,
    Date,
    DateTime,
    Duration,
    MAX_INT64,
    MIN_INT64,
    NANO_SECONDS,
    Time,
)

from ._base import HydrationHandlerTestBase


class TestTimeDehydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.fixture
    def transformer(self, hydration_scope):
        def transformer(value):
            transformer_ = \
                hydration_scope.dehydration_hooks.get_transformer(value)
            assert callable(transformer_)
            return transformer_(value)
        return transformer

    @pytest.fixture
    def assert_transforms(self, transformer):
        def assert_(value, expected):
            struct = transformer(value)
            assert struct == expected
        return assert_

    def test_date(self, assert_transforms):
        date = Date(1991, 8, 24)
        assert_transforms(date, Structure(b"D", 7905))

    def test_native_date(self, assert_transforms):
        date = datetime.date(1991, 8, 24)
        assert_transforms(date, Structure(b"D", 7905))

    def test_time(self, assert_transforms):
        time = Time(1, 2, 3, 4, pytz.FixedOffset(60))
        assert_transforms(time, Structure(b"T", 3723000000004, 3600))

    def test_native_time(self, assert_transforms):
        time = datetime.time(1, 2, 3, 4, pytz.FixedOffset(60))
        assert_transforms(time, Structure(b"T", 3723000004000, 3600))

    def test_local_time(self, assert_transforms):
        time = Time(1, 2, 3, 4)
        assert_transforms(time, Structure(b"t", 3723000000004))

    def test_local_native_time(self, assert_transforms):
        time = datetime.time(1, 2, 3, 4)
        assert_transforms(time, Structure(b"t", 3723000004000))

    def test_local_date_time(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862)
        assert_transforms(dt, Structure(b"d", 1539344261, 474716862))

    def test_native_local_date_time(self, assert_transforms):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716)
        assert_transforms(dt, Structure(b"d", 1539344261, 474716000))

    def test_numpy_local_date_time(self, assert_transforms):
        dt = np.datetime64("2018-10-12T11:37:41.474716862")
        assert_transforms(dt, Structure(b"d", 1539344261, 474716862))

    def test_numpy_nat_local_date_time(self, assert_transforms):
        dt = np.datetime64("NaT")
        assert_transforms(dt, None)

    @pytest.mark.parametrize(("value", "error"), (
        (np.datetime64(10000 - 1970, "Y"), ValueError),
        (np.datetime64("+10000-01-01"), ValueError),
        (np.datetime64(-1970, "Y"), ValueError),
        (np.datetime64("0000-12-31"), ValueError),

    ))
    def test_numpy_invalid_local_date_time(self, value, error, transformer):
        with pytest.raises(error):
            transformer(value)

    def test_pandas_local_date_time(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862")
        assert_transforms(dt, Structure(b"d", 1539344261, 474716862))

    def test_pandas_nat_local_date_time(self, assert_transforms):
        dt = pd.NaT
        assert_transforms(dt, None)

    def test_date_time_fixed_offset(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(60))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, 3600))

    def test_native_date_time_fixed_offset(self, assert_transforms):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(60))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716000, 3600))

    def test_date_time_fixed_native_offset(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      datetime.timezone(datetime.timedelta(minutes=60)))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, 3600))

    def test_native_date_time_fixed_native_offset(self, assert_transforms):
        dt = datetime.datetime(
            2018, 10, 12, 11, 37, 41, 474716,
            datetime.timezone(datetime.timedelta(minutes=60))
        )
        assert_transforms(dt, Structure(b"F", 1539344261, 474716000, 3600))

    def test_pandas_date_time_fixed_offset(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862+0100")
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, 3600))

    def test_date_time_fixed_negative_offset(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.FixedOffset(-60))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, -3600))

    def test_native_date_time_fixed_negative_offset(self, assert_transforms):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.FixedOffset(-60))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716000, -3600))

    def test_date_time_fixed_negative_native_offset(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      datetime.timezone(datetime.timedelta(minutes=-60)))
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, -3600))

    def test_native_date_time_fixed_negative_native_offset(self,
                                                           assert_transforms):
        dt = datetime.datetime(
            2018, 10, 12, 11, 37, 41, 474716,
            datetime.timezone(datetime.timedelta(minutes=-60))
        )
        assert_transforms(dt, Structure(b"F", 1539344261, 474716000, -3600))

    def test_pandas_date_time_fixed_negative_offset(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862-0100")
        assert_transforms(dt, Structure(b"F", 1539344261, 474716862, -3600))

    def test_date_time_zone_id(self, assert_transforms):
        dt = DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                      pytz.timezone("Europe/Stockholm"))
        assert_transforms(
            dt,
            Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        )

    def test_native_date_time_zone_id(self, assert_transforms):
        dt = datetime.datetime(2018, 10, 12, 11, 37, 41, 474716,
                               pytz.timezone("Europe/Stockholm"))
        assert_transforms(
            dt,
            Structure(b"f", 1539344261, 474716000, "Europe/Stockholm")
        )

    def test_pandas_date_time_zone_id(self, assert_transforms):
        dt = pd.Timestamp("2018-10-12T11:37:41.474716862+0200",
                          tz="Europe/Stockholm")
        assert_transforms(
            dt,
            Structure(b"f", 1539344261, 474716862, "Europe/Stockholm")
        )

    def test_duration(self, assert_transforms):
        duration = Duration(months=1, days=2, seconds=3, nanoseconds=4)
        assert_transforms(duration, Structure(b"E", 1, 2, 3, 4))

    def test_native_duration(self, assert_transforms):
        duration = datetime.timedelta(days=1, seconds=2, microseconds=3)
        assert_transforms(duration, Structure(b"E", 0, 1, 2, 3000))

    def test_duration_mixed_sign(self, assert_transforms):
        duration = Duration(months=1, days=-2, seconds=3, nanoseconds=4)
        assert_transforms(duration, Structure(b"E", 1, -2, 3, 4))

    def test_native_duration_mixed_sign(self, assert_transforms):
        duration = datetime.timedelta(days=-1, seconds=2, microseconds=3)
        assert_transforms(duration, Structure(b"E", 0, -1, 2, 3000))

    @pytest.mark.parametrize(
        ("value", "expected_fields"),
        (
            (np.timedelta64(1, "Y"), (12, 0, 0, 0)),
            (np.timedelta64(1, "M"), (1, 0, 0, 0)),
            (np.timedelta64(1, "D"), (0, 1, 0, 0)),
            (np.timedelta64(1, "h"), (0, 0, 3600, 0)),
            (np.timedelta64(1, "m"), (0, 0, 60, 0)),
            (np.timedelta64(1, "s"), (0, 0, 1, 0)),
            (np.timedelta64(MAX_INT64, "s"), (0, 0, MAX_INT64, 0)),
            (np.timedelta64(1, "ms"), (0, 0, 0, 1000000)),
            (np.timedelta64(1, "us"), (0, 0, 0, 1000)),
            (np.timedelta64(1, "ns"), (0, 0, 0, 1)),
            (np.timedelta64(NANO_SECONDS, "ns"), (0, 0, 1, 0)),
            (np.timedelta64(NANO_SECONDS + 1, "ns"), (0, 0, 1, 1)),
            (np.timedelta64(1000, "ps"), (0, 0, 0, 1)),
            (np.timedelta64(1, "ps"), (0, 0, 0, 0)),
            (np.timedelta64(1000000, "fs"), (0, 0, 0, 1)),
            (np.timedelta64(1, "fs"), (0, 0, 0, 0)),
            (np.timedelta64(1000000000, "as"), (0, 0, 0, 1)),
            (np.timedelta64(1, "as"), (0, 0, 0, 0)),
            (np.timedelta64(-1, "Y"), (-12, 0, 0, 0)),
            (np.timedelta64(-1, "M"), (-1, 0, 0, 0)),
            (np.timedelta64(-1, "D"), (0, -1, 0, 0)),
            (np.timedelta64(-1, "h"), (0, 0, -3600, 0)),
            (np.timedelta64(-1, "m"), (0, 0, -60, 0)),
            (np.timedelta64(-1, "s"), (0, 0, -1, 0)),
            # numpy uses MIN_INT64 to encode NaT
            (np.timedelta64(MIN_INT64 + 1, "s"), (0, 0, MIN_INT64 + 1, 0)),
            (np.timedelta64(-1, "ms"), (0, 0, 0, -1000000)),
            (np.timedelta64(-1, "us"), (0, 0, 0, -1000)),
            (np.timedelta64(-1, "ns"), (0, 0, 0, -1)),
            (np.timedelta64(-NANO_SECONDS, "ns"), (0, 0, -1, 0)),
            (np.timedelta64(-NANO_SECONDS - 1, "ns"), (0, 0, -1, -1)),
            (np.timedelta64(-1000, "ps"), (0, 0, 0, -1)),
            (np.timedelta64(-1, "ps"), (0, 0, 0, -1)),
            (np.timedelta64(-1000000, "fs"), (0, 0, 0, -1)),
            (np.timedelta64(-1, "fs"), (0, 0, 0, -1)),
            (np.timedelta64(-1000000000, "as"), (0, 0, 0, -1)),
            (np.timedelta64(-1, "as"), (0, 0, 0, -1)),
        )
    )
    def test_numpy_duration(self, value, expected_fields, assert_transforms):
        assert_transforms(value, Structure(b"E", *expected_fields))

    def test_numpy_nat_duration(self, assert_transforms):
        duration = np.timedelta64("NaT")
        assert_transforms(duration, None)

    @pytest.mark.parametrize(("value", "error"), (
        (np.timedelta64((MAX_INT64 // 60) + 1, "m"), ValueError),
        (np.timedelta64((MIN_INT64 // 60), "m"), ValueError),

    ))
    def test_numpy_invalid_durations(self, value, error, transformer):
        with pytest.raises(error):
            transformer(value)

    @pytest.mark.parametrize(
        ("value", "expected_fields"),
        (
            (
                pd.Timedelta(days=1, seconds=2, microseconds=3, nanoseconds=4),
                (0, 0, AVERAGE_SECONDS_IN_DAY + 2, 3004)
            ),
            (
                pd.Timedelta(days=-1, seconds=2, microseconds=3,
                             nanoseconds=4),
                (0, 0, -AVERAGE_SECONDS_IN_DAY + 2 + 1, -NANO_SECONDS + 3004)
            )
        )
    )
    def test_pandas_duration(self, value, expected_fields, assert_transforms):
        assert_transforms(value, Structure(b"E", *expected_fields))
