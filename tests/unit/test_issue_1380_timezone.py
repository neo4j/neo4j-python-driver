# Copyright (c) "Neo4j"
#
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import (
    time,
    timedelta,
    timezone,
)

import pytest
import pytz

from neo4j._codec.hydration.bolt.v1.temporal import dehydrate_time
from neo4j.time import Time


@pytest.mark.parametrize(
    ("offset_minutes", "expected_seconds"),
    [
        (120, 7200),
        (-210, -12600),
        (0, 0),
    ],
)
def test_native_time_with_standard_timezone(offset_minutes, expected_seconds):
    value = time(
        10,
        11,
        12,
        tzinfo=timezone(timedelta(minutes=offset_minutes)),
    )

    result = dehydrate_time(value)

    assert result.tag == b"T"
    assert result.fields == [
        36672000000000,
        expected_seconds,
    ]


def test_native_time_without_timezone():
    value = time(10, 11, 12)

    result = dehydrate_time(value)

    assert result.tag == b"t"
    assert result.fields == [36672000000000]


def test_neo4j_time_with_pytz_timezone():
    value = Time(
        10,
        11,
        12,
        tzinfo=pytz.FixedOffset(120),
    )

    result = dehydrate_time(value)

    assert result.tag == b"T"
    assert result.fields == [
        36672000000000,
        7200,
    ]


def test_neo4j_time_without_timezone():
    value = Time(10, 11, 12)

    result = dehydrate_time(value)

    assert result.tag == b"t"
    assert result.fields == [36672000000000]
