#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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


from pytest import raises
from pytz import FixedOffset, timezone, utc

from neo4j.exceptions import CypherTypeError
from neo4j.time import Date, Time, DateTime, Duration


def test_native_date_input(cypher_eval):
    from datetime import date
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day]",
                         x=date(1976, 6, 13))
    year, month, day = result
    assert year == 1976
    assert month == 6
    assert day == 13


def test_date_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day]",
                         x=Date(1976, 6, 13))
    year, month, day = result
    assert year == 1976
    assert month == 6
    assert day == 13


def test_date_array_input(cypher_eval):
    data = [DateTime.now().date(), Date(1976, 6, 13)]
    value = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)
    assert value == data


def test_native_time_input(cypher_eval):
    from datetime import time
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.hour, x.minute, x.second, x.nanosecond]",
                         x=time(12, 34, 56, 789012))
    hour, minute, second, nanosecond = result
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012000


def test_whole_second_time_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.hour, x.minute, x.second]",
                         x=Time(12, 34, 56))
    hour, minute, second = result
    assert hour == 12
    assert minute == 34
    assert second == 56


def test_nanosecond_resolution_time_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.hour, x.minute, x.second, x.nanosecond]",
                         x=Time(12, 34, 56.789012345))
    hour, minute, second, nanosecond = result
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012345


def test_time_with_numeric_time_offset_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.hour, x.minute, x.second, "
                         "        x.nanosecond, x.offset]",
                         x=Time(12, 34, 56.789012345, tzinfo=FixedOffset(90)))
    hour, minute, second, nanosecond, offset = result
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012345
    assert offset == "+01:30"


def test_time_array_input(cypher_eval):
    data = [Time(12, 34, 56), Time(10, 0, 0)]
    value = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)
    assert value == data


def test_native_datetime_input(cypher_eval):
    from datetime import datetime
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day, "
                         "        x.hour, x.minute, x.second, x.nanosecond]",
                         x=datetime(1976, 6, 13, 12, 34, 56, 789012))
    year, month, day, hour, minute, second, nanosecond = result
    assert year == 1976
    assert month == 6
    assert day == 13
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012000


def test_whole_second_datetime_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day, "
                         "        x.hour, x.minute, x.second]",
                         x=DateTime(1976, 6, 13, 12, 34, 56))
    year, month, day, hour, minute, second = result
    assert year == 1976
    assert month == 6
    assert day == 13
    assert hour == 12
    assert minute == 34
    assert second == 56


def test_nanosecond_resolution_datetime_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day, "
                         "        x.hour, x.minute, x.second, x.nanosecond]",
                         x=DateTime(1976, 6, 13, 12, 34, 56.789012345))
    year, month, day, hour, minute, second, nanosecond = result
    assert year == 1976
    assert month == 6
    assert day == 13
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012345


def test_datetime_with_numeric_time_offset_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day, "
                         "        x.hour, x.minute, x.second, "
                         "        x.nanosecond, x.offset]",
                         x=DateTime(1976, 6, 13, 12, 34, 56.789012345,
                                    tzinfo=FixedOffset(90)))
    year, month, day, hour, minute, second, nanosecond, offset = result
    assert year == 1976
    assert month == 6
    assert day == 13
    assert hour == 12
    assert minute == 34
    assert second == 56
    assert nanosecond == 789012345
    assert offset == "+01:30"


def test_datetime_with_named_time_zone_input(cypher_eval):
    dt = DateTime(1976, 6, 13, 12, 34, 56.789012345)
    input_value = timezone("US/Pacific").localize(dt)
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.year, x.month, x.day, "
                         "        x.hour, x.minute, x.second, "
                         "        x.nanosecond, x.timezone]",
                         x=input_value)
    year, month, day, hour, minute, second, nanosecond, tz = result
    assert year == input_value.year
    assert month == input_value.month
    assert day == input_value.day
    assert hour == input_value.hour
    assert minute == input_value.minute
    assert second == int(input_value.second)
    assert nanosecond == int(1000000000 * input_value.second % 1000000000)
    assert tz == input_value.tzinfo.zone


def test_datetime_array_input(cypher_eval):
    data = [DateTime(2018, 4, 6, 13, 4, 42.516120), DateTime(1976, 6, 13)]
    value = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)
    assert value == data


def test_duration_input(cypher_eval):
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.months, x.days, x.seconds, "
                         "        x.microsecondsOfSecond]",
                         x=Duration(years=1, months=2, days=3, hours=4,
                                    minutes=5, seconds=6.789012))
    months, days, seconds, microseconds = result
    assert months == 14
    assert days == 3
    assert seconds == 14706
    assert microseconds == 789012


def test_duration_array_input(cypher_eval):
    data = [Duration(1, 2, 3, 4, 5, 6), Duration(9, 8, 7, 6, 5, 4)]
    value = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)
    assert value == data


def test_timedelta_input(cypher_eval):
    from datetime import timedelta
    result = cypher_eval("CYPHER runtime=interpreted WITH $x AS x "
                         "RETURN [x.months, x.days, x.seconds, "
                         "        x.microsecondsOfSecond]",
                         x=timedelta(days=3, hours=4, minutes=5,
                                     seconds=6.789012))
    months, days, seconds, microseconds = result
    assert months == 0
    assert days == 3
    assert seconds == 14706
    assert microseconds == 789012


def test_mixed_array_input(cypher_eval):
    data = [Date(1976, 6, 13), Duration(9, 8, 7, 6, 5, 4)]
    with raises(CypherTypeError):
        _ = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)


def test_date_output(cypher_eval):
    value = cypher_eval("RETURN date('1976-06-13')")
    assert isinstance(value, Date)
    assert value == Date(1976, 6, 13)


def test_whole_second_time_output(cypher_eval):
    value = cypher_eval("RETURN time('12:34:56')")
    assert isinstance(value, Time)
    assert value == Time(12, 34, 56, tzinfo=FixedOffset(0))


def test_nanosecond_resolution_time_output(cypher_eval):
    value = cypher_eval("RETURN time('12:34:56.789012345')")
    assert isinstance(value, Time)
    assert value == Time(12, 34, 56.789012345, tzinfo=FixedOffset(0))


def test_time_with_numeric_time_offset_output(cypher_eval):
    value = cypher_eval("RETURN time('12:34:56.789012345+0130')")
    assert isinstance(value, Time)
    assert value == Time(12, 34, 56.789012345, tzinfo=FixedOffset(90))


def test_whole_second_localtime_output(cypher_eval):
    value = cypher_eval("RETURN localtime('12:34:56')")
    assert isinstance(value, Time)
    assert value == Time(12, 34, 56)


def test_nanosecond_resolution_localtime_output(cypher_eval):
    value = cypher_eval("RETURN localtime('12:34:56.789012345')")
    assert isinstance(value, Time)
    assert value == Time(12, 34, 56.789012345)


def test_whole_second_datetime_output(cypher_eval):
    value = cypher_eval("RETURN datetime('1976-06-13T12:34:56')")
    assert isinstance(value, DateTime)
    assert value == DateTime(1976, 6, 13, 12, 34, 56, tzinfo=utc)


def test_nanosecond_resolution_datetime_output(cypher_eval):
    value = cypher_eval("RETURN datetime('1976-06-13T12:34:56.789012345')")
    assert isinstance(value, DateTime)
    assert value == DateTime(1976, 6, 13, 12, 34, 56.789012345, tzinfo=utc)


def test_datetime_with_numeric_time_offset_output(cypher_eval):
    value = cypher_eval("RETURN "
                        "datetime('1976-06-13T12:34:56.789012345+01:30')")
    assert isinstance(value, DateTime)
    assert value == DateTime(1976, 6, 13, 12, 34, 56.789012345,
                             tzinfo=FixedOffset(90))


def test_datetime_with_named_time_zone_output(cypher_eval):
    value = cypher_eval("RETURN datetime('1976-06-13T12:34:56.789012345"
                        "[Europe/London]')")
    assert isinstance(value, DateTime)
    dt = DateTime(1976, 6, 13, 12, 34, 56.789012345)
    assert value == timezone("Europe/London").localize(dt)


def test_whole_second_localdatetime_output(cypher_eval):
    value = cypher_eval("RETURN localdatetime('1976-06-13T12:34:56')")
    assert isinstance(value, DateTime)
    assert value == DateTime(1976, 6, 13, 12, 34, 56)


def test_nanosecond_resolution_localdatetime_output(cypher_eval):
    value = cypher_eval("RETURN "
                        "localdatetime('1976-06-13T12:34:56.789012345')")
    assert isinstance(value, DateTime)
    assert value == DateTime(1976, 6, 13, 12, 34, 56.789012345)


def test_duration_output(cypher_eval):
    value = cypher_eval("RETURN duration('P1Y2M3DT4H5M6.789S')")
    assert isinstance(value, Duration)
    assert value == Duration(years=1, months=2, days=3, hours=4,
                             minutes=5, seconds=6.789)


def test_nanosecond_resolution_duration_output(cypher_eval):
    value = cypher_eval("RETURN duration('P1Y2M3DT4H5M6.789123456S')")
    assert isinstance(value, Duration)
    assert value == Duration(years=1, months=2, days=3, hours=4,
                             minutes=5, seconds=6.789123456)
