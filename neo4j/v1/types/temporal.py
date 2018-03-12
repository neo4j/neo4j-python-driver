#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


"""
This module defines temporal data types.
"""

from collections import namedtuple
from datetime import date, datetime, time, timedelta
from warnings import warn

from pytz import FixedOffset, timezone, utc


UNIX_EPOCH = date.fromtimestamp(0)

duration = namedtuple("duration", ("years", "months", "days", "hours", "minutes", "seconds"))


def hydrate_date(days):
    """ Hydrant for `Date` values.

    :param days:
    :return: date
    """
    return UNIX_EPOCH + timedelta(days=days)


def hydrate_time(nanoseconds, tz=None):
    """ Hydrant for `Time` and `LocalTime` values.

    :param nanoseconds:
    :param tz:
    :return: time
    """
    microseconds, nanoseconds = divmod(nanoseconds, 1000)
    if nanoseconds != 0:
        warn("Nanosecond resolution is not available on this platform, value is truncated at microsecond resolution")
    seconds, microseconds = divmod(microseconds, 1000000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    t = time(hours, minutes, seconds, microseconds)
    if tz is None:
        return t
    tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
    zone = FixedOffset(tz_offset_minutes)
    zoned_datetime = utc.localize(datetime.combine(UNIX_EPOCH, t)).astimezone(zone)
    return zone.localize(zoned_datetime.time())


def hydrate_datetime(seconds, nanoseconds, tz=None):
    """ Hydrant for `DateTime` and `LocalDateTime` values.

    :param seconds:
    :param nanoseconds:
    :param tz:
    :return: datetime
    """
    microseconds, nanoseconds = divmod(nanoseconds, 1000)
    if nanoseconds != 0:
        warn("Nanosecond resolution is not available on this platform, value is truncated at microsecond resolution")
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    t = datetime.combine(UNIX_EPOCH + timedelta(days=days), time(hours, minutes, seconds, microseconds))
    if tz is None:
        return t
    if isinstance(tz, int):
        tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
        zone = FixedOffset(tz_offset_minutes)
    else:
        zone = timezone(tz)
    zoned_datetime = utc.localize(t).astimezone(zone)
    return zoned_datetime


def hydrate_duration(months, days, seconds, nanoseconds):
    """ Hydrant for `Duration` values.

    :param months:
    :param days:
    :param seconds:
    :param nanoseconds:
    :return: `duration` namedtuple
    """
    microseconds, nanoseconds = divmod(nanoseconds, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    years, months = divmod(months, 12)
    if nanoseconds != 0:
        warn("Nanosecond resolution is not available on this platform, value is truncated at microsecond resolution")
    return duration(years, months, days, hours, minutes, seconds + (microseconds / 1000000.0))


structures = {
    b"D": hydrate_date,
    b"T": hydrate_time,         # time zone offset
    b"t": hydrate_time,         # no time zone
    b"F": hydrate_datetime,     # time zone offset
    b"f": hydrate_datetime,     # time zone name
    b"d": hydrate_datetime,     # no time zone
    b"E": hydrate_duration,
}
