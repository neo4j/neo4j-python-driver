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

from neo4j.packstream.structure import Structure


UNIX_EPOCH_DATE = date(1970, 1, 1)
UNIX_EPOCH_DATETIME_UTC = datetime(1970, 1, 1, 0, 0, 0, 0, utc)

duration = namedtuple("duration", ("years", "months", "days", "hours", "minutes", "seconds"))


def hydrate_date(days):
    """ Hydrator for `Date` values.

    :param days:
    :return: date
    """
    return UNIX_EPOCH_DATE + timedelta(days=days)


def dehydrate_date(value):
    """ Dehydrator for `date` values.

    :param value:
    :type value: date
    :return:
    """
    delta = value - UNIX_EPOCH_DATE
    return Structure(b"D", delta.days)


def hydrate_time(nanoseconds, tz=None):
    """ Hydrator for `Time` and `LocalTime` values.

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
    return FixedOffset(tz_offset_minutes).localize(t)


def dehydrate_time(value):
    """ Dehydrator for `time` values.

    :param value:
    :type value: time
    :return:
    """
    minutes = 60 * value.hour + value.minute
    seconds = 60 * minutes + value.second
    microseconds = 1000000 * seconds + value.microsecond
    nanoseconds = 1000 * microseconds
    if value.tzinfo:
        return Structure(b"T", nanoseconds, value.tzinfo.utcoffset(value).seconds)
    else:
        return Structure(b"t", nanoseconds)


def hydrate_datetime(seconds, nanoseconds, tz=None):
    """ Hydrator for `DateTime` and `LocalDateTime` values.

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
    t = datetime.combine(UNIX_EPOCH_DATE + timedelta(days=days), time(hours, minutes, seconds, microseconds))
    if tz is None:
        return t
    if isinstance(tz, int):
        tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
        zone = FixedOffset(tz_offset_minutes)
    else:
        zone = timezone(tz)
    zoned_datetime = utc.localize(t).astimezone(zone)
    return zoned_datetime


def dehydrate_datetime(value):
    """ Dehydrator for `datetime` values.

    :param value:
    :type value: datetime
    :return:
    """

    def seconds_and_nanoseconds(dt):
        whole_seconds, fraction_of_second = divmod((dt - UNIX_EPOCH_DATETIME_UTC).total_seconds(), 1)
        return int(whole_seconds), int(1000000000 * fraction_of_second)

    # Save the TZ info as this will get lost during the conversion to UTC
    tz = value.tzinfo
    if tz is None:
        # without time zone
        value = utc.localize(value)
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"d", seconds, nanoseconds)
    elif hasattr(tz, "zone") and tz.zone:
        # with named time zone
        value = value.astimezone(utc)
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"f", seconds, nanoseconds, tz.zone)
    else:
        # with time offset
        value = value.astimezone(utc)
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"F", seconds, nanoseconds, tz.utcoffset(value).seconds)


def hydrate_duration(months, days, seconds, nanoseconds):
    """ Hydrator for `Duration` values.

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


def dehydrate_duration(value):
    """ Dehydrator for `duration` values.

    :param value:
    :type value: duration
    :return:
    """
    months = 12 * value.years + value.months
    days = value.days
    seconds, fraction_of_second = divmod(value.seconds, 1)
    seconds = 60 * (60 * value.hours + value.minutes) + int(seconds)
    nanoseconds = 1000 * int(round(1000000 * fraction_of_second))
    return Structure(b"E", months, days, seconds, nanoseconds)


def dehydrate_timedelta(value):
    """ Dehydrator for `timedelta` values.

    :param value:
    :type value: timedelta
    :return:
    """
    months = 0
    days = value.days
    seconds = value.seconds
    nanoseconds = 1000 * value.microseconds
    return Structure(b"E", months, days, seconds, nanoseconds)


__hydration_functions = {
    b"D": hydrate_date,
    b"T": hydrate_time,         # time zone offset
    b"t": hydrate_time,         # no time zone
    b"F": hydrate_datetime,     # time zone offset
    b"f": hydrate_datetime,     # time zone name
    b"d": hydrate_datetime,     # no time zone
    b"E": hydrate_duration,
}

__dehydration_functions = {
    date: dehydrate_date,
    time: dehydrate_time,
    datetime: dehydrate_datetime,
    duration: dehydrate_duration,
    timedelta: dehydrate_timedelta,
}


def hydration_functions():
    return __hydration_functions


def dehydration_functions():
    return __dehydration_functions
