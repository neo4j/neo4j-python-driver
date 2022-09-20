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


from ..v1.temporal import *


def hydrate_datetime(seconds, nanoseconds, tz=None):  # type: ignore[no-redef]
    """ Hydrator for `DateTime` and `LocalDateTime` values.

    :param seconds:
    :param nanoseconds:
    :param tz:
    :return: datetime
    """
    import pytz

    minutes, seconds = map(int, divmod(seconds, 60))
    hours, minutes = map(int, divmod(minutes, 60))
    days, hours = map(int, divmod(hours, 24))
    t = DateTime.combine(
        Date.from_ordinal(get_date_unix_epoch_ordinal() + days),
        Time(hours, minutes, seconds, nanoseconds)
    )
    if tz is None:
        return t
    if isinstance(tz, int):
        tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
        zone = pytz.FixedOffset(tz_offset_minutes)
    else:
        zone = pytz.timezone(tz)
    t = t.replace(tzinfo=pytz.UTC)
    return t.as_timezone(zone)


def dehydrate_datetime(value):  # type: ignore[no-redef]
    """ Dehydrator for `datetime` values.

    :param value:
    :type value: datetime
    :return:
    """

    import pytz

    def seconds_and_nanoseconds(dt):
        if isinstance(dt, datetime):
            dt = DateTime.from_native(dt)
        dt = dt.astimezone(pytz.UTC)
        utc_epoch = DateTime(1970, 1, 1, tzinfo=pytz.UTC)
        dt_clock_time = dt.to_clock_time()
        utc_epoch_clock_time = utc_epoch.to_clock_time()
        t = dt_clock_time - utc_epoch_clock_time
        return t.seconds, t.nanoseconds

    tz = value.tzinfo
    if tz is None:
        # without time zone
        value = pytz.UTC.localize(value)
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"d", seconds, nanoseconds)
    elif hasattr(tz, "zone") and tz.zone and isinstance(tz.zone, str):
        # with named pytz time zone
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"i", seconds, nanoseconds, tz.zone)
    elif hasattr(tz, "key") and tz.key and isinstance(tz.key, str):
        # with named zoneinfo (Python 3.9+) time zone
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"i", seconds, nanoseconds, tz.key)
    else:
        # with time offset
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        offset = tz.utcoffset(value)
        if offset.microseconds:
            raise ValueError("Bolt protocol does not support sub-second "
                             "UTC offsets.")
        offset_seconds = offset.days * 86400 + offset.seconds
        return Structure(b"I", seconds, nanoseconds, offset_seconds)
