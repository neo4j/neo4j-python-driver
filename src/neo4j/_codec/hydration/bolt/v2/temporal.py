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


from datetime import datetime

from ....._optional_deps import pd
from .....time import (
    Date,
    DateTime,
    Time,
)
from ....packstream import Structure
from ..._common import warn_unsupported_tzinfo_impl
from ..v1.temporal import get_date_unix_epoch_ordinal


def hydrate_datetime(seconds, nanoseconds, tz=None):
    """
    Hydrator for ``DateTime`` and ``LocalDateTime`` values.

    :param seconds:
    :param nanoseconds:
    :param tz:
    :returns: datetime
    """
    import pytz

    minutes, seconds = map(int, divmod(seconds, 60))
    hours, minutes = map(int, divmod(minutes, 60))
    days, hours = map(int, divmod(hours, 24))
    t = DateTime.combine(
        Date.from_ordinal(get_date_unix_epoch_ordinal() + days),
        Time(hours, minutes, seconds, nanoseconds),
    )
    if tz is None:
        return t
    if isinstance(tz, int):
        tz_offset_minutes, _tz_offset_seconds = divmod(tz, 60)
        zone = pytz.FixedOffset(tz_offset_minutes)
    else:
        zone = pytz.timezone(tz)
    t = t.replace(tzinfo=pytz.UTC)
    return t.as_timezone(zone)


def dehydrate_datetime(value):
    """
    Dehydrator for ``datetime`` values.

    :param value:
    :type value: datetime
    :returns:
    """
    import pytz

    def seconds_and_nanoseconds(dt):
        dt = dt.astimezone(pytz.utc)
        if isinstance(dt, datetime):
            utc_epoch = pytz.utc.localize(datetime(1970, 1, 1))
            t = dt - utc_epoch
            seconds = t.days * 86400 + t.seconds
            nanoseconds = t.microseconds * 1000
            return seconds, nanoseconds
        utc_epoch = DateTime(1970, 1, 1, tzinfo=pytz.utc)
        dt_clock_time = dt._to_clock_time()
        utc_epoch_clock_time = utc_epoch._to_clock_time()
        t = dt_clock_time - utc_epoch_clock_time
        return t.seconds, t.nanoseconds

    tz = value.tzinfo
    if tz is None:
        # without time zone
        value = pytz.utc.localize(value)
    seconds, nanoseconds = seconds_and_nanoseconds(value)
    return _datetime_structure(value, seconds, nanoseconds, tz)


def _datetime_structure(value, seconds, nanoseconds, tz):
    if tz is None:
        return Structure(b"d", seconds, nanoseconds)
    elif hasattr(tz, "zone") and tz.zone and isinstance(tz.zone, str):
        # with named pytz time zone
        return Structure(b"i", seconds, nanoseconds, tz.zone)
    elif hasattr(tz, "key") and tz.key and isinstance(tz.key, str):
        # with named zoneinfo (Python 3.9+) time zone
        return Structure(b"i", seconds, nanoseconds, tz.key)
    else:
        # with time offset
        offset = tz.utcoffset(None)
        if offset is None:
            # Non-constant offset, but we don't have explicit handling for this
            # tzinfo implementation => we cannot extract the time zone name
            warn_unsupported_tzinfo_impl(tz)
            offset = tz.utcoffset(value)
            if offset is None:
                raise ValueError(
                    f"tzinfo {tz!r} was unable to produce a UTC offset given "
                    f"the datetime {value!r}. Therefore, serialization of "
                    "this datetime is not possible."
                )
        if offset.microseconds:
            raise ValueError(
                "Bolt protocol does not support sub-second UTC offsets."
            )
        offset_seconds = offset.days * 86400 + offset.seconds
        return Structure(b"I", seconds, nanoseconds, offset_seconds)


if pd is not None:

    def dehydrate_pandas_datetime(value):
        """
        Dehydrator for ``pandas.Timestamp`` values.

        :param value:
        :type value: pandas.Timestamp
        :returns:
        """
        if pd.isna(value):
            return None
        tz = value.tzinfo
        if tz is None:
            value = value.tz_localize("UTC")
        utc_epoch = pd.Timestamp(1970, 1, 1).tz_localize("UTC")
        t = value - utc_epoch
        seconds = t.days * 86400 + t.seconds
        nanoseconds = t.microseconds * 1000 + t.nanoseconds
        return _datetime_structure(value, seconds, nanoseconds, tz)
