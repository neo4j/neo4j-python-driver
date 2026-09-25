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
import re

from ..... import _typing as t
from ....._exceptions import QueryApiHttpError
from ....._optional_deps import (
    np,
    pd,
)
from .....time import (
    _NANO_SECONDS,
    Date,
    DateTime,
    Duration,
    MAX_INT64,
    MAX_YEAR,
    MIN_INT64,
    MIN_YEAR,
    Time,
)
from .._common import (
    make_value_dict,
    value_as_str,
)


if t.TYPE_CHECKING:
    import numpy

    from .._common import ValueDict


_ANY_BUILTIN_DATETIME = datetime.datetime(1970, 1, 1)


def hydrate_date(value: object) -> Date:
    value = value_as_str(value)
    try:
        return Date.parse(value)
    except Exception as e:
        raise QueryApiHttpError(f"expected date string, got: {value!r}") from e


def dehydrate_date(value: Date | datetime.date) -> ValueDict[str]:
    return make_value_dict("Date", value.isoformat())


_ZONED_TIME_RE = re.compile(
    r"^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?"
    r"(?:(Z)|([+-])(\d{2})(?::?(\d{2}))?(?::?(\d{2}))?)$"
)


def hydrate_zoned_time(value: object) -> Time:
    from pytz import FixedOffset

    value = value_as_str(value)
    match = _ZONED_TIME_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected zoned datetime string, got: {value!r}"
        )
    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3) or 0)
    nanosecond = 0
    if match.group(4):
        nanosecond = int(match.group(4)[:9].ljust(9, "0"))

    if match.group(5) == "Z":
        offset_min = offset_sec = 0
    else:
        offset = (
            int(match.group(7)) * 3600
            + int(match.group(8) or 0) * 60
            + int(match.group(9) or 0)
        )
        offset_min, offset_sec = divmod(offset, 60)
        if match.group(6) == "-":
            offset_min *= -1
            offset_sec *= -1

    tzinfo: datetime.tzinfo = FixedOffset(offset_min)
    return Time(hour, minute, second, nanosecond, tzinfo)


_LOCAL_TIME_RE = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?$")


def hydrate_local_time(value: object) -> Time:
    value = value_as_str(value)
    match = _LOCAL_TIME_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected local datetime string, got: {value!r}"
        )
    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3) or 0)
    nanosecond = 0
    if match.group(4):
        nanosecond = int(match.group(4)[:9].ljust(9, "0"))

    return Time(hour, minute, second, nanosecond)


def dehydrate_time(value: Time | datetime.time) -> ValueDict[str]:
    tag = "LocalTime" if value.tzinfo is None else "Time"
    return make_value_dict(tag, value.isoformat())


_OFFSET_DATETIME_RE = re.compile(
    r"^(\d{4}|[+-]\d+)-(\d{2})-(\d{2})T"
    r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?"
    r"(?:(Z)|([+-])(\d{2})(?::?(\d{2}))?(?::?(\d{2}))?)$"
)


def hydrate_offset_datetime(value: object) -> DateTime:
    from pytz import (
        FixedOffset,
        UTC,
    )

    value = value_as_str(value)
    match = _OFFSET_DATETIME_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected offset datetime string, got: {value!r}"
        )

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    date = Date(year, month, day)
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6) or 0)
    nanosecond = 0
    if match.group(7):
        nanosecond = int(match.group(7)[:9].ljust(9, "0"))
    time_utc = Time(hour, minute, second, nanosecond)
    dt_utc = t.cast(DateTime, UTC.localize(DateTime.combine(date, time_utc)))

    if match.group(8) == "Z":
        offset_min = offset_sec = 0
    else:
        offset = (
            int(match.group(10)) * 3600
            + int(match.group(11) or 0) * 60
            + int(match.group(12) or 0)
        )
        offset_min, offset_sec = divmod(offset, 60)
        if match.group(9) == "-":
            offset_min *= -1
            offset_sec *= -1
        dt_utc -= Duration(minutes=offset_min, seconds=offset_sec)
    zone: datetime.tzinfo = FixedOffset(offset_min)
    zoned_dt = dt_utc.as_timezone(zone)
    assert isinstance(zoned_dt, DateTime)
    return zoned_dt


_ZONED_DATETIME_RE = re.compile(
    r"^(\d{4}|[+-]\d+)-(\d{2})-(\d{2})T"
    r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?"
    r"(?:(Z)|([+-])(\d{2})(?::?(\d{2}))?(?::?(\d{2}))?)"
    r"(?:\[(.+)])?$"
)


def hydrate_zoned_datetime(value: object) -> DateTime:
    from pytz import (
        FixedOffset,
        timezone,
        UTC,
    )

    value = value_as_str(value)
    match = _ZONED_DATETIME_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected zoned datetime string, got: {value!r}"
        )

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    date = Date(year, month, day)
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6) or 0)
    nanosecond = 0
    if match.group(7):
        nanosecond = int(match.group(7)[:9].ljust(9, "0"))
    time_utc = Time(hour, minute, second, nanosecond)
    dt_utc = t.cast(DateTime, UTC.localize(DateTime.combine(date, time_utc)))

    if match.group(8) == "Z":
        offset_min = offset_sec = 0
    else:
        offset = (
            int(match.group(10)) * 3600
            + int(match.group(11) or 0) * 60
            + int(match.group(12) or 0)
        )
        offset_min, offset_sec = divmod(offset, 60)
        if match.group(9) == "-":
            offset_min *= -1
            offset_sec *= -1
        dt_utc -= Duration(minutes=offset_min, seconds=offset_sec)
    tz_name = match.group(13)
    zone: datetime.tzinfo = (
        timezone(tz_name) if tz_name else FixedOffset(offset_min)
    )
    zoned_dt = dt_utc.as_timezone(zone)
    assert isinstance(zoned_dt, DateTime)
    return zoned_dt


_LOCAL_DATETIME_RE = re.compile(
    r"^(\d{4}|[+-]\d+)-(\d{2})-(\d{2})T"
    r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+)?)?)?$"
)


def hydrate_local_datetime(value: object) -> DateTime:
    value = value_as_str(value)
    match = _LOCAL_DATETIME_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected local datetime string, got: {value!r}"
        )

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    date = Date(year, month, day)
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6) or 0)
    nanosecond = 0
    if match.group(7):
        nanosecond = int(match.group(7)[:9].ljust(9, "0"))
    time_ = Time(hour, minute, second, nanosecond)
    return DateTime.combine(date, time_)


def dehydrate_datetime(
    value: DateTime | datetime.datetime | pd.Timestamp,
) -> ValueDict[str]:
    tz = value.tzinfo
    if tz is None:
        return make_value_dict("LocalDateTime", value.isoformat())
    elif hasattr(tz, "zone") and tz.zone and isinstance(tz.zone, str):
        # with named pytz time zone
        return _dehydrate_zoned_datetime(value, tz.zone)
    elif hasattr(tz, "key") and tz.key and isinstance(tz.key, str):
        # with named zoneinfo (Python 3.9+) time zone
        return _dehydrate_zoned_datetime(value, tz.key)
    else:
        return _dehydrate_zoned_datetime(value)
    t.assert_never(tz)


def _dehydrate_local_datetime(
    value: DateTime | datetime.datetime | pd.Timestamp,
) -> ValueDict[str]:
    assert value.tzinfo is None
    return make_value_dict("LocalDateTime", value.isoformat())


def _dehydrate_zoned_datetime(
    value: DateTime | datetime.datetime | pd.Timestamp,
    zone_name: str | None = None,
) -> ValueDict[str]:
    tz = value.tzinfo
    assert tz is not None
    value = value.replace(tzinfo=None)
    offset: datetime.timedelta | None
    if isinstance(tz, datetime.timezone):
        # offset of the timezone is constant, so any date will do
        offset = tz.utcoffset(_ANY_BUILTIN_DATETIME)
    else:
        offset = tz.utcoffset(value)
    if not isinstance(offset, datetime.timedelta):
        raise TypeError(
            f"Unsupported: date time with unknown offset type: {type(offset)}"
        )
    value_str = value.isoformat()
    if zone_name is None:
        tz_str = _format_tz_offset(offset)
        return make_value_dict("OffsetDateTime", f"{value_str}{tz_str}")
    else:
        tz_str = _format_tzinfo(offset, zone_name)
        return make_value_dict("ZonedDateTime", f"{value_str}{tz_str}")


def _format_tzinfo(
    offset: datetime.timedelta,
    zone_name: str,
) -> str:
    tz_str = _format_tz_offset(offset)
    if zone_name is not None:
        tz_str += f"[{zone_name}]"
    return tz_str


def _format_tz_offset(
    offset: datetime.timedelta,
) -> str:
    total_seconds = int(offset.total_seconds())
    if not -64800 <= total_seconds <= 64800:
        raise ValueError("Unsupported: timezone offset outside valid range")
    if total_seconds < 0:
        sign = "-"
        total_seconds = -total_seconds
    else:
        sign = "+"
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    tz_str = f"{sign}{hours:02}:{minutes:02}"
    if seconds != 0:
        tz_str += f":{seconds:02}"
    return tz_str


if np is not None:
    _NP_YEAR_MAX = np.datetime64(MAX_YEAR + 1 - 1970, "Y")
    _NP_YEAR_MIN = np.datetime64(MIN_YEAR - 1970, "Y")
    _NP_YEAR_DTYPE = np.dtype("datetime64[Y]")

    def dehydrate_np_datetime(
        value: numpy.datetime64,
    ) -> ValueDict[str | None]:
        """
        Dehydrator for ``numpy.datetime64`` values.

        :param value:
        :type value: numpy.datetime64
        :returns:
        """
        if np.isnat(value):
            return make_value_dict("Null", None)
        if not _NP_YEAR_MIN <= value.astype(_NP_YEAR_DTYPE) < _NP_YEAR_MAX:
            # while we could encode years outside the range, they would fail
            # when retrieved from the database.
            raise ValueError(
                f"Year out of range ({MIN_YEAR:d}..{MAX_YEAR:d}) found {value}"
            )
        value_str = str(value.astype("datetime64[s]"))
        assert "." not in value_str
        nanoseconds = (
            value.astype("datetime64[ns]").astype(np.int64) % _NANO_SECONDS
        )
        if nanoseconds:
            value_str += f".{nanoseconds:09d}".rstrip("0")

        return make_value_dict("LocalDateTime", value_str)


_DURATION_RE: t.Final[re.Pattern] = re.compile(
    r"^P(?!$)"
    r"(?:(?P<years>[+-]?\d+)Y)?"
    r"(?:(?P<months>[+-]?\d+)M)?"
    r"(?:(?P<weeks>[+-]?\d+)W)?"
    r"(?:(?P<days>[+-]?\d+)D)?"
    r"(T(?!$)"
    r"(?:(?P<hours>[+-]?\d+)H)?"
    r"(?:(?P<minutes>[+-]?\d+)M)?"
    r"(?:(?P<seconds>[+-]?\d+)(?P<sub_seconds>[.,]\d+)?S)?"
    r")?$"
)


def hydrate_duration(value: object) -> Duration:
    value = value_as_str(value)
    match = _DURATION_RE.match(value)
    if not match:
        raise QueryApiHttpError(f"expected duration string, got: {value!r}")

    ns = 0
    if match.group("sub_seconds"):
        ns = int(match.group("sub_seconds")[1:10].ljust(9, "0"))
    seconds_group = match.group("seconds")
    seconds = int(seconds_group or 0)
    if seconds_group and seconds_group.startswith("-"):
        ns *= -1
    return Duration(
        years=int(match.group("years") or 0),
        months=int(match.group("months") or 0),
        weeks=int(match.group("weeks") or 0),
        days=int(match.group("days") or 0),
        hours=int(match.group("hours") or 0),
        minutes=int(match.group("minutes") or 0),
        seconds=seconds,
        nanoseconds=ns,
    )


def dehydrate_duration(value: Duration) -> ValueDict[str]:
    return make_value_dict("Duration", value.iso_format())


def dehydrate_timedelta(value: datetime.timedelta) -> ValueDict[str]:
    days = value.days
    seconds = value.seconds
    microseconds = value.microseconds
    sec_sign = ""
    if microseconds:
        if seconds < 0:
            if seconds <= -MAX_INT64:
                raise OverflowError(
                    "Timedelta cannot be encoded: out of range"
                )
            sec_sign = "-"
            seconds = abs(seconds + 1)
            microseconds = 1_000_000 - value.microseconds
        sub_seconds = f".{microseconds:06d}".rstrip("0")
    else:
        sub_seconds = ""
    if not MIN_INT64 <= seconds <= MAX_INT64:
        raise OverflowError("Timedelta cannot be encoded: out of range")
    if not days:
        value_str = f"PT{sec_sign}{seconds}{sub_seconds}S"
    elif seconds or microseconds:
        value_str = f"P{days}DT{sec_sign}{seconds}{sub_seconds}S"
    else:
        value_str = f"P{days}D"
    return make_value_dict("Duration", value_str)


if np is not None:
    _NUMPY_DURATION_NS_FALLBACK = object()
    _NUMPY_DURATION_UNITS = {
        "Y": "years",
        "M": "months",
        "W": "weeks",
        "D": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
        "ms": "milliseconds",
        "us": "microseconds",
        "ns": "nanoseconds",
        "ps": _NUMPY_DURATION_NS_FALLBACK,
        "fs": _NUMPY_DURATION_NS_FALLBACK,
        "as": _NUMPY_DURATION_NS_FALLBACK,
    }

    def dehydrate_np_timedelta(value):
        if np.isnat(value):
            return make_value_dict("Null", None)
        unit, step_size = np.datetime_data(value)
        numer = int(value.astype(np.int64))
        try:
            kwarg = _NUMPY_DURATION_UNITS[unit]
        except KeyError:
            raise TypeError(
                f"Unsupported numpy.timedelta64 unit: {unit!r}"
            ) from None
        if kwarg is _NUMPY_DURATION_NS_FALLBACK:
            nanoseconds = value.astype("timedelta64[ns]").astype(np.int64)
            return dehydrate_duration(Duration(nanoseconds=nanoseconds))
        return dehydrate_duration(Duration(**{kwarg: numer * step_size}))


if pd is not None:

    def dehydrate_pandas_timedelta(value):
        return dehydrate_duration(Duration(nanoseconds=value.value))
