#!/usr/bin/env python
# coding: utf-8

# Copyright (c) "Neo4j"
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


"""
This module contains the fundamental types used for temporal accounting as well
as a number of utility functions.
"""

from contextlib import contextmanager
from datetime import (
    timedelta,
    date,
    time,
    datetime,
)
from decimal import (
    Decimal,
    localcontext,
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
)
from functools import total_ordering
from re import compile as re_compile
from time import (
    gmtime,
    mktime,
    struct_time,
)

from neo4j.meta import (
    deprecated,
    deprecation_warn
)
from neo4j.time.arithmetic import (
    nano_add,
    nano_div,
    symmetric_divmod,
    round_half_to_even,
)
from neo4j.time.metaclasses import (
    DateType,
    TimeType,
    DateTimeType,
)


@contextmanager
def _decimal_context(prec=9, rounding=ROUND_HALF_EVEN):
    with localcontext() as ctx:
        ctx.prec = prec
        ctx.rounding = rounding
        yield ctx


def _decimal_context_decorator(prec=9):
    def outer(fn):
        def inner(*args, **kwargs):
            with _decimal_context(prec=prec):
                return fn(*args, **kwargs)

        return inner
    return outer


MIN_INT64 = -(2 ** 63)
MAX_INT64 = (2 ** 63) - 1

MIN_YEAR = 1
"""
The smallest year number allowed in a :class:`.Date` or :class:`.DateTime`
object to be compatible with :class:`datetime.date` and
:class:`datetime.datetime`.
"""

MAX_YEAR = 9999
"""
The largest year number allowed in a :class:`.Date` or :class:`.DateTime`
object to be compatible with :class:`datetime.date` and
:class:`datetime.datetime`.
"""

DATE_ISO_PATTERN = re_compile(r"^(\d{4})-(\d{2})-(\d{2})$")
TIME_ISO_PATTERN = re_compile(
    r"^(\d{2})(:(\d{2})(:((\d{2})"
    r"(\.\d*)?))?)?(([+-])(\d{2}):(\d{2})(:((\d{2})(\.\d*)?))?)?$"
)
DURATION_ISO_PATTERN = re_compile(
    r"^P((\d+)Y)?((\d+)M)?((\d+)D)?"
    r"(T((\d+)H)?((\d+)M)?(((\d+)(\.\d+)?)?S)?)?$"
)

NANO_SECONDS = 1000000000
AVERAGE_SECONDS_IN_MONTH = 2629746
AVERAGE_SECONDS_IN_DAY = 86400


def _is_leap_year(year):
    if year % 4 != 0:
        return False
    if year % 100 != 0:
        return True
    return year % 400 == 0


IS_LEAP_YEAR = {year: _is_leap_year(year) for year in range(MIN_YEAR, MAX_YEAR + 1)}


def _days_in_year(year):
    return 366 if IS_LEAP_YEAR[year] else 365


DAYS_IN_YEAR = {year: _days_in_year(year) for year in range(MIN_YEAR, MAX_YEAR + 1)}


def _days_in_month(year, month):
    if month in (9, 4, 6, 11):
        return 30
    elif month != 2:
        return 31
    else:
        return 29 if IS_LEAP_YEAR[year] else 28


DAYS_IN_MONTH = {(year, month): _days_in_month(year, month)
                 for year in range(MIN_YEAR, MAX_YEAR + 1) for month in range(1, 13)}


def _normalize_day(year, month, day):
    """ Coerce the day of the month to an internal value that may or
    may not match the "public" value.

    With the exception of the last three days of every month, all
    days are stored as-is. The last three days are instead stored
    as -1 (the last), -2 (the second to last) and -3 (the third to
    last).

    Therefore, for a 28-day month, the last week is as follows:

        Day   | 22 23 24 25 26 27 28
        Value | 22 23 24 25 -3 -2 -1

    For a 29-day month, the last week is as follows:

        Day   | 23 24 25 26 27 28 29
        Value | 23 24 25 26 -3 -2 -1

    For a 30-day month, the last week is as follows:

        Day   | 24 25 26 27 28 29 30
        Value | 24 25 26 27 -3 -2 -1

    For a 31-day month, the last week is as follows:

        Day   | 25 26 27 28 29 30 31
        Value | 25 26 27 28 -3 -2 -1

    This slightly unintuitive system makes some temporal arithmetic
    produce a more desirable outcome.

    :param year:
    :param month:
    :param day:
    :return:
    """
    if year < MIN_YEAR or year > MAX_YEAR:
        raise ValueError("Year out of range (%d..%d)" % (MIN_YEAR, MAX_YEAR))
    if month < 1 or month > 12:
        raise ValueError("Month out of range (1..12)")
    days_in_month = DAYS_IN_MONTH[(year, month)]
    if day in (days_in_month, -1):
        return year, month, -1
    if day in (days_in_month - 1, -2):
        return year, month, -2
    if day in (days_in_month - 2, -3):
        return year, month, -3
    if 1 <= day <= days_in_month - 3:
        return year, month, int(day)
    # TODO improve this error message
    raise ValueError("Day %d out of range (1..%d, -1, -2 ,-3)" % (day, days_in_month))


class ClockTime(tuple):
    """ A count of `seconds` and `nanoseconds`. This class can be used to
    mark a particular point in time, relative to an externally-specified
    epoch.

    The `seconds` and `nanoseconds` values provided to the constructor can
    can have any sign but will be normalized internally into a positive or
    negative `seconds` value along with a positive `nanoseconds` value
    between `0` and `999,999,999`. Therefore ``ClockTime(-1, -1)`` is
    normalized to ``ClockTime(-2, 999999999)``.

    Note that the structure of a :class:`.ClockTime` object is similar to
    the ``timespec`` struct in C.
    """

    def __new__(cls, seconds=0, nanoseconds=0):
        seconds, nanoseconds = divmod(
            int(NANO_SECONDS * seconds) + int(nanoseconds), NANO_SECONDS
        )
        return tuple.__new__(cls, (seconds, nanoseconds))

    def __add__(self, other):
        if isinstance(other, (int, float)):
            other = ClockTime(other)
        if isinstance(other, ClockTime):
            return ClockTime(self.seconds + other.seconds, self.nanoseconds + other.nanoseconds)
        if isinstance(other, Duration):
            if other.months or other.days:
                raise ValueError("Cannot add Duration with months or days")
            return ClockTime(self.seconds + other.seconds, self.nanoseconds +
                             int(other.nanoseconds))
        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, (int, float)):
            other = ClockTime(other)
        if isinstance(other, ClockTime):
            return ClockTime(self.seconds - other.seconds, self.nanoseconds - other.nanoseconds)
        if isinstance(other, Duration):
            if other.months or other.days:
                raise ValueError("Cannot subtract Duration with months or days")
            return ClockTime(self.seconds - other.seconds, self.nanoseconds - int(other.nanoseconds))
        return NotImplemented

    def __repr__(self):
        return "ClockTime(seconds=%r, nanoseconds=%r)" % self

    @property
    def seconds(self):
        return self[0]

    @property
    def nanoseconds(self):
        return self[1]


class Clock:
    """ Accessor for time values. This class is fulfilled by implementations
    that subclass :class:`.Clock`. These implementations are contained within
    the ``neo4j.time.clock_implementations`` module, and are not intended to be
    accessed directly.

    Creating a new :class:`.Clock` instance will produce the highest
    precision clock implementation available.

        >>> clock = Clock()
        >>> type(clock)                                         # doctest: +SKIP
        neo4j.time.clock_implementations.LibCClock
        >>> clock.local_time()                                  # doctest: +SKIP
        ClockTime(seconds=1525265942, nanoseconds=506844026)

    """

    __implementations = None

    def __new__(cls):
        if cls.__implementations is None:
            # Find an available clock with the best precision
            import neo4j.time.clock_implementations
            cls.__implementations = sorted((clock for clock in Clock.__subclasses__() if clock.available()),
                                           key=lambda clock: clock.precision(), reverse=True)
        if not cls.__implementations:
            raise RuntimeError("No clock implementations available")
        instance = object.__new__(cls.__implementations[0])
        return instance

    @classmethod
    def precision(cls):
        """ The precision of this clock implementation, represented as a
        number of decimal places. Therefore, for a nanosecond precision
        clock, this function returns `9`.
        """
        raise NotImplementedError("No clock implementation selected")

    @classmethod
    def available(cls):
        """ A boolean flag to indicate whether or not this clock
        implementation is available on this platform.
        """
        raise NotImplementedError("No clock implementation selected")

    @classmethod
    def local_offset(cls):
        """The offset from UTC for local time read from this clock.
        This may raise OverflowError if not supported, because of platform depending C libraries.

        :returns:
        :rtype:

        :raises OverflowError:
        """
        # Adding and subtracting two days to avoid passing a pre-epoch time to
        # `mktime`, which can cause a `OverflowError` on some platforms (e.g.,
        # Windows).
        return ClockTime(-int(mktime(gmtime(172800))) + 172800)

    def local_time(self):
        """ Read and return the current local time from this clock, measured relative to the Unix Epoch.
        This may raise OverflowError if not supported, because of platform depending C libraries.

        :returns:
        :rtype:

        :raises OverflowError:
        """
        return self.utc_time() + self.local_offset()

    def utc_time(self):
        """ Read and return the current UTC time from this clock, measured
        relative to the Unix Epoch.
        """
        raise NotImplementedError("No clock implementation selected")


class Duration(tuple):
    """A difference between two points in time.

    A :class:`.Duration` represents the difference between two points in time.
    Duration objects store a composite value of `months`, `days`, `seconds`,
    and `nanoseconds`. Unlike :class:`datetime.timedelta` however, days, and
    seconds/nanoseconds are never interchanged. All values except seconds and
    nanoseconds are applied separately in calculations.

    A :class:`.Duration` stores four primary instance attributes internally:
    `months`, `days`, `seconds` and `nanoseconds`. These are maintained as
    individual values and are immutable. Each of these four attributes can carry
    its own siggn, with the exception of `nanoseconds`, which always has the same
    sign as `seconds`. The constructor will establish this state, should the
    duration be initialized with conflicting `seconds` and `nanoseconds` signs.
    This structure allows the modelling of durations such as
    `3 months minus 2 days`.

    To determine if a :class:`Duration` `d` is overflowing the accepted values
    of the database, first, all `nanoseconds` outside the range -999_999_999 and
    999_999_999 are transferred into the seconds field. Then, `months`, `days`,
    and `seconds` are summed up like so:
    `months * 2629746 + days * 86400 + d.seconds + d.nanoseconds // 1000000000`.
    (Like the integer division in Python, this one is to be understood as
    rounding down rather than towards 0.)
    This value must be between -(2\ :sup:`63`) and (2\ :sup:`63` - 1) inclusive.

    :param years: will be added times 12 to `months`
    :type years: float
    :param months: will be truncated to :class:`int` (`int(months)`)
    :type months: float
    :param weeks: will be added times 7 to `days`
    :type weeks: float
    :param days: will be truncated to :class:`int` (`int(days)`)
    :type days: float
    :param hours: will be added times 3,600,000,000,000 to `nanoseconds`
    :type hours: float
    :param minutes: will be added times 60,000,000,000 to `nanoseconds`
    :type minutes: float
    :param seconds: will be added times 1,000,000,000 to `nanoseconds``
    :type seconds: float
    :param subseconds: will be added times 1,000,000,000 to `nanosubseconds``

        .. deprecated:: 4.4
            Will be removed in 5.0. Use `milliseconds`, `microseconds`, or
            `nanoseconds` instead or add `subseconds` to `seconds`
    :type subseconds: float
    :param milliseconds: will be added times 1,000,000 to `nanoseconds`
    :type microseconds: float
    :param microseconds: will be added times 1,000 to `nanoseconds`
    :type milliseconds: float
    :param nanoseconds: will be truncated to :class:`int` (`int(nanoseconds)`)
    :type nanoseconds: float

    :raises ValueError: the components exceed the limits as described above.
    """

    # i64: i64:i64: i32

    min = None
    """The lowest duration value possible."""

    max = None
    """The highest duration value possible."""

    def __new__(cls, years=0, months=0, weeks=0, days=0, hours=0, minutes=0,
                seconds=0, subseconds=0, milliseconds=0, microseconds=0,
                nanoseconds=0):

        if subseconds:
            deprecation_warn("`subseconds` will be removed in 5.0. "
                             "Use `nanoseconds` instead.")
            with _decimal_context(prec=9, rounding=ROUND_HALF_EVEN):
                nanoseconds = int(Decimal(subseconds) * NANO_SECONDS)

        mo = int(12 * years + months)
        if mo < MIN_INT64 or mo > MAX_INT64:
            raise ValueError("Months value out of range")
        d = int(7 * weeks + days)
        ns = (int(3600000000000 * hours) +
              int(60000000000 * minutes) +
              int(1000000000 * seconds) +
              int(1000000 * milliseconds) +
              int(1000 * microseconds) +
              int(nanoseconds))
        s, ns = symmetric_divmod(ns, NANO_SECONDS)
        avg_total_seconds = (mo * AVERAGE_SECONDS_IN_MONTH
                             + d * AVERAGE_SECONDS_IN_DAY
                             + s
                             - (1 if ns < 0 else 0))
        if avg_total_seconds < MIN_INT64 or avg_total_seconds > MAX_INT64:
            raise ValueError("Duration value out of range: %r",
                             cls.__repr__((mo, d, s, ns)))
        return tuple.__new__(cls, (mo, d, s, ns))

    def __bool__(self):
        """Falsy if all primary instance attributes are."""
        return any(map(bool, self))

    __nonzero__ = __bool__

    def __add__(self, other):
        """Add a :class:`.Duration` or :class:`datetime.timedelta`.

        :rtype: Duration
        """
        if isinstance(other, Duration):
            return Duration(
                months=self[0] + int(other.months),
                days=self[1] + int(other.days),
                seconds=self[2] + int(other.seconds),
                nanoseconds=self[3] + int(other.nanoseconds)
            )
        if isinstance(other, timedelta):
            return Duration(
                months=self[0], days=self[1] + other.days,
                seconds=self[2] + other.seconds,
                nanoseconds=self[3] + other.microseconds * 1000
            )
        return NotImplemented

    def __sub__(self, other):
        """Subtract a :class:`.Duration` or :class:`datetime.timedelta`.

        :rtype: Duration
        """
        if isinstance(other, Duration):
            return Duration(
                months=self[0] - int(other.months),
                days=self[1] - int(other.days),
                seconds=self[2] - int(other.seconds),
                nanoseconds=self[3] - int(other.nanoseconds)
            )
        if isinstance(other, timedelta):
            return Duration(
                months=self[0],
                days=self[1] - other.days,
                seconds=self[2] - other.seconds,
                nanoseconds=self[3] - other.microseconds * 1000
            )
        return NotImplemented

    def __mul__(self, other):
        """Multiply by an :class:`int`.

        :rtype: Duration
        """
        if isinstance(other, float):
            deprecation_warn("Multiplication with float will be deprecated in "
                             "5.0.")
        if isinstance(other, (int, float)):
            return Duration(
                months=self[0] * other, days=self[1] * other,
                seconds=self[2] * other, nanoseconds=self[3] * other
            )
        return NotImplemented

    @deprecated("Will be removed in 5.0.")
    def __floordiv__(self, other):
        if isinstance(other, int):
            # TODO 5.0: new method (floor months, days, nanoseconds) or remove
            # return Duration(
            #     months=self[0] // other, days=self[1] // other,
            #     nanoseconds=(self[2] * NANO_SECONDS + self[3]) // other
            # )
            seconds = self[2] + Decimal(self[3]) / NANO_SECONDS
            return Duration(months=int(self[0] // other),
                            days=int(self[1] // other),
                            seconds=int(seconds // other))
        return NotImplemented

    @deprecated("Will be removed in 5.0.")
    def __mod__(self, other):
        if isinstance(other, int):
            # TODO 5.0: new method (mod months, days, nanoseconds) or remove
            # return Duration(
            #     months=self[0] % other, days=self[1] % other,
            #     nanoseconds=(self[2] * NANO_SECONDS + self[3]) % other
            # )
            seconds = self[2] + Decimal(self[3]) / NANO_SECONDS
            seconds, subseconds = symmetric_divmod(seconds % other, 1)
            return Duration(months=round_half_to_even(self[0] % other),
                            days=round_half_to_even(self[1] % other),
                            seconds=seconds, subseconds=subseconds)
        return NotImplemented

    @deprecated("Will be removed in 5.0.")
    def __divmod__(self, other):
        if isinstance(other, int):
            return self.__floordiv__(other), self.__mod__(other)
        return NotImplemented

    @deprecated("Will be removed in 5.0.")
    def __truediv__(self, other):
        if isinstance(other, (int, float)):
            return Duration(
                months=round_half_to_even(self[0] / other),
                days=round_half_to_even(self[1] / other),
                nanoseconds=round_half_to_even(
                    self[2] * NANO_SECONDS / other
                    + self[3] / other
                )
            )
        return NotImplemented

    __div__ = __truediv__

    def __pos__(self):
        """"""
        return self

    def __neg__(self):
        """"""
        return Duration(months=-self[0], days=-self[1], seconds=-self[2],
                        nanoseconds=-self[3])

    def __abs__(self):
        """"""
        return Duration(months=abs(self[0]), days=abs(self[1]),
                        seconds=abs(self[2]), nanoseconds=abs(self[3]))

    def __repr__(self):
        """"""
        return "Duration(months=%r, days=%r, seconds=%r, nanoseconds=%r)" % self

    def __str__(self):
        """"""
        return self.iso_format()

    def __copy__(self):
        return self.__new__(self.__class__, months=self[0], days=self[1],
                            seconds=self[2], nanoseconds=self[3])

    def __deepcopy__(self, memodict={}):
        return self.__copy__()

    @classmethod
    def from_iso_format(cls, s):
        """Parse a ISO formatted duration string.

        Accepted formats (all lowercase letters are placeholders):
            'P', a zero length duration
            'PyY', y being a number of years
            'PmM', m being a number of months
            'PdD', d being a number of days

            Any combination of the above, e.g., 'P25Y1D' for 25 years and 1 day.

            'PThH', h being a number of hours
            'PTmM', h being a number of minutes
            'PTsS', h being a number of seconds
            'PTs.sss...S', h being a fractional number of seconds

            Any combination of the above, e.g. 'PT5H1.2S' for 5 hours and 1.2
            seconds.
            Any combination of all options, e.g. 'P13MT100M' for 13 months and
            100 minutes.

        :param s: String to parse
        :type s: str

        :rtype: Duration

        :raises ValueError: if the string does not match the required format.
        """
        match = DURATION_ISO_PATTERN.match(s)
        if match:
            ns = 0
            if match.group(15):
                ns = int(match.group(15)[1:10].ljust(9, "0"))
            return cls(
                years=int(match.group(2) or 0),
                months=int(match.group(4) or 0),
                days=int(match.group(6) or 0),
                hours=int(match.group(9) or 0),
                minutes=int(match.group(11) or 0),
                seconds=int(match.group(14) or 0),
                nanoseconds=ns
            )
        raise ValueError("Duration string must be in ISO format")

    fromisoformat = from_iso_format

    def iso_format(self, sep="T"):
        """Return the :class:`Duration` as ISO formatted string.

        :param sep: the separator before the time components.
        :type sep: str

        :rtype: str
        """
        parts = []
        hours, minutes, seconds, nanoseconds = \
            self.hours_minutes_seconds_nanoseconds
        if hours:
            parts.append("%dH" % hours)
        if minutes:
            parts.append("%dM" % minutes)
        if nanoseconds:
            if seconds >= 0 and nanoseconds >= 0:
                parts.append("%d.%sS" %
                             (seconds,
                              str(nanoseconds).rjust(9, "0").rstrip("0")))
            elif seconds <= 0 and nanoseconds <= 0:
                parts.append("-%d.%sS" %
                             (abs(seconds),
                              str(abs(nanoseconds)).rjust(9, "0").rstrip("0")))

            else:
                assert False and "Please report this issue"
        elif seconds:
            parts.append("%dS" % seconds)
        if parts:
            parts.insert(0, sep)
        years, months, days = self.years_months_days
        if days:
            parts.insert(0, "%dD" % days)
        if months:
            parts.insert(0, "%dM" % months)
        if years:
            parts.insert(0, "%dY" % years)
        if parts:
            parts.insert(0, "P")
            return "".join(parts)
        else:
            return "PT0S"

    @property
    def months(self):
        """The months of the :class:`Duration`.

        :type: int
        """
        return self[0]

    @property
    def days(self):
        """The days of the :class:`Duration`.

        :type: int
        """
        return self[1]

    @property
    def seconds(self):
        """The seconds of the :class:`Duration`.

        :type: int
        """
        return self[2]

    @property
    @deprecated("Will be removed in 5.0. Use `nanoseconds` instead.")
    def subseconds(self):
        """The subseconds of the :class:`Duration`.

        .. deprecated:: 4.4
            Will be removed in 5.0. Use :attr:`.nanoseconds` instead.

        :type: decimal.Decimal
        """
        if self[3] < 0:
            return Decimal(("-0.%09i" % -self[3])[:11])
        else:
            return Decimal(("0.%09i" % self[3])[:11])

    @property
    def nanoseconds(self):
        """The nanoseconds of the :class:`Duration`.

        :type: int
        """
        return self[3]

    @property
    def years_months_days(self):
        """

        :return:
        """
        years, months = symmetric_divmod(self[0], 12)
        return years, months, self[1]

    @property
    @deprecated("Will be removed in 5.0. "
                "Use `hours_minutes_seconds_nanoseconds` instead.")
    def hours_minutes_seconds(self):
        """A 3-tuple of (hours, minutes, seconds).

        Where seconds is a :class:`decimal.Decimal` that combines `seconds` and
        `nanoseconds`.

        .. deprecated:: 4.4
            Will be removed in 5.0.
            Use :attr:`.hours_minutes_seconds_nanoseconds` instead.

        :type: (int, int, decimal.Decimal)
        """
        minutes, seconds = symmetric_divmod(self[2], 60)
        hours, minutes = symmetric_divmod(minutes, 60)
        with _decimal_context(prec=11):
            return hours, minutes, seconds + self.subseconds

    @property
    def hours_minutes_seconds_nanoseconds(self):
        """ A 4-tuple of (hours, minutes, seconds, nanoseconds).

        :type: (int, int, int, int)
        """
        minutes, seconds = symmetric_divmod(self[2], 60)
        hours, minutes = symmetric_divmod(minutes, 60)
        return hours, minutes, seconds, self[3]


Duration.min = Duration(seconds=MIN_INT64, nanoseconds=0)
Duration.max = Duration(seconds=MAX_INT64, nanoseconds=999999999)


class Date(metaclass=DateType):
    """Idealized date representation.

    A :class:`.Date` object represents a date (year, month, and day) in the
    `proleptic Gregorian Calendar
    <https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar>`_.

    Years between `0001` and `9999` are supported, with additional support for
    the "zero date" used in some contexts.

    Each date is based on a proleptic Gregorian ordinal, which models
    1 Jan 0001 as `day 1` and counts each subsequent day up to, and including,
    31 Dec 9999. The standard `year`, `month` and `day` value of each date is
    also available.

    Internally, the day of the month is always stored as-is, with the exception
    of the last three days of that month. These are always stored as
    -1, -2 and -3 (counting from the last day). This system allows some temporal
    arithmetic (particularly adding or subtracting months) to produce a more
    desirable outcome than would otherwise be produced. Externally, the day
    number is always the same as would be written on a calendar.

    :param year: the year. Minimum :attr:`.MIN_YEAR` (0001), maximum
        :attr:`.MAX_YEAR` (9999).
    :type year: int
    :param month: the month. Minimum 1, maximum 12.
    :type month: int
    :param day: the day. Minimum 1, maximum
        :attr:`Date.days_in_month(year, month) <Date.days_in_month>`.
    :type day: int

    A zero date can also be acquired by passing all zeroes to the
    :class:`neo4j.time.Date` constructor or by using the :attr:`ZeroDate`
    constant.
    """

    # CONSTRUCTOR #

    def __new__(cls, year, month, day):
        if year == month == day == 0:
            return ZeroDate
        year, month, day = _normalize_day(year, month, day)
        ordinal = cls.__calc_ordinal(year, month, day)
        return cls.__new(ordinal, year, month, day)

    @classmethod
    def __new(cls, ordinal, year, month, day):
        instance = object.__new__(cls)
        instance.__ordinal = int(ordinal)
        instance.__year = int(year)
        instance.__month = int(month)
        instance.__day = int(day)
        return instance

    def __getattr__(self, name):
        """ Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "isocalendar": self.iso_calendar,
                "isoformat": self.iso_format,
                "isoweekday": self.iso_weekday,
                "strftime": self.__format__,
                "toordinal": self.to_ordinal,
                "timetuple": self.time_tuple,
            }[name]
        except KeyError:
            raise AttributeError("Date has no attribute %r" % name)

    # CLASS METHODS #

    @classmethod
    def today(cls, tz=None):
        """Get the current date.

        :param tz: timezone or None to get a local :class:`.Date`.
        :type tz: datetime.tzinfo or None

        :rtype: Date

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls.from_clock_time(Clock().local_time(), UnixEpoch)
        else:
            return tz.fromutc(
                DateTime.from_clock_time(
                    Clock().utc_time(), UnixEpoch
                ).replace(tzinfo=tz)
            ).date()

    @classmethod
    def utc_today(cls):
        """Get the current date as UTC local date.

        :rtype: Date
        """
        return cls.from_clock_time(Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_timestamp(cls, timestamp, tz=None):
        """:class:`.Date` from a time stamp (seconds since unix epoch).

        :param timestamp: the unix timestamp (seconds since unix epoch).
        :type timestamp: float
        :param tz: timezone. Set to None to create a local :class:`.Date`.
        :type tz: datetime.tzinfo or None

        :rtype: Date

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls.from_clock_time(
                ClockTime(timestamp) + Clock().local_offset(), UnixEpoch
            )
        else:
            return tz.fromutc(
                DateTime.utc_from_timestamp(timestamp).replace(tzinfo=tz)
            ).date()

    @classmethod
    def utc_from_timestamp(cls, timestamp):
        """:class:`.Date` from a time stamp (seconds since unix epoch).

        Returns the `Date` as local date `Date` in UTC.

        :rtype: Date
        """
        return cls.from_clock_time((timestamp, 0), UnixEpoch)

    @classmethod
    def from_ordinal(cls, ordinal):
        """
        The :class:`.Date` that corresponds to the proleptic Gregorian ordinal.

        `0001-01-01` has ordinal 1 and `9999-12-31` has ordinal 3,652,059.
        Values outside of this range trigger a :exc:`ValueError`.
        The corresponding instance method for the reverse date-to-ordinal
        transformation is :meth:`.to_ordinal`.
        The ordinal 0 has a special semantic and will return :attr:`ZeroDate`.

        :rtype: Date

        :raises ValueError: if the ordinal is outside the range [0, 3652059]
            (both values included).
        """
        if ordinal == 0:
            return ZeroDate
        if ordinal >= 736695:
            year = 2018     # Project release year
            month = 1
            day = int(ordinal - 736694)
        elif ordinal >= 719163:
            year = 1970     # Unix epoch
            month = 1
            day = int(ordinal - 719162)
        else:
            year = 1
            month = 1
            day = int(ordinal)
        if day < 1 or day > 3652059:
            # Note: this requires a maximum of 22 bits for storage
            # Could be transferred in 3 bytes.
            raise ValueError("Ordinal out of range (1..3652059)")
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError("Year out of range (%d..%d)" % (MIN_YEAR, MAX_YEAR))
        days_in_year = DAYS_IN_YEAR[year]
        while day > days_in_year:
            day -= days_in_year
            year += 1
            days_in_year = DAYS_IN_YEAR[year]
        days_in_month = DAYS_IN_MONTH[(year, month)]
        while day > days_in_month:
            day -= days_in_month
            month += 1
            days_in_month = DAYS_IN_MONTH[(year, month)]
        year, month, day = _normalize_day(year, month, day)
        return cls.__new(ordinal, year, month, day)

    @classmethod
    def parse(cls, s):
        """Parse a string to produce a :class:`.Date`.

        Accepted formats:
            'Y-M-D'

        :param s: the string to be parsed.
        :type s: str

        :rtype: Date

        :raises ValueError: if the string could not be parsed.
        """
        try:
            numbers = map(int, s.split("-"))
        except (ValueError, AttributeError):
            raise ValueError("Date string must be in format YYYY-MM-DD")
        else:
            numbers = list(numbers)
            if len(numbers) == 3:
                return cls(*numbers)
            raise ValueError("Date string must be in format YYYY-MM-DD")

    @classmethod
    def from_iso_format(cls, s):
        """Parse a ISO formatted Date string.

        Accepted formats:
            'YYYY-MM-DD'

        :param s: the string to be parsed.
        :type s: str

        :rtype: Date

        :raises ValueError: if the string could not be parsed.
        """
        m = DATE_ISO_PATTERN.match(s)
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            return cls(year, month, day)
        raise ValueError("Date string must be in format YYYY-MM-DD")

    @classmethod
    def from_native(cls, d):
        """Convert from a native Python `datetime.date` value.

        :param d: the date to convert.
        :type d: datetime.date

        :rtype: Date
        """
        return Date.from_ordinal(d.toordinal())

    @classmethod
    def from_clock_time(cls, clock_time, epoch):
        """Convert from a ClockTime relative to a given epoch.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :type clock_time: ClockTime or (float, int)
        :param epoch: the epoch to which `clock_time` is relative
        :type epoch: DateTime

        :rtype: Date
        """
        try:
            clock_time = ClockTime(*clock_time)
        except (TypeError, ValueError):
            raise ValueError("Clock time must be a 2-tuple of (s, ns)")
        else:
            ordinal = clock_time.seconds // 86400
            return Date.from_ordinal(ordinal + epoch.date().to_ordinal())

    @classmethod
    def is_leap_year(cls, year):
        """Indicates whether or not `year` is a leap year.

        :param year: the year to look up
        :type year: int

        :rtype: bool

        :raises ValueError: if `year` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError("Year out of range (%d..%d)" % (MIN_YEAR, MAX_YEAR))
        return IS_LEAP_YEAR[year]

    @classmethod
    def days_in_year(cls, year):
        """Return the number of days in `year`.

        :param year: the year to look up
        :type year: int

        :rtype: int

        :raises ValueError: if `year` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError("Year out of range (%d..%d)" % (MIN_YEAR, MAX_YEAR))
        return DAYS_IN_YEAR[year]

    @classmethod
    def days_in_month(cls, year, month):
        """Return the number of days in `month` of `year`.

        :param year: the year to look up
        :type year: int
        :param year: the month to look up
        :type year: int

        :rtype: int

        :raises ValueError: if `year` or `month` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`;
            1 <= year <= 12
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError("Year out of range (%d..%d)" % (MIN_YEAR, MAX_YEAR))
        if month < 1 or month > 12:
            raise ValueError("Month out of range (1..12)")
        return DAYS_IN_MONTH[(year, month)]

    @classmethod
    def __calc_ordinal(cls, year, month, day):
        if day < 0:
            day = cls.days_in_month(year, month) + int(day) + 1
        # The built-in date class does this faster than a
        # long-hand pure Python algorithm could
        return date(year, month, day).toordinal()

    # CLASS ATTRIBUTES #

    min = None
    """The earliest date value possible."""

    max = None
    """The latest date value possible."""

    resolution = None
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    __ordinal = 0

    __year = 0

    __month = 0

    __day = 0

    @property
    def year(self):
        """The year of the date.

        :type: int
        """
        return self.__year

    @property
    def month(self):
        """The month of the date.

        :type: int
        """
        return self.__month

    @property
    def day(self):
        """The day of the date.

        :type: int
        """
        if self.__day == 0:
            return 0
        if self.__day >= 1:
            return self.__day
        return self.days_in_month(self.__year, self.__month) + self.__day + 1

    @property
    def year_month_day(self):
        """3-tuple of (year, month, day) describing the date.

        :rtype: (int, int, int)
        """
        return self.year, self.month, self.day

    @property
    def year_week_day(self):
        """3-tuple of (year, week_of_year, day_of_week) describing the date.

        `day_of_week` will be 1 for Monday and 7 for Sunday.

        :rtype: (int, int, int)
        """
        ordinal = self.__ordinal
        year = self.__year

        def day_of_week(o):
            return ((o - 1) % 7) + 1

        def iso_week_1(y):
            j4 = Date(y, 1, 4)
            return j4 + Duration(days=(1 - day_of_week(j4.to_ordinal())))

        if ordinal >= Date(year, 12, 29).to_ordinal():
            week1 = iso_week_1(year + 1)
            if ordinal < week1.to_ordinal():
                week1 = iso_week_1(year)
            else:
                year += 1
        else:
            week1 = iso_week_1(year)
            if ordinal < week1.to_ordinal():
                year -= 1
                week1 = iso_week_1(year)
        return (year, int((ordinal - week1.to_ordinal()) / 7 + 1),
                day_of_week(ordinal))

    @property
    def year_day(self):
        """2-tuple of (year, day_of_the_year) describing the date.

        This is the number of the day relative to the start of the year,
        with `1 Jan` corresponding to `1`.

        :rtype: (int, int)
        """
        return (self.__year,
                self.toordinal() - Date(self.__year, 1, 1).toordinal() + 1)

    # OPERATIONS #

    def __hash__(self):
        """"""
        return hash(self.toordinal())

    def __eq__(self, other):
        """`==` comparison with :class:`.Date` or :class:`datetime.date`."""
        if isinstance(other, (Date, date)):
            return self.toordinal() == other.toordinal()
        return False

    def __ne__(self, other):
        """`!=` comparison with :class:`.Date` or :class:`datetime.date`."""
        return not self.__eq__(other)

    def __lt__(self, other):
        """`<` comparison with :class:`.Date` or :class:`datetime.date`."""
        if isinstance(other, (Date, date)):
            return self.toordinal() < other.toordinal()
        raise TypeError("'<' not supported between instances of 'Date' and %r" % type(other).__name__)

    def __le__(self, other):
        """`<=` comparison with :class:`.Date` or :class:`datetime.date`."""
        if isinstance(other, (Date, date)):
            return self.toordinal() <= other.toordinal()
        raise TypeError("'<=' not supported between instances of 'Date' and %r" % type(other).__name__)

    def __ge__(self, other):
        """`>=` comparison with :class:`.Date` or :class:`datetime.date`."""
        if isinstance(other, (Date, date)):
            return self.toordinal() >= other.toordinal()
        raise TypeError("'>=' not supported between instances of 'Date' and %r" % type(other).__name__)

    def __gt__(self, other):
        """`>` comparison with :class:`.Date` or :class:`datetime.date`."""
        if isinstance(other, (Date, date)):
            return self.toordinal() > other.toordinal()
        raise TypeError("'>' not supported between instances of 'Date' and %r" % type(other).__name__)

    def __add__(self, other):
        """Add a :class:`.Duration`.

        :rtype: Date

        :raises ValueError: if the added duration has a time component.
        """
        def add_months(d, months):
            years, months = symmetric_divmod(months, 12)
            year = d.__year + years
            month = d.__month + months
            while month > 12:
                year += 1
                month -= 12
            while month < 1:
                year -= 1
                month += 12
            d.__year = year
            d.__month = month

        def add_days(d, days):
            assert 1 <= d.__day <= 28 or -28 <= d.__day <= -1
            if d.__day >= 1:
                new_days = d.__day + days
                if 1 <= new_days <= 27:
                    d.__day = new_days
                    return
            d0 = Date.from_ordinal(d.__ordinal + days)
            d.__year, d.__month, d.__day = d0.__year, d0.__month, d0.__day

        if isinstance(other, Duration):
            if other.seconds or other.nanoseconds:
                raise ValueError("Cannot add a Duration with seconds or "
                                 "nanoseconds to a Date")
            if other.months == other.days == 0:
                return self
            new_date = self.replace()
            # Add days before months as the former sometimes
            # requires the current ordinal to be correct.
            if other.days:
                add_days(new_date, other.days)
            if other.months:
                add_months(new_date, other.months)
            new_date.__ordinal = self.__calc_ordinal(new_date.year, new_date.month, new_date.day)
            return new_date
        return NotImplemented

    def __sub__(self, other):
        """Subtract a :class:`.Date` or :class:`.Duration`.

        :returns: If a :class:`.Date` is subtracted, the time between the two
            dates is returned as :class:`.Duration`. If a :class:`.Duration` is
            subtracted, a new :class:`.Date` is returned.
        :rtype: Date or Duration

        :raises ValueError: if the added duration has a time component.
        """
        if isinstance(other, (Date, date)):
            return Duration(days=(self.toordinal() - other.toordinal()))
        try:
            return self.__add__(-other)
        except TypeError:
            return NotImplemented

    def __copy__(self):
        return self.__new(self.__ordinal, self.__year, self.__month, self.__day)

    def __deepcopy__(self, *args, **kwargs):
        return self.__copy__()

    # INSTANCE METHODS #

    def replace(self, **kwargs):
        """Return a :class:`.Date` with one or more components replaced.

        :Keyword Arguments:
           * **year** (`int`): overwrite the year -
             default: `self.year`
           * **month** (`int`): overwrite the month -
             default: `self.month`
           * **day** (`int`): overwrite the day -
             default: `self.day`
        """
        return Date(kwargs.get("year", self.__year),
                    kwargs.get("month", self.__month),
                    kwargs.get("day", self.__day))

    def time_tuple(self):
        """Convert the date to :class:`time.struct_time`.

        :rtype: time.struct_time
        """
        _, _, day_of_week = self.year_week_day
        _, day_of_year = self.year_day
        return struct_time((self.year, self.month, self.day, 0, 0, 0, day_of_week - 1, day_of_year, -1))

    def to_ordinal(self):
        """The date's proleptic Gregorian ordinal.

        The corresponding class method for the reverse ordinal-to-date
        transformation is :meth:`.Date.from_ordinal`.

        :rtype: int
        """
        return self.__ordinal

    def to_clock_time(self, epoch):
        """Convert the date to :class:`ClockTime` relative to `epoch`.

        :param epoch: the epoch to which the date is relative
        :type epoch: Date

        :rtype: ClockTime
        """
        try:
            return ClockTime(86400 * (self.to_ordinal() - epoch.to_ordinal()))
        except AttributeError:
            raise TypeError("Epoch has no ordinal value")

    def to_native(self):
        """Convert to a native Python :class:`datetime.date` value.

        :rtype: datetime.date
        """
        return date.fromordinal(self.to_ordinal())

    def weekday(self):
        """The day of the week where Monday is 0 and Sunday is 6.

        :rtype: int
        """
        return self.year_week_day[2] - 1

    def iso_weekday(self):
        """The day of the week where Monday is 1 and Sunday is 7.

        :rtype: int
        """
        return self.year_week_day[2]

    def iso_calendar(self):
        """Alias for :attr:`.year_week_day`"""
        return self.year_week_day

    def iso_format(self):
        """Return the :class:`.Date` as ISO formatted string.

        :rtype: str
        """
        if self.__ordinal == 0:
            return "0000-00-00"
        return "%04d-%02d-%02d" % self.year_month_day

    def __repr__(self):
        """"""
        if self.__ordinal == 0:
            return "neo4j.time.ZeroDate"
        return "neo4j.time.Date(%r, %r, %r)" % self.year_month_day

    def __str__(self):
        """"""
        return self.iso_format()

    def __format__(self, format_spec):
        """"""
        raise NotImplementedError()


Date.min = Date.from_ordinal(1)
Date.max = Date.from_ordinal(3652059)
Date.resolution = Duration(days=1)

ZeroDate = object.__new__(Date)
"""
A :class:`neo4j.time.Date` instance set to `0000-00-00`.
This has an ordinal value of `0`.
"""


class Time(metaclass=TimeType):
    """Time of day.

    The :class:`.Time` class is a nanosecond-precision drop-in replacement for
    the standard library :class:`datetime.time` class.

    A high degree of API compatibility with the standard library classes is
    provided.

    :class:`neo4j.time.Time` objects introduce the concept of `ticks`.
    This is simply a count of the number of seconds since midnight,
    in many ways analogous to the :class:`neo4j.time.Date` ordinal.
    `ticks` values can be fractional, with a minimum value of `0` and a maximum
    of `86399.999999999`.

    Local times are represented by :class:`.Time` with no `tzinfo`.

    .. note::
        Staring with version 5.0, :attr:`.ticks` will change to be integers
        counting nanoseconds since midnight. Currently available as
        :attr:`.ticks_ns`.

    :param hour: the hour of the time. Must be in range 0 <= hour < 24.
    :type hour: int
    :param minute: the minute of the time. Must be in range 0 <= minute < 60.
    :type minute: int
    :param second: the second of the time. Here, a float is accepted to denote
        sub-second precision up to nanoseconds.
        Must be in range 0 <= second < 60.

        Starting with version 5 0, this parameter will only accept :class:`int`.
    :type second: float
    :param nanosecond: the nanosecond
        .Must be in range 0 <= nanosecond < 999999999.
    :type nanosecond: int
    :param tzinfo: timezone or None to get a local :class:`.Time`.
    :type tzinfo: datetime.tzinfo or None

    :raises ValueError: if one of the parameters is out of range.
    """

    # CONSTRUCTOR #

    def __new__(cls, hour=0, minute=0, second=0, nanosecond=0, tzinfo=None):
        hour, minute, second, nanosecond = cls.__normalize_nanosecond(
            hour, minute, second, nanosecond
        )
        ticks = (3600000000000 * hour
                 + 60000000000 * minute
                 + 1000000000 * second
                 + nanosecond)
        return cls.__new(ticks, hour, minute, second, nanosecond, tzinfo)

    @classmethod
    def __new(cls, ticks, hour, minute, second, nanosecond, tzinfo):
        instance = object.__new__(cls)
        instance.__ticks = int(ticks)
        instance.__hour = int(hour)
        instance.__minute = int(minute)
        instance.__second = int(second)
        instance.__nanosecond = int(nanosecond)
        instance.__tzinfo = tzinfo
        return instance

    def __getattr__(self, name):
        """Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "isoformat": self.iso_format,
                "utcoffset": self.utc_offset,
            }[name]
        except KeyError:
            raise AttributeError("Date has no attribute %r" % name)

    # CLASS METHODS #

    @classmethod
    def now(cls, tz=None):
        """Get the current time.

        :param tz: optional timezone
        :type tz: datetime.tzinfo
        :rtype: Time

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls.from_clock_time(Clock().local_time(), UnixEpoch)
        else:
            return tz.fromutc(DateTime.from_clock_time(Clock().utc_time(), UnixEpoch)).time().replace(tzinfo=tz)

    @classmethod
    def utc_now(cls):
        """Get the current time as UTC local time.

        :rtype: Time
        """
        return cls.from_clock_time(Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_iso_format(cls, s):
        """Parse a ISO formatted time string.

        Accepted formats:
            Local times:
                'hh'
                'hh:mm'
                'hh:mm:ss'
                'hh:mm:ss.ssss...'
            Times with timezones (UTC offset):
                '<local time>+hh:mm'
                '<local time>+hh:mm:ss'
                '<local time>+hh:mm:ss.ssss....'
                '<local time>-hh:mm'
                '<local time>-hh:mm:ss'
                '<local time>-hh:mm:ss.ssss....'

                Where the UTC offset will only respect hours and minutes.
                Seconds and sub-seconds are ignored.

        :param s: String to parse
        :type s: str

        :rtype: Time

        :raises ValueError: if the string does not match the required format.
        """
        from pytz import FixedOffset
        m = TIME_ISO_PATTERN.match(s)
        if m:
            hour = int(m.group(1))
            minute = int(m.group(3) or 0)
            second = int(m.group(6) or 0)
            nanosecond = m.group(7)
            if nanosecond:
                nanosecond = int(nanosecond[1:10].ljust(9, "0"))
            else:
                nanosecond = 0
            if m.group(8) is None:
                return cls(hour, minute, second, nanosecond)
            else:
                offset_multiplier = 1 if m.group(9) == "+" else -1
                offset_hour = int(m.group(10))
                offset_minute = int(m.group(11))
                # pytz only supports offsets of minute resolution
                # so we can ignore this part
                # offset_second = float(m.group(13) or 0.0)
                offset = 60 * offset_hour + offset_minute
                return cls(hour, minute, second, nanosecond,
                           tzinfo=FixedOffset(offset_multiplier * offset))
        raise ValueError("Time string is not in ISO format")

    @classmethod
    def from_ticks(cls, ticks, tz=None):
        """Create a time from legacy ticks (seconds since midnight).

        .. deprecated:: 4.4
            Staring from 5.0, this method's signature will be replaced with that
            of :meth:`.from_ticks_ns`.

        :param ticks: seconds since midnight
        :type ticks: float
        :param tz: optional timezone
        :type tz: datetime.tzinfo

        :rtype: Time

        :raises ValueError: if ticks is out of bounds (0 <= ticks < 86400)
        """
        if 0 <= ticks < 86400:
            ticks = Decimal(ticks) * NANO_SECONDS
            ticks = int(ticks.quantize(Decimal("1."), rounding=ROUND_HALF_EVEN))
            assert 0 <= ticks < 86400000000000
            return cls.from_ticks_ns(ticks, tz=tz)
        raise ValueError("Ticks out of range (0..86400)")

    @classmethod
    def from_ticks_ns(cls, ticks, tz=None):
        """Create a time from ticks (nanoseconds since midnight).

        :param ticks: nanoseconds since midnight
        :type ticks: int
        :param tz: optional timezone
        :type tz: datetime.tzinfo

        :rtype: Time

        :raises ValueError: if ticks is out of bounds
            (0 <= ticks < 86400000000000)
        """
        # TODO 5.0: this will become from_ticks
        if not isinstance(ticks, int):
            raise TypeError("Ticks must be int")
        if 0 <= ticks < 86400000000000:
            second, nanosecond = divmod(ticks, NANO_SECONDS)
            minute, second = divmod(second, 60)
            hour, minute = divmod(minute, 60)
            return cls.__new(ticks, hour, minute, second, nanosecond, tz)
        raise ValueError("Ticks out of range (0..86400000000000)")

    @classmethod
    def from_native(cls, t):
        """Convert from a native Python :class:`datetime.time` value.

        :param t: time to convert from
        :type t: datetime.time

        :rtype: Time
        """
        nanosecond = t.microsecond * 1000
        return Time(t.hour, t.minute, t.second, nanosecond, t.tzinfo)

    @classmethod
    def from_clock_time(cls, clock_time, epoch):
        """Convert from a :class:`.ClockTime` relative to a given epoch.

        This method, in contrast to most others of this package, assumes days of
        exactly 24 hours.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :type clock_time: ClockTime or (float, int)
        :param epoch: the epoch to which `clock_time` is relative
        :type epoch: DateTime

        :rtype: Time
        """
        clock_time = ClockTime(*clock_time)
        ts = clock_time.seconds % 86400
        nanoseconds = int(NANO_SECONDS * ts + clock_time.nanoseconds)
        ticks = (epoch.time().ticks_ns + nanoseconds) % (86400 * NANO_SECONDS)
        return Time.from_ticks_ns(ticks)

    @classmethod
    def __normalize_hour(cls, hour):
        hour = int(hour)
        if 0 <= hour < 24:
            return hour
        raise ValueError("Hour out of range (0..23)")

    @classmethod
    def __normalize_minute(cls, hour, minute):
        hour = cls.__normalize_hour(hour)
        minute = int(minute)
        if 0 <= minute < 60:
            return hour, minute
        raise ValueError("Minute out of range (0..59)")

    @classmethod
    def __normalize_second(cls, hour, minute, second):
        hour, minute = cls.__normalize_minute(hour, minute)
        second = int(second)
        if 0 <= second < 60:
            return hour, minute, second
        raise ValueError("Second out of range (0..59)")

    @classmethod
    def __normalize_nanosecond(cls, hour, minute, second, nanosecond):
        # TODO 5.0: remove -----------------------------------------------------
        seconds, extra_ns = divmod(second, 1)
        if extra_ns:
            deprecation_warn("Float support second will be removed in 5.0. "
                             "Use `nanosecond` instead.")
        # ----------------------------------------------------------------------
        hour, minute, second = cls.__normalize_second(hour, minute, second)
        nanosecond = int(nanosecond
                         + round_half_to_even(extra_ns * NANO_SECONDS))
        if 0 <= nanosecond < NANO_SECONDS:
            return hour, minute, second, nanosecond + extra_ns
        raise ValueError("Nanosecond out of range (0..%s)" % (NANO_SECONDS - 1))

    # CLASS ATTRIBUTES #

    min = None
    """The earliest time value possible."""

    max = None
    """The latest time value possible."""

    resolution = None
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    __ticks = 0

    __hour = 0

    __minute = 0

    __second = 0

    __nanosecond = 0

    __tzinfo = None

    @property
    def ticks(self):
        """The total number of seconds since midnight.

        .. deprecated:: 4.4
            will return :attr:`.ticks_ns` starting with version 5.0.

        :type: float
        """
        with _decimal_context(prec=15):
            return self.__ticks / NANO_SECONDS

    @property
    def ticks_ns(self):
        """The total number of nanoseconds since midnight.

        .. note:: This will be removed in 5.0 and replace :attr:`.ticks`.

        :type: int
        """
        # TODO 5.0: this will replace self.ticks
        return self.__ticks

    @property
    def hour(self):
        """The hours of the time.

        :type: int
        """
        return self.__hour

    @property
    def minute(self):
        """The minutes of the time.

        :type: int
        """
        return self.__minute

    @property
    def second(self):
        """The seconds of the time.

        This contains seconds and nanoseconds of the time.
        `int(:attr:`.seconds`)` will yield the seconds without nanoseconds.

        :type: float
        """
        # TODO 5.0: return plain self.__second
        with _decimal_context(prec=11):
            return self.__second + Decimal(("0.%09i" % self.__nanosecond)[:11])

    @property
    def nanosecond(self):
        """The nanoseconds of the time.

        :type: int
        """
        return self.__nanosecond

    @property
    @deprecated("hour_minute_second will be removed in 5.0. "
                "Use `hour_minute_second_nanosecond` instead.")
    def hour_minute_second(self):
        """The time as a tuple of (hour, minute, second).

        .. deprecated: 4.4
            Will be removed in 5.0.
            Use :attr:`.hour_minute_second_nanosecond` instead.

        :type: (int, int, float)"""
        return self.__hour, self.__minute, self.second

    @property
    def hour_minute_second_nanosecond(self):
        """The time as a tuple of (hour, minute, second, nanosecond).

        :type: (int, int, int, int)"""
        return self.__hour, self.__minute, self.__second, self.__nanosecond

    @property
    def tzinfo(self):
        """The timezone of this time.

        :type: datetime.tzinfo or None"""
        return self.__tzinfo

    # OPERATIONS #

    def __hash__(self):
        """"""
        return hash(self.__ticks) ^ hash(self.tzinfo)

    def __eq__(self, other):
        """`==` comparison with :class:`.Time` or :class:`datetime.time`."""
        if isinstance(other, Time):
            return self.__ticks == other.__ticks and self.tzinfo == other.tzinfo
        if isinstance(other, time):
            other_ticks = (3600000000000 * other.hour
                           + 60000000000 * other.minute
                           + NANO_SECONDS * other.second
                           + 1000 * other.microsecond)
            return self.ticks_ns == other_ticks and self.tzinfo == other.tzinfo
        return False

    def __ne__(self, other):
        """`!=` comparison with :class:`.Time` or :class:`datetime.time`."""
        return not self.__eq__(other)

    def __lt__(self, other):
        """`<` comparison with :class:`.Time` or :class:`datetime.time`."""
        if isinstance(other, Time):
            return (self.tzinfo == other.tzinfo
                    and self.ticks_ns < other.ticks_ns)
        if isinstance(other, time):
            if self.tzinfo != other.tzinfo:
                return False
            other_ticks = 3600 * other.hour + 60 * other.minute + other.second + (other.microsecond / 1000000)
            return self.ticks_ns < other_ticks
        return NotImplemented

    def __le__(self, other):
        """`<=` comparison with :class:`.Time` or :class:`datetime.time`."""
        if isinstance(other, Time):
            return (self.tzinfo == other.tzinfo
                    and self.ticks_ns <= other.ticks_ns)
        if isinstance(other, time):
            if self.tzinfo != other.tzinfo:
                return False
            other_ticks = 3600 * other.hour + 60 * other.minute + other.second + (other.microsecond / 1000000)
            return self.ticks_ns <= other_ticks
        return NotImplemented

    def __ge__(self, other):
        """`>=` comparison with :class:`.Time` or :class:`datetime.time`."""
        if isinstance(other, Time):
            return (self.tzinfo == other.tzinfo
                    and self.ticks_ns >= other.ticks_ns)
        if isinstance(other, time):
            if self.tzinfo != other.tzinfo:
                return False
            other_ticks = 3600 * other.hour + 60 * other.minute + other.second + (other.microsecond / 1000000)
            return self.ticks_ns >= other_ticks
        return NotImplemented

    def __gt__(self, other):
        """`>` comparison with :class:`.Time` or :class:`datetime.time`."""
        if isinstance(other, Time):
            return (self.tzinfo == other.tzinfo
                    and self.ticks_ns >= other.ticks_ns)
        if isinstance(other, time):
            if self.tzinfo != other.tzinfo:
                return False
            other_ticks = 3600 * other.hour + 60 * other.minute + other.second + (other.microsecond / 1000000)
            return self.ticks_ns >= other_ticks
        return NotImplemented

    def __copy__(self):
        return self.__new(self.__ticks, self.__hour, self.__minute,
                          self.__second, self.__nanosecond, self.__tzinfo)

    def __deepcopy__(self, *args, **kwargs):
        return self.__copy__()

    # INSTANCE METHODS #

    def replace(self, **kwargs):
        """Return a :class:`.Time` with one or more components replaced.

        :Keyword Arguments:
           * **hour** (`int`): overwrite the hour -
             default: `self.hour`
           * **minute** (`int`): overwrite the minute -
             default: `self.minute`
           * **second** (`int`): overwrite the second -
             default: `int(self.second)`
           * **nanosecond** (`int`): overwrite the nanosecond -
             default: `self.nanosecond`
           * **tzinfo** (`datetime.tzinfo` or `None`): overwrite the timezone -
             default: `self.tzinfo`

        :rtype: Time
        """
        return Time(hour=kwargs.get("hour", self.__hour),
                    minute=kwargs.get("minute", self.__minute),
                    second=kwargs.get("second", self.__second),
                    nanosecond=kwargs.get("nanosecond", self.__nanosecond),
                    tzinfo=kwargs.get("tzinfo", self.__tzinfo))

    def utc_offset(self):
        """Return the UTC offset of this time.

        :return: None if this is a local time (:attr:`.tzinfo` is None), else
            returns `self.tzinfo.utcoffset(self)`.
        :rtype: datetime.timedelta

        :raises ValueError: if `self.tzinfo.utcoffset(self)` is not None and a
            :class:`timedelta` with a magnitude greater equal 1 day or that is
            not a whole number of minutes.
        :raises TypeError: if `self.tzinfo.utcoffset(self)` does return anything but
            None or a :class:`datetime.timedelta`.
        """
        if self.tzinfo is None:
            return None
        value = self.tzinfo.utcoffset(self)
        if value is None:
            return None
        if isinstance(value, timedelta):
            s = value.total_seconds()
            if not (-86400 < s < 86400):
                raise ValueError("utcoffset must be less than a day")
            if s % 60 != 0 or value.microseconds != 0:
                raise ValueError("utcoffset must be a whole number of minutes")
            return value
        raise TypeError("utcoffset must be a timedelta")

    def dst(self):
        """Get the daylight saving time adjustment (DST).

        :return: None if this is a local time (:attr:`.tzinfo` is None), else
            returns `self.tzinfo.dst(self)`.
        :rtype: datetime.timedelta

        :raises ValueError: if `self.tzinfo.dst(self)` is not None and a
            :class:`timedelta` with a magnitude greater equal 1 day or that is
            not a whole number of minutes.
        :raises TypeError: if `self.tzinfo.dst(self)` does return anything but
            None or a :class:`datetime.timedelta`.
        """
        if self.tzinfo is None:
            return None
        value = self.tzinfo.dst(self)
        if value is None:
            return None
        if isinstance(value, timedelta):
            if value.days != 0:
                raise ValueError("dst must be less than a day")
            if value.seconds % 60 != 0 or value.microseconds != 0:
                raise ValueError("dst must be a whole number of minutes")
            return value
        raise TypeError("dst must be a timedelta")

    def tzname(self):
        """Get the name of the :class:`.Time`'s timezone.

        :returns: None if the time is local (i.e., has no timezone), else return
            `self.tzinfo.tzname(self)`

        :rtype: str or None
        """
        if self.tzinfo is None:
            return None
        return self.tzinfo.tzname(self)

    def to_clock_time(self):
        """Convert to :class:`.ClockTime`.

        :rtype: ClockTime
        """
        seconds, nanoseconds = divmod(self.ticks_ns, NANO_SECONDS)
        return ClockTime(seconds, nanoseconds)

    def to_native(self):
        """Convert to a native Python `datetime.time` value.

        This conversion is lossy as the native time implementation only supports
        a resolution of microseconds instead of nanoseconds.

        :rtype: datetime.time
        """
        h, m, s, ns = self.hour_minute_second_nanosecond
        µs = round_half_to_even(ns / 1000)
        tz = self.tzinfo
        return time(h, m, s, µs, tz)

    def iso_format(self):
        """Return the :class:`.Time` as ISO formatted string.

        :rtype: str
        """
        s = "%02d:%02d:%02d.%09d" % self.hour_minute_second_nanosecond
        if self.tzinfo is not None:
            offset = self.tzinfo.utcoffset(self)
            s += "%+03d:%02d" % divmod(offset.total_seconds() // 60, 60)
        return s

    def __repr__(self):
        """"""
        if self.tzinfo is None:
            return "neo4j.time.Time(%r, %r, %r, %r)" % \
                   self.hour_minute_second_nanosecond
        else:
            return "neo4j.time.Time(%r, %r, %r, %r, tzinfo=%r)" % \
                   (self.hour_minute_second_nanosecond + (self.tzinfo,))

    def __str__(self):
        """"""
        return self.iso_format()

    def __format__(self, format_spec):
        """"""
        raise NotImplementedError()

Time.min = Time(hour=0, minute=0, second=0, nanosecond=0)
Time.max = Time(hour=23, minute=59, second=59, nanosecond=999999999)
Time.resolution = Duration(nanoseconds=1)

Midnight = Time.min
"""
A :class:`.Time` instance set to `00:00:00`.
This has a :attr:`.ticks_ns` value of `0`.
"""

Midday = Time(hour=12)
"""
A :class:`.Time` instance set to `12:00:00`.
This has a :attr:`.ticks_ns` value of `43200000000000`.
"""


@total_ordering
class DateTime(metaclass=DateTimeType):
    """A point in time represented as a date and a time.

    The :class:`.DateTime` class is a nanosecond-precision drop-in replacement
    for the standard library :class:`datetime.datetime` class.

    As such, it contains both :class:`.Date` and :class:`.Time` information and
    draws functionality from those individual classes.

    A :class:`.DateTime` object is fully compatible with the Python time zone
    library `pytz <http://pytz.sourceforge.net/>`_. Functions such as
    `normalize` and `localize` can be used in the same way as they are with the
    standard library classes.

    Regular construction of a :class:`.DateTime` object requires at
    least the `year`, `month` and `day` arguments to be supplied. The
    optional `hour`, `minute` and `second` arguments default to zero and
    `tzinfo` defaults to :const:`None`.

    `year`, `month`, and `day` are passed to the constructor of :class:`.Date`.
    `hour`, `minute`, `second`, `nanosecond`, and `tzinfo` are passed to the
    constructor of :class:`.Time`. See their documentation for more details.

        >>> dt = DateTime(2018, 4, 30, 12, 34, 56, 789123456); dt
        neo4j.time.DateTime(2018, 4, 30, 12, 34, 56, 789123456)
        >>> dt.second
        56.789123456
    """

    # CONSTRUCTOR #

    def __new__(cls, year, month, day, hour=0, minute=0, second=0, nanosecond=0,
                tzinfo=None):
        return cls.combine(Date(year, month, day),
                           Time(hour, minute, second, nanosecond, tzinfo))

    def __getattr__(self, name):
        """ Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "astimezone": self.as_timezone,
                "isocalendar": self.iso_calendar,
                "isoformat": self.iso_format,
                "isoweekday": self.iso_weekday,
                "strftime": self.__format__,
                "toordinal": self.to_ordinal,
                "timetuple": self.time_tuple,
                "utcoffset": self.utc_offset,
                "utctimetuple": self.utc_time_tuple,
            }[name]
        except KeyError:
            raise AttributeError("DateTime has no attribute %r" % name)

    # CLASS METHODS #

    @classmethod
    def now(cls, tz=None):
        """Get the current date and time.

        :param tz: timezone. Set to None to create a local :class:`.DateTime`.
        :type tz: datetime.tzinfo` or Non

        :rtype: DateTime

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls.from_clock_time(Clock().local_time(), UnixEpoch)
        else:
            return tz.fromutc(cls.from_clock_time(
                Clock().utc_time(), UnixEpoch
            ).replace(tzinfo=tz))

    @classmethod
    def utc_now(cls):
        """Get the current date and time in UTC

        :rtype: DateTime
        """
        return cls.from_clock_time(Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_iso_format(cls, s):
        """Parse a ISO formatted date with time string.

        :param s: String to parse
        :type s: str

        :rtype: Time

        :raises ValueError: if the string does not match the ISO format.
        """
        try:
            return cls.combine(Date.from_iso_format(s[0:10]),
                               Time.from_iso_format(s[11:]))
        except ValueError:
            raise ValueError("DateTime string is not in ISO format")

    @classmethod
    def from_timestamp(cls, timestamp, tz=None):
        """:class:`.DateTime` from a time stamp (seconds since unix epoch).

        :param timestamp: the unix timestamp (seconds since unix epoch).
        :type timestamp: float
        :param tz: timezone. Set to None to create a local :class:`.DateTime`.
        :type tz: datetime.tzinfo or None

        :rtype: DateTime

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls.from_clock_time(
                ClockTime(timestamp) + Clock().local_offset(), UnixEpoch
            )
        else:
            return tz.fromutc(
                cls.utc_from_timestamp(timestamp).replace(tzinfo=tz)
            )

    @classmethod
    def utc_from_timestamp(cls, timestamp):
        """:class:`.DateTime` from a time stamp (seconds since unix epoch).

        Returns the `DateTime` as local date `DateTime` in UTC.

        :rtype: DateTime
        """
        return cls.from_clock_time((timestamp, 0), UnixEpoch)

    @classmethod
    def from_ordinal(cls, ordinal):
        """:class:`.DateTime` from an ordinal.

        For more info about ordinals see :meth:`.Date.from_ordinal`.

        :rtype: DateTime
        """
        return cls.combine(Date.from_ordinal(ordinal), Midnight)

    @classmethod
    def combine(cls, date, time):
        """Combine a :class:`.Date` and a :class:`.Time` to a :class:`DateTime`.

        :param date: the date
        :type date: Date
        :param time: the time
        :type time: Time

        :rtype: DateTime

        :raises AssertionError: if the parameter types don't match.
        """
        assert isinstance(date, Date)
        assert isinstance(time, Time)
        instance = object.__new__(cls)
        instance.__date = date
        instance.__time = time
        return instance

    @classmethod
    def parse(cls, date_string, format):
        raise NotImplementedError()

    @classmethod
    def from_native(cls, dt):
        """Convert from a native Python :class:`datetime.datetime` value.

        :param dt: the datetime to convert
        :type dt: datetime.datetime

        :rtype: DateTime
        """
        return cls.combine(Date.from_native(dt.date()), Time.from_native(dt.timetz()))

    @classmethod
    def from_clock_time(cls, clock_time, epoch):
        """Convert from a :class:`ClockTime` relative to a given epoch.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :type clock_time: ClockTime or (float, int)
        :param epoch: the epoch to which `clock_time` is relative
        :type epoch: DateTime

        :rtype: DateTime

        :raises ValueError: if `clock_time` is invalid.
        """
        try:
            seconds, nanoseconds = ClockTime(*clock_time)
        except (TypeError, ValueError):
            raise ValueError("Clock time must be a 2-tuple of (s, ns)")
        else:
            ordinal, seconds = divmod(seconds, 86400)
            ticks = epoch.time().ticks_ns + seconds * NANO_SECONDS + nanoseconds
            days, ticks = divmod(ticks, 86400 * NANO_SECONDS)
            ordinal += days
            date_ = Date.from_ordinal(ordinal + epoch.date().to_ordinal())
            time_ = Time.from_ticks_ns(ticks)
            return cls.combine(date_, time_)

    # CLASS ATTRIBUTES #

    min = None
    """The earliest date time value possible."""

    max = None
    """The latest date time value possible."""

    resolution = None
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    @property
    def year(self):
        """The year of the :class:`.DateTime`.

        See :attr:`.Date.year`.
        """
        return self.__date.year

    @property
    def month(self):
        """The year of the :class:`.DateTime`.

        See :attr:`.Date.year`."""
        return self.__date.month

    @property
    def day(self):
        """The day of the :class:`.DateTime`'s date.

        See :attr:`.Date.day`."""
        return self.__date.day

    @property
    def year_month_day(self):
        """The year_month_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_month_day`."""
        return self.__date.year_month_day

    @property
    def year_week_day(self):
        """The year_week_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_week_day`."""
        return self.__date.year_week_day

    @property
    def year_day(self):
        """The year_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_day`."""
        return self.__date.year_day

    @property
    def hour(self):
        """The hour of the :class:`.DateTime`'s time.

        See :attr:`.Time.hour`."""
        return self.__time.hour

    @property
    def minute(self):
        """The minute of the :class:`.DateTime`'s time.

        See :attr:`.Time.minute`."""
        return self.__time.minute

    @property
    def second(self):
        """The second of the :class:`.DateTime`'s time.

        See :attr:`.Time.second`."""
        return self.__time.second

    @property
    def nanosecond(self):
        """The nanosecond of the :class:`.DateTime`'s time.

        See :attr:`.Time.nanosecond`."""
        return self.__time.nanosecond

    @property
    def tzinfo(self):
        """The tzinfo of the :class:`.DateTime`'s time.

        See :attr:`.Time.tzinfo`."""
        return self.__time.tzinfo

    @property
    def hour_minute_second(self):
        """The hour_minute_second of the :class:`.DateTime`'s time.

        See :attr:`.Time.hour_minute_second`."""
        return self.__time.hour_minute_second

    @property
    def hour_minute_second_nanosecond(self):
        """The hour_minute_second_nanosecond of the :class:`.DateTime`'s time.

        See :attr:`.Time.hour_minute_second_nanosecond`."""
        return self.__time.hour_minute_second_nanosecond

    # OPERATIONS #

    def __hash__(self):
        """"""
        return hash(self.date()) ^ hash(self.time())

    def __eq__(self, other):
        """
        `==` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        if isinstance(other, (DateTime, datetime)):
            return self.date() == other.date() and self.time() == other.time()
        return False

    def __ne__(self, other):
        """
        `!=` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        return not self.__eq__(other)

    def __lt__(self, other):
        """
        `<` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        if isinstance(other, (DateTime, datetime)):
            if self.date() == other.date():
                return self.time() < other.time()
            else:
                return self.date() < other.date()
        return NotImplemented

    def __le__(self, other):
        """
        `<=` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        if isinstance(other, (DateTime, datetime)):
            if self.date() == other.date():
                return self.time() <= other.time()
            else:
                return self.date() < other.date()
        return NotImplemented

    def __ge__(self, other):
        """
        `>=` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        if isinstance(other, (DateTime, datetime)):
            if self.date() == other.date():
                return self.time() >= other.time()
            else:
                return self.date() > other.date()
        return NotImplemented

    def __gt__(self, other):
        """
        `>` comparison with :class:`.DateTime` or :class:`datetime.datetime`.
        """
        if isinstance(other, (DateTime, datetime)):
            if self.date() == other.date():
                return self.time() > other.time()
            else:
                return self.date() > other.date()
        return NotImplemented

    def __add__(self, other):
        """Add a :class:`datetime.timedelta`.

        :rtype: DateTime
        """
        if isinstance(other, timedelta):
            t = (self.to_clock_time()
                 + ClockTime(86400 * other.days + other.seconds,
                             other.microseconds * 1000))
            days, seconds = symmetric_divmod(t.seconds, 86400)
            date_ = Date.from_ordinal(days + 1)
            time_ = Time.from_ticks_ns(round_half_to_even(
                seconds * NANO_SECONDS + t.nanoseconds
            ))
            return self.combine(date_, time_)
        return NotImplemented

    def __sub__(self, other):
        """Subtract a datetime or a timedelta.

         Supported :class:`.DateTime` (returns :class:`.Duration`),
         :class:`datetime.datetime` (returns :class:`datetime.timedelta`), and
         :class:`datetime.timedelta` (returns :class:`.DateTime`).

        :rtype: Duration or datetime.timedelta or DateTime
        """
        if isinstance(other, DateTime):
            self_month_ordinal = 12 * (self.year - 1) + self.month
            other_month_ordinal = 12 * (other.year - 1) + other.month
            months = self_month_ordinal - other_month_ordinal
            days = self.day - other.day
            t = self.time().to_clock_time() - other.time().to_clock_time()
            return Duration(months=months, days=days, seconds=t.seconds,
                            nanoseconds=t.nanoseconds)
        if isinstance(other, datetime):
            days = self.to_ordinal() - other.toordinal()
            t = (self.time().to_clock_time()
                 - ClockTime(
                       3600 * other.hour + 60 * other.minute + other.second,
                       other.microsecond * 1000
                    ))
            return timedelta(days=days, seconds=t.seconds,
                             microseconds=(t.nanoseconds // 1000))
        if isinstance(other, Duration):
            return NotImplemented
        if isinstance(other, timedelta):
            return self.__add__(-other)
        return NotImplemented

    def __copy__(self):
        return self.combine(self.__date, self.__time)

    def __deepcopy__(self, *args, **kwargs):
        return self.__copy__()

    # INSTANCE METHODS #

    def date(self):
        """The date

        :rtype: Date
        """
        return self.__date

    def time(self):
        """The time without timezone info

        :rtype: Time
        """
        return self.__time.replace(tzinfo=None)

    def timetz(self):
        """The time with timezone info

        :rtype: Time
        """
        return self.__time

    def replace(self, **kwargs):
        """Return a :class:`.DateTime` with one or more components replaced.

        See :meth:`.Date.replace` and :meth:`.Time.replace` for available
        arguments.

        :rtype: DateTime
        """
        date_ = self.__date.replace(**kwargs)
        time_ = self.__time.replace(**kwargs)
        return self.combine(date_, time_)

    def as_timezone(self, tz):
        """Convert this :class:`.DateTime` to another timezone.

        :param tz: the new timezone
        :type tz: datetime.tzinfo or None

        :return: the same object if `tz` is None. Else, a new :class:`.DateTime`
            that's the same point in time but in a different timezone.
        :rtype: DateTime
        """
        if self.tzinfo is None:
            return self
        utc = (self - self.utcoffset()).replace(tzinfo=tz)
        return tz.fromutc(utc)

    def utc_offset(self):
        """Get the date times utc offset.

        See :meth:`.Time.utc_offset`.
        """

        return self.__time.utc_offset()

    def dst(self):
        """Get the daylight saving time adjustment (DST).

        See :meth:`.Time.dst`.
        """
        return self.__time.dst()

    def tzname(self):
        """Get the timezone name.

        See :meth:`.Time.tzname`.
        """
        return self.__time.tzname()

    def time_tuple(self):
        raise NotImplementedError()

    def utc_time_tuple(self):
        raise NotImplementedError()

    def to_ordinal(self):
        """Get the ordinal of the :class:`.DateTime`'s date.

        See :meth:`.Date.to_ordinal`
        """
        return self.__date.to_ordinal()

    def to_clock_time(self):
        """Convert to :class:`.ClockTime`.

        :rtype: ClockTime
        """
        total_seconds = 0
        for year in range(1, self.year):
            total_seconds += 86400 * DAYS_IN_YEAR[year]
        for month in range(1, self.month):
            total_seconds += 86400 * Date.days_in_month(self.year, month)
        total_seconds += 86400 * (self.day - 1)
        seconds, nanoseconds = divmod(self.__time.ticks_ns, NANO_SECONDS)
        return ClockTime(total_seconds + seconds, nanoseconds)

    def to_native(self):
        """Convert to a native Python :class:`datetime.datetime` value.

        This conversion is lossy as the native time implementation only supports
        a resolution of microseconds instead of nanoseconds.

        :rtype: datetime.datetime
        """
        y, mo, d = self.year_month_day
        h, m, s, ns = self.hour_minute_second_nanosecond
        ms = int(ns / 1000)
        tz = self.tzinfo
        return datetime(y, mo, d, h, m, s, ms, tz)

    def weekday(self):
        """Get the weekday.

        See :meth:`.Date.weekday`
        """
        return self.__date.weekday()

    def iso_weekday(self):
        """Get the ISO weekday.

        See :meth:`.Date.iso_weekday`
        """
        return self.__date.iso_weekday()

    def iso_calendar(self):
        """Get date as ISO tuple.

        See :meth:`.Date.iso_calendar`
        """
        return self.__date.iso_calendar()

    def iso_format(self, sep="T"):
        """Return the :class:`.DateTime` as ISO formatted string.

        This method joins `self.date().iso_format()` (see
        :meth:`.Date.iso_format`) and `self.timetz().iso_format()` (see
        :meth:`.Time.iso_format`) with `sep` in between.

        :param sep: the separator between the formatted date and time.
        :type sep: str

        :rtype: str
        """
        return "%s%s%s" % (self.date().iso_format(), sep,
                           self.timetz().iso_format())

    def __repr__(self):
        """"""
        if self.tzinfo is None:
            fields = (*self.year_month_day,
                      *self.hour_minute_second_nanosecond)
            return "neo4j.time.DateTime(%r, %r, %r, %r, %r, %r, %r)" % fields
        else:
            fields = (*self.year_month_day,
                      *self.hour_minute_second_nanosecond, self.tzinfo)
            return ("neo4j.time.DateTime(%r, %r, %r, %r, %r, %r, %r, tzinfo=%r)"
                    % fields)

    def __str__(self):
        """"""
        return self.iso_format()

    def __format__(self, format_spec):
        """"""
        raise NotImplementedError()


DateTime.min = DateTime.combine(Date.min, Time.min)
DateTime.max = DateTime.combine(Date.max, Time.max)
DateTime.resolution = Time.resolution

Never = DateTime.combine(ZeroDate, Midnight)
"""
A :class:`.DateTime` instance set to `0000-00-00T00:00:00`.
This has a :class:`.Date` component equal to :attr:`ZeroDate` and a
:class:`.Time` component equal to :attr:`Midnight`.
"""

UnixEpoch = DateTime(1970, 1, 1, 0, 0, 0)
"""A :class:`.DateTime` instance set to `1970-01-01T00:00:00`."""
